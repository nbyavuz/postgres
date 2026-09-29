# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use TestAio;

plan skip_all => 'Injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

foreach my $method (TestAio::supported_io_methods())
{
	test_io_method($method, 6);
}

done_testing();

sub test_io_method
{
	my ($method, $num_slots) = @_;
	my $node = PostgreSQL::Test::Cluster->new($method);
	$node->init();
	TestAio::configure($node);
	$node->append_conf(
		'postgresql.conf', qq(
io_method = $method
fsync = on
data_sync_retry = on
io_max_concurrency = 2
io_min_workers = 1
io_max_workers = 1
autovacuum = off
checkpoint_timeout = '1h'
));
	$node->start();
	$node->safe_psql('postgres', 'CREATE EXTENSION test_aio');

	# More relations than io_max_concurrency exercise draining both at the
	# concurrency limit and at the end of the checkpoint's sync phase.
	for my $slot (0 .. $num_slots - 1)
	{
		$node->safe_psql(
			'postgres', qq(
CREATE TABLE fsync_$slot (i int);
INSERT INTO fsync_$slot VALUES (0);
));
	}
	$node->safe_psql('postgres', 'CHECKPOINT');
	test_fsync_requests($node, $method);
	test_worker_settings($node) if $method eq 'worker';
	test_worker_reopen($node) if $method eq 'worker';
	configure_slots($node, $num_slots, 'false');
	dirty_relations($node, $num_slots);
	$node->safe_psql('postgres', 'CHECKPOINT');
	is( $node->safe_psql(
			'postgres', qq(
SELECT count(*) FROM generate_series(0, $num_slots - 1) slot,
  LATERAL inj_fsync_stats(slot) s WHERE attempts = 1 AND successes = 1
)),
		$num_slots,
		"$method: checkpoint completes every relation fsync successfully");

	if ($method eq 'worker')
	{
		# Queue-lock contention may cause synchronous fallback.  If necessary,
		# submit another checkpoint rather than assuming every IO uses a worker.
		my $workers = 0;
		for (1 .. 10)
		{
			$workers = $node->safe_psql(
				'postgres', qq(
SELECT sum(worker_completions) FROM generate_series(0, $num_slots - 1) slot,
  LATERAL inj_fsync_stats(slot) s
));
			last if $workers > 0;
			dirty_relations($node, $num_slots);
			$node->safe_psql('postgres', 'CHECKPOINT');
		}
		cmp_ok($workers, '>', 0,
			'worker: fsync completion runs in an IO worker');
	}

	configure_slots($node, $num_slots, 'true');
	dirty_relations($node, $num_slots);
	my $before = checkpoint_lsn($node);
	my $checkpoint = $node->background_psql('postgres');
	my $pid = $checkpoint->query_safe('SELECT pg_backend_pid()');
	$checkpoint->query_until(
		qr/checkpoint_started/, q(
\echo checkpoint_started
CHECKPOINT;
));

	# Hash scan order is unspecified.  Release whichever relation has reached
	# its completion gate, then check again while the next completion is held.
	for my $remaining (reverse 1 .. $num_slots)
	{
		$node->poll_query_until(
			'postgres', qq(
SELECT count(*) > 0 FROM generate_series(0, $num_slots - 1) slot,
  LATERAL inj_fsync_stats(slot) s WHERE waiting
)) or die 'no fsync reached its completion gate';
		$node->poll_query_until(
			'postgres', qq(
SELECT wait_event = 'CheckpointDone' FROM pg_stat_activity WHERE pid = $pid
)) or die 'CHECKPOINT is not waiting';
		is(checkpoint_lsn($node), $before,
			"$method: checkpoint not published with $remaining fsyncs held");
		my $slot = $node->safe_psql(
			'postgres', qq(
SELECT slot FROM generate_series(0, $num_slots - 1) slot,
  LATERAL inj_fsync_stats(slot) s WHERE waiting ORDER BY slot LIMIT 1
));
		$node->safe_psql('postgres', "SELECT inj_fsync_release($slot)");
		$node->poll_query_until('postgres',
			"SELECT NOT waiting FROM inj_fsync_stats($slot)")
		  or die 'fsync did not leave its completion gate';
	}
	$checkpoint->query_safe('');
	$checkpoint->quit;
	isnt(checkpoint_lsn($node), $before,
		"$method: checkpoint published after releasing all completions");
	is( $node->safe_psql(
			'postgres', qq(
SELECT count(*) FROM generate_series(0, $num_slots - 1) slot,
  LATERAL inj_fsync_stats(slot) s WHERE attempts = 1 AND successes = 1
)),
		$num_slots,
		"$method: all held fsyncs completed successfully");

	test_retries($node, $method);
	test_cancel($node, $method);
	$node->stop();
}

sub configure_slots
{
	my ($node, $num_slots, $hold) = @_;
	for my $slot (0 .. $num_slots - 1)
	{
		$node->safe_psql('postgres',
			"SELECT inj_fsync_configure($slot, 'fsync_$slot', $hold)");
	}
}

sub dirty_relations
{
	my ($node, $num_slots) = @_;
	for my $slot (0 .. $num_slots - 1)
	{
		$node->safe_psql('postgres', "UPDATE fsync_$slot SET i = i + 1");
	}
}

sub checkpoint_lsn
{
	my ($node) = @_;
	return $node->safe_psql('postgres',
		'SELECT checkpoint_lsn FROM pg_control_checkpoint()');
}

sub test_retries
{
	my ($node, $method) = @_;
	my $path =
	  $node->safe_psql('postgres', "SELECT pg_relation_filepath('fsync_0')");

	# A possible deletion error must be retried, even though this relation
	# still exists.  The second attempt receives the real, successful result.
	$node->safe_psql(
		'postgres', q(
SELECT inj_fsync_configure(0, 'fsync_0', false, 1);
UPDATE fsync_0 SET i = i + 1;
));
	my $offset = -s $node->logfile;
	$node->safe_psql('postgres', 'CHECKPOINT');
	is( $node->safe_psql(
			'postgres', 'SELECT attempts, successes FROM inj_fsync_stats(0)'),
		'2|1',
		"$method: missing-file error is retried successfully");
	ok( $node->log_contains(
			qr/could not fsync file "\Q$path\E" but retrying/, $offset),
		"$method: retry reports the affected relation");

	# The same error on the retry must fail the checkpoint, rather than being
	# ignored or retried indefinitely.  data_sync_retry keeps this an ERROR.
	$node->safe_psql(
		'postgres', q(
SELECT inj_fsync_configure(0, 'fsync_0', false, 2);
UPDATE fsync_0 SET i = i + 1;
));
	my $before = checkpoint_lsn($node);
	$offset = -s $node->logfile;
	my ($ret, $stdout, $stderr) = $node->psql('postgres', 'CHECKPOINT');
	isnt($ret, 0, "$method: repeated fsync failure fails CHECKPOINT");
	like(
		$stderr,
		qr/checkpoint request failed/,
		"$method: CHECKPOINT reports failure to its caller");
	is( $node->safe_psql(
			'postgres', 'SELECT attempts, successes FROM inj_fsync_stats(0)'),
		'2|0',
		"$method: repeated missing-file error stops after one retry");
	ok( $node->log_contains(
			qr/ERROR:  could not fsync file "\Q$path\E":/, $offset),
		"$method: checkpoint failure reports the affected relation");
	is(checkpoint_lsn($node), $before,
		"$method: failed checkpoint is not published");

	# Both injected failures have been consumed.  Do not dirty the relation
	# again: the failed checkpoint must have retained its pending fsync request.
	$node->safe_psql('postgres', 'CHECKPOINT');
	is( $node->safe_psql(
			'postgres', 'SELECT attempts, successes FROM inj_fsync_stats(0)'),
		'3|1',
		"$method: next checkpoint retries the retained fsync request");
	isnt(checkpoint_lsn($node), $before,
		"$method: checkpoint succeeds after fsync failure is removed");
}

sub test_fsync_requests
{
	my ($node, $method) = @_;
	for my $datasync ('false', 'true')
	{
		my $op = $datasync eq 'true' ? 'fdatasync' : 'fsync';
		$node->safe_psql('postgres',
			"SELECT inj_fsync_configure(0, 'fsync_0', false)");
		is( $node->safe_psql(
				'postgres', "SELECT fsync_rel('fsync_0', $datasync)"),
			'0',
			"$method: explicit $op succeeds");
		is( $node->safe_psql(
				'postgres',
				'SELECT attempts, successes FROM inj_fsync_stats(0)'),
			'1|1',
			"$method: explicit $op completes through AIO");

		# An invalid descriptor distinguishes a no-op from successful syncing.
		# Also require local synchronous completion, including with io_uring.
		$node->safe_psql('postgres',
			"SELECT inj_fsync_configure(0, 'fsync_0', false)");
		is( $node->safe_psql(
				'postgres',
				"SELECT fsync_rel('fsync_0', $datasync, false, true)"),
			'0',
			"$method: disabled $op ignores an invalid descriptor");
		is( $node->safe_psql(
				'postgres', q(
SELECT attempts, successes, worker_completions, synchronous_completions
FROM inj_fsync_stats(0)
)),
			'1|1|0|1',
			"$method: disabled $op completes synchronously in the issuer");
	}
}

sub test_worker_settings
{
	my ($node) = @_;
	my $writethrough = $node->safe_psql(
		'postgres', q(
SELECT 'fsync_writethrough' = ANY(enumvals)
FROM pg_settings WHERE name = 'wal_sync_method'
));
	for my $op ('fsync', 'fdatasync', 'writethrough')
	{
	  SKIP:
		{
			skip 'fsync_writethrough is not supported', 1
			  if $op eq 'writethrough' && $writethrough ne 't';
			my $datasync = $op eq 'fdatasync' ? 'true' : 'false';
			my $full = $op eq 'writethrough' ? 'true' : 'false';
			my $counts;
			$node->safe_psql('postgres',
				"SELECT inj_fsync_configure(0, 'fsync_0', false, 0, true)");
			# Retry submissions only to accommodate legitimate local fallback.
			# The observation points count real syscall paths in the worker.
			for (1 .. 100)
			{
				my $result = $node->safe_psql('postgres',
					"SELECT fsync_rel('fsync_0', $datasync, true, false, $full)"
				);
				die "$op failed: $result" if $result ne '0';
				$counts = $node->safe_psql(
					'postgres', q(
SELECT fsync_calls, datasync_calls, writethrough_calls FROM inj_fsync_stats(0)
));
				last if $counts ne '0|0|0';
			}
			my $expected =
				$op eq 'fsync'     ? qr/^[1-9]\d*\|0\|0$/
			  : $op eq 'fdatasync' ? qr/^0\|[1-9]\d*\|0$/
			  :                      qr/^0\|0\|[1-9]\d*$/;
			like($counts, $expected,
				"worker: issuer selects $op despite conflicting worker settings"
			);
		}
	}
}

sub test_worker_reopen
{
	my ($node) = @_;
	my $worker_query =
	  q(SELECT pid FROM pg_stat_activity WHERE backend_type = 'io worker');
	my $pid = $node->safe_psql('postgres', $worker_query);
	my $enoent =
	  $node->safe_psql('postgres', "SELECT -errno_from_string('ENOENT')");
	for my $slru ('false', 'true')
	{
		my $target = $slru eq 'true' ? 'SLRU' : 'relation';
		my $result;
		for (1 .. 100)
		{
			$result =
			  $node->safe_psql('postgres', "SELECT fsync_missing($slru)");
			# Local fallback can use the issuer's descriptor.
			last if $result ne '0';
		}
		is($result, $enoent, "worker: $target reopen preserves ENOENT");
		is($node->safe_psql('postgres', $worker_query),
			$pid,
			"worker: $target reopen failure does not terminate the worker");

		# Require subsequent work in that same worker, not just its continued
		# presence in pg_stat_activity while it is on the way out.
		$node->safe_psql('postgres',
			"SELECT inj_fsync_configure(0, 'fsync_0', false)");
		my $completed = 0;
		for (1 .. 100)
		{
			$node->safe_psql('postgres',
				"SELECT fsync_rel('fsync_0', false)");
			$completed = $node->safe_psql('postgres',
				'SELECT worker_completions FROM inj_fsync_stats(0)');
			last if $completed > 0;
		}
		cmp_ok($completed, '>', 0,
			"worker: accepts work after $target reopen failure");
		is($node->safe_psql('postgres', $worker_query),
			$pid, "worker: same worker completes subsequent IO");
	}
}

sub test_cancel
{
	my ($node, $method) = @_;
	$node->safe_psql(
		'postgres', q(
SELECT inj_fsync_configure(0, 'fsync_0', true, 1);
UPDATE fsync_0 SET i = i + 1;
));
	my $checkpoint = $node->background_psql('postgres');
	$checkpoint->query_until(
		qr/cancel_checkpoint_started/, q(
\echo cancel_checkpoint_started
CHECKPOINT;
));
	$node->poll_query_until('postgres',
		'SELECT waiting FROM inj_fsync_stats(0)')
	  or die 'fsync did not reach cancellation gate';

	# Queue the forget while completion is outstanding.  Once the injected
	# ENOENT is seen, the checkpointer must absorb the forget and skip retrying.
	$node->safe_psql(
		'postgres', q(
SELECT fsync_forget('fsync_0');
SELECT inj_fsync_release(0);
));
	$checkpoint->query_safe('');
	$checkpoint->quit;
	is( $node->safe_psql(
			'postgres', 'SELECT attempts, successes FROM inj_fsync_stats(0)'),
		'1|0',
		"$method: canceled in-flight fsync is not retried");

	$node->safe_psql('postgres', 'CHECKPOINT');
	is( $node->safe_psql(
			'postgres', 'SELECT attempts FROM inj_fsync_stats(0)'),
		'1',
		"$method: canceled fsync does not survive to the next checkpoint");
	$node->safe_psql('postgres', 'UPDATE fsync_0 SET i = i + 1; CHECKPOINT');
	is( $node->safe_psql(
			'postgres', 'SELECT attempts, successes FROM inj_fsync_stats(0)'),
		'2|1',
		"$method: a new request for the canceled relation is synchronized");
}

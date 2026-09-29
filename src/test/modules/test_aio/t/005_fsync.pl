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

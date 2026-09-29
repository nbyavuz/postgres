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

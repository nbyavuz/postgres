# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use FindBin;
use lib $FindBin::RealBin;
use TestAio;

plan skip_all => 'injection points not supported'
  unless $ENV{enable_injection_points} eq 'yes';

foreach my $method (TestAio::supported_io_methods())
{
	my $node = PostgreSQL::Test::Cluster->new("targets_$method");
	$node->init;
	TestAio::configure($node);
	$node->append_conf(
		'postgresql.conf', qq(
io_method = $method
fsync = on
track_commit_timestamp = on
autovacuum = off
checkpoint_timeout = '1h'
max_files_per_process = 64
io_max_concurrency = 32
# Use exactly one persistent worker so descriptor leaks accumulate in one process.
io_min_workers = 1
io_max_workers = 1
));
	$node->start;
	$node->safe_psql(
		'postgres', q(
CREATE EXTENSION test_aio;
CREATE TABLE target(i int);
INSERT INTO target SELECT generate_series(1, 1000);
));
	$node->safe_psql('postgres', 'VACUUM target');

	# Compatible row locks force creation of a multixact and both SLRUs.
	my $locker = $node->background_psql('postgres');
	$locker->query_safe('BEGIN; SELECT * FROM target WHERE i = 1 FOR SHARE');
	$node->safe_psql('postgres',
		'SELECT * FROM target WHERE i = 1 FOR SHARE');
	$locker->query_safe('COMMIT');
	$locker->quit;
	$node->safe_psql('postgres', 'CHECKPOINT');

	my $tablespace = PostgreSQL::Test::Utils::tempdir();
	$node->safe_psql('postgres',
		"CREATE TABLESPACE fsync_space LOCATION '$tablespace'");
	$node->safe_psql(
		'postgres', q(
CREATE TABLE tablespace_target(i int) TABLESPACE fsync_space;
INSERT INTO tablespace_target VALUES (1);
CHECKPOINT;
SELECT fsync_test_configure('tablespace_target');
SELECT fsync_test_request();
CHECKPOINT;
));
	is($node->safe_psql('postgres', 'SELECT fsync_test_count()'),
		'1', "$method: tablespace relation synced");

	# Make empty inactive segments as left behind by truncation.  A regular
	# read-style reopen would reject segment 1 because segment 0 is not full.
	# Explicit requests keep this fixture independent of RELSEG_SIZE.
	my $path =
	  $node->safe_psql('postgres', q(SELECT pg_relation_filepath('target')));
	foreach my $fork ('', '_fsm', '_vm')
	{
		append_to_file($node->data_dir . "/$path$fork.1", '');
	}
	foreach my $fork ('main', 'fsm', 'vm')
	{
		foreach my $segment (0, 1)
		{
			$node->safe_psql(
				'postgres', qq(
SELECT fsync_test_configure('target', fork => '$fork', segno => $segment);
SELECT fsync_test_request();
CHECKPOINT;
));
			is($node->safe_psql('postgres', 'SELECT fsync_test_count()'),
				'1', "$method: $fork segment $segment synced");
			is( $node->safe_psql('postgres', 'SELECT fsync_test_count(true)'),
				$method eq 'worker' ? '1' : '0',
				"$method: $fork segment $segment executed in expected process"
			);
		}
	}

	my @slrus = (
		'pg_xact', 'pg_commit_ts',
		'pg_multixact/offsets', 'pg_multixact/members');
	foreach my $index (0 .. $#slrus)
	{
		my $slru = $slrus[$index];
		my $handler = $index + 1;
		$node->safe_psql(
			'postgres', qq(
SELECT fsync_test_configure('target', handler => $handler, wait => true);
SELECT fsync_test_request();
));
		my $checkpoint = $node->background_psql('postgres');
		$checkpoint->query_until(
			qr/sent/, q(
\echo sent
CHECKPOINT;
\echo completed
));
		ok( $node->poll_query_until(
				'postgres', q(
SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event = 'fsync_completion')
)),
			"$method: $slru fsync held");
		$checkpoint->{run}->pump_nb;
		unlike($checkpoint->{stdout}, qr/completed/,
			"$method: checkpoint waits for $slru");
		is( $node->safe_psql(
				'postgres', qq(
SELECT count(*) FROM pg_aios WHERE operation = 'fsync'
 AND target = 'sync_filetag' AND off IS NULL AND length IS NULL
 AND target_desc = 'segment 0 of SLRU "$slru"'
)),
			'1',
			"$method: $slru target identity");
		is( $node->safe_psql('postgres', 'SELECT fsync_test_count(true)'),
			$method eq 'worker' ? '1' : '0',
			"$method: $slru worker execution");
		$node->safe_psql('postgres', 'SELECT fsync_test_release()');
		$checkpoint->query_safe('SELECT 1');
		$checkpoint->quit;
	}

	# Queue more requests than either cap.  Completed-but-unreaped requests
	# still occupy sync.c slots, so this does not depend on device speed.
	# Enough operations also expose missing transient descriptor closes in
	# the single persistent worker.
	for my $i (1 .. 96)
	{
		$node->safe_psql('postgres', "CREATE TABLE batch_$i(i int)");
	}
	$node->safe_psql('postgres', 'CHECKPOINT');
	for my $round (1 .. 3)
	{
		$node->safe_psql('postgres',
			q(SELECT fsync_test_configure('target')));
		my $sql = '';
		for my $i (1 .. 96)
		{
			$sql .=
			  "SELECT fsync_test_configure('batch_$i'); SELECT fsync_test_request();\n";
		}
		# Reset observation once more, after queuing and before CHECKPOINT.
		$sql .= "SELECT fsync_test_configure('target'); CHECKPOINT;";
		$node->safe_psql('postgres', $sql);
		is($node->safe_psql('postgres', q(SELECT fsync_test_peak(false))),
			'32', "$method: relation batch $round reaches full concurrency");
	}

	# Synthetic SLRU segment files let us exercise descriptor limits without
	# allocating millions of transaction IDs.  These empty files are never
	# read as SLRU pages and are removed before shutting down.
	my @fixtures;
	for my $seg (100 .. 195)
	{
		my $file = $node->data_dir . '/pg_xact/' . sprintf('%04X', $seg);
		append_to_file($file, '');
		push @fixtures, $file;
	}
	for my $mixed (0, 1)
	{
		my $sql = '';
		for my $i (1 .. 96)
		{
			my $seg = $i + 99;
			$sql .=
			  "SELECT fsync_test_configure('target', handler => 1, segno => $seg); SELECT fsync_test_request();\n";
			$sql .=
			  "SELECT fsync_test_configure('batch_$i'); SELECT fsync_test_request();\n"
			  if $mixed;
		}
		$sql .=
		  "SELECT fsync_test_configure('target', handler => 1, segno => 100, error => 'ENOENT', failures => 1); CHECKPOINT;";
		$node->safe_psql('postgres', $sql);
		is($node->safe_psql('postgres', q(SELECT fsync_test_count())),
			'2', "$method: transient retry in mixed=$mixed batch");
		is( $node->safe_psql(
				'postgres', q(
SELECT fsync_test_peak(true) = fsync_test_limit(true)
)),
			't',
			"$method: transient admission respects its cap, mixed=$mixed");
	}
	foreach my $file (@fixtures)
	{
		unlink $file or die "could not remove $file: $!";
	}
	$node->stop;
}

done_testing();

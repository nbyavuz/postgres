# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use TestAio;


my $have_injection_points = ($ENV{enable_injection_points} // '') eq 'yes';
my @checkpoint_tables = map { "fsync_checkpoint_$_" } 1 .. 5;
my $completion_wait_armed = 0;
my @startup_files;
my $node;

END
{
	if ($completion_wait_armed && defined $node)
	{
		eval {
			$node->safe_psql('postgres',
				'SELECT inj_io_completion_continue()');
		};
	}

	foreach my $file (@startup_files)
	{
		unlink($file) if -e $file;
	}
}

$node = PostgreSQL::Test::Cluster->new('fsync');
$node->init();

TestAio::configure($node);

$node->append_conf(
	'postgresql.conf', qq(
autovacuum=off
fsync=on
io_method=worker
io_max_concurrency=2
io_min_workers=2
io_max_workers=2
io_worker_idle_timeout=0ms
io_worker_launch_interval=0ms
max_files_per_process=64
recovery_init_sync_method=fsync
));

$node->start();
setup($node);
$node->stop();

foreach my $method (TestAio::supported_io_methods())
{
	$node->adjust_conf('postgresql.conf', 'io_method', $method);
	$node->start();
	test_io_method($method, $node);
	$node->stop();
}

done_testing();


sub setup
{
	my $node = shift;
	my $checkpoint_ddl = join(
		"\n",
		map {
			qq(
CREATE TABLE $_ (id int PRIMARY KEY, value int)
  WITH (AUTOVACUUM_ENABLED = false);
INSERT INTO $_ VALUES (1, 0);
)
		} @checkpoint_tables);

	$node->safe_psql(
		'postgres', qq(
CREATE EXTENSION test_aio;

CREATE TABLE fsync_direct (id int PRIMARY KEY, value int)
  WITH (AUTOVACUUM_ENABLED = false);
INSERT INTO fsync_direct VALUES (1, 0);

$checkpoint_ddl

CHECKPOINT;
));
}


sub test_io_method
{
	my $method = shift;
	my $node = shift;

	is($node->safe_psql('postgres', 'SHOW io_method'),
		$method, "$method: io_method is configured");

	if ($method eq 'worker')
	{
		wait_for_worker_count($method, $node, 2);
	}

	test_direct_fsync($method, $node, 0);
	test_direct_fsync($method, $node, 1);
	test_checkpoint_relations($method, $node);
	test_checkpoint_slru($method, $node);
	test_startup_sync($method, $node);

	test_worker_reopen_and_cleanup($method, $node)
	  if $method eq 'worker';
}


sub test_direct_fsync
{
	my $method = shift;
	my $node = shift;
	my $datasync = shift;
	my $kind = $datasync ? 'fdatasync' : 'fsync';
	my $sql_datasync = $datasync ? 'true' : 'false';
	my $expected_datasync = $datasync ? 't' : 'f';

	is( $node->safe_psql(
			'postgres', "SELECT aio_fsync_rel('fsync_direct', $sql_datasync)"
		),
		0,
		"$method: direct $kind returns zero");
	is($node->safe_psql('postgres', 'SELECT 1'),
		1, "$method: server is usable after direct $kind");

	subtest "$method: direct $kind instrumentation" => sub {
		plan skip_all => 'Injection points not supported by this build'
		  unless $have_injection_points;

		my $relfilenode = $node->safe_psql('postgres',
			"SELECT pg_relation_filenode('fsync_direct')");

		$node->safe_psql('postgres', 'SELECT aio_fsync_completions_reset()');

		my ($pid, $output) = run_paused_query(
			method => $method,
			node => $node,
			name => "direct $kind",
			attach => sub {
				my $pid = shift;

				return qq(
SELECT inj_io_completion_wait(
  pid => $pid,
  relfilenode => $relfilenode,
  operation => 'fsync',
  target => 'smgr')
);
			},
			query => "SELECT aio_fsync_rel('fsync_direct', $sql_datasync)",
			inspect => sub {
				my $pid = shift;
				my $observed = $node->safe_psql(
					'postgres', qq(
SELECT count(*),
       bool_and(off IS NULL),
       bool_and(length IS NULL),
       bool_and(target_desc LIKE '%/$relfilenode"')
FROM pg_aios
WHERE pid = $pid
  AND operation = 'fsync'
  AND target = 'smgr'
));

				is($observed, '1|t|t|t',
					"$method: direct $kind is visible in pg_aios");
			});

		like($output, qr/^0\r?$/m,
			"$method: paused direct $kind returns zero");

		my $completion = $node->safe_psql(
			'postgres', qq(
SELECT operation,
       target,
       raw_result,
       target_desc LIKE '%/$relfilenode"',
       relfilenode,
       datasync,
       owner_pid
FROM aio_fsync_completions()
WHERE owner_pid = $pid
  AND relfilenode = $relfilenode
ORDER BY sequence DESC
LIMIT 1
));

		is( $completion,
			"fsync|smgr|0|t|$relfilenode|$expected_datasync|$pid",
			"$method: direct $kind completion is recorded");
	};
}


sub test_checkpoint_relations
{
	my $method = shift;
	my $node = shift;

	dirty_checkpoint_relations($node);
	$node->safe_psql('postgres', 'CHECKPOINT');
	is($node->safe_psql('postgres', 'SELECT 1'),
		1, "$method: relation checkpoint completes");

	subtest "$method: relation checkpoint instrumentation" => sub {
		plan skip_all => 'Injection points not supported by this build'
		  unless $have_injection_points;

		dirty_checkpoint_relations($node);

		my @relfilenodes = checkpoint_relfilenodes($node);
		my $relfilenode_array = join(',', @relfilenodes);
		my $checkpointer_pid = get_checkpointer_pid($node);

		$node->safe_psql('postgres', 'SELECT aio_fsync_completions_reset()');

		my ($pid, $output) = run_paused_query(
			method => $method,
			node => $node,
			name => 'relation checkpoint',
			owner_pid => $checkpointer_pid,
			attach => sub {
				my $pid = shift;

				return qq(
SELECT inj_io_completion_wait(
  pid => $pid,
  operation => 'fsync',
  target => 'smgr',
  max_waits => 1)
);
			},
			query => 'CHECKPOINT',
			inspect => sub {
				my $pid = shift;
				my $expected_active = $method eq 'sync' ? 1 : 2;

				ok( $node->poll_query_until(
						'postgres', qq(
SELECT count(*)
FROM pg_aios
WHERE pid = $pid
  AND operation = 'fsync'
  AND target = 'smgr'
),
						$expected_active),
					"$method: relation checkpoint reaches expected depth");

				my $active = $node->safe_psql(
					'postgres', qq(
SELECT count(*)
FROM pg_aios
WHERE pid = $pid
  AND operation = 'fsync'
  AND target = 'smgr'
));

				is($active, $expected_active,
					"$method: relation checkpoint has expected active fsyncs"
				);
				cmp_ok($active, '<=', 2,
					"$method: relation checkpoint respects depth");

				my $invalid = $node->safe_psql(
					'postgres', qq(
SELECT count(*)
FROM pg_aios
WHERE pid = $pid
  AND operation = 'fsync'
  AND target = 'smgr'
  AND (off IS NOT NULL OR length IS NOT NULL)
));

				is($invalid, 0,
					"$method: relation checkpoint fsyncs have no offset or length"
				);
			});

		like($output, qr/aio_fsync_query_done/,
			"$method: paused relation checkpoint completes");

		my $covered = $node->safe_psql(
			'postgres', qq(
SELECT count(DISTINCT relfilenode)
FROM aio_fsync_completions()
WHERE owner_pid = $pid
  AND operation = 'fsync'
  AND target = 'smgr'
  AND raw_result = 0
  AND relfilenode = ANY (ARRAY[$relfilenode_array]::oid[])
));

		is($covered, scalar(@relfilenodes),
			"$method: checkpoint completions cover every relation");

		my $invalid = $node->safe_psql(
			'postgres', qq(
SELECT count(*)
FROM aio_fsync_completions()
WHERE owner_pid = $pid
  AND relfilenode = ANY (ARRAY[$relfilenode_array]::oid[])
  AND (operation <> 'fsync'
       OR target <> 'smgr'
       OR raw_result <> 0
       OR target_desc NOT LIKE '%/' || relfilenode || '"')
));

		is($invalid, 0,
			"$method: checkpoint relation completions have expected details");

		check_worker_completion_pids(
			$method,
			$node,
			qq(owner_pid = $pid
  AND target = 'smgr'
  AND relfilenode = ANY (ARRAY[$relfilenode_array]::oid[])),
			'relation checkpoint') if $method eq 'worker';
	};
}


sub test_checkpoint_slru
{
	my $method = shift;
	my $node = shift;

	my $segment = (dirty_pg_xact($node))[1];
	$node->safe_psql('postgres', 'CHECKPOINT');
	is($node->safe_psql('postgres', 'SELECT 1'),
		1, "$method: pg_xact checkpoint for segment $segment completes");

	subtest "$method: pg_xact checkpoint instrumentation" => sub {
		plan skip_all => 'Injection points not supported by this build'
		  unless $have_injection_points;

		my $segment = (dirty_pg_xact($node))[1];
		my $target_desc = qq(segment $segment of SLRU "pg_xact");
		my $checkpointer_pid = get_checkpointer_pid($node);

		$node->safe_psql('postgres', 'SELECT aio_fsync_completions_reset()');

		my ($pid, $output) = run_paused_query(
			method => $method,
			node => $node,
			name => "pg_xact checkpoint segment $segment",
			owner_pid => $checkpointer_pid,
			attach => sub {
				my $pid = shift;

				return qq(
SELECT inj_io_completion_wait(
  pid => $pid,
  operation => 'fsync',
  target => 'sync_filetag',
  filetag_handler => 1,
  filetag_segno => $segment)
);
			},
			query => 'CHECKPOINT',
			inspect => sub {
				my $pid = shift;
				my $observed = $node->safe_psql(
					'postgres', qq(
SELECT count(*),
       bool_and(off IS NULL),
       bool_and(length IS NULL)
FROM pg_aios
WHERE pid = $pid
  AND operation = 'fsync'
  AND target = 'sync_filetag'
  AND target_desc = '$target_desc'
));

				is($observed, '1|t|t',
					"$method: pg_xact segment $segment is visible in pg_aios"
				);
			});

		like($output, qr/aio_fsync_query_done/,
			"$method: paused pg_xact checkpoint completes");

		my $completion = $node->safe_psql(
			'postgres', qq(
SELECT operation,
       target,
       raw_result,
       target_desc,
       filetag_handler,
       filetag_segno,
       owner_pid
FROM aio_fsync_completions()
WHERE owner_pid = $pid
  AND filetag_handler = 1
  AND filetag_segno = $segment
ORDER BY sequence DESC
LIMIT 1
));

		is( $completion,
			"fsync|sync_filetag|0|$target_desc|1|$segment|$pid",
			"$method: pg_xact segment $segment completion is recorded");

		check_worker_completion_pids(
			$method,
			$node,
			qq(owner_pid = $pid
  AND filetag_handler = 1
  AND filetag_segno = $segment),
			"pg_xact segment $segment") if $method eq 'worker';
	};
}


sub test_startup_sync
{
	my $method = shift;
	my $node = shift;
	my @files =
	  map { $node->data_dir . "/aio_fsync_startup_${method}_$_" } 1 .. 5;

	foreach my $file (@files)
	{
		PostgreSQL::Test::Utils::append_to_file($file,
			"$method startup sync\n");
		push(@startup_files, $file);
	}

	$node->safe_psql('postgres', 'SELECT aio_fsync_completions_reset()');
	$node->stop('immediate');
	$node->start();

	is($node->safe_psql('postgres', 'SELECT 1'),
		1, "$method: server starts after immediate stop");
	wait_for_worker_count($method, $node, 2) if $method eq 'worker';

	foreach my $file (@files)
	{
		unlink($file) or die "could not remove \"$file\": $!";
	}

	my %removed = map { $_ => 1 } @files;
	@startup_files = grep { !$removed{$_} } @startup_files;

	subtest "$method: startup sync instrumentation" => sub {
		plan skip_all => 'Injection points not supported by this build'
		  unless $have_injection_points;

		my $summary = $node->safe_psql(
			'postgres', qq(
SELECT count(*),
       count(*) FILTER (
         WHERE raw_result NOT IN (
           0,
           -errno_from_string('EBADF'),
           -errno_from_string('EINVAL'))),
       count(*) FILTER (WHERE target_desc <> 'generic file sync')
FROM aio_fsync_completions()
WHERE operation = 'fsync'
  AND target = 'sync'
));
		my ($count, $failed, $wrong_desc) = split(/\|/, $summary);

		cmp_ok($count, '>', 2,
			"$method: startup records more fsyncs than the AIO depth");
		is($failed, 0,
			"$method: startup fsyncs have no unexpected raw errors");
		is($wrong_desc, 0,
			"$method: startup fsyncs have generic target descriptions");

		if ($method eq 'worker')
		{
			my $worker_array = join(',', current_worker_pids($node));
			my $invalid = $node->safe_psql(
				'postgres', qq(
SELECT count(*)
FROM aio_fsync_completions()
WHERE operation = 'fsync'
  AND target = 'sync'
  AND (NOT synchronous
       OR executor_pid = ANY (ARRAY[$worker_array]::int[]))
));

			is($invalid, 0,
				"$method: startup fsyncs are synchronous outside IO workers");
		}
	};
}


sub test_worker_reopen_and_cleanup
{
	my $method = shift;
	my $node = shift;
	my $error;

	eval {
		set_worker_count($method, $node, 1);

		my $worker_pid = $node->safe_psql(
			'postgres',
			q(SELECT pid FROM pg_stat_activity
			  WHERE backend_type = 'io worker'));
		my $failure = $node->safe_psql(
			'postgres', qq(
SELECT aio_fsync_slru(0, 16777215),
       -errno_from_string('ENOENT')
));

		my ($result, $expected) = split(/\|/, $failure);
		is($result, $expected,
			"$method: nonexistent pg_xact target returns negative ENOENT");
		is( $node->safe_psql(
				'postgres',
				q(SELECT pid FROM pg_stat_activity
				  WHERE backend_type = 'io worker')),
			$worker_pid,
			"$method: worker survives reopen failure");

		is($node->safe_psql('postgres', 'SELECT aio_fsync_slru(0, 0)'),
			0, "$method: worker succeeds after reopen failure");
		is($node->safe_psql('postgres', 'SELECT 1'),
			1, "$method: server is usable after reopen failure");

		my $cleanup = $node->safe_psql(
			'postgres', qq(
SELECT count(*),
       count(*) FILTER (WHERE result <> 0)
FROM (
  SELECT aio_fsync_slru(0, 0) AS result
  FROM generate_series(1, 65)
) AS fsyncs
));

		is($cleanup, '65|0',
			"$method: 65 sequential descriptor reopens all succeed");
		is( $node->safe_psql(
				'postgres',
				q(SELECT pid FROM pg_stat_activity
				  WHERE backend_type = 'io worker')),
			$worker_pid,
			"$method: worker survives descriptor cleanup test");
		is($node->safe_psql('postgres', 'SELECT 1'),
			1, "$method: server is usable after descriptor cleanup test");

		1;
	} or $error = $@;

	my $restore_error;
	eval {
		set_worker_count($method, $node, 2);
		1;
	} or $restore_error = $@;

	die $error if defined $error;
	die $restore_error if defined $restore_error;
}


sub dirty_checkpoint_relations
{
	my $node = shift;
	my $updates = join("\n",
		map { "UPDATE $_ SET value = value + 1 WHERE id = 1;" }
		  @checkpoint_tables);

	$node->safe_psql('postgres', $updates);
}


sub checkpoint_relfilenodes
{
	my $node = shift;
	my $relations = join(', ', map { "'$_'::regclass" } @checkpoint_tables);
	my $output = $node->safe_psql(
		'postgres', qq(
SELECT pg_relation_filenode(rel)
FROM unnest(ARRAY[$relations]) AS relations(rel)
ORDER BY 1
));

	return split(/\n/, $output);
}


sub dirty_pg_xact
{
	my $node = shift;
	my $xid =
	  $node->safe_psql('postgres', 'SELECT pg_current_xact_id()::text');
	my $block_size = $node->safe_psql('postgres',
		q(SELECT setting::int FROM pg_settings WHERE name = 'block_size'));
	my $transactions_per_segment = $block_size * 4 * 32;
	my $segment = int($xid / $transactions_per_segment);

	return ($xid, $segment);
}


sub run_paused_query
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my %args = @_;
	my $method = $args{method};
	my $node = $args{node};
	my $name = $args{name};
	my $marker = 'aio_fsync_query_done';
	my $psql = $node->background_psql('postgres', on_error_stop => 1);
	my $query_pid = $psql->query_safe('SELECT pg_backend_pid()');
	my $owner_pid = $args{owner_pid} // $query_pid;
	my ($error, $output);

	eval {
		arm_completion_wait($node, $args{attach}->($owner_pid));

		$psql->{stdin} .= "$args{query};\n\\echo '$marker'\n";
		$psql->{run}->pump_nb();

		ok( $node->poll_query_until(
				'postgres',
				q(SELECT count(*) > 0 FROM pg_stat_activity
				  WHERE wait_event = 'completion_wait'),
				't'),
			"$method: $name reaches completion wait");

		$args{inspect}->($owner_pid);
		release_completion_wait($node);

		ok( pump_until(
				$psql->{run}, $psql->{timeout},
				\$psql->{stdout}, qr/\Q$marker\E/),
			"$method: $name background query finishes");

		$output = $psql->{stdout};
		is($psql->{stderr}, '', "$method: $name has no psql errors");
		1;
	} or $error = $@;

	if ($completion_wait_armed)
	{
		my $release_error;

		eval {
			release_completion_wait($node);
			1;
		} or $release_error = $@;
		$error = $release_error unless defined $error;
	}

	my $quit_error;
	eval {
		$psql->quit();
		1;
	} or $quit_error = $@;
	$error = $quit_error unless defined $error;

	die $error if defined $error;

	return ($owner_pid, $output);
}


sub arm_completion_wait
{
	my $node = shift;
	my $sql = shift;

	$node->safe_psql('postgres', $sql);
	$completion_wait_armed = 1;
}


sub release_completion_wait
{
	my $node = shift;

	$node->safe_psql('postgres', 'SELECT inj_io_completion_continue()');
	$completion_wait_armed = 0;
}


sub current_worker_pids
{
	my $node = shift;
	my $output = $node->safe_psql(
		'postgres',
		q(SELECT pid FROM pg_stat_activity
		  WHERE backend_type = 'io worker'
		  ORDER BY pid));

	return split(/\n/, $output);
}

sub get_checkpointer_pid
{
	my $node = shift;

	return $node->safe_psql(
		'postgres',
		q(SELECT pid FROM pg_stat_activity
		  WHERE backend_type = 'checkpointer'));
}


sub check_worker_completion_pids
{
	my $method = shift;
	my $node = shift;
	my $where = shift;
	my $name = shift;
	my $worker_array = join(',', current_worker_pids($node));
	my $invalid = $node->safe_psql(
		'postgres', qq(
SELECT count(*)
FROM aio_fsync_completions()
WHERE $where
  AND NOT (executor_pid = ANY (ARRAY[$worker_array]::int[]))
));

	is($invalid, 0,
		"$method: $name completions execute in current IO workers");
}


sub set_worker_count
{
	my $method = shift;
	my $node = shift;
	my $count = shift;

	$node->safe_psql('postgres', "ALTER SYSTEM SET io_min_workers = $count");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');

	is($node->safe_psql('postgres', 'SHOW io_min_workers'),
		$count, "$method: io_min_workers is $count");
	wait_for_worker_count($method, $node, $count);
}


sub wait_for_worker_count
{
	my $method = shift;
	my $node = shift;
	my $count = shift;

	ok( $node->poll_query_until(
			'postgres',
			q(SELECT count(*) FROM pg_stat_activity
			  WHERE backend_type = 'io worker'),
			$count),
		"$method: IO worker count reaches $count");
}

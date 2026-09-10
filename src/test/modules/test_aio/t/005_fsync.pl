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
	my $node = PostgreSQL::Test::Cluster->new("fsync_$method");
	$node->init;
	TestAio::configure($node);
	$node->append_conf(
		'postgresql.conf', qq(
io_method = $method
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
io_max_concurrency = 8
));
	$node->start;
	$node->safe_psql(
		'postgres', q(
CREATE EXTENSION test_aio;
CREATE TABLE fsync_target(i int);
INSERT INTO fsync_target VALUES (1);
CHECKPOINT;
));

	# With only this tag queued, the wait also exercises the final partial
	# batch, rather than just waiting to make room for another request.
	$node->safe_psql(
		'postgres', q(
SELECT fsync_test_configure('fsync_target', wait => true);
SELECT fsync_test_request();
));
	my $checkpoint = $node->background_psql('postgres');
	$checkpoint->query_until(
		qr/checkpoint_sent/, q(
\echo checkpoint_sent
CHECKPOINT;
\echo checkpoint_done
));
	ok( $node->poll_query_until(
			'postgres', q(
SELECT EXISTS (SELECT FROM pg_stat_activity
 WHERE wait_event = 'fsync_completion')
)),
		"$method: fsync completion is held");
	$checkpoint->{run}->pump_nb;
	unlike($checkpoint->{stdout}, qr/checkpoint_done/,
		"$method: checkpoint has not completed");
	is( $node->safe_psql(
			'postgres', q(
SELECT count(*) FROM pg_aios
 WHERE operation = 'fsync' AND target = 'smgr'
 AND target_desc = 'file "' || pg_relation_filepath('fsync_target') || '"'
 AND off IS NULL AND length IS NULL
)),
		'1',
		"$method: pg_aios describes the held fsync");
	is( $node->safe_psql('postgres', 'SELECT fsync_test_count(true)'),
		$method eq 'worker' ? '1' : '0',
		"$method: expected process executed fsync");
	$node->safe_psql('postgres', 'SELECT fsync_test_release()');
	$checkpoint->query_safe('SELECT 1');
	$checkpoint->quit;

	foreach my $case (
		[ 'retry succeeds', 'ENOENT', 1, '', 2 ],
		[ 'canceled request', 'ENOENT', 1, 'cancel', 1 ])
	{
		my ($name, $error, $failures, $cleanup, $attempts) = @$case;
		$node->safe_psql(
			'postgres', qq(
SELECT fsync_test_configure('fsync_target', '$error', $failures,
                           cleanup => '$cleanup');
SELECT fsync_test_request();
CHECKPOINT;
));
		is($node->safe_psql('postgres', 'SELECT fsync_test_count()'),
			$attempts, "$method: $name");
	}

	# An already-handled ENOENT must not become a failed retry in cleanup.
	my $log_offset = -s $node->logfile;
	my ($ret, $out, $err) = $node->psql(
		'postgres', q(
SELECT fsync_test_configure('fsync_target', 'ENOENT', 1,
                           cleanup => 'retry');
SELECT fsync_test_request();
CHECKPOINT;
));
	isnt($ret, 0, "$method: interrupted retry fails checkpoint");
	$node->wait_for_log(qr/ERROR:  test fsync retry interruption/,
		$log_offset);
	$node->safe_psql(
		'postgres', q(
SELECT fsync_test_configure('fsync_target');
CHECKPOINT;
));
	is($node->safe_psql('postgres', 'SELECT fsync_test_count()'),
		'1', "$method: interrupted retry remains pending");

	# Both normal error reporting and cleanup leave failed requests pending.
	$node->safe_psql('postgres', q(ALTER SYSTEM SET data_sync_retry = on));
	$node->restart;
	foreach
	  my $case ([ 'EIO', 1, '' ], [ 'ENOENT', 2, '' ], [ 'EIO', 1, 'before' ])
	{
		my ($error, $failures, $cleanup) = @$case;
		$log_offset = -s $node->logfile;
		($ret, $out, $err) = $node->psql(
			'postgres', qq(
SELECT fsync_test_configure('fsync_target', '$error', $failures,
                           cleanup => '$cleanup');
SELECT fsync_test_request();
CHECKPOINT;
));
		isnt($ret, 0,
			"$method: $error/$cleanup fails checkpoint with retry on");
		$node->wait_for_log(
			$cleanup eq 'before'
			? qr/ERROR:  test fsync checkpoint interruption/
			: qr/ERROR:  could not fsync file/,
			$log_offset);
		$node->safe_psql(
			'postgres', q(
SELECT fsync_test_configure('fsync_target');
CHECKPOINT;
));
		is($node->safe_psql('postgres', 'SELECT fsync_test_count()'),
			'1', "$method: $error/$cleanup is retried by next checkpoint");
	}
	$node->safe_psql('postgres', q(ALTER SYSTEM SET data_sync_retry = off));
	$node->restart;

	foreach
	  my $case ([ 'EIO', 1, '' ], [ 'ENOENT', 2, '' ], [ 'EIO', 1, 'before' ])
	{
		my ($error, $failures, $cleanup) = @$case;
		$log_offset = -s $node->logfile;
		($ret, $out, $err) = $node->psql(
			'postgres', qq(
SELECT fsync_test_configure('fsync_target', '$error', $failures,
                           cleanup => '$cleanup');
SELECT fsync_test_request();
CHECKPOINT;
));
		isnt($ret, 0, "$method: $error/$cleanup fails with retry off");
		$node->wait_for_log(qr/PANIC:  could not fsync file/, $log_offset);
		$node->wait_for_log(qr/database system is shut down/, $log_offset);
		$node->_update_pid(0);
		$node->start;
	}
	$node->stop;
}

done_testing();

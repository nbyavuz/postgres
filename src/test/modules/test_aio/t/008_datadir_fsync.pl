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
	my $node = PostgreSQL::Test::Cluster->new("datadir_$method");
	my $wal = PostgreSQL::Test::Utils::tempdir();
	my $space = PostgreSQL::Test::Utils::tempdir();
	$node->init(extra => [ '--waldir', $wal ]);
	TestAio::configure($node);
	$node->append_conf(
		'postgresql.conf', qq(
io_method = $method
fsync = on
recovery_init_sync_method = fsync
max_files_per_process = 64
io_max_concurrency = 32
));
	$node->start;
	$node->safe_psql('postgres',
		"CREATE TABLESPACE fsync_space LOCATION '$space'");
	$node->safe_psql(
		'postgres', q(
CREATE TABLE target(i int) TABLESPACE fsync_space;
INSERT INTO target VALUES (1);
CHECKPOINT;
));
	my $path =
	  $node->safe_psql('postgres', q(SELECT pg_relation_filepath('target')));
	$node->stop('immediate');

	# Marker enables observation and holds startup just before its final
	# drain.  Filesystem coordination works before SQL is available.
	my $gate = $node->data_dir . '/test_aio_datadir';
	append_to_file($gate, '');
	my $offset = -s $node->logfile;
	command_ok(
		[
			'pg_ctl', '-D', $node->data_dir, '-l',
			$node->logfile, '-W', 'start'
		],
		"$method: launch crash recovery");
	$node->wait_for_log(qr/test datadir final drain:/, $offset);
	$node->_update_pid(1);
	my $log = substr(slurp_file($node->logfile), $offset);
	unlike(
		$log,
		qr/database system is ready to accept connections/,
		"$method: startup has not completed before final drain");
	like(
		$log,
		qr/LOG:  could not fsync file .* (?:I\/O|Input\/output) error/,
		"$method: startup logs injected EIO and continues draining");
	my ($pending, $staged, $completed, $reaped) =
	  $log =~
	  /test datadir final drain: (\d+) pending, (\d+) staged, (\d+) completed, (\d+) reaped/;
	cmp_ok($pending, '>', 0, "$method: final drain has outstanding entries");
	is($staged - $reaped,
		$pending, "$method: pending entries account for unreaped operations");

	if ($method ne 'io_uring')
	{
		is($completed, $staged,
			"$method: startup fsyncs execute synchronously");
	}
	unlink $gate or die "could not release startup gate: $!";
	$node->wait_for_log(qr/test datadir synchronization drained/, $offset);
	$node->wait_for_log(qr/database system is ready to accept connections/,
		$offset);
	$log = substr(slurp_file($node->logfile), $offset);
	like(
		$log,
		qr/test datadir synced: \Q$path\E(?:\r?\n)/,
		"$method: tablespace relation synchronized");
	like(
		$log,
		qr/test datadir synced: pg_wal\/[0-9A-F]{24}(?:\r?\n)/,
		"$method: external WAL synchronized");
	like(
		$log,
		qr/test datadir synced: \.(?:\r?\n)/,
		"$method: data directory itself synchronized") unless $windows_os;
	is($node->safe_psql('postgres', 'SELECT * FROM target'),
		'1', "$method: recovery completes after synchronization error");
	$node->stop;
}

done_testing();

# Copyright (c) 2026, PostgreSQL Global Development Group

# A clean shutdown during recovery initialization must not lose the directory
# sync required by an earlier crash.  In particular, initialization can wait
# for archived checkpoint WAL before any redo has taken place.

use strict;
use warnings FATAL => 'all';
use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'test requires Unix directory permissions' if $windows_os;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', 'fsync = on');
$primary->start;
$primary->backup('backup');

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
$standby->start;
$primary->wait_for_replay_catchup($standby);
$standby->stop('immediate');

my ($control_before) =
  run_command([ 'pg_controldata', $standby->data_dir ]);
like(
	$control_before,
	qr/Database cluster state:\s+in archive recovery/,
	'crashed standby requires a directory sync');

# Observe SyncDataDirectory() without relying on progress-report timing or
# platform-specific syscall tracing.  An unreadable dummy directory at the
# top level of PGDATA produces a nonfatal diagnostic during the fsync scan.
# Other startup code has no reason to visit this directory.  Leave it in place
# across both startups so that a deferred sync can also be detected.
my $probe = $standby->data_dir . '/sync_probe';
mkdir($probe) or die "could not create $probe: $!";
chmod(0000, $probe) or die "could not change permissions of $probe: $!";
my $sync_pattern = qr/could not open directory "\.\/sync_probe"/;

# Use a fresh log so the helper does not see the earlier immediate shutdown.
my $logfile = $standby->rotate_logfile;
my $perlbin = $^X;
my $restore_timeout = $PostgreSQL::Test::Utils::timeout_default;
$standby->append_conf(
	'postgresql.conf', qq{
fsync = on
recovery_init_sync_method = fsync
recovery_target_timeline = 'current'
log_min_messages = debug2
restore_command = '"$perlbin" "$FindBin::RealBin/wait_for_shutdown" "$logfile" $restore_timeout'
});

# Start a fresh postmaster, rather than triggering its automatic crash restart:
# FatalError must be false so that fast shutdown asks the checkpointer for a
# shutdown restartpoint.  Do not wait for connections, since restore_command
# deliberately blocks before the checkpoint record has been read.
command_ok(
	[
		'pg_ctl',
		'--pgdata' => $standby->data_dir,
		'--log' => $logfile,
		'--no-wait', 'start',
	],
	'start fresh postmaster after crash');
$standby->wait_for_log(qr/restore_command waiting for shutdown/);
$standby->_update_pid(1);
$standby->wait_for_log(
	qr/checkpointer updated shared memory configuration values/);
$standby->stop('fast');

my $shutdown_log = slurp_file($logfile);
unlike(
	$shutdown_log,
	qr/timed out waiting for shutdown request/,
	'restore_command was interrupted by shutdown');
unlike(
	$shutdown_log,
	qr/checkpoint record is at|redo starts at/,
	'shutdown occurred before reading the checkpoint record');

my ($control_after) =
  run_command([ 'pg_controldata', $standby->data_dir ]);
like(
	$control_after,
	qr/Database cluster state:\s+shut down in recovery/,
	'checkpointer recorded a clean recovery shutdown');

# A restart from DB_SHUTDOWNED_IN_RECOVERY normally skips the directory sync.
# Check both startup attempts: the first crash's durability barrier must not
# disappear just because startup was interrupted by a clean shutdown.
$standby->append_conf('postgresql.conf', "restore_command = ''");
$standby->start;
like(slurp_file($logfile), $sync_pattern,
	'crash-required directory sync is not lost across interrupted startup');

chmod(0700, $probe) or die "could not restore permissions of $probe: $!";
rmdir($probe) or die "could not remove $probe: $!";
$standby->stop;
$primary->stop;

done_testing();

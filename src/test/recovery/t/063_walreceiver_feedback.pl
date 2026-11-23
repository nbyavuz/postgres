# Copyright (c) 2026, PostgreSQL Global Development Group

# Check feedback from a real standby before it needs to stream WAL.
use strict;
use warnings FATAL => 'all';

use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1, has_archiving => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '128MB'");
$primary->start;
$primary->safe_psql('postgres', 'CREATE TABLE t (i integer)');
my $initial = $primary->safe_psql('postgres',
	"SELECT lsn FROM pg_create_physical_replication_slot('feedback', true)");
$primary->backup('old_timeline');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('old_timeline'); SELECT pg_switch_wal()");
$primary->stop;
$primary->set_standby_mode;
$primary->start;
$primary->promote;

sub finish_segment
{
	my $seg = $primary->safe_psql('postgres',
		'SELECT pg_walfile_name(pg_current_wal_insert_lsn())');
	$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
	$primary->poll_query_until('postgres',
		"SELECT last_archived_wal >= '$seg' FROM pg_stat_archiver")
	  or die "WAL segment $seg was not archived";
}

$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('new_timeline')");
finish_segment();
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (1)');
finish_segment();

# Put the new timeline's segments in local pg_wal, but leave out its history
# file. The standby must first stream the old timeline. At its end, the WAL
# receiver waits for instructions and startup can use the local segments.
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'old_timeline', has_streaming => 1);
for my $file (grep { /^00000002[0-9A-F]{16}$/ }
	slurp_dir($primary->archive_dir))
{
	copy($primary->archive_dir . "/$file", $standby->data_dir . '/pg_wal')
	  or die "copy $file: $!";
}
$standby->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'feedback'
recovery_min_apply_delay = '1d'
wal_receiver_status_interval = 1
));
$standby->start;
$standby->wait_for_event('startup', 'RecoveryApplyDelay');
my $replay = $standby->lsn('replay');
my $received = $standby->lsn('receive');
my $slot = q{FROM pg_replication_slots WHERE slot_name = 'feedback'};
is( $standby->safe_psql(
		'postgres',
		'SELECT pg_last_wal_receive_lsn() < pg_last_wal_replay_lsn()'),
	't',
	'local replay passed the end of the old WAL stream');
ok( $primary->poll_query_until(
		'postgres',
		"SELECT active AND restart_lsn = '$replay' AND restart_lsn > '$initial' $slot"
	),
	'primary slot advanced exactly to completed local replay');
is( $standby->safe_psql(
		'postgres', 'SELECT status FROM pg_stat_wal_receiver'),
	'feedback',
	'standby replaced the waiting WAL receiver with a feedback receiver');

# Clear and restore each connection setting while replay is still delayed.
for my $setting (qw(primary_conninfo primary_slot_name))
{
	my $value =
	  $setting eq 'primary_conninfo' ? $primary->connstr : 'feedback';
	$standby->append_conf('postgresql.conf', "$setting = ''");
	$standby->reload;
	$primary->poll_query_until('postgres', "SELECT NOT active $slot")
	  or die "clearing $setting did not release the slot";
	$standby->append_conf('postgresql.conf', "$setting = '$value'");
	$standby->reload;
	ok( $primary->poll_query_until(
			'postgres', "SELECT active AND restart_lsn = '$replay' $slot"),
		"feedback reconnected after restoring $setting");
}
is($standby->lsn('receive'),
	$received, 'feedback did not advance the receive position');

# This transaction is not in the standby's local segments.
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (2)');
$standby->append_conf('postgresql.conf', 'recovery_min_apply_delay = 0');
$standby->reload;
$primary->wait_for_replay_catchup($standby);
is($standby->safe_psql('postgres', 'SELECT sum(i) FROM t'),
	'3', 'streaming resumed and delivered new WAL');
$standby->stop;

# Now exercise archive recovery with the primary initially unreachable. A
# delayed commit lets us bring it back before replay enters the next segment.
$primary->backup('archive');
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (3)');
finish_segment();
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('archive_target')");
finish_segment();
my $archive = PostgreSQL::Test::Cluster->new('archive');
$archive->init_from_backup(
	$primary, 'archive',
	has_streaming => 1,
	has_restoring => 1);
$archive->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'feedback'
recovery_min_apply_delay = '1d'
recovery_target_name = 'archive_target'
wal_retrieve_retry_interval = '100ms'
wal_receiver_status_interval = 1
));
$primary->stop;
$archive->start;
$archive->wait_for_log(qr/could not connect to the primary server/);
$archive->wait_for_event('startup', 'RecoveryApplyDelay');
is( $archive->safe_psql(
		'postgres', 'SELECT pg_last_wal_receive_lsn() IS NULL'),
	't',
	'standby reached the archived commit with the primary down');

# Keep a snapshot from before the delayed commit. Its xmin should be
# reported until the reader disconnects, even after replay passes the commit.
my $reader = $archive->background_psql('postgres');
$reader->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $xmin = $reader->query_safe(
	'SELECT backend_xmin FROM pg_stat_activity WHERE pid = pg_backend_pid()');
$primary->start;
my $delayed_lsn = $archive->lsn('replay');
$primary->poll_query_until('postgres',
	"SELECT active AND restart_lsn = '$delayed_lsn' $slot")
  or die 'feedback did not reconnect while replay was delayed';
$archive->append_conf('postgresql.conf', 'recovery_min_apply_delay = 0');
$archive->reload;
$archive->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'standby did not reach the archived restore point';
$replay = $archive->lsn('replay');
ok( $primary->poll_query_until(
		'postgres', "SELECT active AND restart_lsn = '$replay' $slot"),
	'retried feedback and reported the archived restore point');
# Lose an established connection without resuming replay or reloading.
my $old_sender = $primary->safe_psql('postgres', "SELECT active_pid $slot");
$primary->safe_psql('postgres', "SELECT pg_terminate_backend($old_sender)");
ok( $primary->poll_query_until(
		'postgres',
		"SELECT active_pid <> $old_sender AND restart_lsn = '$replay' $slot"),
	'feedback reconnected at the same paused replay position');
is( $archive->safe_psql(
		'postgres', 'SELECT pg_last_wal_receive_lsn() IS NULL'),
	't',
	'no WAL was streamed to reach the restore point');

# The named receiver can publish and withdraw xmin without giving up the slot.
my $receiver_pid =
  $archive->safe_psql('postgres', 'SELECT pid FROM pg_stat_wal_receiver');
my $sender = q{FROM pg_stat_replication WHERE application_name = 'archive'};
$archive->append_conf('postgresql.conf', 'hot_standby_feedback = on');
$archive->reload;
$primary->poll_query_until('postgres',
	"SELECT active AND xmin = '$xmin' $slot")
  or die 'named slot did not receive standby snapshot xmin';
$archive->append_conf('postgresql.conf', 'hot_standby_feedback = off');
$archive->reload;
ok( $primary->poll_query_until(
		'postgres',
		"SELECT active AND xmin IS NULL AND catalog_xmin IS NULL $slot"),
	'disabling xmin feedback cleared the named slot');
is($archive->safe_psql('postgres', 'SELECT pid FROM pg_stat_wal_receiver'),
	$receiver_pid, 'named feedback connection survived both reloads');

# With neither a named slot nor xmin feedback, there is no receiver. Turn
# on xmin feedback alone to start a slotless session from the same pause.
$archive->append_conf('postgresql.conf',
	"primary_slot_name = ''\nwal_receiver_create_temp_slot = on");
$archive->reload;
$primary->poll_query_until('postgres', "SELECT count(*) = 0 $sender")
  or die 'feedback connection did not stop';
$archive->append_conf('postgresql.conf', 'hot_standby_feedback = on');
$archive->reload;
ok( $primary->poll_query_until(
		'postgres',
		"SELECT state = 'feedback' AND backend_xmin = '$xmin' $sender"),
	'slotless feedback protects the standby snapshot on the primary');
is( $primary->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_replication_slots WHERE active OR temporary'
	),
	'0',
	'slotless feedback did not acquire or create a slot');
$reader->quit;
ok( $primary->poll_query_until(
		'postgres', "SELECT age(backend_xmin) < age('$xmin'::xid) $sender"),
	'periodic feedback advanced xmin after the standby reader disconnected');
$archive->append_conf('postgresql.conf', 'hot_standby_feedback = off');
$archive->reload;
ok( $primary->poll_query_until('postgres', "SELECT count(*) = 0 $sender"),
	'disabling slotless feedback removed the sender holding xmin');

$archive->append_conf('postgresql.conf', "primary_slot_name = 'feedback'");
$archive->reload;
$primary->poll_query_until('postgres', "SELECT active $slot")
  or die 'named feedback did not resume before promotion';
$archive->promote;
ok($primary->poll_query_until('postgres', "SELECT NOT active $slot"),
	'promotion released the feedback slot');

done_testing();

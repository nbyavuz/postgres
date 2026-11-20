# Copyright (c) 2026, PostgreSQL Global Development Group

# Check when an archive-replaying standby starts feedback after losing a stream.
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
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('feedback', true)");
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
		'SELECT pg_walfile_name(pg_switch_wal())');
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
is( $standby->safe_psql(
		'postgres',
		'SELECT pg_last_wal_receive_lsn() < pg_last_wal_replay_lsn()'),
	't',
	'local replay passed the end of the old WAL stream');
is( $standby->safe_psql(
		'postgres', 'SELECT status FROM pg_stat_wal_receiver'),
	'waiting',
	'normal end of a timeline does not trigger feedback');
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (2)');
$standby->append_conf('postgresql.conf', 'recovery_min_apply_delay = 0');
$standby->reload;
$primary->wait_for_replay_catchup($standby);
is($standby->safe_psql('postgres', 'SELECT sum(i) FROM t'),
	'3', 'streaming resumed and delivered new WAL');
$standby->stop;

# Establish a real stream. Hold the startup process at a replay pause while
# interrupting that stream and building a backlog of archived WAL.
$primary->backup('archive');
my $archive = PostgreSQL::Test::Cluster->new('archive');
$archive->init_from_backup(
	$primary, 'archive',
	has_streaming => 1,
	has_restoring => 1);
$archive->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'feedback'
max_standby_archive_delay = -1
wal_retrieve_retry_interval = '100ms'
wal_receiver_status_interval = 1
));
$archive->start;
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (3)');
$primary->wait_for_catchup('archive', 'replay');
$archive->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$archive->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'standby did not pause';
is( $archive->safe_psql(
		'postgres', 'SELECT status FROM pg_stat_wal_receiver'),
	'streaming',
	'caught-up stream is still streaming');
my $slot = q{FROM pg_replication_slots WHERE slot_name = 'feedback'};
$primary->safe_psql('postgres',
	"SELECT pg_terminate_backend(active_pid) $slot");
$archive->poll_query_until('postgres',
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver')
  or die 'standby did not lose the stream';
my $received = $archive->lsn('receive');
$archive->append_conf('postgresql.conf', "recovery_min_apply_delay = '1d'");
$archive->reload;
# The standby can replay the archived INSERT, but not yet its commit.
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (4)');
finish_segment();
my $log_offset = -s $archive->logfile;
$primary->stop;
$archive->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$archive->wait_for_event('startup', 'RecoveryApplyDelay');
$archive->wait_for_log(qr/could not connect to the primary server/,
	$log_offset);
is($archive->lsn('receive'),
	$received, 'archive replay continued with the primary down');

# Keep a snapshot from before the delayed commit. Its xmin should be
# reported until the reader disconnects, even after replay passes the commit.
my $reader = $archive->background_psql('postgres');
$reader->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $xmin = $reader->query_safe(
	'SELECT backend_xmin FROM pg_stat_activity WHERE pid = pg_backend_pid()');
$primary->start;
my $delayed_lsn = $archive->lsn('replay');
# A reconnected sender inherits the slot's position, but not its reply time.
my $feedback_slot = q{FROM pg_replication_slots s
  JOIN pg_stat_replication r ON r.pid = s.active_pid
  WHERE s.slot_name = 'feedback' AND r.state = 'feedback' AND r.reply_time IS NOT NULL};
$primary->poll_query_until('postgres',
	"SELECT restart_lsn = '$delayed_lsn' $feedback_slot")
  or die 'feedback did not reconnect while replay was delayed';

# Clear and restore each connection setting without advancing replay.
for my $setting (qw(primary_conninfo primary_slot_name))
{
	my $value =
	  $setting eq 'primary_conninfo' ? $primary->connstr : 'feedback';
	$archive->append_conf('postgresql.conf', "$setting = ''");
	$archive->reload;
	$primary->poll_query_until('postgres', "SELECT NOT active $slot")
	  or die "clearing $setting did not release the slot";
	$archive->append_conf('postgresql.conf', "$setting = '$value'");
	$archive->reload;
	ok( $primary->poll_query_until(
			'postgres', "SELECT restart_lsn = '$delayed_lsn' $feedback_slot"),
		"feedback reconnected after restoring $setting");
}
my $feedback_pid =
  $primary->safe_psql('postgres',
	"SELECT pid $feedback_slot AND restart_lsn = '$delayed_lsn'");

# Hold replay after the restore point. The sender that reported the delayed
# position must report progress again before the standby can resume streaming.
my $blocker = $archive->background_psql('postgres');
$blocker->query_safe('BEGIN; LOCK t IN ACCESS SHARE MODE');
my $marker = $primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('archive_target')");
$primary->safe_psql('postgres',
	'BEGIN; LOCK t IN ACCESS EXCLUSIVE MODE; COMMIT');
finish_segment();

$archive->append_conf('postgresql.conf', 'recovery_min_apply_delay = 0');
$archive->reload;
ok( $primary->poll_query_until(
		'postgres',
		"SELECT restart_lsn >= '$marker' $feedback_slot AND r.pid = $feedback_pid"
	),
	'the same feedback sender advanced the slot to the archived restore point'
);
is( $archive->safe_psql(
		'postgres', qq[SELECT pg_last_wal_receive_lsn() = '$received'
			AND pg_last_wal_replay_lsn() >= '$marker']),
	't',
	'standby replayed the restore point without streaming');

# Pause before releasing the lock so the standby cannot exhaust the archive.
$archive->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$blocker->quit;
$archive->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'standby did not pause during archive replay';
my $replay = $archive->lsn('replay');
$primary->safe_psql('postgres', "SELECT pg_terminate_backend($feedback_pid)");
ok( $primary->poll_query_until(
		'postgres',
		"SELECT pid <> $feedback_pid AND restart_lsn = '$replay' $feedback_slot"
	),
	'feedback reconnected at the same paused replay position');
is($archive->lsn('receive'),
	$received, 'archive feedback did not advance the receive LSN');

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
$archive->poll_query_until('postgres',
	"SELECT status = 'feedback' FROM pg_stat_wal_receiver")
  or die 'named feedback receiver was not ready for handoff';

# Exhaust the archive and request a transaction that is only on the primary.
my $handoff_pid =
  $primary->safe_psql('postgres', "SELECT pid $feedback_slot");
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (5)');
$archive->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->wait_for_catchup('archive', 'replay');
is($primary->safe_psql('postgres', "SELECT active_pid $slot"),
	$handoff_pid,
	'the same walsender handed the named slot back to streaming');
is($archive->safe_psql('postgres', 'SELECT max(i) FROM t'),
	'5', 'streaming delivered new WAL');

done_testing();

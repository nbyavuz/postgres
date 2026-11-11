# Copyright (c) 2026, PostgreSQL Global Development Group

# Feedback must advance a physical slot without changing the WAL source.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(
	has_archiving => 1,
	allows_streaming => 'logical',
	extra => ['--wal-segsize=1']);
$primary->append_conf(
	'postgresql.conf', qq(
min_wal_size = '2MB'
max_wal_size = '16MB'
wal_keep_size = 0
checkpoint_timeout = '1h'
autovacuum = off
synchronous_standby_names = '*'
synchronous_commit = local
));
$primary->start;
$primary->safe_psql('postgres', 'CREATE TABLE feedback_test (n integer)');
$primary->backup('backup');
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('feedback_slot', true)");
my $original = $primary->safe_psql('postgres',
	"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
);
my $old_segment =
  $primary->safe_psql('postgres', "SELECT pg_walfile_name('$original')");

# These records do not invoke recovery_min_apply_delay.  They let replay pass
# whole segments before stopping at the first commit record below.
for (1 .. 4)
{
	$primary->safe_psql('postgres',
		"SELECT pg_logical_emit_message(false, 'feedback', 'test'); SELECT pg_switch_wal()"
	);
}

# Put several segments in the archive.  A long apply delay gives us a
# deterministic place to request a replay pause before archive exhaustion.
for (1 .. 5)
{
	$primary->safe_psql('postgres',
		"INSERT INTO feedback_test VALUES ($_); SELECT pg_switch_wal()");
}
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup(
	$primary, 'backup',
	has_streaming => 1,
	has_restoring => 1);
$standby->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'feedback_slot'
recovery_min_apply_delay = '1h'
wal_retrieve_retry_interval = '100ms'
wal_receiver_status_interval = '1s'
hot_standby_feedback = on
));
$standby->start;
$standby->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE wait_event = 'RecoveryApplyDelay'"
) or die 'standby did not enter archive replay delay';
$standby->poll_query_until('postgres',
	"SELECT status = 'feedback' FROM pg_stat_wal_receiver")
  or die 'feedback receiver did not start during archive recovery';
$primary->poll_query_until('postgres',
	"SELECT state = 'feedback' AND sent_lsn IS NULL AND sync_state = 'async' FROM pg_stat_replication"
) or die 'upstream did not enter feedback mode';
pass('feedback connection established without streaming');

$primary->poll_query_until('postgres',
	"SELECT restart_lsn > '$original' FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'physical slot did not advance';
pass('archive replay advances the physical slot');
$primary->poll_query_until('postgres',
	"SELECT xmin IS NOT NULL FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'hot standby feedback was not received';
pass('hot standby feedback works during archive recovery');

# Persist slot progress and allow normal checkpoint recycling to remove an
# acknowledged segment, while the receiver still is not streaming.
$primary->safe_psql('postgres', 'CHECKPOINT');
ok(!-f $primary->data_dir . "/pg_wal/$old_segment",
	'checkpoint releases WAL acknowledged through archive replay');
is( $primary->safe_psql('postgres', 'SELECT state FROM pg_stat_replication'),
	'feedback',
	'WAL reclamation did not require a streaming connection');

# Commands must reject bad history/positions before entering COPY mode.
for my $case (
	[
		"START_FEEDBACK 0/0 TIMELINE 1",
		qr/invalid standby feedback position/
	],
	[
		"START_FEEDBACK FFFFFFFF/FFFFFFFF TIMELINE 1",
		qr/invalid standby feedback position/
	],
	[
		"START_FEEDBACK $original TIMELINE 99",
		qr/not in this server's history/
	],
	[ "START_FEEDBACK $original TIMELINE 0", qr/invalid timeline/ ],
	[ "START_FEEDBACK SLOT absent $original TIMELINE 1", qr/does not exist/ ],
	[
		"START_FEEDBACK SLOT feedback_slot $original TIMELINE 1",
		qr/is active for PID/
	])
{
	my ($result, $stdout, $stderr) =
	  $primary->psql('postgres', $case->[0], replication => 'true');
	isnt($result, 0, "$case->[0] fails");
	like($stderr, $case->[1], 'feedback command reports the expected error');
}

# Pause between records, preserving archive as the source, then exercise
# receiver restart and reload while replay cannot make any progress.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$standby->adjust_conf('postgresql.conf', 'recovery_min_apply_delay', '0');
$standby->reload;
$standby->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'replay did not pause';
my $paused = $standby->lsn('replay');
$primary->poll_query_until('postgres',
	"SELECT restart_lsn = '$paused' FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'last replay position was not reported';
my $pid =
  $standby->safe_psql('postgres', 'SELECT pid FROM pg_stat_wal_receiver');
$primary->safe_psql('postgres',
	"SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE state = 'feedback'"
);
$standby->poll_query_until('postgres',
	"SELECT status = 'feedback' AND pid <> $pid FROM pg_stat_wal_receiver")
  or die 'feedback receiver did not restart';
is($standby->lsn('replay'),
	$paused, 'feedback reconnects while replay is paused');

# Keepalives must work even when periodic status reports are disabled. Wait
# for a later reply rather than assuming a particular scheduling delay.
$standby->adjust_conf('postgresql.conf', 'wal_receiver_status_interval', '0');
$standby->append_conf('postgresql.conf', "wal_receiver_timeout = '1s'");
$standby->reload;
my $reply_time = $primary->safe_psql('postgres',
	'SELECT reply_time FROM pg_stat_replication');
$primary->poll_query_until('postgres',
	"SELECT reply_time > '$reply_time'::timestamptz + interval '1 second' FROM pg_stat_replication"
) or die 'feedback keepalives did not elicit a reply';
is( $primary->safe_psql(
		'postgres', 'SELECT flush_lsn FROM pg_stat_replication'),
	$paused,
	'keepalives do not acknowledge past paused replay');
$standby->adjust_conf('postgresql.conf', 'wal_receiver_status_interval',
	"'1s'");
$standby->adjust_conf('postgresql.conf', 'wal_receiver_timeout', "'60s'");
$standby->reload;

# Disconnect the primary completely.  Its fast shutdown must not wait for a
# feedback sender to transmit WAL, and reconnect must work without redo.
$primary->stop;
is($standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	't', 'standby stays available during upstream outage');
$primary->start;
$primary->poll_query_until('postgres',
	"SELECT state = 'feedback' AND flush_lsn = '$paused' FROM pg_stat_replication"
) or die 'feedback did not reconnect after upstream restart';
pass('feedback resumes after upstream restart without replay progress');

$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('replacement_slot', true)");
my $replacement_lsn = $primary->safe_psql('postgres',
	"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'replacement_slot'"
);
$standby->adjust_conf('postgresql.conf', 'primary_slot_name',
	'replacement_slot');
$standby->reload;
$primary->poll_query_until('postgres',
	"SELECT active FROM pg_replication_slots WHERE slot_name = 'replacement_slot'"
) or die 'feedback connection did not reload its slot';
$primary->poll_query_until('postgres',
	"SELECT NOT active FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'old feedback slot remained active';
pass('slot configuration reload works during archive replay');
ok( $primary->poll_query_until(
		'postgres',
		"SELECT s.restart_lsn = '$replacement_lsn' AND r.flush_lsn = '$paused' FROM pg_replication_slots s JOIN pg_stat_replication r ON r.pid = s.active_pid WHERE s.slot_name = 'replacement_slot'"
	),
	'feedback does not retreat a slot ahead of replay');

# Return to the original slot, then allow ordinary source selection to reach
# streaming.  There must never be two owners of the slot.
$standby->adjust_conf('postgresql.conf', 'primary_slot_name',
	'feedback_slot');
$standby->reload;
$primary->poll_query_until('postgres',
	"SELECT active FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'original slot was not reacquired';
$primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('replacement_slot')");

# A disabled upstream connection must also be processed in archive mode.
my $conninfo = $standby->safe_psql('postgres', 'SHOW primary_conninfo');
$standby->adjust_conf('postgresql.conf', 'primary_conninfo', "''");
$standby->reload;
$standby->poll_query_until('postgres',
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver')
  or die 'feedback did not stop after clearing primary_conninfo';
$conninfo =~ s/'/''/g;
$standby->adjust_conf('postgresql.conf', 'primary_conninfo', "'$conninfo'");
$standby->reload;
$standby->poll_query_until('postgres',
	"SELECT status = 'feedback' FROM pg_stat_wal_receiver")
  or die 'feedback did not resume after restoring primary_conninfo';
pass('primary_conninfo reload works without changing WAL source');

# Releasing hot-standby feedback must work without waiting for streaming.
$standby->adjust_conf('postgresql.conf', 'hot_standby_feedback', 'off');
$standby->reload;
$primary->poll_query_until('postgres',
	"SELECT xmin IS NULL AND catalog_xmin IS NULL FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'disabled hot standby feedback did not release its horizons';
pass('hot standby feedback can be disabled during archive recovery');
$standby->adjust_conf('postgresql.conf', 'hot_standby_feedback', 'on');
$standby->reload;

# Without a slot, the same session carries hot-standby feedback in PGPROC.
$standby->adjust_conf('postgresql.conf', 'primary_slot_name', "''");
$standby->reload;
$primary->poll_query_until('postgres',
	"SELECT r.state = 'feedback' AND r.backend_xmin IS NOT NULL AND NOT s.active FROM pg_stat_replication r CROSS JOIN pg_replication_slots s WHERE s.slot_name = 'feedback_slot'"
) or die 'slotless archive feedback did not start';
pass('archive feedback also works without a replication slot');
$standby->adjust_conf('postgresql.conf', 'primary_slot_name',
	'feedback_slot');
$standby->reload;
$primary->poll_query_until('postgres',
	"SELECT active FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
) or die 'physical slot was not reacquired after slotless feedback';

# Logical failover-slot advancement must wake when the physical slot receives
# an acknowledgement from archive replay, not just from streaming.
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('logical_feedback', 'test_decoding', false, false, true)"
);
$primary->append_conf('postgresql.conf',
	"synchronized_standby_slots = 'feedback_slot'");
$primary->reload;
$primary->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (6)');
my $logical_target = $primary->lsn('flush');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
my $logical = $primary->background_psql('postgres');
$logical->query_safe('SET client_min_messages = error');
$logical->query_until(
	qr/advance_started/, qq(
\\echo advance_started
SELECT pg_replication_slot_advance('logical_feedback', '$logical_target');
));
$primary->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE wait_event = 'WaitForStandbyConfirmation'"
) or die 'logical slot did not wait for physical feedback';

# Leave the startup process in archive recovery at the logical target.
$standby->append_conf(
	'postgresql.conf', qq(
recovery_target_lsn = '$logical_target'
recovery_target_action = 'pause'
));
$standby->stop;
$standby->start;
$standby->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'archive recovery did not reach the logical target';
$logical->query_safe('SELECT 1');
$logical->quit;
is( $primary->safe_psql('postgres', 'SELECT state FROM pg_stat_replication'),
	'feedback',
	'archive feedback releases logical failover-slot advancement');
$primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('logical_feedback')");
$primary->adjust_conf('postgresql.conf', 'synchronized_standby_slots', "''");
$primary->reload;
$standby->adjust_conf('postgresql.conf', 'recovery_target_lsn', undef);
$standby->adjust_conf('postgresql.conf', 'recovery_target_action', undef);

# Crash while the acknowledged WAL is no longer on the primary.  Keep an
# apply delay on restart so that archive recovery has to reestablish feedback.
$primary->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (7)');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$standby->adjust_conf('postgresql.conf', 'recovery_min_apply_delay', '1h');
$standby->stop('immediate');
$standby->start;
$standby->poll_query_until('postgres',
	"SELECT status = 'feedback' FROM pg_stat_wal_receiver")
  or die 'feedback did not resume after standby crash';
pass(
	'standby recovers after WAL acknowledged through archive replay is removed'
);
$standby->adjust_conf('postgresql.conf', 'recovery_min_apply_delay', '0');
$standby->reload;
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->wait_for_catchup($standby);
$standby->poll_query_until('postgres',
	"SELECT status = 'streaming' FROM pg_stat_wal_receiver")
  or die 'feedback-to-streaming transition failed';
is($standby->safe_psql('postgres', 'SELECT count(*) FROM feedback_test'),
	7, 'all archived transactions are recovered before streaming');

# Fail a working stream while local and archived WAL are available.  A paused
# startup retains the failed stream as its source until replay resumes; after
# consuming that WAL it must be able to fall back and reconnect.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$standby->adjust_conf('postgresql.conf', 'recovery_min_apply_delay', '1h');
$standby->reload;
$primary->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (8)');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$standby->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'streaming standby did not pause';
$primary->safe_psql('postgres',
	'SELECT pg_terminate_backend(pid) FROM pg_stat_replication');
$standby->poll_query_until('postgres',
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver')
  or die 'streaming receiver did not stop';
$primary->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (9)');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$standby->adjust_conf('postgresql.conf', 'recovery_min_apply_delay', '0');
$standby->reload;
$primary->wait_for_catchup($standby);

$standby->stop('immediate');
$standby->start;
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM feedback_test'),
	9,
	'standby survives streaming failure and a crash after archive acknowledgements'
);
$standby->stop;

# Exercise a live upstream promotion while downstream replay is still on the
# ancestor timeline.  Both timelines are archived into the same archive.
my $upstream = PostgreSQL::Test::Cluster->new('upstream');
$upstream->init_from_backup(
	$primary, 'backup',
	has_streaming => 1,
	has_restoring => 1);
my $archive_command = $primary->safe_psql('postgres', 'SHOW archive_command');
$archive_command =~ s/'/''/g;
$upstream->append_conf(
	'postgresql.conf', qq(
archive_mode = on
archive_command = '$archive_command'
synchronous_standby_names = ''
));
$upstream->start;
$primary->wait_for_catchup($upstream);
$upstream->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('cascade_feedback', true)");

my $cascade = PostgreSQL::Test::Cluster->new('cascade');
$cascade->init_from_backup($primary, 'backup', has_restoring => 1);
$cascade->enable_streaming($upstream);
$cascade->append_conf(
	'postgresql.conf', qq(
primary_slot_name = 'cascade_feedback'
recovery_min_apply_delay = '1h'
recovery_target_name = 'feedback_new_timeline'
recovery_target_action = 'pause'
wal_retrieve_retry_interval = '100ms'
wal_receiver_status_interval = '1s'
));
$cascade->start;
$cascade->poll_query_until('postgres',
	"SELECT status = 'feedback' AND receive_start_tli = 1 FROM pg_stat_wal_receiver"
) or die 'cascade did not report progress on the ancestor timeline';
pass('feedback-only sessions work with a cascading upstream');
my $old_sender =
  $upstream->safe_psql('postgres', 'SELECT pid FROM pg_stat_replication');

$upstream->promote;
$upstream->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (8)');
my $new_target = $upstream->safe_psql('postgres',
	"SELECT pg_create_restore_point('feedback_new_timeline')");
$upstream->safe_psql('postgres', 'SELECT pg_switch_wal()');
$upstream->poll_query_until('postgres',
	"SELECT state = 'feedback' AND pid <> $old_sender FROM pg_stat_replication"
) or die 'feedback did not reconnect to the promoted upstream';
pass('ancestor-timeline feedback reconnects after upstream promotion');

my ($result, $stdout, $stderr) = $upstream->psql(
	'postgres',
	"START_FEEDBACK $new_target TIMELINE 1",
	replication => 'true');
isnt($result, 0, 'feedback beyond the ancestor fork point is rejected');
like(
	$stderr,
	qr/invalid standby feedback position/,
	'timeline validation rejects an LSN from the wrong history interval');

my $target_segment =
  $upstream->safe_psql('postgres', "SELECT pg_walfile_name('$new_target')");
$upstream->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$target_segment' FROM pg_stat_archiver")
  or die 'new-timeline target segment was not archived';

$cascade->adjust_conf('postgresql.conf', 'recovery_min_apply_delay', '0');
$cascade->reload;
$cascade->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'cascade did not reach the new-timeline restore point';
$cascade->poll_query_until('postgres',
	"SELECT status = 'feedback' AND receive_start_tli = 2 FROM pg_stat_wal_receiver"
) or die 'feedback did not renegotiate the new timeline';
$upstream->poll_query_until('postgres',
	"SELECT restart_lsn >= '$new_target' FROM pg_replication_slots WHERE slot_name = 'cascade_feedback'"
) or die 'new-timeline feedback did not advance the physical slot';
pass('archive timeline switch establishes a new feedback session');
$cascade->promote;
is( $cascade->safe_psql(
		'postgres', 'SELECT count(*) FROM pg_stat_wal_receiver'),
	0,
	'promotion stops a feedback-only receiver');
$cascade->stop;
$upstream->stop;

# Invalidated slots must not be revived by the new command.
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('logical_rejected', 'test_decoding')"
);
my $valid_position = $primary->lsn('flush');
($result, $stdout, $stderr) = $primary->psql(
	'postgres',
	"START_FEEDBACK SLOT logical_rejected $valid_position TIMELINE 1",
	replication => 'true');
isnt($result, 0, 'feedback rejects logical slots');
like(
	$stderr,
	qr/cannot use a logical replication slot/,
	'logical slot rejection reports its type');
$primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('logical_rejected')");
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('invalid_feedback', true)");
$primary->append_conf('postgresql.conf', "max_slot_wal_keep_size = 0");
$primary->reload;
$primary->advance_wal(32);
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->poll_query_until('postgres',
	"SELECT invalidation_reason = 'wal_removed' FROM pg_replication_slots WHERE slot_name = 'invalid_feedback'"
) or die 'test slot was not invalidated';
$valid_position = $primary->lsn('flush');
($result, $stdout, $stderr) = $primary->psql(
	'postgres',
	"START_FEEDBACK SLOT invalid_feedback $valid_position TIMELINE 1",
	replication => 'true');
isnt($result, 0, 'feedback rejects invalidated physical slots');
like($stderr, qr/invalidated/,
	'invalidated slot remains invalid after feedback attempt');
$primary->stop;
done_testing();

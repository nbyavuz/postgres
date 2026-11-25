# Copyright (c) 2026, PostgreSQL Global Development Group

# An early streaming attempt must not strand a standby with usable archived
# WAL, even if its upstream accepts connections but is behind it.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(
	allows_streaming => 1,
	has_archiving => 1,
	extra => ['--wal-segsize=1']);
$primary->start;
$primary->safe_psql('postgres', 'CREATE TABLE t (i integer)');
$primary->backup('base');

my $upstream = PostgreSQL::Test::Cluster->new('upstream');
$upstream->init_from_backup($primary, 'base', has_streaming => 1);
$upstream->start;
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (1)');
$primary->wait_for_catchup('upstream', 'replay');
$upstream->append_conf('postgresql.conf', "primary_conninfo = ''");
$upstream->reload;
$upstream->poll_query_until('postgres',
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver')
  or die 'intermediate standby did not stop receiving WAL';

# The first archive segment includes the delayed commit. The next segment is
# within one segment of the intermediate standby's WAL end: the downstream
# receiver would wait indefinitely for it if the early probe had no deadline.
# This one record spans a segment boundary. Recovery may have read its first
# half from the archive when it tries streaming; it must finish the record
# from the archive if the intermediate standby cannot supply the rest.
$primary->safe_psql('postgres',
	"SELECT pg_logical_emit_message(false, 'cross_segment', repeat('x', 1048576))"
);
$primary->safe_psql('postgres',
	'INSERT INTO t VALUES (2); SELECT pg_switch_wal()');
my $marker = $primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('retry_target')");
my $seg =
  $primary->safe_psql('postgres', "SELECT pg_walfile_name('$marker')");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$primary->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$seg' FROM pg_stat_archiver")
  or die 'target segment was not archived';

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'base', has_restoring => 1);
my $conninfo = $upstream->connstr;
$standby->append_conf(
	'postgresql.conf', qq(
primary_conninfo = '$conninfo'
recovery_min_apply_delay = '1d'
recovery_target_name = 'retry_target'
recovery_prefetch = off
wal_receiver_timeout = 0
wal_retrieve_retry_interval = '500ms'
log_min_messages = debug1
));
$standby->start;
$standby->wait_for_event('startup', 'RecoveryApplyDelay');
is( $standby->safe_psql(
		'postgres', 'SHOW streaming_replication_retry_interval'),
	'-1',
	'early streaming retries are disabled by default');
is( $standby->safe_psql(
		'postgres', 'SELECT count(*) FROM pg_stat_wal_receiver'),
	'0',
	'no feedback connection is required');
$standby->append_conf('postgresql.conf',
	"streaming_replication_retry_interval = '1ms'\nrecovery_min_apply_delay = 0"
);
$standby->reload;
$standby->wait_for_log(
	qr/WAL streaming retry did not supply the requested WAL/);
$standby->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'standby did not resume archive replay';
is($standby->safe_psql('postgres', 'SELECT max(i) FROM t'),
	'2', 'replayed a commit absent from the streaming upstream');
is($standby->lsn('replay'), $marker,
	'fallback respected the recovery target');

done_testing();

# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise START_FEEDBACK independently of the standby's WAL receiver.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all =>
  'raw replication connection requires local socket authentication'
  unless $PostgreSQL::Test::Utils::use_unix_sockets;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');
$primary->append_conf(
	'postgresql.conf', qq(
synchronous_standby_names = 'feedback'
synchronous_commit = local
synchronized_standby_slots = 'feedback_slot'
wal_sender_timeout = 0
));
$primary->start;
plan skip_all => 'this test requires working raw_connect()'
  unless $primary->raw_connect_works();

my $user = $primary->safe_psql('postgres', 'SELECT current_user');
my $sock;

sub send_message
{
	my ($type, $body) = @_;
	print {$sock} $type, pack('N', 4 + length($body)), $body or die $!;
}

sub read_bytes
{
	my ($len) = @_;
	my $buf = '';
	while (length($buf) < $len)
	{
		read($sock, $buf, $len - length($buf), length($buf))
		  or die 'unexpected EOF on replication connection';
	}
	return $buf;
}

sub receive_message
{
	my ($type, $len) = unpack('aN', read_bytes(5));
	my $body = read_bytes($len - 4);
	die "replication command failed: $body" if $type eq 'E';
	return ($type, $body);
}

sub connect_replication
{
	my ($node) = @_;
	$sock = $node->raw_connect();
	binmode $sock;
	send_message('',
		pack('N', 196608)
		  . "user\0$user\0replication\0true\0application_name\0feedback\0\0");
	1 while (receive_message())[0] ne 'Z';
}

sub pack_lsn
{
	my ($lsn) = @_;
	return pack('NN', map { hex } split(m{/}, $lsn));
}

# Wait for the requested keepalive before inspecting the effects of a report.
sub report_progress
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my $positions = join('', map { pack_lsn($_) } @_);
	send_message('d', 'r' . $positions . pack('NNC', 0, 1, 1));
	my ($type, $body) = receive_message();
	is($type . substr($body, 0, 1),
		'dk', 'received a keepalive instead of WAL');
	return substr($body, 1, 8);
}

sub report_xmins
{
	my ($xmin, $catalog_xmin, $epoch) = @_;
	send_message('d',
		'h' . pack('N6', 0, 1, $xmin, $epoch, $catalog_xmin, $epoch));
}

local $SIG{ALRM} = sub { die 'timed out waiting for replication protocol' };
alarm $PostgreSQL::Test::Utils::timeout_default;

my ($xmin, $epoch) = split /\|/, $primary->safe_psql(
	'postgres', q[
  SELECT xid & 4294967295, xid >> 32
  FROM (SELECT pg_current_xact_id()::text::bigint AS xid) s]);

# A slotless horizon belongs to the command, not the reusable connection.
connect_replication($primary);
send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0], 'W', 'started slotless feedback');
report_xmins(0, $xmin, $epoch);
my $sender = q{FROM pg_stat_replication WHERE application_name = 'feedback'};
$primary->poll_query_until('postgres',
	"SELECT backend_xmin = '$xmin' $sender")
  or die 'slotless catalog xmin did not set the sender horizon';
send_message('c', '');
is_deeply([ map { (receive_message())[0] } 1 .. 3 ],
	[qw(c C Z)], 'finished slotless feedback');
is($primary->safe_psql('postgres', "SELECT backend_xmin IS NULL $sender"),
	't', 'CopyDone cleared the horizon on the still-open connection');

# A primary status request is valid while streaming, but not in feedback mode.
send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0],
	'W', 'entered COPY before testing disallowed message');
my $log_offset = -s $primary->logfile;
send_message('d', 'p' . pack('NN', 0, 1));
is(read($sock, my $byte, 1), 0, 'disallowed message closes the connection');
$primary->wait_for_log(qr/unexpected message type "p"/, $log_offset);
$sock->close;

my $initial = $primary->safe_psql('postgres',
	"SELECT lsn FROM pg_create_physical_replication_slot('feedback_slot', true)"
);
$primary->safe_psql(
	'postgres', q[
  CREATE TABLE t (i int);
  SELECT pg_create_physical_replication_slot('unreserved');
  SELECT pg_create_logical_replication_slot('decode_slot', 'test_decoding', false, false, true);
]);

for my $case (
	[ 'unreserved TIMELINE 1', qr/has never previously reserved WAL/ ],
	[ 'decode_slot TIMELINE 1', qr/cannot use a logical replication slot/ ],
	[
		'feedback_slot TIMELINE 2',
		qr/timeline 2 is not in this server's history/
	])
{
	my ($spec, $expected) = @$case;
	my (undef, undef, $stderr) = $primary->psql(
		'postgres',
		"START_FEEDBACK SLOT $spec",
		replication => 'true');
	like($stderr, $expected, "rejected $spec");
}

connect_replication($primary);
send_message('Q', "START_FEEDBACK SLOT feedback_slot TIMELINE 1\0");
is((receive_message())[0], 'W', 'entered bidirectional COPY');

my $slot = q{FROM pg_replication_slots WHERE slot_name = 'feedback_slot'};
report_xmins($xmin, $xmin, $epoch);
$primary->poll_query_until('postgres',
	"SELECT xmin = '$xmin' AND catalog_xmin = '$xmin' $slot")
  or die 'named slot did not receive both horizons';
send_message('c', '');
is_deeply([ map { (receive_message())[0] } 1 .. 3 ],
	[qw(c C Z)], 'finished named feedback');
is( $primary->safe_psql(
		'postgres',
		"SELECT NOT active AND xmin = '$xmin' AND catalog_xmin = '$xmin' $slot"
	),
	't',
	'inactive named slot retains both horizons');
send_message('Q', "START_FEEDBACK SLOT feedback_slot TIMELINE 1\0");
is((receive_message())[0], 'W', 'reacquired named slot');
report_xmins(0, 0, 0);
ok( $primary->poll_query_until(
		'postgres',
		"SELECT active AND xmin IS NULL AND catalog_xmin IS NULL $slot"),
	'zero feedback cleared both named-slot horizons');

$primary->safe_psql('postgres', 'INSERT INTO t VALUES (0)');
my $earlier =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

# Generate WAL and wait until the commit needs a synchronous standby.
my $writer = $primary->background_psql('postgres');
$writer->query_safe('SET synchronous_commit = on');
my $pid = $writer->query_safe('SELECT pg_backend_pid()');
$writer->query_until(qr/committing/,
	"\\echo committing\nINSERT INTO t VALUES (1);\n");
my $wait = "SELECT wait_event FROM pg_stat_activity WHERE pid = $pid";
$primary->poll_query_until('postgres', $wait, 'SyncRep')
  or die 'commit did not enter synchronous replication wait';

# Neither a future position nor replay ahead of the standby's flush is safe.
my $lsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');
my $restart =
  "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'feedback_slot'";
report_progress(('FFFFFFFF/FFFFFFFF') x 3);
is($primary->safe_psql('postgres', $restart),
	$initial, 'ignored replay ahead of the primary');
report_progress($lsn, $earlier, $lsn);
is($primary->safe_psql('postgres', $restart),
	$initial, 'ignored replay ahead of standby flush');
report_progress($lsn, $lsn, $earlier);
is($primary->safe_psql('postgres', $restart),
	$earlier, 'advanced only to replay');
report_progress(($initial) x 3);
is($primary->safe_psql('postgres', $restart),
	$earlier, 'stale report did not rewind the slot');

# The physical slot gates logical decoding, separately from synchronous commit.
my $decoder = $primary->background_psql('postgres');
$decoder->query_until(qr/decoding/,
		"\\echo decoding\n"
	  . "SELECT 'decoded row' FROM pg_logical_slot_peek_changes('decode_slot', '$lsn', NULL) WHERE data LIKE '%i[integer]:0';\n"
);
$primary->wait_for_event('client backend', 'WaitForStandbyConfirmation');
cmp_ok(report_progress(($lsn) x 3),
	'ge', pack_lsn($lsn), 'keepalive covers the new WAL');
is($primary->safe_psql('postgres', $restart),
	$lsn, 'advanced to the replayed commit');
like(
	$decoder->query_until(qr/^decoded row\r?$/m, ''),
	qr/^decoded row\r?$/m,
	'feedback released logical decoding waiting on the physical slot');
$decoder->quit;
is($primary->safe_psql('postgres', $wait),
	'SyncRep', 'feedback did not release the commit');
is( $primary->safe_psql(
		'postgres', q[
  SELECT state, sync_state, sent_lsn IS NULL, reply_time IS NOT NULL
  FROM pg_stat_replication WHERE application_name = 'feedback']),
	'feedback|async|t|t',
	'feedback sender is visible but not streaming');

send_message('c', '');
is_deeply([ map { (receive_message())[0] } 1 .. 3 ],
	[qw(c C Z)], 'CopyDone ends feedback and returns to command mode');
is( $primary->safe_psql(
		'postgres',
		"SELECT active FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
	),
	'f',
	'CopyDone released the physical slot');
$primary->adjust_conf('postgresql.conf', 'synchronous_standby_names', "''");
$primary->reload;
$writer->quit;

send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0],
	'W', 'can start feedback again on the same connection');

# Keep a named session open across promotion: the old timeline is now valid
# only up to the fork, even though later WAL is durable on the new primary.
my $primary_sock = $sock;
$primary->backup('base');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
$standby->start;
my $before = $standby->safe_psql('postgres',
	"SELECT lsn FROM pg_create_physical_replication_slot('feedback_slot', true)"
);
connect_replication($standby);
send_message('Q', "START_FEEDBACK SLOT feedback_slot TIMELINE 1\0");
is((receive_message())[0], 'W', 'started feedback on cascading sender');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (2)');
my $received = $primary->lsn('flush');
$primary->wait_for_catchup($standby, 'flush', $received);
report_progress(($received) x 3);
is($standby->safe_psql('postgres', $restart),
	$before, 'cascading sender ignored WAL it received but has not replayed');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->wait_for_replay_catchup($standby);
my $named_sock = $sock;
connect_replication($standby);
send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0], 'W', 'started slotless feedback before promotion');
report_xmins($xmin, $xmin, $epoch);
my $horizon = "SELECT count(*) $sender AND backend_xmin = '$xmin'";
$standby->poll_query_until('postgres', $horizon, '1')
  or die 'slotless feedback did not set xmin before promotion';
$standby->promote;
$standby->wait_for_log(
	qr/cannot continue slotless feedback after a timeline switch/);
$standby->poll_query_until('postgres', $horizon, '0')
  or die 'slotless feedback kept xmin on the old timeline after promotion';
$sock->close;
$sock = $named_sock;
my (undef, undef, $timeline_error) = $standby->psql(
	'postgres',
	'START_FEEDBACK TIMELINE 1',
	replication => 'true');
like(
	$timeline_error,
	qr/requested timeline 1 is not this server's current timeline 2/,
	'slotless feedback cannot start on the old timeline');
my ($fork) = slurp_file($standby->data_dir . '/pg_wal/00000002.history') =~
  /^1\s+(\S+)/m;
my $after =
  $standby->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');
report_progress(($after) x 3);
is($standby->safe_psql('postgres', $restart),
	$before, 'ignored replay past the fork');
report_progress(($fork) x 3);
is($standby->safe_psql('postgres', "SELECT ($restart) = '$fork'::pg_lsn"),
	't', 'accepted replay exactly to the fork');
$standby->stop;
$sock->close;
$primary->stop;
$primary_sock->close;
alarm 0;
done_testing();

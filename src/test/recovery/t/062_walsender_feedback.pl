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
# Match the client's application_name so the test would notice a synchronous
# acknowledgment. Disable timer-driven keepalives so we see the requested one.
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

my $user = $primary->safe_psql('postgres', 'SELECT current_user');
my $sock;
my $timestamp = pack('NN', 0, 1);    # Arbitrary nonzero 64-bit timestamp.

# Messages have a one-byte type and a four-byte length in network byte order.
# The length includes the length field itself, but not the type byte.
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
	die "replication command failed: $body" if $type eq 'E';   # ErrorResponse
	return ($type, $body);
}

sub connect_replication
{
	my ($node) = @_;
	$sock = $node->raw_connect();
	binmode $sock;
	# The startup packet has no type byte. Send protocol version 3.0 followed
	# by null-terminated name/value pairs, then wait for ReadyForQuery ('Z').
	send_message('',
		pack('nn', 3, 0)
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
	# A standby status update ('r') travels inside CopyData ('d'). Its LSNs
	# are the write, flush, and apply positions, in that order.
	my $positions = join('', map { pack_lsn($_) } @_);
	my $request_reply = pack('C', 1);
	send_message('d', 'r' . $positions . $timestamp . $request_reply);

	# A keepalive's CopyData body starts with 'k', then the eight-byte WAL end.
	my ($type, $body) = receive_message();
	is($type . substr($body, 0, 1),
		'dk', 'received a keepalive instead of WAL');
	return substr($body, 1, 8);
}

# Send a Query ('Q'); CopyBothResponse ('W') means feedback mode has started.
connect_replication($primary);
send_message('Q', "START_FEEDBACK SLOT feedback_slot TIMELINE 1\0");
is((receive_message())[0], 'W', 'entered bidirectional COPY');
is( $primary->safe_psql(
		'postgres',
		"SELECT active FROM pg_replication_slots WHERE slot_name = 'feedback_slot'"
	),
	't',
	'feedback acquired the physical slot');

$primary->safe_psql('postgres', 'INSERT INTO t VALUES (0)');
my $earlier = $primary->lsn('flush');

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
my $lsn = $primary->lsn('flush');
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
# Both LSNs are in network byte order, so their byte strings sort numerically.
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

# Expect CopyDone ('c'), CommandComplete ('C'), then ReadyForQuery ('Z').
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

# A primary status request ('p' followed by a timestamp) is valid while
# streaming, but not in feedback mode.
my $log_offset = -s $primary->logfile;
send_message('d', 'p' . $timestamp);
$primary->wait_for_log(qr/unexpected message type "p"/, $log_offset);
# Keep the socket open to verify that the server itself ended the session.
ok( $primary->poll_query_until(
		'postgres', q[SELECT count(*) = 0 FROM pg_stat_activity
			WHERE backend_type = 'walsender' AND application_name = 'feedback']),
	'disallowed message closes the connection');
$sock->close;

# Keep a named session open across promotion: the old timeline is now valid
# only up to the fork, even though later WAL is durable on the new primary.
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
$standby->promote;
my ($fork) = slurp_file($standby->data_dir . '/pg_wal/00000002.history') =~
  /^1\s+(\S+)/m;
my $after = $standby->lsn('flush');
report_progress(($after) x 3);
is($standby->safe_psql('postgres', $restart),
	$before, 'ignored replay past the fork');
report_progress(($fork) x 3);
is($standby->safe_psql('postgres', "SELECT ($restart) = '$fork'::pg_lsn"),
	't', 'accepted replay exactly to the fork');
$standby->stop;
$sock->close;
$primary->stop;
done_testing();

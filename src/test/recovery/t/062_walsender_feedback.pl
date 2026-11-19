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
$primary->init(allows_streaming => 1);
# Match the client's application_name so the test would notice a synchronous
# acknowledgment. Disable timer-driven keepalives so we see the requested one.
$primary->append_conf(
	'postgresql.conf', qq(
synchronous_standby_names = 'feedback'
wal_sender_timeout = 0
));
$primary->start;
plan skip_all => 'this test requires working raw_connect()'
  unless $primary->raw_connect_works();

my $user = $primary->safe_psql('postgres', 'SELECT current_user');
my $sock = $primary->raw_connect();
binmode $sock;

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

# The startup packet has no type byte. Send protocol version 3.0 followed by
# null-terminated name/value pairs, then wait for ReadyForQuery ('Z').
send_message('',
	pack('nn', 3, 0)
	  . "user\0$user\0replication\0true\0application_name\0feedback\0\0");
1 while (receive_message())[0] ne 'Z';

# Send a Query ('Q'); CopyBothResponse ('W') means feedback mode has started.
send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0], 'W', 'entered bidirectional COPY');

# Generate WAL and wait until the commit needs a synchronous standby.
my $writer = $primary->background_psql('postgres');
$writer->query_safe('SET synchronous_commit = on');
my $pid = $writer->query_safe('SELECT pg_backend_pid()');
$writer->query_until(qr/committing/,
	"\\echo committing\nCREATE TABLE t (i int);\n");
my $wait = "SELECT wait_event FROM pg_stat_activity WHERE pid = $pid";
$primary->poll_query_until('postgres', $wait, 'SyncRep')
  or die 'commit did not enter synchronous replication wait';

# A standby status update ('r') travels inside CopyData ('d'). Report the
# commit's flush LSN as the write, flush, and apply positions, in that order.
my $lsn = $primary->lsn('flush');
my $pos = pack('NN', map { hex } split(m{/}, $lsn));
my $positions = $pos x 3;
my $timestamp = pack('NN', 0, 1);    # Arbitrary nonzero 64-bit timestamp.
my $request_reply = pack('C', 1);
send_message('d', 'r' . $positions . $timestamp . $request_reply);

# A keepalive's CopyData body starts with 'k', then the eight-byte WAL end.
# Both LSNs are in network byte order, so their byte strings sort numerically.
my ($type, $body) = receive_message();
is($type . substr($body, 0, 1), 'dk', 'received a keepalive instead of WAL');
cmp_ok(substr($body, 1, 8), 'ge', $pos, 'keepalive covers the new WAL');
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
$primary->stop;
done_testing();

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
$primary->append_conf(
	'postgresql.conf', qq(
synchronous_standby_names = 'feedback'
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
	$sock = $primary->raw_connect();
	binmode $sock;
	send_message('',
		pack('N', 196608)
		  . "user\0$user\0replication\0true\0application_name\0feedback\0\0");
	1 while (receive_message())[0] ne 'Z';
}

local $SIG{ALRM} = sub { die 'timed out waiting for replication protocol' };
alarm $PostgreSQL::Test::Utils::timeout_default;

# A primary status request is valid while streaming, but not in feedback mode.
connect_replication();
send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0],
	'W', 'entered COPY before testing disallowed message');
my $log_offset = -s $primary->logfile;
send_message('d', 'p' . pack('NN', 0, 1));
is(read($sock, my $byte, 1), 0, 'disallowed message closes the connection');
$primary->wait_for_log(qr/unexpected message type "p"/, $log_offset);
$sock->close;

connect_replication();
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

# Report the flushed commit LSN in all three positions and request a reply.
my $lsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');
my $pos = pack('NN', map { hex } split(m{/}, $lsn));
send_message('d', 'r' . ($pos x 3) . pack('NNC', 0, 1, 1));
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

send_message('c', '');
is_deeply([ map { (receive_message())[0] } 1 .. 3 ],
	[qw(c C Z)], 'CopyDone ends feedback and returns to command mode');
$primary->adjust_conf('postgresql.conf', 'synchronous_standby_names', "''");
$primary->reload;
$writer->quit;

send_message('Q', "START_FEEDBACK\0");
is((receive_message())[0],
	'W', 'can start feedback again on the same connection');
$primary->stop;
$sock->close;
alarm 0;
done_testing();

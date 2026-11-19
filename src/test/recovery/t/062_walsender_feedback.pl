# Copyright (c) 2026, PostgreSQL Global Development Group

# A feedback-only walsender can report the end of WAL in a keepalive, but
# must not send WAL or release a commit waiting for a synchronous standby.
# Act as the standby so we can choose which messages the walsender receives.

use strict;
use warnings FATAL => 'all';

use IO::Select;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time);

# The wire client below relies on local authentication without a password.
plan skip_all =>
  'raw replication connection requires local socket authentication'
  unless $PostgreSQL::Test::Utils::use_unix_sockets;

# Enable replication connections on the primary; there is no streaming standby.
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);

# Use the same name for synchronous_standby_names and the feedback client:
# a streaming standby with that name could release the writer.  Disable
# timed keepalives (0) so we only see the one requested by the client.
my $standby_name = 'feedback_standby';
$primary->append_conf(
	'postgresql.conf', qq(
synchronous_standby_names = '$standby_name'
wal_sender_timeout = 0
));
$primary->start;
plan skip_all => 'this test requires working raw_connect()'
  unless $primary->raw_connect_works();

# Use a separate physical replication connection for START_FEEDBACK.
my $socket = $primary->raw_connect();
binmode $socket;
send_startup_packet();
# A Query ('Q') contains a NUL-terminated command.  CopyBothResponse ('W')
# means the server accepted it and entered bidirectional COPY.
my $start_feedback = "START_FEEDBACK\0";
send_message('Q', $start_feedback);
expect_message('W', 'START_FEEDBACK entered bidirectional COPY');

# CREATE TABLE generates WAL.  Wait for psql's marker rather than for the
# command to finish: its commit must block until a synchronous standby replies.
my $writer = $primary->background_psql('postgres');
$writer->query_safe('SET synchronous_commit = on');
my $writer_pid = $writer->query_safe('SELECT pg_backend_pid()');
my $writer_wait =
  "SELECT wait_event FROM pg_stat_activity WHERE pid = $writer_pid";
$writer->query_until(qr/starting write/,
	"\\echo starting write\nCREATE TABLE feedback_test (i integer);\n");
# SyncRep confirms this writer reached the synchronous replication wait.
$primary->poll_query_until('postgres', $writer_wait, 'SyncRep')
  or die 'commit did not start waiting for a synchronous standby';

# Report a position that covers the waiting commit.  Use a fixed, nonzero
# send time: one microsecond after the PostgreSQL epoch, in two 32-bit words.
# A zero timestamp would show up as NULL in pg_stat_replication.reply_time.
my $report_time = pack('NN', 0, 1);
my $reported_lsn = $primary->lsn('flush');
my $wal_end = report_progress($reported_lsn, $report_time);
# Compare the positions as pg_lsn values; psql prints true as 't'.
is( $primary->safe_psql(
		'postgres', "SELECT '$wal_end'::pg_lsn >= '$reported_lsn'::pg_lsn"),
	't',
	'keepalive covers the waiting commit');
is($primary->safe_psql('postgres', $writer_wait),
	'SyncRep', 'feedback did not release the synchronous commit');

# The primary records the reply time.  Even though this standby is named in
# synchronous_standby_names, its sender is asynchronous and has sent no WAL.
is( $primary->safe_psql(
		'postgres', qq[
	SELECT state = 'feedback' AND sync_state = 'async'
	       AND sent_lsn IS NULL AND reply_time IS NOT NULL
	FROM pg_stat_replication WHERE application_name = '$standby_name']),
	't',
	'primary shows an asynchronous feedback sender that has sent no WAL');

# Release the writer by clearing the list of synchronous standbys.
$primary->adjust_conf('postgresql.conf', 'synchronous_standby_names', "''");
$primary->reload;
$writer->quit;

# CopyDone ('c') has no body.  Before sending another query, expect CopyDone,
# CommandComplete ('C') and ReadyForQuery ('Z'), in that order.
send_message('c', '');
expect_message('c', 'server replied with CopyDone');
expect_message('C', 'server sent CommandComplete for START_FEEDBACK');
expect_message('Z', 'server sent ReadyForQuery');
send_message('Q', $start_feedback);
expect_message('W', 'same connection accepted a second START_FEEDBACK');

# 'p' (PqReplMsg_PrimaryStatusRequest) asks the primary to send its status.
# It is valid during streaming but not during feedback.  Keep the socket open
# so only the server can end the session.
my $log_offset = -s $primary->logfile;
send_message('d', 'p' . $report_time);    # 'd' is CopyData.
$primary->wait_for_log(qr/unexpected message type "p"/, $log_offset);
ok( $primary->poll_query_until(
		'postgres', qq[
	SELECT NOT EXISTS (SELECT * FROM pg_stat_activity
	                   WHERE application_name = '$standby_name')]),
	'walsender closed the connection after a disallowed message');

$socket->close;
$primary->stop;
done_testing();

# The startup packet has no type byte.  Its body starts with the protocol
# version (3.0, two network-order 16-bit integers), followed by NUL-terminated
# parameter names and values.  An empty name ends the list.
sub send_startup_packet
{
	my $user = $primary->safe_psql('postgres', 'SELECT current_user');
	my $version = pack('nn', 3, 0);
	# replication=true selects physical replication; application_name must
	# match the synchronous standby name configured above.
	my $parameters = join("\0",
		'user', $user, 'replication', 'true',
		'application_name', $standby_name, '', '');
	send_message('', $version . $parameters);
	# Allow one timeout for the whole startup exchange, including partial reads.
	my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
	while (1)
	{
		my ($type) = receive_message($deadline);
		last if $type eq 'Z';    # ReadyForQuery ends the startup exchange.
	}
}

# Send one standby status update, then read the requested keepalive.  Once
# it arrives we know that the walsender has processed this particular report.
sub report_progress
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($lsn, $send_time) = @_;
	# An LSN is printed as high/low hexadecimal halves.  Send each half as
	# a network-order 32-bit integer; this also works on 32-bit Perl.
	my ($high, $low) = split(m{/}, $lsn);
	my $position = pack('NN', hex($high), hex($low));
	# 'r' identifies a standby status update inside CopyData ('d').  The
	# LSNs are write, flush, replay, in order.  A final byte of 1 asks for
	# an immediate reply.
	my $request_reply = pack('C', 1);
	my $positions = $position x 3;
	send_message('d', 'r' . $positions . $send_time . $request_reply);

	my $body =
	  expect_message('d', 'received CopyData after the status update');
	# The first byte names the replication message: 'k' for keepalive,
	# 'w' for WAL data.  The next two network-order 32-bit integers form
	# the WAL end.  Format them as a PostgreSQL LSN for comparison in SQL.
	my ($kind, $wal_end_high, $wal_end_low) = unpack('aNN', $body);
	is($kind, 'k', 'walsender replied with a keepalive instead of WAL');
	return sprintf('%X/%X', $wal_end_high, $wal_end_low);
}

# Each message has a four-byte length ('N' for network byte order).  The
# length includes the length field itself but not the one-byte message type.
sub send_message
{
	my ($type, $body) = @_;
	print {$socket} $type, pack('N', 4 + length($body)), $body or die $!;
}

sub receive_message
{
	my ($deadline) = @_;
	# The five-byte header is one type byte followed by the four-byte length.
	my ($type, $length) = unpack('aN', read_bytes(5, $deadline));
	my $body = read_bytes($length - 4, $deadline);
	die "walsender returned an error: $body" if $type eq 'E';  # ErrorResponse
	return ($type, $body);
}

# Return the payload for checks that need more than the message type.
sub expect_message
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($expected, $description) = @_;
	my ($type, $body) = receive_message();
	is($type, $expected, $description) or die 'unexpected walsender response';
	return $body;
}

# A socket read may return only part of the requested header or body.
sub read_bytes
{
	my ($length, $deadline) = @_;
	my $bytes = '';
	while (length($bytes) < $length)
	{
		if (defined $deadline)
		{
			my $remaining = $deadline - time();
			die "timed out waiting for ReadyForQuery at startup\n"
			  if $remaining <= 0;
			next unless IO::Select->new($socket)->can_read($remaining);
		}
		# Don't buffer input in Perl: can_read() checks the socket itself.
		sysread($socket, $bytes, $length - length($bytes), length($bytes))
		  or die 'unexpected EOF on replication connection';
	}
	return $bytes;
}

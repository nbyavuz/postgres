# Copyright (c) 2026, PostgreSQL Global Development Group

# A feedback-only walsender can advance a physical slot to a valid replay
# position, but must not send WAL or release a synchronous commit.
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

# Enable logical decoding to check how it uses the physical slot's progress.
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');

# Use the same name for synchronous_standby_names and the feedback client:
# a streaming standby with that name could release the writer.  Disable
# timed keepalives (0) so we only see the one requested by the client.  Use
# local commits for setup; only the writer below should wait for a standby.
# Logical decoding on the failover slot must wait for feedback_slot.
my $standby_name = 'feedback_standby';
$primary->append_conf(
	'postgresql.conf', qq(
synchronous_standby_names = '$standby_name'
synchronous_commit = local
synchronized_standby_slots = 'feedback_slot'
wal_sender_timeout = 0
));
$primary->start;
plan skip_all => 'this test requires working raw_connect()'
  unless $primary->raw_connect_works();

# Of the two physical slots, only feedback_slot reserves WAL at creation.
# Use test_decoding so we can recognize the inserted row in the decoded output.
my $create_feedback_slot = q[
	SELECT lsn FROM pg_create_physical_replication_slot('feedback_slot', immediately_reserve => true)];
my $initial_lsn = $primary->safe_psql('postgres', $create_feedback_slot);
$primary->safe_psql(
	'postgres', q[
	CREATE TABLE feedback_test (i integer);
	SELECT pg_create_physical_replication_slot('unreserved');
	SELECT pg_create_logical_replication_slot('decode_slot', 'test_decoding', failover => true);
]);

# Feedback requires both a slot and a timeline.  This primary is on timeline
# 1 and has never been promoted, so timeline 2 is not in its history.
for my $case (
	[ 'START_FEEDBACK', qr/syntax error/ ],
	[ 'START_FEEDBACK SLOT feedback_slot', qr/syntax error/ ],
	[
		'START_FEEDBACK SLOT unreserved TIMELINE 1',
		qr/has never previously reserved WAL/
	],
	[
		'START_FEEDBACK SLOT decode_slot TIMELINE 1',
		qr/cannot use a logical replication slot/
	],
	[
		'START_FEEDBACK SLOT feedback_slot TIMELINE 2',
		qr/timeline 2 is not in this server's history/
	])
{
	my ($command, $expected) = @$case;
	my (undef, undef, $stderr) =
	  $primary->psql('postgres', $command, replication => 'true');
	like($stderr, $expected, "rejected $command");
}

# Use a separate physical replication connection for START_FEEDBACK.
my $socket;
connect_replication($primary);
# A Query ('Q') contains a NUL-terminated command.  CopyBothResponse ('W')
# means the server accepted it and entered bidirectional COPY.
my $start_feedback = "START_FEEDBACK SLOT feedback_slot TIMELINE 1\0";
send_message('Q', $start_feedback);
expect_message('W', 'START_FEEDBACK entered bidirectional COPY');
my $slot_active =
  "SELECT active FROM pg_replication_slots WHERE slot_name = 'feedback_slot'";
is($primary->safe_psql('postgres', $slot_active),
	't', 'feedback acquired the physical slot');

# Insert row 0 with a local commit.  The decoder will look for this row
# while the next insert (row 1) is waiting for a synchronous standby.
$primary->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (0)');
my $earlier_lsn = $primary->lsn('flush');

# Wait for psql's marker rather than for the second insert to finish.
my $writer = $primary->background_psql('postgres');
$writer->query_safe('SET synchronous_commit = on');
my $writer_pid = $writer->query_safe('SELECT pg_backend_pid()');
my $writer_wait =
  "SELECT wait_event FROM pg_stat_activity WHERE pid = $writer_pid";
$writer->query_until(qr/starting write/,
	"\\echo starting write\nINSERT INTO feedback_test VALUES (1);\n");
# SyncRep confirms this writer reached the synchronous replication wait.
$primary->poll_query_until('postgres', $writer_wait, 'SyncRep')
  or die 'commit did not start waiting for a synchronous standby';

# Use a fixed, nonzero send time: one microsecond after the PostgreSQL
# epoch, in two 32-bit words.
# A zero timestamp would show up as NULL in pg_stat_replication.reply_time.
my $report_time = pack('NN', 0, 1);
my $reported_lsn = $primary->lsn('flush');
my $slot_restart =
  "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'feedback_slot'";

# The largest possible LSN is beyond everything flushed on this primary.
my $future_lsn = 'FFFFFFFF/FFFFFFFF';
report_progress($future_lsn, $future_lsn, $future_lsn);
is($primary->safe_psql('postgres', $slot_restart),
	$initial_lsn, 'ignored replay ahead of the primary');
report_progress($reported_lsn, $earlier_lsn, $reported_lsn);
is($primary->safe_psql('postgres', $slot_restart),
	$initial_lsn, 'ignored replay ahead of standby flush');
report_progress($reported_lsn, $reported_lsn, $earlier_lsn);
is($primary->safe_psql('postgres', $slot_restart),
	$earlier_lsn, 'advanced only to replay');
report_progress($initial_lsn, $initial_lsn, $initial_lsn);
is($primary->safe_psql('postgres', $slot_restart),
	$earlier_lsn, 'stale report did not rewind the slot');

# Wait for the decoder to block on the physical slot.  Reporting replay
# through the waiting commit should release the decoder, but not the writer.
my $decoder = $primary->background_psql('postgres');
$decoder->query_until(
	qr/starting decoding/, qq[\\echo starting decoding
	SELECT 'decoded row' FROM pg_logical_slot_peek_changes('decode_slot', '$reported_lsn', NULL)
	WHERE data LIKE '%i[integer]:0';
]);
$primary->wait_for_event('client backend', 'WaitForStandbyConfirmation');
my $wal_end = report_progress($reported_lsn, $reported_lsn, $reported_lsn);
# Compare the positions as pg_lsn values; psql prints true as 't'.
is( $primary->safe_psql(
		'postgres', "SELECT '$wal_end'::pg_lsn >= '$reported_lsn'::pg_lsn"),
	't',
	'keepalive covers the waiting commit');
is($primary->safe_psql('postgres', $slot_restart),
	$reported_lsn, 'advanced to the replayed commit');
like(
	$decoder->query_until(qr/^decoded row\r?$/m, ''),
	qr/^decoded row\r?$/m,
	'feedback released logical decoding waiting on the physical slot');
$decoder->quit;
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
is($primary->safe_psql('postgres', $slot_active),
	'f', 'CopyDone released the physical slot');
send_message('Q', $start_feedback);
expect_message('W', 'same connection accepted a second START_FEEDBACK');
is($primary->safe_psql('postgres', $slot_active),
	't', 'second START_FEEDBACK reacquired the same slot');

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

# On a cascading walsender the upper bound is local replay, even when its
# walreceiver has already flushed newer WAL.  Keep this session open across
# promotion so that the sender must recheck the bounds of the old timeline.
$primary->backup('base');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
$standby->start;
my $before_promotion = $standby->safe_psql('postgres', $create_feedback_slot);
connect_replication($standby);
send_message('Q', $start_feedback);
expect_message('W', 'started feedback on cascading sender');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
# Row 2 generates WAL that can reach the paused standby but cannot be replayed.
$primary->safe_psql('postgres', 'INSERT INTO feedback_test VALUES (2)');
my $received = $primary->lsn('flush');
$primary->wait_for_catchup($standby, 'flush', $received);
report_progress($received, $received, $received);
is($standby->safe_psql('postgres', $slot_restart),
	$before_promotion,
	'cascading sender ignored WAL it received but has not replayed');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->wait_for_replay_catchup($standby);
$standby->promote;

# The history file for timeline 2 gives the end of timeline 1 in the second
# column.  The last record on timeline 1 may end exactly at that position.
my ($fork) = slurp_file($standby->data_dir . '/pg_wal/00000002.history') =~
  /^1\s+(\S+)/m;
my $after_promotion = $standby->lsn('flush');
report_progress($after_promotion, $after_promotion, $after_promotion);
is($standby->safe_psql('postgres', $slot_restart),
	$before_promotion, 'ignored replay past the fork');
report_progress($fork, $fork, $fork);
is( $standby->safe_psql(
		'postgres', "SELECT ($slot_restart) = '$fork'::pg_lsn"),
	't',
	'accepted replay exactly to the fork');
$standby->stop;
$socket->close;
$primary->stop;
done_testing();

# The startup packet has no type byte.  Its body starts with the protocol
# version (3.0, two network-order 16-bit integers), followed by NUL-terminated
# parameter names and values.  An empty name ends the list.
sub connect_replication
{
	my ($node) = @_;
	my $user = $node->safe_psql('postgres', 'SELECT current_user');
	$socket = $node->raw_connect();
	binmode $socket;
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
	my ($write, $flush, $replay) = @_;
	# Write, flush and replay are each printed as high/low hexadecimal halves.
	# Send each half as a network-order integer; this also works on 32-bit Perl.
	my $positions = '';
	foreach my $lsn ($write, $flush, $replay)
	{
		my ($high, $low) = split(m{/}, $lsn);
		$positions .= pack('NN', hex($high), hex($low));
	}
	# 'r' identifies a standby status update inside CopyData ('d').  The
	# LSNs are write, flush, replay, in order.  A final byte of 1 asks for
	# an immediate reply.
	my $request_reply = pack('C', 1);
	send_message('d', 'r' . $positions . $report_time . $request_reply);

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

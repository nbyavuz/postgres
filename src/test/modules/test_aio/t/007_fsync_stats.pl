# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use FindBin;
use lib $FindBin::RealBin;
use TestAio;

plan skip_all => 'injection points not supported'
  unless $ENV{enable_injection_points} eq 'yes';

my $stats = q(SELECT fsyncs, fsync_time FROM pg_stat_io
 WHERE backend_type = 'checkpointer' AND object = 'relation' AND context = 'normal');

foreach my $method (TestAio::supported_io_methods())
{
	my $node = PostgreSQL::Test::Cluster->new("stats_$method");
	$node->init;
	TestAio::configure($node);
	$node->append_conf(
		'postgresql.conf', qq(
io_method = $method
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
));
	$node->start;
	$node->safe_psql(
		'postgres', q(
CREATE EXTENSION test_aio;
CREATE TABLE target(i int);
INSERT INTO target VALUES (1);
CHECKPOINT;
));
	# Freeze the newly installed function tuples so first calls to helpers
	# and statistics functions cannot add catalog hint-bit fsync requests.
	$node->safe_psql('postgres', 'VACUUM FREEZE pg_proc');

	foreach my $timing ('off', 'on')
	{
		$node->safe_psql('postgres',
			"ALTER SYSTEM SET track_io_timing = $timing");
		$node->restart;
		# Drain recovery/startup and catalog activity before measuring the
		# explicit requests below.  Reading statistics does not dirty target.
		$node->safe_psql(
			'postgres', q(
SELECT fsync_test_configure('target');
CHECKPOINT;
));
		$node->wait_for_event('checkpointer', 'CheckpointerMain');

		foreach my $handler (0, 1)
		{
			my ($before_count, $before_time) = split /\|/,
			  $node->safe_psql('postgres', $stats);
			$node->safe_psql(
				'postgres', qq(
SELECT fsync_test_configure('target', handler => $handler, wait => true);
SELECT fsync_test_request();
));
			my $checkpoint = $node->background_psql('postgres');
			$checkpoint->query_until(
				qr/sent/, q(
\echo sent
CHECKPOINT;
));
			ok( $node->poll_query_until(
					'postgres', q(
SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event = 'fsync_completion')
)),
				"$method: timing=$timing handler=$handler completion held");
			# In worker mode, ensure that sync.c actually enters its wait path.
			$node->wait_for_event('checkpointer', 'AioIoCompletion')
			  if $method eq 'worker';
			$node->safe_psql('postgres', 'SELECT fsync_test_release()');
			$checkpoint->query_safe('SELECT 1');
			$checkpoint->quit;
			$node->wait_for_event('checkpointer', 'CheckpointerMain');
			my ($after_count, $after_time) = split /\|/,
			  $node->safe_psql('postgres', $stats);
			is( $after_count - $before_count,
				$handler == 0 ? 1 : 0,
				"$method: timing=$timing handler=$handler does not double count"
			);

			if ($timing eq 'on' && $handler == 0)
			{
				cmp_ok($after_time, '>', $before_time,
					"$method: relation fsync contributes timing");
			}
			else
			{
				is($after_time, $before_time,
					"$method: timing=$timing handler=$handler adds no relation timing"
				);
			}
		}
	}
	$node->stop;
}

done_testing();

/* -------------------------------------------------------------------------
 *
 * pgstat_database.c
 *	  Implementation of database statistics.
 *
 * This file contains the implementation of function database. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_database.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"
#include "storage/procsignal.h"


static bool pgstat_should_report_connstat(void);


int	pgStatXactCommit = 0;
int	pgStatXactRollback = 0;
PgStat_Counter pgStatBlockReadTime = 0;
PgStat_Counter pgStatBlockWriteTime = 0;
PgStat_Counter pgStatActiveTime = 0;
PgStat_Counter pgStatTransactionIdleTime = 0;
SessionEndType pgStatSessionEndCause = DISCONNECT_NORMAL;


static PgStat_Counter pgLastSessionReportTime = 0;


/* ----------
 * pgstat_drop_database() -
 *
 * Remove entry for the database being dropped.
 *
 * Some entries might be left alone due to lock failure or some stats are
 * flushed after this but we will still clean the dead DB eventually via
 * future invocations of pgstat_vacuum_stat().
 *	----------
 */
void
pgstat_drop_database(Oid dboid)
{
	/*
	 * FIXME: need to do this using the transactional mechanism. Not so much
	 * because of rollbacks, but so the stats are removed on a standby
	 * too. Maybe a dedicated drop type?
	 */
	pgstat_drop_database_and_contents(dboid);
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 * Report a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_StatDBEntry *dbentry;

	Assert(IsUnderPostmaster);
	if (!pgstat_track_counts)
		return;

	dbentry = pgstat_pending_db_prepare(MyDatabaseId);

	switch (reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Since we drop the information about the database as soon as it
			 * replicates, there is no point in counting these conflicts.
			 */
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			dbentry->n_conflict_tablespace++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			dbentry->n_conflict_lock++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			dbentry->n_conflict_snapshot++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			dbentry->n_conflict_bufferpin++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			dbentry->n_conflict_startup_deadlock++;
			break;
	}
}

/* --------
 * pgstat_report_deadlock() -
 *
 * Report a detected deadlock.
 * --------
 */
void
pgstat_report_deadlock(void)
{
	PgStat_StatDBEntry *dbent;

	if (!pgstat_track_counts)
		return;

	dbent = pgstat_pending_db_prepare(MyDatabaseId);
	dbent->n_deadlocks++;
}

/* --------
 * pgstat_report_checksum_failures_in_db() -
 *
 * Report one or more checksum failures.
 * --------
 */
void
pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount)
{
	PgStatSharedRef *shared_ref;
	PgStatShm_StatDBEntry *sharedent;

	if (!pgstat_track_counts)
		return;

	/*
	 * Update the shared stats directly - checksum failures should never be
	 * common enough for that to be a problem.
	 */
	shared_ref =
		pgstat_shared_stat_locked(PGSTAT_KIND_DB, dboid, InvalidOid, false);

	sharedent = (PgStatShm_StatDBEntry *) shared_ref->shared_stats;
	sharedent->stats.n_checksum_failures += failurecount;

	pgstat_shared_stat_unlock(shared_ref);
}

/* --------
 * pgstat_report_checksum_failure() -
 *
 * Reports one checksum failure in the current database.
 * --------
 */
void
pgstat_report_checksum_failure(void)
{
	pgstat_report_checksum_failures_in_db(MyDatabaseId, 1);
}

/* --------
 * pgstat_report_tempfile() -
 *
 * Report a temporary file.
 * --------
 */
void
pgstat_report_tempfile(size_t filesize)
{
	PgStat_StatDBEntry *dbent;

	if (!pgstat_track_counts)
		return;

	if (filesize == 0)			/* Is there a case where filesize is really 0? */
		return;

	dbent = pgstat_pending_db_prepare(MyDatabaseId);
	dbent->n_temp_bytes += filesize; /* needs check overflow */
	dbent->n_temp_files++;
}

/* --------
 * pgstat_report_connect() -
 *
 *	Tell the collector about a new connection.
 * --------
 */
void
pgstat_report_connect(Oid dboid)
{
	PgStat_StatDBEntry *dbentry;

	if (!pgstat_should_report_connstat())
		return;

	dbentry = pgstat_pending_db_prepare(MyDatabaseId);
	dbentry->n_sessions++;
}

/* --------
 * pgstat_report_disconnect() -
 *
 *	Tell the collector about a disconnect.
 * --------
 */
void
pgstat_report_disconnect(Oid dboid)
{
	PgStat_StatDBEntry *dbentry;

	if (!pgstat_should_report_connstat())
		return;

	dbentry = pgstat_pending_db_prepare(MyDatabaseId);

    switch (pgStatSessionEndCause)
    {
        case DISCONNECT_NOT_YET:
        case DISCONNECT_NORMAL:
            /* we don't collect these */
            break;
        case DISCONNECT_CLIENT_EOF:
            dbentry->n_sessions_abandoned++;
            break;
        case DISCONNECT_FATAL:
            dbentry->n_sessions_fatal++;
            break;
        case DISCONNECT_KILLED:
            dbentry->n_sessions_killed++;
            break;
    }
}

/* ----------
 * pgstat_update_connstat() -
 *
 * Update pending connection stats.
 * ----------
 */
void
pgstat_update_dbstats(TimestampTz now)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_pending_db_prepare(MyDatabaseId);

	/*
	 * Accumulate xact commit/rollback and I/O timings to stats entry of the
	 * current database.
	 */
	dbentry->n_xact_commit += pgStatXactCommit;
	dbentry->n_xact_rollback += pgStatXactRollback;
	dbentry->n_block_read_time += pgStatBlockReadTime;
	dbentry->n_block_write_time += pgStatBlockWriteTime;

	if (pgstat_should_report_connstat())
	{
		long		secs;
		int			usecs;

		/*
		 * pgLastSessionReportTime is initialized to MyStartTimestamp by
		 * pgstat_report_connect().
		 */
		TimestampDifference(pgLastSessionReportTime, now, &secs, &usecs);
		pgLastSessionReportTime = now;

		dbentry->total_session_time += (PgStat_Counter) secs * 1000000 + usecs;
		dbentry->total_active_time += pgStatActiveTime;
		dbentry->total_idle_in_xact_time += pgStatTransactionIdleTime;
	}

	pgStatXactCommit = 0;
	pgStatXactRollback = 0;
	pgStatBlockReadTime = 0;
	pgStatBlockWriteTime = 0;
	pgStatActiveTime = 0;
	pgStatTransactionIdleTime = 0;
}

/* --------
 * pgstat_should_report_connstats() -
 *
 *	We report session statistics only for normal backend processes.  Parallel
 *	workers run in parallel, so they don't contribute to session times, even
 *	though they use CPU time. Walsender processes could be considered here,
 *	but they have different session characteristics from normal backends (for
 *	example, they are always "active"), so they would skew session statistics.
 * ----------
 */
static bool
pgstat_should_report_connstat(void)
{
	return MyBackendType == B_BACKEND;
}

/*
 * pgstat_flush_db - flush out a local database stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
bool
pgstat_flush_database(PgStatSharedRef *shared_ref, bool nowait)
{
	PgStatShm_StatDBEntry *sharedent;
	PgStat_StatDBEntry *pendingent = (PgStat_StatDBEntry *) shared_ref->pending;

	if (shared_ref == NULL)
		return false;

	if (!pgstat_shared_stat_lock(shared_ref, nowait))
		return false;			/* failed to acquire lock, skip */

	sharedent = (PgStatShm_StatDBEntry *) shared_ref->shared_stats;

#define PGSTAT_ACCUM_DBCOUNT(item)		\
	(sharedent)->stats.item += (pendingent)->item

	PGSTAT_ACCUM_DBCOUNT(n_tuples_returned);
	PGSTAT_ACCUM_DBCOUNT(n_tuples_fetched);
	PGSTAT_ACCUM_DBCOUNT(n_tuples_inserted);
	PGSTAT_ACCUM_DBCOUNT(n_tuples_updated);
	PGSTAT_ACCUM_DBCOUNT(n_tuples_deleted);
	PGSTAT_ACCUM_DBCOUNT(n_blocks_fetched);
	PGSTAT_ACCUM_DBCOUNT(n_blocks_hit);

	PGSTAT_ACCUM_DBCOUNT(n_deadlocks);
	PGSTAT_ACCUM_DBCOUNT(n_temp_bytes);
	PGSTAT_ACCUM_DBCOUNT(n_temp_files);
	PGSTAT_ACCUM_DBCOUNT(n_checksum_failures);

	PGSTAT_ACCUM_DBCOUNT(n_xact_commit);
	PGSTAT_ACCUM_DBCOUNT(n_xact_rollback);

	PGSTAT_ACCUM_DBCOUNT(n_block_read_time);
	PGSTAT_ACCUM_DBCOUNT(n_block_write_time);

	PGSTAT_ACCUM_DBCOUNT(n_sessions);
	PGSTAT_ACCUM_DBCOUNT(total_session_time);
	PGSTAT_ACCUM_DBCOUNT(total_active_time);
	PGSTAT_ACCUM_DBCOUNT(total_idle_in_xact_time);
	PGSTAT_ACCUM_DBCOUNT(n_sessions_abandoned);
	PGSTAT_ACCUM_DBCOUNT(n_sessions_fatal);
	PGSTAT_ACCUM_DBCOUNT(n_sessions_killed);
#undef PGSTAT_ACCUM_DBCOUNT

	pgstat_shared_stat_unlock(shared_ref);

	memset(pendingent, 0, sizeof(*pendingent));

	return true;
}

/*
 * Find or create a local PgStat_StatDBEntry entry for dboid.
 */
PgStat_StatDBEntry *
pgstat_pending_db_prepare(Oid dboid)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_pending_prepare(PGSTAT_KIND_DB, dboid, InvalidOid);

	return shared_ref->pending;

}

/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 * Support function for the SQL-callable pgstat* functions. Returns the
 * collected statistics for one database or NULL. NULL doesn't necessarily
 * mean that the database doesn't exist, just that there are no statistics,
 * so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dboid)
{
	return (PgStat_StatDBEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_DB, dboid, InvalidOid);
}

void
AtEOXact_PgStat_Database(bool isCommit, bool parallel)
{
	/* Don't count parallel worker transaction stats */
	if (!parallel)
	{
		/*
		 * Count transaction commit or abort.  (We use counters, not just
		 * bools, in case the reporting message isn't sent right away.)
		 */
		if (isCommit)
			pgStatXactCommit++;
		else
			pgStatXactRollback++;
	}
}

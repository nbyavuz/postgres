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


int	pgStatXactCommit = 0;
int	pgStatXactRollback = 0;
PgStat_Counter pgStatBlockReadTime = 0;
PgStat_Counter pgStatBlockWriteTime = 0;
PgStat_Counter pgStatActiveTime = 0;
PgStat_Counter pgStatTransactionIdleTime = 0;
SessionEndType pgStatSessionEndCause = DISCONNECT_NORMAL;


/* ----------
 * pgstat_drop_database() -
 *
 *	Tell the collector that we just dropped a database.
 *	(If the message gets lost, we will still clean the dead DB eventually
 *	via future invocations of pgstat_vacuum_stat().)
 * ----------
 */
void
pgstat_drop_database(Oid databaseid)
{
	PgStat_MsgDropdb msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DROPDB);
	msg.m_databaseid = databaseid;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Tell the collector about a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_MsgRecoveryConflict msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RECOVERYCONFLICT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_reason = reason;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Tell the collector about a deadlock detected.
 * --------
 */
void
pgstat_report_deadlock(void)
{
	PgStat_MsgDeadlock msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DEADLOCK);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_checksum_failures_in_db() -
 *
 *	Tell the collector about one or more checksum failures.
 * --------
 */
void
pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount)
{
	PgStat_MsgChecksumFailure msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_CHECKSUMFAILURE);
	msg.m_databaseid = dboid;
	msg.m_failurecount = failurecount;
	msg.m_failure_time = GetCurrentTimestamp();

	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_checksum_failure() -
 *
 *	Tell the collector about a checksum failure.
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
 *	Tell the collector about a temporary file.
 * --------
 */
void
pgstat_report_tempfile(size_t filesize)
{
	PgStat_MsgTempFile msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TEMPFILE);
	msg.m_databaseid = MyDatabaseId;
	msg.m_filesize = filesize;
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_send_connstats() -
 *
 *	Tell the collector about session statistics.
 *	The parameter "disconnect" will be true when the backend exits.
 *	"last_report" is the last time we were called (0 if never).
 * ----------
 */
void
pgstat_send_connstats(bool disconnect, TimestampTz last_report)
{
	PgStat_MsgConn msg;
	long		secs;
	int			usecs;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_CONNECTION);
	msg.m_databaseid = MyDatabaseId;

	/* session time since the last report */
	TimestampDifference(((last_report == 0) ? MyStartTimestamp : last_report),
						GetCurrentTimestamp(),
						&secs, &usecs);
	msg.m_session_time = secs * 1000000 + usecs;

	msg.m_disconnect = disconnect ? pgStatSessionEndCause : DISCONNECT_NOT_YET;

	msg.m_active_time = pgStatActiveTime;
	pgStatActiveTime = 0;

	msg.m_idle_in_xact_time = pgStatTransactionIdleTime;
	pgStatTransactionIdleTime = 0;

	/* report a new session only the first time */
	msg.m_count = (last_report == 0) ? 1 : 0;

	pgstat_send(&msg, sizeof(PgStat_MsgConn));
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

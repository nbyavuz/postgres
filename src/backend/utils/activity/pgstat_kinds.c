/* -------------------------------------------------------------------------
 *
 * pgstat_kinds.c
 *	  Implementation of different types of activity statistics.
 *
 * This file contains the implementation of various types of statistics. It is
 * kept separate from pgstat.c to enforce the line between the statistics
 * access / storage implementation and the details of how e.g. relation
 * statistics work.
 *
 * Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_kinds.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/twophase_rmgr.h"
#include "postmaster/autovacuum.h"
#include "replication/slot.h"
#include "storage/procsignal.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"


static void pgstat_snapshot_archiver(void);
static void pgstat_snapshot_bgwriter(void);
static void pgstat_snapshot_checkpointer(void);
static void pgstat_snapshot_replslot(void);
static void pgstat_snapshot_slru(void);
static void pgstat_snapshot_wal(void);

static bool pgstat_flush_table(PgStatSharedRef *shared_ref, bool nowait);
static bool pgstat_flush_function(PgStatSharedRef *shared_ref, bool nowait);
static bool pgstat_flush_db(PgStatSharedRef *shared_ref, bool nowait);

static PgStat_StatDBEntry *pgstat_pending_db_prepare(Oid dboid);
static PgStat_TableStatus *pgstat_pending_tab_prepare(Oid rel_id, bool isshared);

static void pgstat_truncdrop_save_counters(PgStat_TableXactStatus *trans, bool is_drop);


/*
 * Define the different kinds of statistics. If reasonably possible, handling
 * specific to one kind of stats should go through this abstraction, rather
 * than making pgstat.c aware.
 *
 * See comments for struct pgstat_kind_info for details about the individual
 * fields.
 */
const pgstat_kind_info pgstat_kind_infos[PGSTAT_KIND_WAL + 1] = {

	/* stats types with a variable number of stats */

	[PGSTAT_KIND_DB] = {
		.is_global = false,
		/* so pg_stat_database entries can be seen in all databases */
		.accessed_across_databases = true,

		.shared_size = sizeof(PgStatShm_StatDBEntry),
		.shared_data_off = offsetof(PgStatShm_StatDBEntry, stats),
		.shared_data_len = sizeof(((PgStatShm_StatDBEntry*) 0)->stats),
		.pending_size = sizeof(PgStat_StatDBEntry),

		.flush_pending_cb = pgstat_flush_db,
	},

	[PGSTAT_KIND_TABLE] = {
		.is_global = false,
		.shared_size = sizeof(PgStatShm_StatTabEntry),
		.shared_data_off = offsetof(PgStatShm_StatTabEntry, stats),
		.shared_data_len = sizeof(((PgStatShm_StatTabEntry*) 0)->stats),
		.pending_size = sizeof(PgStat_TableStatus),

		.flush_pending_cb = pgstat_flush_table,
	},

	[PGSTAT_KIND_FUNCTION] = {
		.is_global = false,
		.shared_size = sizeof(PgStatShm_StatFuncEntry),
		.shared_data_off = offsetof(PgStatShm_StatFuncEntry, stats),
		.shared_data_len = sizeof(((PgStatShm_StatFuncEntry*) 0)->stats),
		.pending_size = sizeof(PgStat_BackendFunctionEntry),

		.flush_pending_cb = pgstat_flush_function,
	},


	/* global stats */

	[PGSTAT_KIND_ARCHIVER] = {
		.is_global = true,

		.snapshot_cb = pgstat_snapshot_archiver,
	},

	[PGSTAT_KIND_BGWRITER] = {
		.is_global = true,

		.snapshot_cb = pgstat_snapshot_bgwriter,
	},

	[PGSTAT_KIND_CHECKPOINTER] = {
		.is_global = true,

		.snapshot_cb = pgstat_snapshot_checkpointer,
	},

	[PGSTAT_KIND_REPLSLOT] = {
		.is_global = true,

		.snapshot_cb = pgstat_snapshot_replslot,
	},

	[PGSTAT_KIND_SLRU] = {
		.is_global = true,

		.snapshot_cb = pgstat_snapshot_slru,
	},

	[PGSTAT_KIND_WAL] = {
		.is_global = true,

		.snapshot_cb = pgstat_snapshot_wal,
	},

};


/* ----------
 * pending stats state that is directly modified from outside the stats system
 * ----------
 */

/* BgWriter global statistics counters */
PgStat_BgWriterStats BgWriterStats = {0};

/* CheckPointer global statistics counters */
PgStat_CheckPointerStats CheckPointerStats = {0};

/* WAL global statistics counters */
PgStat_WalStats	WalStats = {0};

PgStat_Counter pgStatBlockReadTime = 0;
PgStat_Counter pgStatBlockWriteTime = 0;
PgStat_Counter pgStatActiveTime = 0;
PgStat_Counter pgStatTransactionIdleTime = 0;
SessionEndType pgStatSessionEndCause = DISCONNECT_NORMAL;


int	pgStatXactCommit = 0;
int	pgStatXactRollback = 0;
bool have_slrustats = false;

/*
 * WAL usage counters saved from pgWALUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by substracting
 * the previous counters from the current ones.
 */
WalUsage prevWalUsage;


/* ----------
 * pending stats only modified in this file
 * ----------
 */

/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static instr_time total_func_time;


/*
 * SLRU statistics counts waiting to be written to the shared activity
 * statistics.  We assume this variable inits to zeroes.  Entries are
 * one-to-one with slru_names[].
 * Changes of SLRU counters are reported within critical sections so we use
 * static memory in order to avoid memory allocation.
 */
static PgStat_SLRUStats pending_SLRUStats[SLRU_NUM_ELEMENTS];



/*
 * Copy stats between relations. This is used for things like REINDEX
 * CONCURRENTLY.
 */
void
pgstat_copy_relation_stats(Relation dst, Relation src)
{
	PgStat_StatTabEntry *srcstats;
	PgStatShm_StatTabEntry *dstshstats;
	PgStatSharedRef *dst_ref;

	srcstats = pgstat_fetch_stat_tabentry_extended(src->rd_rel->relisshared,
												   RelationGetRelid(src));
	if (!srcstats)
		return;

	dst_ref = pgstat_shared_ref_get(PGSTAT_KIND_TABLE,
									dst->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
									RelationGetRelid(dst),
									true);
	pgstat_shared_stat_lock(dst_ref, false);

	dstshstats = (PgStatShm_StatTabEntry *) dst_ref->shared_stats;
	dstshstats->stats = *srcstats;

	pgstat_shared_stat_unlock(dst_ref);
}


/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 *
 *  We don't scribble on shared stats while resetting to avoid locking on
 *  shared stats struct. Instead, just record the current counters in another
 *  shared struct, which is protected by StatsLock. See
 *  pgstat_fetch_stat_(archiver|bgwriter|checkpointer) for the reader side.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	TimestampTz now = GetCurrentTimestamp();
	PgStat_Shared_Reset_Target t;

	if (strcmp(target, "archiver") == 0)
		t = RESET_ARCHIVER;
	else if (strcmp(target, "bgwriter") == 0)
		t = RESET_BGWRITER;
	else if (strcmp(target, "wal") == 0)
		t = RESET_WAL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\", \"bgwriter\" or \"wal\".")));

	/* Reset statistics for the cluster. */

	switch (t)
	{
		case RESET_ARCHIVER:
			LWLockAcquire(StatsLock, LW_EXCLUSIVE);
			pgstat_copy_global_stats(&StatsShmem->archiver.reset_offset,
									 &StatsShmem->archiver.stats,
									 sizeof(PgStat_ArchiverStats),
									 &StatsShmem->archiver.changecount);
			StatsShmem->archiver.reset_offset.stat_reset_timestamp = now;
			LWLockRelease(StatsLock);
			break;

		case RESET_BGWRITER:
			LWLockAcquire(StatsLock, LW_EXCLUSIVE);
			pgstat_copy_global_stats(&StatsShmem->bgwriter.reset_offset,
									 &StatsShmem->bgwriter.stats,
									 sizeof(PgStat_BgWriterStats),
									 &StatsShmem->bgwriter.changecount);
			pgstat_copy_global_stats(&StatsShmem->checkpointer.reset_offset,
									 &StatsShmem->checkpointer.stats,
									 sizeof(PgStat_CheckPointerStats),
									 &StatsShmem->checkpointer.changecount);
			StatsShmem->bgwriter.reset_offset.stat_reset_timestamp = now;
			LWLockRelease(StatsLock);
			break;

		case RESET_WAL:

			/*
			 * Differently from the two cases above, WAL statistics has many
			 * writer processes with the shared stats protected by
			 * StatsShmem->wal.lock.
			 */
			LWLockAcquire(&StatsShmem->wal.lock, LW_EXCLUSIVE);
			MemSet(&StatsShmem->wal.stats, 0, sizeof(PgStat_WalStats));
			StatsShmem->wal.stats.stat_reset_timestamp = now;
			LWLockRelease(&StatsShmem->wal.lock);
			break;
	}
}

static void
pgstat_reset_slru_counter_internal(int index, TimestampTz ts)
{
	LWLockAcquire(&StatsShmem->slru.lock, LW_EXCLUSIVE);

	MemSet(&StatsShmem->slru.stats[index], 0, sizeof(PgStat_SLRUStats));
	StatsShmem->slru.stats[index].stat_reset_timestamp = ts;

	LWLockRelease(&StatsShmem->slru.lock);
}

/* ----------
 * pgstat_reset_slru_counter() -
 *
 *	Tell the statistics collector to reset a single SLRU counter, or all
 *	SLRU counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_slru_counter(const char *name)
{
	int			i;
	TimestampTz ts = GetCurrentTimestamp();

	if (name)
	{
		i = pgstat_slru_index(name);
		pgstat_reset_slru_counter_internal(i, ts);
	}
	else
	{
		for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
			pgstat_reset_slru_counter_internal(i, ts);
	}
}

/* ----------
 * pgstat_reset_replslot_counter() -
 *
 *	Tell the statistics collector to reset a single replication slot
 *	counter, or all replication slots counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_replslot_counter(const char *name)
{
	int			startidx;
	int			endidx;
	int			i;
	TimestampTz ts;

	/*
	 * AFIXME: pgstats has business no looking into slot.c structures at
	 * this level of detail.
	 */

	if (name)
	{
		ReplicationSlot *slot;

		/* Check if the slot exits with the given name. */
		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		slot = SearchNamedReplicationSlot(name);
		LWLockRelease(ReplicationSlotControlLock);

		if (!slot)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("replication slot \"%s\" does not exist",
							name)));

		/*
		 * Nothing to do for physical slots as we collect stats only for
		 * logical slots.
		 */
		if (SlotIsPhysical(slot))
			return;

		/* reset this one entry */
		startidx = endidx = slot - ReplicationSlotCtl->replication_slots;
	}
	else
	{
		/* reset all existent entries */
		startidx = 0;
		endidx = max_replication_slots - 1;
	}

	ts = GetCurrentTimestamp();
	LWLockAcquire(&StatsShmem->replslot.lock, LW_EXCLUSIVE);
	for (i = startidx; i <= endidx; i++)
	{
		PgStat_ReplSlotStats *statent = &StatsShmem->replslot.stats[i];
		size_t off;

		off = offsetof(PgStat_ReplSlotStats, slotname) + sizeof(char[NAMEDATALEN]);

		memset(((char *) statent) + off, 0, sizeof(StatsShmem->replslot.stats[i]) - off);
		statent->stat_reset_timestamp = ts;
	}
	LWLockRelease(&StatsShmem->replslot.lock);
}

void
pgstat_drop_relation(Relation rel)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	pgstat_schedule_drop(PGSTAT_KIND_TABLE,
						 rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
						 RelationGetRelid(rel));

	if (pgstat_info &&
		pgstat_info->trans != NULL &&
		pgstat_info->trans->nest_level == nest_level)
	{
		pgstat_truncdrop_save_counters(pgstat_info->trans, true);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

void
pgstat_drop_function(Oid proid)
{
	pgstat_schedule_drop(PGSTAT_KIND_FUNCTION,
						 MyDatabaseId,
						 proid);
}


/*
 *  Find or create a local PgStat_StatDBEntry entry for dboid.
 */
static PgStat_StatDBEntry *
pgstat_pending_db_prepare(Oid dboid)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_pending_prepare(PGSTAT_KIND_DB, dboid, InvalidOid);

	return shared_ref->pending;

}

/* ----------
 * pgstat_pending_tab_prepare() -
 *  Find or create a PgStat_TableStatus entry for rel. New entry is created and
 *  initialized if not exists.
 * ----------
 */
static PgStat_TableStatus *
pgstat_pending_tab_prepare(Oid rel_id, bool isshared)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_pending_prepare(PGSTAT_KIND_TABLE,
										isshared ? InvalidOid : MyDatabaseId,
										rel_id);

	return shared_ref->pending;
}


/* ------------------------------------------------------------
 * Functions for flushing the different types of pending statistics
 *------------------------------------------------------------
 */


/*
 * pgstat_flush_table - flush out a pending table stats entry
 *
 * Some of the stats numbers are copied to pending database stats entry after
 * successful flush-out.
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
static bool
pgstat_flush_table(PgStatSharedRef *shared_ref, bool nowait)
{
	static const PgStat_TableCounts all_zeroes;
	Oid			dboid;			/* database OID of the table */
	PgStat_TableStatus *lstats; /* pending stats entry  */
	PgStatShm_StatTabEntry *shtabstats;	/* table entry of shared stats */
	PgStat_StatDBEntry *ldbstats;	/* pending database entry */

	Assert(shared_ref->shared_entry->key.kind == PGSTAT_KIND_TABLE);
	lstats = (PgStat_TableStatus *) shared_ref->pending;
	dboid = shared_ref->shared_entry->key.dboid;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (memcmp(&lstats->t_counts, &all_zeroes,
			   sizeof(PgStat_TableCounts)) == 0)
	{
		return true;
	}

	if (!pgstat_shared_stat_lock(shared_ref, nowait))
		return false;			/* failed to acquire lock, skip */

	/* add the values to the shared entry. */
	shtabstats = (PgStatShm_StatTabEntry *) shared_ref->shared_stats;
	shtabstats->stats.numscans += lstats->t_counts.t_numscans;
	shtabstats->stats.tuples_returned += lstats->t_counts.t_tuples_returned;
	shtabstats->stats.tuples_fetched += lstats->t_counts.t_tuples_fetched;
	shtabstats->stats.tuples_inserted += lstats->t_counts.t_tuples_inserted;
	shtabstats->stats.tuples_updated += lstats->t_counts.t_tuples_updated;
	shtabstats->stats.tuples_deleted += lstats->t_counts.t_tuples_deleted;
	shtabstats->stats.tuples_hot_updated += lstats->t_counts.t_tuples_hot_updated;

	/*
	 * If table was truncated or vacuum/analyze has ran, first reset the
	 * live/dead counters.
	 */
	if (lstats->t_counts.t_truncdropped)
	{
		shtabstats->stats.n_live_tuples = 0;
		shtabstats->stats.n_dead_tuples = 0;
	}

	shtabstats->stats.n_live_tuples += lstats->t_counts.t_delta_live_tuples;
	shtabstats->stats.n_dead_tuples += lstats->t_counts.t_delta_dead_tuples;
	shtabstats->stats.changes_since_analyze += lstats->t_counts.t_changed_tuples;
	shtabstats->stats.inserts_since_vacuum += lstats->t_counts.t_tuples_inserted;
	shtabstats->stats.blocks_fetched += lstats->t_counts.t_blocks_fetched;
	shtabstats->stats.blocks_hit += lstats->t_counts.t_blocks_hit;

	/* Clamp n_live_tuples in case of negative delta_live_tuples */
	shtabstats->stats.n_live_tuples = Max(shtabstats->stats.n_live_tuples, 0);
	/* Likewise for n_dead_tuples */
	shtabstats->stats.n_dead_tuples = Max(shtabstats->stats.n_dead_tuples, 0);

	pgstat_shared_stat_unlock(shared_ref);

	/* The entry is successfully flushed so the same to add to database stats */
	ldbstats = pgstat_pending_db_prepare(dboid);
	ldbstats->n_tuples_returned += lstats->t_counts.t_tuples_returned;
	ldbstats->n_tuples_fetched += lstats->t_counts.t_tuples_fetched;
	ldbstats->n_tuples_inserted += lstats->t_counts.t_tuples_inserted;
	ldbstats->n_tuples_updated += lstats->t_counts.t_tuples_updated;
	ldbstats->n_tuples_deleted += lstats->t_counts.t_tuples_deleted;
	ldbstats->n_blocks_fetched += lstats->t_counts.t_blocks_fetched;
	ldbstats->n_blocks_hit += lstats->t_counts.t_blocks_hit;

	return true;
}

/*
 * pgstat_flush_function - flush out a local function stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
static bool
pgstat_flush_function(PgStatSharedRef *shared_ref, bool nowait)
{
	PgStat_BackendFunctionEntry *localent;	/* local stats entry */
	PgStatShm_StatFuncEntry *shfuncent = NULL; /* shared stats entry */

	Assert(shared_ref->shared_entry->key.kind == PGSTAT_KIND_FUNCTION);
	localent = (PgStat_BackendFunctionEntry *) shared_ref->pending;

	/* localent always has non-zero content */

	if (!pgstat_shared_stat_lock(shared_ref, nowait))
		return false;			/* failed to acquire lock, skip */

	shfuncent = (PgStatShm_StatFuncEntry *) shared_ref->shared_stats;

	shfuncent->stats.f_numcalls += localent->f_counts.f_numcalls;
	shfuncent->stats.f_total_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_total_time);
	shfuncent->stats.f_self_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_self_time);

	pgstat_shared_stat_unlock(shared_ref);

	return true;
}

/*
 * pgstat_flush_db - flush out a local database stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
static bool
pgstat_flush_db(PgStatSharedRef *shared_ref, bool nowait)
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

	PGSTAT_ACCUM_DBCOUNT(n_sessions);
	PGSTAT_ACCUM_DBCOUNT(total_session_time);
	PGSTAT_ACCUM_DBCOUNT(total_active_time);
	PGSTAT_ACCUM_DBCOUNT(total_idle_in_xact_time);
	PGSTAT_ACCUM_DBCOUNT(n_sessions_abandoned);
	PGSTAT_ACCUM_DBCOUNT(n_sessions_fatal);
	PGSTAT_ACCUM_DBCOUNT(n_sessions_killed);
#undef PGSTAT_ACCUM_DBCOUNT

	/*
	 * Accumulate xact commit/rollback and I/O timings to stats entry of the
	 * current database.
	 */
	if (OidIsValid(shared_ref->shared_entry->key.dboid))
	{
		sharedent->stats.n_xact_commit += pgStatXactCommit;
		sharedent->stats.n_xact_rollback += pgStatXactRollback;
		sharedent->stats.n_block_read_time += pgStatBlockReadTime;
		sharedent->stats.n_block_write_time += pgStatBlockWriteTime;
		pgStatXactCommit = 0;
		pgStatXactRollback = 0;
		pgStatBlockReadTime = 0;
		pgStatBlockWriteTime = 0;
	}
	else
	{
		sharedent->stats.n_xact_commit = 0;
		sharedent->stats.n_xact_rollback = 0;
		sharedent->stats.n_block_read_time = 0;
		sharedent->stats.n_block_write_time = 0;
	}

	pgstat_shared_stat_unlock(shared_ref);

	memset(pendingent, 0, sizeof(*pendingent));

	return true;
}


/*
 * XXXX: always try to flush WAL stats. We don't want to manipulate another
 * counter during XLogInsert so we don't have an effecient short cut to know
 * whether any counter gets incremented.
 */
bool
walstats_pending(void)
{
	static const PgStat_WalStats all_zeroes;

	return memcmp(&WalStats, &all_zeroes,
				  offsetof(PgStat_WalStats, stat_reset_timestamp)) != 0;
}


/*
 * pgstat_flush_wal - flush out a locally pending WAL stats entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if not all pending stats have been flushed out.
 */
bool
pgstat_flush_wal(bool nowait)
{
	PgStat_WalStats *s = &StatsShmem->wal.stats;
	PgStat_WalStats *l = &WalStats;
	WalUsage	all_zeroes PG_USED_FOR_ASSERTS_ONLY = {0};

	/*
	 * We don't update the WAL usage portion of the local WalStats elsewhere.
	 * Instead, fill in that portion with the difference of pgWalUsage since
	 * the previous call.
	 */
	Assert(memcmp(&l->wal_usage, &all_zeroes, sizeof(WalUsage)) == 0);
	WalUsageAccumDiff(&l->wal_usage, &pgWalUsage, &prevWalUsage);

	/*
	 * This function can be called even if nothing at all has happened. Avoid
	 * taking lock for nothing in that case.
	 */
	if (!walstats_pending())
		return false;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&StatsShmem->wal.lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&StatsShmem->wal.lock,
									   LW_EXCLUSIVE))
	{
		MemSet(&WalStats, 0, sizeof(WalStats));
		return true;			/* failed to acquire lock, skip */
	}

	s->wal_usage.wal_records += l->wal_usage.wal_records;
	s->wal_usage.wal_fpi += l->wal_usage.wal_fpi;
	s->wal_usage.wal_bytes += l->wal_usage.wal_bytes;
	s->wal_buffers_full += l->wal_buffers_full;
	s->wal_write += l->wal_write;
	s->wal_write_time += l->wal_write_time;
	s->wal_sync += l->wal_sync;
	s->wal_sync_time += l->wal_sync_time;
	LWLockRelease(&StatsShmem->wal.lock);

	/*
	 * Save the current counters for the subsequent calculation of WAL usage.
	 */
	prevWalUsage = pgWalUsage;

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&WalStats, 0, sizeof(WalStats));

	return false;
}

/*
 * pgstat_flush_slru - flush out locally pending SLRU stats entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true. Writer processes are mutually excluded
 * using LWLock, but readers are expected to use change-count protocol to avoid
 * interference with writers.
 *
 * Returns true if not all pending stats have been flushed out.
 */
bool
pgstat_flush_slru(bool nowait)
{
	int			i;

	if (!have_slrustats)
		return false;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&StatsShmem->slru.lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&StatsShmem->slru.lock,
									   LW_EXCLUSIVE))
		return true;			/* failed to acquire lock, skip */


	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		PgStat_SLRUStats *sharedent = &StatsShmem->slru.stats[i];
		PgStat_SLRUStats *pendingent = &pending_SLRUStats[i];

		sharedent->blocks_zeroed += pendingent->blocks_zeroed;
		sharedent->blocks_hit += pendingent->blocks_hit;
		sharedent->blocks_read += pendingent->blocks_read;
		sharedent->blocks_written += pendingent->blocks_written;
		sharedent->blocks_exists += pendingent->blocks_exists;
		sharedent->flush += pendingent->flush;
		sharedent->truncate += pendingent->truncate;
	}

	/* done, clear the pending entry */
	MemSet(pending_SLRUStats, 0, SizeOfSlruStats);

	LWLockRelease(&StatsShmem->slru.lock);

	have_slrustats = false;

	return false;
}



/* ------------------------------------------------------------
 * Functions to add new stats.
 *------------------------------------------------------------
 */


/* ----------
 * pgstat_report_autovac() -
 *
 *	Called from autovacuum.c to report startup of an autovacuum process.
 *	We are called before InitPostgres is done, so can't rely on MyDatabaseId;
 *	the db OID must be passed in, instead.
 * ----------
 */
void
pgstat_report_autovac(Oid dboid)
{
	PgStatSharedRef *shared_ref;
	PgStatShm_StatDBEntry *dbentry;

	/* can't get here in single user mode */
	Assert(IsUnderPostmaster);

	/*
	 * End-of-vacuum is reported instantly. Report the start the same way for
	 * consistency. Vacuum doesn't run frequently and is a long-lasting
	 * operation so it doesn't matter if we get blocked here a little.
	 */
	shared_ref =
		pgstat_shared_ref_get(PGSTAT_KIND_DB, dboid, InvalidOid, true);

	pgstat_shared_stat_lock(shared_ref, false);

	dbentry = (PgStatShm_StatDBEntry *) shared_ref->shared_stats;
	dbentry->stats.last_autovac_time = GetCurrentTimestamp();

	pgstat_shared_stat_unlock(shared_ref);
}


/* ---------
 * pgstat_report_vacuum() -
 *
 *	Report about the table we just vacuumed.
 * ---------
 */
void
pgstat_report_vacuum(Oid tableoid, bool shared,
					 PgStat_Counter livetuples, PgStat_Counter deadtuples)
{
	PgStatSharedRef *shared_ref;
	PgStatShm_StatTabEntry *shtabentry;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);
	TimestampTz ts;

	if (!pgstat_track_counts)
		return;

	/* Store the data in the table's hash table entry. */
	ts = GetCurrentTimestamp();

	/*
	 * Differently from ordinary operations, maintenance commands take longer
	 * time and getting blocked at the end of work doesn't matter.
	 * Furthermore, this can prevent the stats updates made by the
	 * transactions that ends after this vacuum from being canceled by a
	 * delayed vacuum report. Update shared stats entry directly for the above
	 * reasons.
	 */
	shared_ref =
		pgstat_shared_stat_locked(PGSTAT_KIND_TABLE, dboid, tableoid, false);

	shtabentry = (PgStatShm_StatTabEntry *) shared_ref->shared_stats;
	shtabentry->stats.n_live_tuples = livetuples;
	shtabentry->stats.n_dead_tuples = deadtuples;

	/*
	 * It is quite possible that a non-aggressive VACUUM ended up skipping
	 * various pages, however, we'll zero the insert counter here regardless.
	 * It's currently used only to track when we need to perform an "insert"
	 * autovacuum, which are mainly intended to freeze newly inserted tuples.
	 * Zeroing this may just mean we'll not try to vacuum the table again
	 * until enough tuples have been inserted to trigger another insert
	 * autovacuum.  An anti-wraparound autovacuum will catch any persistent
	 * stragglers.
	 */
	shtabentry->stats.inserts_since_vacuum = 0;

	if (IsAutoVacuumWorkerProcess())
	{
		shtabentry->stats.autovac_vacuum_timestamp = ts;
		shtabentry->stats.autovac_vacuum_count++;
	}
	else
	{
		shtabentry->stats.vacuum_timestamp = ts;
		shtabentry->stats.vacuum_count++;
	}

	pgstat_shared_stat_unlock(shared_ref);
}

/* --------
 * pgstat_report_analyze() -
 *
 *	Report about the table we just analyzed.
 *
 * Caller must provide new live- and dead-tuples estimates, as well as a
 * flag indicating whether to reset the changes_since_analyze counter.
 * --------
 */
void
pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter)
{
	PgStatSharedRef *shared_ref;
	PgStatShm_StatTabEntry *tabentry;
	Oid			dboid = (rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId);

	if (!pgstat_track_counts)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from what is already written to shared stats now, else
	 * they'll be double-counted after commit.  (This approach also ensures
	 * that the shared stats ends up with the right numbers if we abort
	 * instead of committing.)
	 */
	if (pgstat_count_rel(rel))
	{
		PgStat_TableXactStatus *trans;

		for (trans = rel->pgstat_info->trans; trans; trans = trans->upper)
		{
			livetuples -= trans->tuples_inserted - trans->tuples_deleted;
			deadtuples -= trans->tuples_updated + trans->tuples_deleted;
		}
		/* count stuff inserted by already-aborted subxacts, too */
		deadtuples -= rel->pgstat_info->t_counts.t_delta_dead_tuples;
		/* Since ANALYZE's counts are estimates, we could have underflowed */
		livetuples = Max(livetuples, 0);
		deadtuples = Max(deadtuples, 0);
	}

	/*
	 * Differently from ordinary operations, maintenance commands take longer
	 * time and getting blocked at the end of work doesn't matter.
	 * Furthermore, this can prevent the stats updates made by the
	 * transactions that ends after this analyze from being canceled by a
	 * delayed analyze report. Update shared stats entry directly for the
	 * above reasons.
	 */
	shared_ref = pgstat_shared_stat_locked(PGSTAT_KIND_TABLE, dboid,
										   RelationGetRelid(rel),
										   false);
	/* can't get dropped while accessed */
	Assert(shared_ref != NULL && shared_ref->shared_stats != NULL);

	tabentry = (PgStatShm_StatTabEntry *) shared_ref->shared_stats;

	tabentry->stats.n_live_tuples = livetuples;
	tabentry->stats.n_dead_tuples = deadtuples;

	/*
	 * If commanded, reset changes_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (resetcounter)
		tabentry->stats.changes_since_analyze = 0;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->stats.autovac_analyze_timestamp = GetCurrentTimestamp();
		tabentry->stats.autovac_analyze_count++;
	}
	else
	{
		tabentry->stats.analyze_timestamp = GetCurrentTimestamp();
		tabentry->stats.analyze_count++;
	}

	pgstat_shared_stat_unlock(shared_ref);
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Report a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_StatDBEntry *dbent;

	Assert(IsUnderPostmaster);
	if (!pgstat_track_counts)
		return;

	dbent = pgstat_pending_db_prepare(MyDatabaseId);

	switch (reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Since we drop the information about the database as soon as it
			 * replicates, there is no point in counting these conflicts.
			 */
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			dbent->n_conflict_tablespace++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			dbent->n_conflict_lock++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			dbent->n_conflict_snapshot++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			dbent->n_conflict_bufferpin++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			dbent->n_conflict_startup_deadlock++;
			break;
	}
}


/* --------
 * pgstat_report_deadlock() -
 *
 *	Report a deadlock detected.
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
 * pgstat_report_checksum_failures_in_db(dboid, failure_count) -
 *
 *	Reports about one or more checksum failures.
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
	 * Update the shared stats directly - it's unlikely that we'd get another
	 * report for the same database with the current users of
	 * pgstat_report_checksum_failures_in_db(). That also allows us to only
	 * have pending entries for a the current DB (and one for shared
	 * relations).
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
 *	Reports about a checksum failure.
 * --------
 */
void
pgstat_report_checksum_failure(void)
{
	PgStat_StatDBEntry *dbent;

	if (!pgstat_track_counts)
		return;

	dbent = pgstat_pending_db_prepare(MyDatabaseId);
	dbent->n_checksum_failures++;
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Report a temporary file.
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

/* ----------
 * pgstat_report_replslot() -
 *
 *	Report replication slot activity.
 * ----------
 */
void
pgstat_report_replslot(uint32 index,
					   const char *slotname,
					   int spilltxns, int spillcount, int spillbytes,
					   int streamtxns, int streamcount, int streambytes)
{
	PgStat_ReplSlotStats *statent;

	Assert(index < max_replication_slots);
	Assert(slotname[0] != '\0' && strlen(slotname) < NAMEDATALEN);

	if (!pgstat_track_counts)
		return;

	statent = &StatsShmem->replslot.stats[index];

	LWLockAcquire(&StatsShmem->replslot.lock, LW_EXCLUSIVE);

	/* clear the counters if not used */
	if (statent->index == -1)
	{
		memset(statent, 0, sizeof(*statent));
		statent->index = index;
		strlcpy(statent->slotname, slotname, NAMEDATALEN);
	}
	else if (strcmp(slotname, statent->slotname) != 0)
	{
		/* AFIXME: Is there a valid way this can happen? */
		elog(ERROR, "stats out of sync");
	}
	else
	{
		Assert(statent->index == index);
	}

	statent->spill_txns += spilltxns;
	statent->spill_count += spillcount;
	statent->spill_bytes += spillbytes;
	statent->stream_txns += streamtxns;
	statent->stream_count += streamcount;
	statent->stream_bytes += streambytes;

	LWLockRelease(&StatsShmem->replslot.lock);
}

/* ----------
 * pgstat_report_replslot_drop() -
 *
 *	Tell the collector about dropping the replication slot.
 * ----------
 */
void
pgstat_report_replslot_drop(uint32 index, const char *slotname)
{
	PgStat_ReplSlotStats *statent;

	Assert(index < max_replication_slots);
	Assert(slotname[0] != '\0' && strlen(slotname) < NAMEDATALEN);

	if (!pgstat_track_counts)
		return;

	statent = &StatsShmem->replslot.stats[index];

	LWLockAcquire(&StatsShmem->replslot.lock, LW_EXCLUSIVE);
	/*
	 * NB: need to accept that there might not be any stats, e.g. if we threw
	 * away stats after a crash restart.
	 */
	statent->index = -1;
	LWLockRelease(&StatsShmem->replslot.lock);
}

/* ----------
 * pgstat_init_function_usage() -
 *
 *  Initialize function call usage data.
 *  Called by the executor before invoking a function.
 * ----------
 */
void
pgstat_init_function_usage(FunctionCallInfo fcinfo,
						   PgStat_FunctionCallUsage *fcu)
{
	PgStatSharedRef *shared_ref;
	PgStat_BackendFunctionEntry *pending;

	if (pgstat_track_functions <= fcinfo->flinfo->fn_stats)
	{
		/* stats not wanted */
		fcu->fs = NULL;
		return;
	}

	shared_ref = pgstat_pending_prepare(PGSTAT_KIND_FUNCTION,
											 MyDatabaseId,
											 fcinfo->flinfo->fn_oid);
	pending = shared_ref->pending;

	fcu->fs = &pending->f_counts;

	/* save stats for this function, later used to compensate for recursion */
	fcu->save_f_total_time = pending->f_counts.f_total_time;

	/* save current backend-wide total time */
	fcu->save_total = total_func_time;

	/* get clock time as of function start */
	INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/* ----------
 * find_funcstat_entry - find any existing PgStat_BackendFunctionEntry entry
 *		for specified function
 *
 * If no entry, return NULL, don't create a new one
 * ----------
 */
PgStat_BackendFunctionEntry *
find_funcstat_entry(Oid func_id)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_pending_fetch(PGSTAT_KIND_FUNCTION, MyDatabaseId, func_id);

	if (shared_ref)
		return shared_ref->pending;
	return NULL;
}

/*
 * Calculate function call usage and update stat counters.
 * Called by the executor after invoking a function.
 *
 * In the case of a set-returning function that runs in value-per-call mode,
 * we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 * calls for what the user considers a single call of the function.  The
 * finalize flag should be TRUE on the last call.
 */
void
pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	PgStat_FunctionCounts *fs = fcu->fs;
	instr_time	f_total;
	instr_time	f_others;
	instr_time	f_self;

	/* stats not wanted? */
	if (fs == NULL)
		return;

	/* total elapsed time in this function call */
	INSTR_TIME_SET_CURRENT(f_total);
	INSTR_TIME_SUBTRACT(f_total, fcu->f_start);

	/* self usage: elapsed minus anything already charged to other calls */
	f_others = total_func_time;
	INSTR_TIME_SUBTRACT(f_others, fcu->save_total);
	f_self = f_total;
	INSTR_TIME_SUBTRACT(f_self, f_others);

	/* update backend-wide total time */
	INSTR_TIME_ADD(total_func_time, f_self);

	/*
	 * Compute the new f_total_time as the total elapsed time added to the
	 * pre-call value of f_total_time.  This is necessary to avoid
	 * double-counting any time taken by recursive calls of myself.  (We do
	 * not need any similar kluge for self time, since that already excludes
	 * any recursive calls.)
	 */
	INSTR_TIME_ADD(f_total, fcu->save_f_total_time);

	/* update counters in function stats table */
	if (finalize)
		fs->f_numcalls++;
	fs->f_total_time = f_total;
	INSTR_TIME_ADD(fs->f_self_time, f_self);
}



/*
 * Initialize a relcache entry to count access statistics.  Called whenever a
 * relation is opened.
 *
 * We assume that a relcache entry's pgstat_info field is zeroed by relcache.c
 * when the relcache entry is made; thereafter it is long-lived data.
 */
void
pgstat_relation_init(Relation rel)
{
	char		relkind = rel->rd_rel->relkind;

	/* We only count stats for things that have storage */
	if (!RELKIND_HAS_STORAGE(relkind))
	{
		rel->pgstat_enabled = false;
		rel->pgstat_info = NULL;
		return;
	}

	if (!pgstat_track_counts)
	{
		/* We're not counting at all */
		rel->pgstat_enabled = false;
		rel->pgstat_info = NULL;
		return;
	}

	rel->pgstat_enabled = true;
	return;
}

/*
 * Prepare for statistics for this relation to be collected. This ensures we
 * have a reference to the shared stats entry. That is important because a
 * relation drop in another connection can otherwise lead to the shared stats
 * entry being dropped, which we then later would re-create when flushing
 * stats.
 *
 * This is separate from pgstat_relation_init() as it is not uncommon for
 * relcache entries to be opened without ever getting stats reported.
 */
void
pgstat_relation_assoc(Relation rel)
{
	Assert(rel->pgstat_enabled);
	Assert(rel->pgstat_info == NULL);

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = pgstat_pending_tab_prepare(RelationGetRelid(rel),
												  rel->rd_rel->relisshared);
	/* mark this relation as the owner */

	/* don't allow link a stats to multiple relcache entries */
	Assert(rel->pgstat_info->relation == NULL);
	rel->pgstat_info->relation = rel;
}

/*
 * Break the mutual link between a relcache entry and a local stats entry.
 * This must be called always when one end of the link is removed.
 */
void
pgstat_relation_delink(Relation rel)
{
	/* remove the link to stats info if any */
	if (rel && rel->pgstat_info)
	{
		/* link sanity check */
		Assert(rel->pgstat_info->relation == rel);
		rel->pgstat_info->relation = NULL;
		rel->pgstat_info = NULL;
	}
}

/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 *  Find any existing PgStat_TableStatus entry for rel_id in the current
 *  database. If not found, try finding from shared tables.
 *
 *  If no entry found, return NULL, don't create a new one
 * ----------
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_pending_fetch(PGSTAT_KIND_TABLE, MyDatabaseId, rel_id);
	if (!shared_ref)
		shared_ref = pgstat_pending_fetch(PGSTAT_KIND_TABLE, InvalidOid, rel_id);

	if (shared_ref)
		return shared_ref->pending;
	return NULL;
}


/*
 * pgstat_count_heap_insert - count a tuple insertion of n tuples
 */
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
	if (pgstat_count_rel(rel))
	{
		/* We have to log the effect at the proper transactional level */
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_inserted += n;
	}
}

/*
 * pgstat_count_heap_update - count a tuple update
 */
void
pgstat_count_heap_update(Relation rel, bool hot)
{
	if (pgstat_count_rel(rel))
	{
		/* We have to log the effect at the proper transactional level */
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_updated++;

		/* t_tuples_hot_updated is nontransactional, so just advance it */
		if (hot)
			pgstat_info->t_counts.t_tuples_hot_updated++;
	}
}

/*
 * pgstat_count_heap_delete - count a tuple deletion
 */
void
pgstat_count_heap_delete(Relation rel)
{
	if (pgstat_count_rel(rel))
	{
		/* We have to log the effect at the proper transactional level */
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_deleted++;
	}
}

/*
 * pgstat_truncdrop_save_counters
 *
 * Whenever a table is truncated/dropped, we save its i/u/d counters so that they
 * can be cleared, and if the (sub)xact that executed the truncate/drop later
 * aborts, the counters can be restored to the saved (pre-truncate) values.
 *
 * Note that for truncate we do this on the first truncate in any particular
 * subxact level only.
 */
static void
pgstat_truncdrop_save_counters(PgStat_TableXactStatus *trans, bool is_drop)
{
	if (!trans->truncdropped || is_drop)
	{
		trans->inserted_pre_truncdrop = trans->tuples_inserted;
		trans->updated_pre_truncdrop = trans->tuples_updated;
		trans->deleted_pre_truncdrop = trans->tuples_deleted;
		trans->truncdropped = true;
	}
}

/*
 * pgstat_truncdrop_restore_counters - restore counters when a truncate aborts
 */
static void
pgstat_truncdrop_restore_counters(PgStat_TableXactStatus *trans)
{
	if (trans->truncdropped)
	{
		trans->tuples_inserted = trans->inserted_pre_truncdrop;
		trans->tuples_updated = trans->updated_pre_truncdrop;
		trans->tuples_deleted = trans->deleted_pre_truncdrop;
	}
}

/*
 * pgstat_count_truncate - update tuple counters due to truncate
 */
void
pgstat_count_truncate(Relation rel)
{
	if (pgstat_count_rel(rel))
	{
		/* We have to log the effect at the proper transactional level */
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_truncdrop_save_counters(pgstat_info->trans, false);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * pgstat_update_heap_dead_tuples - update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so t_delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void
pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
	if (pgstat_count_rel(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		pgstat_info->t_counts.t_delta_dead_tuples -= delta;
	}
}

/*
 * Perform relation stats specific end-of-transaction work. Helper for
 * AtEOXact_PgStat.
 *
 * Transfer transactional insert/update counts into the base tabstat entries.
 * We don't bother to free any of the transactional state, since it's all in
 * TopTransactionContext and will go away anyway.
 */
void
AtEOXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat;

		Assert(trans->nest_level == 1);
		Assert(trans->upper == NULL);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);
		/* restore pre-truncate/drop stats (if any) in case of aborted xact */
		if (!isCommit)
		{
			pgstat_truncdrop_restore_counters(trans);
		}

		/* count attempted actions regardless of commit/abort */
		tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
		tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
		tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
		if (isCommit)
		{
			tabstat->t_counts.t_truncdropped = trans->truncdropped;
			if (trans->truncdropped)
			{
				/* forget live/dead stats seen by backend thus far */
				tabstat->t_counts.t_delta_live_tuples = 0;
				tabstat->t_counts.t_delta_dead_tuples = 0;
			}
			/* insert adds a live tuple, delete removes one */
			tabstat->t_counts.t_delta_live_tuples +=
				trans->tuples_inserted - trans->tuples_deleted;
			/* update and delete each create a dead tuple */
			tabstat->t_counts.t_delta_dead_tuples +=
				trans->tuples_updated + trans->tuples_deleted;
			/* insert, update, delete each count as one change event */
			tabstat->t_counts.t_changed_tuples +=
				trans->tuples_inserted + trans->tuples_updated +
				trans->tuples_deleted;
		}
		else
		{
			/* inserted tuples are dead, deleted tuples are unaffected */
			tabstat->t_counts.t_delta_dead_tuples +=
				trans->tuples_inserted + trans->tuples_updated;
			/* an aborted xact generates no changed_tuple events */
		}
		tabstat->trans = NULL;
	}
}

/*
 * Perform relation stats specific end-of-subtransaction work. Helper for
 * AtEOSubXact_PgStat.
 *
 * This just merge the sub-transaction's transactional stats into the parent.
 */
void
AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
{
	PgStat_TableXactStatus *trans;
	PgStat_TableXactStatus *next_trans;

	for (trans = xact_state->first; trans != NULL; trans = next_trans)
	{
		PgStat_TableStatus *tabstat;

		next_trans = trans->next;
		Assert(trans->nest_level == nestDepth);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);

		if (isCommit)
		{
			if (trans->upper && trans->upper->nest_level == nestDepth - 1)
			{
				if (trans->truncdropped)
				{
					/* propagate the truncate/drop status one level up */
					pgstat_truncdrop_save_counters(trans->upper, false);
					/* replace upper xact stats with ours */
					trans->upper->tuples_inserted = trans->tuples_inserted;
					trans->upper->tuples_updated = trans->tuples_updated;
					trans->upper->tuples_deleted = trans->tuples_deleted;
				}
				else
				{
					trans->upper->tuples_inserted += trans->tuples_inserted;
					trans->upper->tuples_updated += trans->tuples_updated;
					trans->upper->tuples_deleted += trans->tuples_deleted;
				}
				tabstat->trans = trans->upper;
				pfree(trans);
			}
			else
			{
				/*
				 * When there isn't an immediate parent state, we can just
				 * reuse the record instead of going through a
				 * palloc/pfree pushup (this works since it's all in
				 * TopTransactionContext anyway).  We have to re-link it
				 * into the parent level, though, and that might mean
				 * pushing a new entry into the pgStatXactStack.
				 */
				PgStat_SubXactStatus *upper_xact_state;

				upper_xact_state = get_tabstat_stack_level(nestDepth - 1);
				trans->next = upper_xact_state->first;
				upper_xact_state->first = trans;
				trans->nest_level = nestDepth - 1;
			}
		}
		else
		{
			/*
			 * On abort, update top-level tabstat counts, then forget the
			 * subtransaction
			 */

			/* first restore values obliterated by truncate/drop */
			pgstat_truncdrop_restore_counters(trans);
			/* count attempted actions regardless of commit/abort */
			tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
			tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
			tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
			/* inserted tuples are dead, deleted tuples are unaffected */
			tabstat->t_counts.t_delta_dead_tuples +=
				trans->tuples_inserted + trans->tuples_updated;
			tabstat->trans = trans->upper;
			pfree(trans);
		}
	}
}

/*
 * Generate 2PC records for all the pending transaction-dependent stats work.
 */
void
AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
	PgStat_TableXactStatus *trans;

	Assert(xact_state);
	Assert(xact_state->nest_level == 1);
	Assert(xact_state->prev == NULL);

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat PG_USED_FOR_ASSERTS_ONLY;
		TwoPhasePgStatRecord record;

		Assert(trans->nest_level == 1);
		Assert(trans->upper == NULL);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);

		record.tuples_inserted = trans->tuples_inserted;
		record.tuples_updated = trans->tuples_updated;
		record.tuples_deleted = trans->tuples_deleted;
		record.inserted_pre_truncdrop = trans->inserted_pre_truncdrop;
		record.updated_pre_truncdrop = trans->updated_pre_truncdrop;
		record.deleted_pre_truncdrop = trans->deleted_pre_truncdrop;
		record.t_truncdropped = trans->truncdropped;

		RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
							   &record, sizeof(TwoPhasePgStatRecord));
	}
}

/*
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the activity stats facility immediately, while the effects on
 * live and dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat_Relations is not called during PREPARE.
 */
void
PostPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat;

		tabstat = trans->parent;
		tabstat->trans = NULL;
	}

}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void
pgstat_twophase_postcommit(TransactionId xid, uint16 info,
						   void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = pgstat_pending_tab_prepare(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, commit case */
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_truncdropped = rec->t_truncdropped;
	if (rec->t_truncdropped)
	{
		/* forget live/dead stats seen by backend thus far */
		pgstat_info->t_counts.t_delta_live_tuples = 0;
		pgstat_info->t_counts.t_delta_dead_tuples = 0;
	}
	pgstat_info->t_counts.t_delta_live_tuples +=
		rec->tuples_inserted - rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_updated + rec->tuples_deleted;
	pgstat_info->t_counts.t_changed_tuples +=
		rec->tuples_inserted + rec->tuples_updated +
		rec->tuples_deleted;
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void
pgstat_twophase_postabort(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = pgstat_pending_tab_prepare(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, abort case */
	if (rec->t_truncdropped)
	{
		rec->tuples_inserted = rec->inserted_pre_truncdrop;
		rec->tuples_updated = rec->updated_pre_truncdrop;
		rec->tuples_deleted = rec->deleted_pre_truncdrop;
	}
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_inserted + rec->tuples_updated;
}


/* ----------
 * pgstat_report_archiver() -
 *
 *		Report archiver statistics
 * ----------
 */
void
pgstat_report_archiver(const char *xlog, bool failed)
{
	TimestampTz now = GetCurrentTimestamp();

	changecount_before_write(&StatsShmem->archiver.changecount);

	if (failed)
	{
		++StatsShmem->archiver.stats.failed_count;
		memcpy(&StatsShmem->archiver.stats.last_failed_wal, xlog,
			   sizeof(StatsShmem->archiver.stats.last_failed_wal));
		StatsShmem->archiver.stats.last_failed_timestamp = now;
	}
	else
	{
		++StatsShmem->archiver.stats.archived_count;
		memcpy(&StatsShmem->archiver.stats.last_archived_wal, xlog,
			   sizeof(StatsShmem->archiver.stats.last_archived_wal));
		StatsShmem->archiver.stats.last_archived_timestamp = now;
	}

	changecount_after_write(&StatsShmem->archiver.changecount);
}

/* ----------
 * pgstat_report_bgwriter() -
 *
 *		Report bgwriter statistics
 * ----------
 */
void
pgstat_report_bgwriter(void)
{
	static const PgStat_BgWriterStats all_zeroes;
	PgStat_BgWriterStats *s = &StatsShmem->bgwriter.stats;
	PgStat_BgWriterStats *l = &BgWriterStats;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_BgWriterStats)) == 0)
		return;

	changecount_before_write(&StatsShmem->bgwriter.changecount);

	s->buf_written_clean += l->buf_written_clean;
	s->maxwritten_clean += l->maxwritten_clean;
	s->buf_alloc += l->buf_alloc;

	changecount_after_write(&StatsShmem->bgwriter.changecount);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}

/* ----------
 * pgstat_report_checkpointer() -
 *
 *		Report checkpointer statistics
 * ----------
 */
void
pgstat_report_checkpointer(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_CheckPointerStats all_zeroes;
	PgStat_CheckPointerStats *s = &StatsShmem->checkpointer.stats;
	PgStat_CheckPointerStats *l = &CheckPointerStats;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&CheckPointerStats, &all_zeroes,
			   sizeof(PgStat_CheckPointerStats)) == 0)
		return;

	changecount_before_write(&StatsShmem->checkpointer.changecount);

	s->timed_checkpoints += l->timed_checkpoints;
	s->requested_checkpoints += l->requested_checkpoints;
	s->checkpoint_write_time += l->checkpoint_write_time;
	s->checkpoint_sync_time += l->checkpoint_sync_time;
	s->buf_written_checkpoints += l->buf_written_checkpoints;
	s->buf_written_backend += l->buf_written_backend;
	s->buf_fsync_backend += l->buf_fsync_backend;

	changecount_after_write(&StatsShmem->checkpointer.changecount);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&CheckPointerStats, 0, sizeof(CheckPointerStats));
}

/* ----------
 * pgstat_report_wal() -
 *
 *		Report WAL statistics
 * ----------
 */
void
pgstat_report_wal(bool force)
{
	pgstat_flush_wal(force);
}

/* ----------
 * pgstat_update_connstat() -
 *
 *		Update local connection stats
 * ----------
 */
void
pgstat_update_connstats(bool disconnect)
{
	static TimestampTz last_report = 0;
	static SessionEndType session_end_type = DISCONNECT_NOT_YET;
	TimestampTz now;
	long		secs;
	int			usecs;
	PgStat_StatDBEntry *ldbstats;	/* local database entry */

	Assert(MyBackendType == B_BACKEND);

	if (session_end_type != DISCONNECT_NOT_YET)
		return;

	now = GetCurrentTimestamp();
	if (last_report == 0)
		last_report = MyStartTimestamp;
	TimestampDifference(last_report, now, &secs, &usecs);
	last_report = now;

	if (disconnect)
		session_end_type = pgStatSessionEndCause;

	ldbstats = pgstat_pending_db_prepare(MyDatabaseId);

	ldbstats->n_sessions = (last_report == 0 ? 1 : 0);
	ldbstats->total_session_time += secs * 1000000 + usecs;
	ldbstats->total_active_time += pgStatActiveTime;
	pgStatActiveTime = 0;
	ldbstats->total_idle_in_xact_time += pgStatTransactionIdleTime;
	pgStatTransactionIdleTime = 0;

	switch (session_end_type)
	{
		case DISCONNECT_NOT_YET:
		case DISCONNECT_NORMAL:
			/* we don't collect these */
			break;
		case DISCONNECT_CLIENT_EOF:
			ldbstats->n_sessions_abandoned++;
			break;
		case DISCONNECT_FATAL:
			ldbstats->n_sessions_fatal++;
			break;
		case DISCONNECT_KILLED:
			ldbstats->n_sessions_killed++;
			break;
	}
}


/*
 * pgstat_slru_index
 *
 * Determine index of entry for a SLRU with a given name. If there's no exact
 * match, returns index of the last "other" entry used for SLRUs defined in
 * external projects.
 */
int
pgstat_slru_index(const char *name)
{
	int			i;

	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		if (strcmp(slru_names[i], name) == 0)
			return i;
	}

	/* return index of the last entry (which is the "other" one) */
	return (SLRU_NUM_ELEMENTS - 1);
}

/*
 * pgstat_slru_name
 *
 * Returns SLRU name for an index. The index may be above SLRU_NUM_ELEMENTS,
 * in which case this returns NULL. This allows writing code that does not
 * know the number of entries in advance.
 */
const char *
pgstat_slru_name(int slru_idx)
{
	if (slru_idx < 0 || slru_idx >= SLRU_NUM_ELEMENTS)
		return NULL;

	return slru_names[slru_idx];
}

/*
 * slru_entry
 *
 * Returns pointer to entry with counters for given SLRU (based on the name
 * stored in SlruCtl as lwlock tranche name).
 */
static PgStat_SLRUStats *
slru_entry(int slru_idx)
{
	/*
	 * The postmaster should never register any SLRU statistics counts; if it
	 * did, the counts would be duplicated into child processes via fork().
	 */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	Assert((slru_idx >= 0) && (slru_idx < SLRU_NUM_ELEMENTS));

	return &pending_SLRUStats[slru_idx];
}

/*
 * SLRU statistics count accumulation functions --- called from slru.c
 */

void
pgstat_count_slru_page_zeroed(int slru_idx)
{
	slru_entry(slru_idx)->blocks_zeroed += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_hit(int slru_idx)
{
	slru_entry(slru_idx)->blocks_hit += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_exists(int slru_idx)
{
	slru_entry(slru_idx)->blocks_exists += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_read(int slru_idx)
{
	slru_entry(slru_idx)->blocks_read += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_written(int slru_idx)
{
	slru_entry(slru_idx)->blocks_written += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_flush(int slru_idx)
{
	slru_entry(slru_idx)->flush += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_truncate(int slru_idx)
{
	slru_entry(slru_idx)->truncate += 1;
	have_slrustats = true;
}



/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 *	Find database stats entry on backends in a palloc'ed memory.
 *
 *  The returned entry is stored in static memory so the content is valid until
 *	the next call of the same function for the different database.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dboid)
{
	return (PgStat_StatDBEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_DB, dboid, InvalidOid);
}

/* ----------
 * pgstat_fetch_stat_tabentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the activity statistics for one table or NULL. NULL doesn't mean
 *	that the table doesn't exist, it is just not yet known by the
 *	activity statistics facilities, so the caller is better off to
 *	report ZERO instead.
 * ----------
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	PgStat_StatTabEntry *tabentry;

	tabentry = pgstat_fetch_stat_tabentry_extended(false, relid);
	if (tabentry != NULL)
		return tabentry;

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	tabentry = pgstat_fetch_stat_tabentry_extended(true, relid);
	return tabentry;
}


/* ----------
 * pgstat_fetch_stat_tabentry_extended() -
 *
 *	Find table stats entry on backends in dbent. The returned entry is stored
 *	in static memory so the content is valid until the next call of the same
 *	function for the different table.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry_extended(bool shared, Oid reloid)
{
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	return (PgStat_StatTabEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_TABLE, dboid, reloid);
}


/* ----------
 * pgstat_fetch_stat_funcentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one function or NULL.
 *
 *  The returned entry is stored in static memory so the content is valid until
 *	the next call of the same function for the different function id.
 * ----------
 */
PgStat_StatFuncEntry *
pgstat_fetch_stat_funcentry(Oid func_id)
{
	return (PgStat_StatFuncEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_FUNCTION, MyDatabaseId, func_id);
}


static void
pgstat_snapshot_archiver(void)
{
	PgStat_ArchiverStats reset;
	PgStat_ArchiverStats *reset_offset = &StatsShmem->archiver.reset_offset;

	pgstat_copy_global_stats(&stats_snapshot.archiver,
							 &StatsShmem->archiver.stats,
							 sizeof(PgStat_ArchiverStats),
							 &StatsShmem->archiver.changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(PgStat_ArchiverStats));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	if (stats_snapshot.archiver.archived_count == reset.archived_count)
	{
		stats_snapshot.archiver.last_archived_wal[0] = 0;
		stats_snapshot.archiver.last_archived_timestamp = 0;
	}
	stats_snapshot.archiver.archived_count -= reset.archived_count;

	if (stats_snapshot.archiver.failed_count == reset.failed_count)
	{
		stats_snapshot.archiver.last_failed_wal[0] = 0;
		stats_snapshot.archiver.last_failed_timestamp = 0;
	}
	stats_snapshot.archiver.failed_count -= reset.failed_count;

	stats_snapshot.archiver.stat_reset_timestamp = reset.stat_reset_timestamp;
}


/*
 * ---------
 * pgstat_fetch_[cache_]stat_bgwriter() -
 *
 *	Support function for the SQL-callable pgstat* functions.  The returned
 *  entry is stored in static memory so the content is valid until the next
 *  call.
 * ---------
 */

static void
pgstat_snapshot_bgwriter(void)
{
	PgStat_BgWriterStats reset;
	PgStat_BgWriterStats *reset_offset = &StatsShmem->bgwriter.reset_offset;

	pgstat_copy_global_stats(&stats_snapshot.bgwriter,
							 &StatsShmem->bgwriter.stats,
							 sizeof(PgStat_BgWriterStats),
							 &StatsShmem->bgwriter.changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(PgStat_BgWriterStats));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	stats_snapshot.bgwriter.buf_written_clean -= reset.buf_written_clean;
	stats_snapshot.bgwriter.maxwritten_clean -= reset.maxwritten_clean;
	stats_snapshot.bgwriter.buf_alloc -= reset.buf_alloc;
	stats_snapshot.bgwriter.stat_reset_timestamp = reset.stat_reset_timestamp;
}

static void
pgstat_snapshot_checkpointer(void)
{
	PgStat_CheckPointerStats reset;
	PgStat_CheckPointerStats *reset_offset = &StatsShmem->checkpointer.reset_offset;

	pgstat_copy_global_stats(&stats_snapshot.checkpointer,
							 &StatsShmem->checkpointer.stats,
							 sizeof(PgStat_CheckPointerStats),
							 &StatsShmem->checkpointer.changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(PgStat_CheckPointerStats));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	stats_snapshot.checkpointer.timed_checkpoints -= reset.timed_checkpoints;
	stats_snapshot.checkpointer.requested_checkpoints -= reset.requested_checkpoints;
	stats_snapshot.checkpointer.buf_written_checkpoints -= reset.buf_written_checkpoints;
	stats_snapshot.checkpointer.buf_written_backend -= reset.buf_written_backend;
	stats_snapshot.checkpointer.buf_fsync_backend -= reset.buf_fsync_backend;
	stats_snapshot.checkpointer.checkpoint_write_time -= reset.checkpoint_write_time;
	stats_snapshot.checkpointer.checkpoint_sync_time -= reset.checkpoint_sync_time;
}

static void
pgstat_snapshot_wal(void)
{
	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&stats_snapshot.wal, &StatsShmem->wal.stats, sizeof(PgStat_WalStats));
	LWLockRelease(StatsLock);
}

static void
pgstat_snapshot_slru(void)
{
	LWLockAcquire(&StatsShmem->slru.lock, LW_SHARED);

	memcpy(stats_snapshot.slru, &StatsShmem->slru.stats, SizeOfSlruStats);

	LWLockRelease(&StatsShmem->slru.lock);
}

static void
pgstat_snapshot_replslot(void)
{
	if (stats_snapshot.replslot == NULL)
	{
		stats_snapshot.replslot = (PgStat_ReplSlotStats *)
			MemoryContextAlloc(TopMemoryContext,
							   sizeof(PgStat_ReplSlotStats) * max_replication_slots);
	}

	stats_snapshot.replslot_count = 0;

	LWLockAcquire(&StatsShmem->replslot.lock, LW_EXCLUSIVE);

	for (int i = 0; i < max_replication_slots; i++)
	{
		PgStat_ReplSlotStats *statent = &StatsShmem->replslot.stats[i];

		if (statent->index != -1)
		{
			stats_snapshot.replslot[stats_snapshot.replslot_count++] = *statent;
		}
	}

	LWLockRelease(&StatsShmem->replslot.lock);
}


PgStat_ArchiverStats *
pgstat_fetch_stat_archiver(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_ARCHIVER);

	return &stats_snapshot.archiver;
}

PgStat_BgWriterStats *
pgstat_fetch_stat_bgwriter(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_BGWRITER);

	return &stats_snapshot.bgwriter;
}


PgStat_CheckPointerStats *
pgstat_fetch_stat_checkpointer(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_CHECKPOINTER);

	return &stats_snapshot.checkpointer;
}

PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_WAL);

	return &stats_snapshot.wal;
}

PgStat_SLRUStats *
pgstat_fetch_slru(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_SLRU);

	return stats_snapshot.slru;
}

PgStat_ReplSlotStats *
pgstat_fetch_replslot(int *nslots_p)
{
	pgstat_snapshot_global(PGSTAT_KIND_REPLSLOT);

	*nslots_p = stats_snapshot.replslot_count;
	return stats_snapshot.replslot;
}

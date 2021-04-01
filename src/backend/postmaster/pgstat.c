/* ----------
 * pgstat.c
 *
 *	Activity Statistics facility.
 *
 *  Collects activity statistics, e.g. per-table access statistics, of
 *  all backends in shared memory. The activity numbers are first stored
 *  locally in each process, then flushed to shared memory at commit
 *  time or by idle-timeout.
 *
 * To avoid congestion on the shared memory, shared stats is updated no more
 * often than once per PGSTAT_MIN_INTERVAL (10000ms). If some local numbers
 * remain unflushed for lock failure, retry with intervals that is initially
 * PGSTAT_RETRY_MIN_INTERVAL (1000ms) then doubled at every retry. Finally we
 * force update after PGSTAT_MAX_INTERVAL (60000ms) since the first trial.
 *
 *  The first process that uses activity statistics facility creates the area
 *  then load the stored stats file if any, and the last process at shutdown
 *  writes the shared stats to the file then destroy the area before exit.
 *
 *	Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 *	src/backend/postmaster/pgstat.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "common/hashfn.h"
#include "lib/dshash.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_MIN_INTERVAL			10000	/* Minimum interval of stats data
											 * updates; in milliseconds. */

#define PGSTAT_RETRY_MIN_INTERVAL	1000	/* Initial retry interval after
											 * PGSTAT_MIN_INTERVAL */

#define PGSTAT_MAX_INTERVAL			60000	/* Longest interval of stats data
											 * updates */

/* ----------
 * The initial size hints for the hash tables used in the activity statistics.
 * ----------
 */
#define PGSTAT_TABLE_HASH_SIZE	512
#define PGSTAT_FUNCTION_HASH_SIZE	512


/* hash table entry for finding the PgStatSharedRef for a key */
typedef struct PgStatSharedRefHashEntry
{
	PgStatHashKey key;			/* hash key */
	char		status;			/* for simplehash use */
	PgStatSharedRef *shared_ref;
} PgStatSharedRefHashEntry;

/* hash table for statistics snapshots entry */
typedef struct PgStatSnapshotEntry
{
	PgStatHashKey key;
	char		status;			/* for simplehash use */
	void	   *data;			/* the stats data itself */
} PgStatSnapshotEntry;

/* entry type for oid hash */
typedef struct pgstat_oident
{
	Oid			oid;
	char		status;
} pgstat_oident;


typedef struct PgStat_PendingDroppedStatsItem
{
	PgStat_DroppedStatsItem item;
	dlist_node	node;
} PgStat_PendingDroppedStatsItem;


/* ----------
 * Shared memory struct for statistics
 * ----------
 */

StaticAssertDecl(sizeof(TimestampTz) == sizeof(pg_atomic_uint64),
				 "size of pg_atomic_uint64 doesn't match TimestampTz");



/* ----------
 * Hash Table Types
 * ----------
 */

/* for caching shared hashtable lookups */
#define SH_PREFIX pgstat_shared_ref_hash
#define SH_ELEMENT_TYPE PgStatSharedRefHashEntry
#define SH_KEY_TYPE PgStatHashKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) \
	hash_bytes((unsigned char *)&key, sizeof(PgStatHashKey))
#define SH_EQUAL(tb, a, b) (memcmp(&a, &b, sizeof(PgStatHashKey)) == 0)
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

/* for stats snapshot entries */
#define SH_PREFIX pgstat_snapshot
#define SH_ELEMENT_TYPE PgStatSnapshotEntry
#define SH_KEY_TYPE PgStatHashKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) \
	hash_bytes((unsigned char *)&key, sizeof(PgStatHashKey))
#define SH_EQUAL(tb, a, b) (memcmp(&a, &b, sizeof(PgStatHashKey)) == 0)
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

/* for OID hashes. */
StaticAssertDecl(sizeof(Oid) == 4, "oid is not compatible with uint32");
#define SH_PREFIX pgstat_oid
#define SH_ELEMENT_TYPE pgstat_oident
#define SH_KEY_TYPE Oid
#define SH_KEY oid
#define SH_HASH_KEY(tb, key) murmurhash32(key)
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"



/* ----------
 * Local function forward declarations
 * ----------
 */

static void pgstat_setup_memcxt(void);
static void pgstat_write_statsfile(void);
static void pgstat_read_statsfile(void);
static void pgstat_shutdown_hook(int code, Datum arg);

static PgStatShm_StatEntryHeader *pgstat_shared_stat_entry_init(PgStatTypes stattype,
																PgStatShmHashEntry *shhashent,
																int init_refcount);
static void pgstat_shared_ref_release(PgStatHashKey key, PgStatSharedRef *shared_ref);
static inline size_t shared_stat_entry_len(PgStatTypes stattype);
static inline void* shared_stat_entry_data(PgStatTypes stattype, PgStatShm_StatEntryHeader *entry);

static bool pgstat_shared_refs_need_gc(void);
static void pgstat_shared_refs_gc(void);

static void pgstat_shared_refs_release_all(void);

static bool pgstat_drop_stats_entry(dshash_seq_status *hstat);

static void pgstat_pending_delete(PgStatSharedRef *shared_ref);

static pgstat_oid_hash * collect_oids(Oid catalogid, AttrNumber anum_oid);

static bool pgstat_flush_object_stats(bool nowait);



/* ----------
 * GUC parameters
 * ----------
 */
bool		pgstat_track_counts = false;
int			pgstat_track_functions = TRACK_FUNC_OFF;
int			pgstat_fetch_consistency = STATS_FETCH_CONSISTENCY_NONE;

/* ----------
 * Built from GUC parameters
 * ----------
 */
char      *pgstat_stat_directory = NULL;

/* No longer used, but will be removed with GUC */
char      *pgstat_stat_filename = NULL;
char      *pgstat_stat_tmpname = NULL;


/* ----------
 * Stats shared memory state
 * ----------
 */

/* backend-lifetime storages */
StatsShmemStruct *StatsShmem = NULL;
static dsa_area *StatsDSA = NULL;
/* The shared hash to index activity stats entries. */
static dshash_table *pgStatSharedHash = NULL;



/* ----------
 * Local variables for the stats subsystem
 *
 * NB: This is only for generic infrastructure, not for specific types of
 * stats.
 * ----------
 */

/*
 * Backend local references to shared stats entries. If there are pending
 * updates to a stats entry, the PgStatSharedRef is added to the pgStatPending
 * list.
 *
 * When a stats entry is dropped each backend needs to release its reference
 * to it before the memory can be released. To trigger that
 * StatsShmem->gc_count is incremented - which each backend compares to their
 * copy of pgStatSharedRefAge on a regular basis.
 */
static pgstat_shared_ref_hash_hash *pgStatSharedRefHash = NULL;
static int	pgStatSharedRefAge = 0;	/* cache age of pgStatShmLookupCache */

/*
 * List of PgStatSharedRefs with unflushed pending stats.
 *
 * Newly pending entries should only ever be added to the end of the list,
 * otherwise pgstat_flush_object_stats() might not see them immediately.
 */
static dlist_head pgStatPending = DLIST_STATIC_INIT(pgStatPending);

/*
 * Memory context containing the pgStatSharedRefHash table, the
 * pgStatSharedRef entries, and pending data. Mostly to make it easier to
 * track memory usage.
 */
static MemoryContext pgStatSharedRefContext = NULL;


static PgStat_SubXactStatus *pgStatXactStack = NULL;


/*
 * Force the next stats flush to happen regardless of
 * PGSTAT_MIN_INTERVAL. Useful in test scripts.
 */
static bool pgStatForceNextFlush = false;


/*
 * Memory context containing stats snapshot related data, to free snapshot in
 * bulk.
 */
static MemoryContext pgStatSnapshotContext = NULL;


/* the current statistics snapshot */
PgStatSnapshot stats_snapshot;



/* ----------
 * Constants
 * ----------
 */

/* parameter for the shared hash */
static const dshash_parameters dsh_params = {
	sizeof(PgStatHashKey),
	sizeof(PgStatShmHashEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_STATS
};


/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/*
 * The size of the shared memory allocation for stats stored in the shared
 * stats hash table. This allocation will be done as part of the main shared
 * memory, rather than dynamic shared memory, allowing it to be initialized in
 * postmaster.
 */
static Size
stats_dsa_init_size(void)
{
	return dsa_minimum_size() + 2 * 1024 * 1024;
}

static Size
stats_replslot_size(void)
{
	return sizeof(PgStat_ReplSlotStats) * max_replication_slots;
}

/*
 * StatsShmemSize
 *		Compute shared memory space needed for activity statistic
 */
Size
StatsShmemSize(void)
{
	Size		sz;

	sz = MAXALIGN(sizeof(StatsShmemStruct));
	sz = add_size(sz, MAXALIGN(stats_dsa_init_size()));
	sz = add_size(sz, MAXALIGN(stats_replslot_size()));

	return sz;
}

/*
 * StatsShmemInit - initialize during shared-memory creation
 */
void
StatsShmemInit(void)
{
	bool		found;
	Size		sz;

	sz = StatsShmemSize();
	StatsShmem = (StatsShmemStruct *)
		ShmemInitStruct("Stats area", sz, &found);

	if (!IsUnderPostmaster)
	{
		dsa_area *dsa;
		dshash_table *dsh;
		char *p = (char *) StatsShmem;

		Assert(!found);

		/* the allocation of StatsShmem itself */
		p += MAXALIGN(sizeof(StatsShmemStruct));

		/*
		 * Create a small dsa allocation in plain shared memory. Doing so
		 * initially makes it easier to manage server startup, and it also is
		 * a small efficiency win.
		 */
		StatsShmem->raw_dsa_area = p;
		p += MAXALIGN(stats_dsa_init_size());
		dsa = dsa_create_in_place(StatsShmem->raw_dsa_area,
								  stats_dsa_init_size(),
								  LWTRANCHE_STATS, 0);
		dsa_pin(dsa);

		/*
		 * Same with the dshash table.
		 *
		 * FIXME: we need to guarantee this can be allocated in plain shared
		 * memory, rather than allocating dsm segments.
		 */
		dsh = dshash_create(dsa, &dsh_params, 0);
		StatsShmem->hash_handle = dshash_get_hash_table_handle(dsh);


		/*
		 * Postmaster will never access these again, thus free the local
		 * dsa/dshash references.
		 */
		dshash_detach(dsh);
		dsa_detach(dsa);

		pg_atomic_init_u64(&StatsShmem->gc_count, 0);


		/*
		 * Initialize global statistics.
		 */

		StatsShmem->replslot.stats = (PgStat_ReplSlotStats *) p;
		p += MAXALIGN(stats_replslot_size());
		LWLockInitialize(&StatsShmem->replslot.lock, LWTRANCHE_STATS);
		for (int i = 0; i < max_replication_slots; i++)
		{
			StatsShmem->replslot.stats[i].index = -1;
		}

		LWLockInitialize(&StatsShmem->slru.lock, LWTRANCHE_STATS);

		LWLockInitialize(&StatsShmem->wal.lock, LWTRANCHE_STATS);
	}
	else
	{
		Assert(found);
	}
}



/* ------------------------------------------------------------
 * Public functions used by backends follow
 *------------------------------------------------------------
 */

/*
 * pgstat_restore_stats() - read on-disk stats into memory at server start.
 *
 * Should only be called by the startup process or in single user mode.
 */
void
pgstat_restore_stats(void)
{
	pgstat_read_statsfile();
}

/*
 * pgstat_discard_stats() -
 *
 * Remove the stats file.  This is currently used only if WAL recovery is
 * needed after a crash.
 *
 * Should only be called by the startup process or in single user mode.
 */
void
pgstat_discard_stats(void)
{
	int ret;

	/* NB: this needs to be done even in single user mode */

	ret = unlink(PGSTAT_STAT_PERMANENT_FILENAME);
	if (ret != 0)
	{
		if (errno == ENOENT)
			elog(DEBUG2,
				 "didn't need to unlink permanent stats file \"%s\" - didn't exist",
				 PGSTAT_STAT_PERMANENT_FILENAME);
		else
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not unlink permanent statistics file \"%s\": %m",
							PGSTAT_STAT_PERMANENT_FILENAME)));
	}
	else
	{
		ereport(DEBUG2,
				(errcode_for_file_access(),
				 errmsg("unlinked permanent statistics file \"%s\": %m",
						PGSTAT_STAT_PERMANENT_FILENAME)));
	}
}

/*
 * pgstat_write_stats() - write in-memory stats onto disk during shutdown.
 */
void
pgstat_write_stats(void)
{
	pgstat_write_statsfile();
}

/* ----------
 * pgstat_initialize() -
 *
 *	Initialize pgstats state, and set up our on-proc-exit hook.
 *	Called from InitPostgres and AuxiliaryProcessMain. For auxiliary process,
 *	MyBackendId is invalid. Otherwise, MyBackendId must be set,
 *	but we must not have started any transaction yet (since the
 *	exit hook must run after the last transaction exit).
 *	NOTE: MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
 * ----------
 */
void
pgstat_initialize(void)
{
	MemoryContext oldcontext;

	/* should only get initialized once */
	Assert(StatsDSA == NULL);

	/* stats shared memory persists for the backend lifetime */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	StatsDSA = dsa_attach_in_place(StatsShmem->raw_dsa_area, NULL);
	dsa_pin_mapping(StatsDSA);

	pgStatSharedHash = dshash_attach(StatsDSA, &dsh_params,
									 StatsShmem->hash_handle, 0);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_report_wal() can
	 * calculate how much pgWalUsage counters are increased by substracting
	 * prevWalUsage from pgWalUsage.
	 */
	prevWalUsage = pgWalUsage;

	/* need to be called before dsm shutdown */
	before_shmem_exit(pgstat_shutdown_hook, 0);
}

void
pgstat_schedule_drop(PgStatTypes stattype, Oid dboid, Oid objoid)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PgStat_SubXactStatus *xact_state;
	PgStat_PendingDroppedStatsItem *drop = (PgStat_PendingDroppedStatsItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PgStat_PendingDroppedStatsItem));

	xact_state = get_tabstat_stack_level(nest_level);

	drop->item.type = stattype;
	drop->item.dboid = dboid;
	drop->item.objoid = objoid;

	dlist_push_tail(&xact_state->pending_drops, &drop->node);
	xact_state->pending_drops_count++;
}

int
pgstat_pending_stats_drops(PgStat_DroppedStatsItem **items)
{
	PgStat_SubXactStatus *xact_state = pgStatXactStack;
	int nitems = 0;
	dlist_iter iter;

	if (xact_state == NULL)
		return 0;

	Assert(xact_state->nest_level == 1);
	Assert(xact_state->prev == NULL);

	*items = palloc(xact_state->pending_drops_count
					* sizeof(PgStat_PendingDroppedStatsItem));

	dlist_foreach(iter, &xact_state->pending_drops)
	{
		PgStat_PendingDroppedStatsItem *pending =
			dlist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);

		Assert(nitems < xact_state->pending_drops_count);
		(*items)[nitems++] = pending->item;
	}

	Assert(nitems == xact_state->pending_drops_count);

	return nitems;
}

static void
pgstat_perform_drop(PgStat_DroppedStatsItem *drop)
{
	PgStatShmHashEntry *shent;
	PgStatHashKey key;

	key.type = drop->type;
	key.dboid = drop->dboid;
	key.objoid = drop->objoid;

	if (pgStatSharedRefHash)
	{
		PgStatSharedRefHashEntry *lohashent;

		lohashent = pgstat_shared_ref_hash_lookup(pgStatSharedRefHash, key);

		if (lohashent)
		{
			if (lohashent->shared_ref && lohashent->shared_ref->pending)
				pgstat_pending_delete(lohashent->shared_ref);

			pgstat_shared_ref_release(lohashent->key, lohashent->shared_ref);
		}
	}

	shent = dshash_find(pgStatSharedHash, &key, true);
	if (shent)
	{
		dsa_pointer pdsa;

		Assert(shent->body != InvalidDsaPointer);
		pdsa = shent->body;

		/*
		 * Signal that the entry is dropped - this will eventually cause other
		 * backends to release their references.
		 */

		if (shent->dropped)
			elog(ERROR, "can only drop stats once");
		shent->dropped = true;

		if (pg_atomic_fetch_sub_u32(&shent->refcount, 1) == 1)
		{
			dshash_delete_entry(pgStatSharedHash, shent);
			dsa_free(StatsDSA, pdsa);
		}
		else
		{
			dshash_release_lock(pgStatSharedHash, shent);
		}
	}
}

void
pgstat_perform_drops(int ndrops, struct PgStat_DroppedStatsItem *items, bool is_redo)
{
	if (ndrops == 0)
		return;

	for (int i = 0; i < ndrops; i++)
		pgstat_perform_drop(&items[i]);

	pg_atomic_fetch_add_u64(&StatsShmem->gc_count, 1);
}

static void
pgstat_eoxact_stats_drops(PgStat_SubXactStatus *xact_state, bool isCommit)
{
	dlist_mutable_iter iter;

	if (xact_state->pending_drops_count == 0)
	{
		Assert(dlist_is_empty(&xact_state->pending_drops));
		return;
	}

	dlist_foreach_modify(iter, &xact_state->pending_drops)
	{
		PgStat_PendingDroppedStatsItem *pending =
			dlist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);

		elog(DEBUG2, "dropping stats %u/%u/%u",
			 pending->item.type,
			 pending->item.dboid,
			 pending->item.objoid);

		if (isCommit)
		{
			pgstat_perform_drop(&pending->item);
		}

		dlist_delete(&pending->node);
		xact_state->pending_drops_count--;
		pfree(pending);
	}

	pg_atomic_fetch_add_u64(&StatsShmem->gc_count, 1);
}

/* ----------
 * pgstat_drop_database() -
 *
 *	Remove entry for the database that we just dropped.
 *
 *  Some entries might be left alone due to lock failure or some stats are
 *	flushed after this but we will still clean the dead DB eventually via
 *	future invocations of pgstat_vacuum_stat().
 *	----------
 */
void
pgstat_drop_database(Oid dboid)
{
	dshash_seq_status hstat;
	PgStatShmHashEntry *p;
	uint64		not_freed_count = 0;

	Assert(OidIsValid(dboid));

	Assert(pgStatSharedHash != NULL);

	/*
	 * FIXME: need to do this using the transactional mechanism. Not such much
	 * because of rollbacks, but so the stats are removed on a standby
	 * too. Maybe a dedicated drop type?
	 */

	/* some of the dshash entries are to be removed, take exclusive lock. */
	dshash_seq_init(&hstat, pgStatSharedHash, true);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		if (p->dropped)
			continue;

		if (p->key.dboid == dboid)
		{
			/*
			 * Even statistics for a dropped database might currently be
			 * accessed (consider e.g. database stats for pg_stat_database).
			 */
			if (!pgstat_drop_stats_entry(&hstat))
				not_freed_count++;
		}
	}
	dshash_seq_term(&hstat);

	/*
	 * If some of the stats data could not be freed, signal the reference
	 * holders to run garbage collection of their cached pgStatShmLookupCache.
	 */
	if (not_freed_count > 0)
		pg_atomic_fetch_add_u64(&StatsShmem->gc_count, 1);
}

/* ----------
 * pgstat_report_stat() -
 *
 *	Must be called by processes that performs DML: tcop/postgres.c, logical
 *	receiver processes, SPI worker, etc. to apply the so far collected
 *	per-table and function usage statistics to the shared statistics hashes.
 *
 *	Updates are applied not more frequent than the interval of
 *	PGSTAT_MIN_INTERVAL milliseconds. They are also postponed on lock
 *	failure if force is false and there's no pending updates longer than
 *	PGSTAT_MAX_INTERVAL milliseconds. Postponed updates are retried in
 *	succeeding calls of this function.
 *
 *	Returns the time until the next timing when updates are applied in
 *	milliseconds if there are no updates held for more than
 *	PGSTAT_MIN_INTERVAL milliseconds.
 *
 *	Note that this is called only out of a transaction, so it is fine to use
 *	transaction stop time as an approximation of current time.
 *	----------
 */
long
pgstat_report_stat(bool force)
{
	static TimestampTz next_flush = 0;
	static TimestampTz pending_since = 0;
	static long retry_interval = 0;
	bool		partial_flush;
	TimestampTz now;
	bool		nowait;
	int			i;
	uint64		oldval;

	/* "absorb" the forced flush even if there's nothing to flush */
	if (pgStatForceNextFlush)
	{
		force = true;
		pgStatForceNextFlush = false;
	}

	/* Don't expend a clock check if nothing to do */
	if (dlist_is_empty(&pgStatPending) &&
		!have_slrustats
		&& !walstats_pending())
	{
		return 0;
	}

	now = GetCurrentTransactionStopTimestamp();

	if (!force)
	{
		/*
		 * Don't flush stats too frequently.  Return the time to the next
		 * flush.
		 */
		if (now < next_flush)
		{
			/* Record the epoch time if retrying. */
			if (pending_since == 0)
				pending_since = now;

			return (next_flush - now) / 1000;
		}

		/* But, don't keep pending updates longer than PGSTAT_MAX_INTERVAL. */

		if (pending_since > 0 &&
			TimestampDifferenceExceeds(pending_since, now, PGSTAT_MAX_INTERVAL))
			force = true;
	}

	/* for backends, update connection statistics */
	if (MyBackendType == B_BACKEND)
		pgstat_update_connstats(false);

	/* don't wait for lock acquisition when !force */
	nowait = !force;

	partial_flush = false;

	/* flush database / relation / function stats */
	partial_flush |= pgstat_flush_object_stats(nowait);

	/* flush wal stats */
	partial_flush |= pgstat_flush_wal(nowait);

	/* flush SLRU stats */
	partial_flush |= pgstat_flush_slru(nowait);

	/*
	 * Publish the time of the last flush, but we don't notify the change of
	 * the timestamp itself. Readers will get sufficiently recent timestamp.
	 * If we failed to update the value, concurrent processes should have
	 * updated it to sufficiently recent time.
	 *
	 * XXX: The loop might be unnecessary for the reason above.
	 */
	oldval = pg_atomic_read_u64(&StatsShmem->stats_timestamp);

	for (i = 0; i < 10; i++)
	{
		if (oldval >= now ||
			pg_atomic_compare_exchange_u64(&StatsShmem->stats_timestamp,
										   &oldval, (uint64) now))
			break;
	}

	/*
	 * Some of the pending stats may have not been flushed due to lock
	 * contention.  If we have such pending stats here, let the caller know
	 * the retry interval.
	 */
	if (partial_flush)
	{
		/* Retain the epoch time */
		if (pending_since == 0)
			pending_since = now;

		/* The interval is doubled at every retry. */
		if (retry_interval == 0)
			retry_interval = PGSTAT_RETRY_MIN_INTERVAL * 1000;
		else
			retry_interval = retry_interval * 2;

		/*
		 * Determine the next retry interval so as not to get shorter than the
		 * previous interval.
		 */
		if (!TimestampDifferenceExceeds(pending_since,
										now + 2 * retry_interval,
										PGSTAT_MAX_INTERVAL))
			next_flush = now + retry_interval;
		else
		{
			next_flush = pending_since + PGSTAT_MAX_INTERVAL * 1000;
			retry_interval = next_flush - now;
		}

		return retry_interval / 1000;
	}

	/* Set the next time to update stats */
	next_flush = now + PGSTAT_MIN_INTERVAL * 1000;
	retry_interval = 0;
	pending_since = 0;

	return 0;
}

/* ----------
 * pgstat_clear_snapshot() -
 *
 *	Discard any data collected in the current transaction.  Any subsequent
 *	request will cause new snapshots to be read.
 *
 *	This is also invoked during transaction commit or abort to discard
 *	the no-longer-wanted snapshot.
 * ----------
 */
void
pgstat_clear_snapshot(void)
{
	memset(&stats_snapshot.global_valid, 0, sizeof(stats_snapshot.global_valid));
	stats_snapshot.stats = NULL;
	stats_snapshot.mode = STATS_FETCH_CONSISTENCY_NONE;

	/* Release memory, if any was allocated */
	if (pgStatSnapshotContext)
	{
		MemoryContextDelete(pgStatSnapshotContext);

		/* Reset variables */
		pgStatSnapshotContext = NULL;
	}

	/* forward to stats sub-subsystems */
	pgbestat_clear_snapshot();
}

/*
 * Force locally pending stats to be flushed during the next
 * pgstat_report_stat() call. This is useful for writing tests.
 */
void
pgstat_force_next_flush(void)
{
	pgStatForceNextFlush = true;
}

/* ----------
 * pgstat_vacuum_stat() -
 *
 *  Delete shared stat entries that are not in system catalogs.
 *
 *  To avoid holding exclusive lock on dshash for a long time, the process is
 *  performed in three steps.
 *
 *   1: Collect existent oids of every kind of object.
 *   2: Collect victim entries by scanning with shared lock.
 *   3: Try removing every nominated entry without waiting for lock.
 *
 *  As the consequence of the last step, some entries may be left alone due to
 *  lock failure, but as explained by the comment of pgstat_vacuum_stat, they
 *  will be deleted by later vacuums.
 * ----------
 */
void
pgstat_vacuum_stat(void)
{
#ifdef NOT_USED
	pgstat_oid_hash *dboids;		/* database ids */
	pgstat_oid_hash *relids;	/* relation ids in the current database */
	pgstat_oid_hash *funcids;	/* function ids in the current database */
	uint64		not_freed_count = 0;
	dshash_seq_status dshstat;
	PgStatShmHashEntry *ent;

	/* collect oids of existent objects */
	dboids = collect_oids(DatabaseRelationId, Anum_pg_database_oid);
	relids = collect_oids(RelationRelationId, Anum_pg_class_oid);
	funcids = collect_oids(ProcedureRelationId, Anum_pg_proc_oid);

	/* some of the dshash entries are to be removed, take exclusive lock. */
	dshash_seq_init(&dshstat, pgStatSharedHash, true);
	while ((ent = dshash_seq_next(&dshstat)) != NULL)
	{
		PgStatShm_StatEntryHeader *header;

		CHECK_FOR_INTERRUPTS();

		header = dsa_get_address(StatsDSA, ent->body);
		if (header->dropped)
			continue;

		/*
		 * Don't drop entries for other than database objects not of the
		 * current database.
		 */
		if (ent->key.type != PGSTAT_TYPE_DB &&
			ent->key.dboid != MyDatabaseId)
			continue;

		switch (ent->key.type)
		{
			case PGSTAT_TYPE_DB:
				/*
				 * don't remove database entry for shared tables and existing
				 * tables
				 */
				if (ent->key.dboid == 0 ||
					pgstat_oid_lookup(dboids, ent->key.dboid) != NULL)
					continue;

				break;

			case PGSTAT_TYPE_TABLE:
				/* don't remove existing relations */
				if (pgstat_oid_lookup(relids, ent->key.objoid) != NULL)
					continue;

				break;

			case PGSTAT_TYPE_FUNCTION:
				/* don't remove existing functions  */
				if (pgstat_oid_lookup(funcids, ent->key.objoid) != NULL)
					continue;

				break;
			default:
				elog(ERROR, "unexpected");
				break;
		}

		/* drop this entry */
		if (!pgstat_drop_stats_entry(&dshstat))
			not_freed_count++;
	}
	dshash_seq_term(&dshstat);
	pgstat_oid_destroy(dboids);
	pgstat_oid_destroy(relids);
	pgstat_oid_destroy(funcids);

	/*
	 * If some of the stats data could not be freed, signal the reference
	 * holders to run garbage collection of their cached pgStatShmLookupCache.
	 */
	if (not_freed_count > 0)
		pg_atomic_fetch_add_u64(&StatsShmem->gc_count, 1);
#endif
}


/* ------------------------------------------------------------
 * Stats reset functions
 * ------------------------------------------------------------
 */

/* ----------
 * pgstat_reset_counters() -
 *
 *	Reset counters for our database.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_counters(void)
{
	dshash_seq_status hstat;
	PgStatShmHashEntry *p;

	/* dshash entry is not modified, take shared lock */
	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		PgStatShm_StatEntryHeader *header;

		if (p->key.dboid != MyDatabaseId)
			continue;

		header = dsa_get_address(StatsDSA, p->body);

		if (p->dropped)
			continue;

		LWLockAcquire(&p->lock, LW_EXCLUSIVE);
		memset(shared_stat_entry_data(p->key.type, header), 0,
			   shared_stat_entry_len(p->key.type));

		if (p->key.type == PGSTAT_TYPE_DB)
		{
			PgStatShm_StatDBEntry *dbstat = (PgStatShm_StatDBEntry *) header;

			dbstat->stats.stat_reset_timestamp = GetCurrentTimestamp();
		}
		LWLockRelease(&p->lock);
	}
	dshash_seq_term(&hstat);
}

/* ----------
 * pgstat_reset_single_counter() -
 *
 *	Reset a single counter.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_single_counter(Oid objoid, PgStat_Single_Reset_Type type)
{
	PgStatSharedRef *db_ref;
	PgStatSharedRef *counter_ref;

	PgStatShm_StatEntryHeader *header;
	PgStatShm_StatDBEntry *dbentry;
	PgStatTypes stattype;
	TimestampTz ts = GetCurrentTimestamp();

	db_ref = pgstat_shared_ref_get(PGSTAT_TYPE_DB, MyDatabaseId, InvalidOid,
								   false);
	if (db_ref == NULL)
		return;

	dbentry = (PgStatShm_StatDBEntry *) db_ref->shared_stats;

	/* Set the reset timestamp for the whole database */
	pgstat_shared_stat_lock(db_ref, false);
	dbentry->stats.stat_reset_timestamp = ts;
	pgstat_shared_stat_unlock(db_ref);

	/* Remove object if it exists, ignore if not */
	switch (type)
	{
		case RESET_TABLE:
			stattype = PGSTAT_TYPE_TABLE;
			break;
		case RESET_FUNCTION:
			stattype = PGSTAT_TYPE_FUNCTION;
			break;
		default:
			return;
	}

	counter_ref = pgstat_shared_ref_get(stattype, MyDatabaseId, objoid, false);
	if (!counter_ref || counter_ref->shared_entry->dropped)
		return;

	pgstat_shared_stat_lock(counter_ref, false);

	header = counter_ref->shared_stats;
	memset(shared_stat_entry_data(stattype, header), 0,
		   shared_stat_entry_len(stattype));

	pgstat_shared_stat_unlock(counter_ref);
}


/* ------------------------------------------------------------
 * Helper functions
 *------------------------------------------------------------
 */


/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create pgStatSnapshotContext if not already done.
 * ----------
 */
static void
pgstat_setup_memcxt(void)
{
	if (unlikely(!pgStatSharedRefContext))
		pgStatSharedRefContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "Backend statistics data",
								  ALLOCSET_SMALL_SIZES);

	if (unlikely(!pgStatSnapshotContext))
		pgStatSnapshotContext =
			AllocSetContextCreate(TopMemoryContext,
								  "Backend statistics snapshot",
								  ALLOCSET_SMALL_SIZES);
}


/* ----------
 * pgstat_write_statsfile() -
 *		Write the global statistics file, as well as DB files.
 *
 * This function is called in the last process that is accessing the shared
 * stats so locking is not required.
 * ----------
 */
static void
pgstat_write_statsfile(void)
{
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = PGSTAT_STAT_PERMANENT_TMPFILE;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	int			rc;
	dshash_seq_status hstat;
	PgStatShmHashEntry *ps;

	Assert(StatsDSA);

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Set the timestamp of the stats file.
	 */
	pg_atomic_write_u64(&StatsShmem->stats_timestamp, GetCurrentTimestamp());

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write bgwriter global stats struct
	 */
	rc = fwrite(&StatsShmem->bgwriter.stats, sizeof(PgStat_BgWriterStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write checkpointer global stats struct
	 */
	rc = fwrite(&StatsShmem->checkpointer.stats, sizeof(PgStat_CheckPointerStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write archiver global stats struct
	 */
	rc = fwrite(&StatsShmem->archiver.stats, sizeof(PgStat_ArchiverStats), 1,
				fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write WAL global stats struct
	 */
	rc = fwrite(&StatsShmem->wal.stats, sizeof(PgStat_WalStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write SLRU stats struct
	 */
	rc = fwrite(&StatsShmem->slru.stats,
				sizeof(PgStat_SLRUStats[SLRU_NUM_ELEMENTS]),
				1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the stats entry
	 */
	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((ps = dshash_seq_next(&hstat)) != NULL)
	{
		PgStatShm_StatEntryHeader *shstats;
		size_t		len;

		CHECK_FOR_INTERRUPTS();

		/* we may have some "dropped" entries not yet removed, skip them */
		if (ps->dropped)
			continue;

		shstats = (PgStatShm_StatEntryHeader *) dsa_get_address(StatsDSA, ps->body);

		/* if not dropped the valid-entry refcount should exist */
		Assert(pg_atomic_read_u32(&ps->refcount) > 0);

		fputc('S', fpout);
		rc = fwrite(&ps->key, sizeof(PgStatHashKey), 1, fpout);

		/* Write except the header part of the etnry */
		len = shared_stat_entry_len(ps->key.type);
		rc = fwrite(shared_stat_entry_data(ps->key.type, shstats), len, 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}
	dshash_seq_term(&hstat);

	/*
	 * Write replication slot stats struct
	 */
	for (int i = 0; i < max_replication_slots; i++)
	{
		PgStat_ReplSlotStats *statent = &StatsShmem->replslot.stats[i];

		if (statent->index == -1)
			continue;

		fputc('R', fpout);
		rc = fwrite(statent, sizeof(*statent), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}
}

/* ----------
 * pgstat_read_statsfile() -
 *
 *	Reads in existing activity statistics file into the shared stats hash.
 *
 * This function is called in the only process that is accessing the shared
 * stats so locking is not required.
 * ----------
 */
static void
pgstat_read_statsfile(void)
{
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;

	/* shouldn't be called from postmaster */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	elog(DEBUG2, "reading stats file \"%s\"", statfile);

	/*
	 * Set the current timestamp (will be kept only in case we can't load an
	 * existing statsfile).
	 */
	StatsShmem->bgwriter.stats.stat_reset_timestamp = GetCurrentTimestamp();
	StatsShmem->archiver.stats.stat_reset_timestamp =
		StatsShmem->bgwriter.stats.stat_reset_timestamp;
	StatsShmem->wal.stats.stat_reset_timestamp =
		StatsShmem->bgwriter.stats.stat_reset_timestamp;

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * returns zero for anything and the activity statistics simply starts
	 * from scratch with empty counters.
	 *
	 * ENOENT is a possibility if the activity statistics is not running or
	 * has not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * Read bgwiter stats struct
	 */
	if (fread(&StatsShmem->bgwriter.stats, 1, sizeof(PgStat_BgWriterStats), fpin) !=
		sizeof(PgStat_BgWriterStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->bgwriter.stats, 0, sizeof(PgStat_BgWriterStats));
		goto done;
	}

	/*
	 * Read checkpointer stats struct
	 */
	if (fread(&StatsShmem->checkpointer.stats, 1, sizeof(PgStat_CheckPointerStats), fpin) !=
		sizeof(PgStat_CheckPointerStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->checkpointer.stats, 0, sizeof(PgStat_CheckPointerStats));
		goto done;
	}

	/*
	 * Read archiver stats struct
	 */
	if (fread(&StatsShmem->archiver.stats, 1, sizeof(PgStat_ArchiverStats),
			  fpin) != sizeof(PgStat_ArchiverStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->archiver.stats, 0, sizeof(PgStat_ArchiverStats));
		goto done;
	}

	/*
	 * Read WAL stats struct
	 */
	if (fread(&StatsShmem->wal.stats, 1, sizeof(PgStat_WalStats), fpin)
		!= sizeof(PgStat_WalStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->wal.stats, 0, sizeof(PgStat_WalStats));
		goto done;
	}

	/*
	 * Read SLRU stats struct
	 */
	if (fread(&StatsShmem->slru.stats, 1, SizeOfSlruStats, fpin) != SizeOfSlruStats)
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * We found an existing activity statistics file. Read it and put all the
	 * hash table entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
			case 'S':
				{
					PgStatHashKey key;
					PgStatShmHashEntry *p;
					PgStatShm_StatEntryHeader *header;
					size_t		len;

					CHECK_FOR_INTERRUPTS();

					if (fread(&key, 1, sizeof(key), fpin) != sizeof(key))
					{
						ereport(LOG,
								(errmsg("corrupted statistics file \"%s\"", statfile)));
						goto done;
					}

					p = dshash_find_or_insert(pgStatSharedHash, &key, &found);

					/* don't allow duplicate entries */
					if (found)
					{
						ereport(LOG,
								(errmsg("corrupted statistics file \"%s\"",
										statfile)));
						goto done;
					}

					header = pgstat_shared_stat_entry_init(key.type, p, 1);
					dshash_release_lock(pgStatSharedHash, p);

					/* Avoid overwriting header part */
					len = shared_stat_entry_len(key.type);

					if (fread(shared_stat_entry_data(key.type, header), 1, len, fpin) != len)
					{
						ereport(LOG,
								(errmsg("corrupted statistics file \"%s\"", statfile)));
						goto done;
					}

					break;
				}

			case 'R':
				{
					PgStat_ReplSlotStats tmp;

					if (fread(&tmp, 1, sizeof(tmp), fpin) != sizeof(tmp))
					{
						ereport(LOG,
								(errmsg("corrupted statistics file \"%s\"", statfile)));
						goto done;
					}

					if (tmp.index < max_replication_slots)
						StatsShmem->replslot.stats[tmp.index] = tmp;
				}
				break;

			case 'E':
				goto done;

			default:
				ereport(LOG,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);

	elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
	unlink(statfile);

	return;
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to shared stats.  Without this,
 * operations triggered during backend exit (such as temp table deletions)
 * won't be counted.
 */
static void
pgstat_shutdown_hook(int code, Datum arg)
{
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	/*
	 * If we got as far as discovering our own database ID, we can report what
	 * we did to the shared stats.  Otherwise, we'd be sending an invalid
	 * database ID, so forget it.  (This means that accesses to pg_database
	 * during failed backend starts might never get counted.)
	 */
	if (OidIsValid(MyDatabaseId))
	{
		if (MyBackendType == B_BACKEND)
			pgstat_update_connstats(true);
	}

	pgstat_report_stat(true);

	/*
	 * We need to clean up temporary slots before detaching shared statistics
	 * so that the statistics for temporary slots are properly removed.
	 */
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	ReplicationSlotCleanup();

	Assert(StatsDSA);

	/* We shouldn't leave a reference to shared stats. */
	pgstat_shared_refs_release_all();

	dshash_detach(pgStatSharedHash);
	pgStatSharedHash = NULL;

	/* We are going to exit. Don't bother destroying local hashes. */
	dlist_init(&pgStatPending);

	dsa_detach(StatsDSA);
	StatsDSA = NULL;
}

static PgStatShm_StatEntryHeader *
pgstat_shared_stat_entry_init(PgStatTypes stattype,
							  PgStatShmHashEntry *shhashent,
							  int init_refcount)
{
	/* Create new stats entry. */
	dsa_pointer chunk;
	PgStatShm_StatEntryHeader *shheader;

	LWLockInitialize(&shhashent->lock, LWTRANCHE_STATS);
	pg_atomic_init_u32(&shhashent->refcount, init_refcount);
	shhashent->dropped = false;

	chunk = dsa_allocate0(StatsDSA, pgstat_types[stattype].shared_size);
	shheader = dsa_get_address(StatsDSA, chunk);
	shheader->magic = 0xdeadbeef;

	/* Link the new entry from the hash entry. */
	shhashent->body = chunk;

	return shheader;
}

/*
 * Helper function for pgstat_shared_ref_get().
 */
static bool
pgstat_shared_ref_get_cached(PgStatHashKey key, PgStatSharedRef **shared_ref_p)
{
	bool found;
	PgStatSharedRefHashEntry *cache_entry;

	pgstat_setup_memcxt();

	if (!pgStatSharedRefHash)
	{
		pgStatSharedRefHash =
			pgstat_shared_ref_hash_create(pgStatSharedRefContext,
										  PGSTAT_TABLE_HASH_SIZE, NULL);
		pgStatSharedRefAge =
			pg_atomic_read_u64(&StatsShmem->gc_count);
	}

	/*
	 * pgStatSharedRefAge increments quite slowly than the time the
	 * following loop takes so this is expected to iterate no more than
	 * twice.
	 *
	 * XXX: Why is this a good place to do this?
	 */
	while (pgstat_shared_refs_need_gc())
		pgstat_shared_refs_gc();

	/*
	 * We immediately insert a cache entry, because it avoids 1) multiple
	 * hashtable lookups in case of a cache miss 2) having to deal with
	 * out-of-memory errors after incrementing
	 * PgStatShm_StatEntryHeader->refcount.
	 */

	cache_entry = pgstat_shared_ref_hash_insert(pgStatSharedRefHash, key, &found);

	if (!found || !cache_entry->shared_ref)
	{
		PgStatSharedRef *shared_ref;

		cache_entry->shared_ref = shared_ref =
			MemoryContextAlloc(pgStatSharedRefContext,
							   sizeof(PgStatSharedRef));
		shared_ref->shared_stats = NULL;
		shared_ref->shared_entry = NULL;
		shared_ref->pending = NULL;

		found = false;
	}
	else if (cache_entry->shared_ref->shared_stats == NULL)
	{
		Assert(cache_entry->shared_ref->shared_entry == NULL);
		found = false;
	}
	else
	{
		PgStatSharedRef *shared_ref PG_USED_FOR_ASSERTS_ONLY;

		shared_ref = cache_entry->shared_ref;
		Assert(shared_ref->shared_entry != NULL);
		Assert(shared_ref->shared_stats != NULL);

		Assert(shared_ref->shared_stats->magic == 0xdeadbeef);
		/* should have at least our reference */
		Assert(pg_atomic_read_u32(&shared_ref->shared_entry->refcount) > 0);
	}

	*shared_ref_p = cache_entry->shared_ref;
	return found;
}


/*
 * Get a shared stats reference. If create is true, the shared stats object is
 * created if it does not exist.
 */
PgStatSharedRef *
pgstat_shared_ref_get(PgStatTypes type, Oid dboid, Oid objoid, bool create)
{
	PgStatHashKey key;
	PgStatShmHashEntry *shhashent;
	PgStatShm_StatEntryHeader *shheader = NULL;
	PgStatSharedRef *shared_ref;
	bool		shfound;

	key.type = type;
	key.dboid = dboid;
	key.objoid = objoid;

	Assert(pgStatSharedHash != NULL);

	/*
	 * First check the lookup cache hashtable in local memory. If we find a
	 * match here we can avoid taking locks / contention.
	 */
	if (pgstat_shared_ref_get_cached(key, &shared_ref))
		return shared_ref;

	Assert(shared_ref != NULL);

	/*
	 * Do a lookup in the hash table first - it's quite likely that the entry
	 * already exists, and that way we only need a shared lock.
	 */
	shhashent = dshash_find(pgStatSharedHash, &key, false);

	if (shhashent)
		shfound = true;
	else if (create)
	{
		/*
		 * It's possible that somebody created the entry since the above
		 * lookup, fall through to the same path as before if so.
		 */
		shhashent = dshash_find_extended(pgStatSharedHash, &key,
										 true, false, true, &shfound);
		if (!shfound)
		{
			/*
			 * Initialize refcount to 2, (1 marking it as valid, one for the
			 * local reference). That prevents another backend from freeing
			 * the entry once we release the lock below. The entry can't be
			 * freed before the initialization because it can't be found as
			 * long as we hold the dshash partition lock.
			 */
			shheader = pgstat_shared_stat_entry_init(type, shhashent, 2);

			dshash_release_lock(pgStatSharedHash, shhashent);

			shared_ref->shared_stats = shheader;
			shared_ref->shared_entry = shhashent;
		}
	}
	else
	{
		/*
		 * XXX: In case of a non-existing shared entry, should we delete the
		 * ref again? Probably not worth it, it is likely to soon be created.
		 */
		shfound = false;

		pgstat_shared_ref_release(key, shared_ref);

		return NULL;
	}

	if (shfound)
	{
		shheader = dsa_get_address(StatsDSA, shhashent->body);

		Assert(shheader->magic == 0xdeadbeef);
		Assert(shhashent->dropped || pg_atomic_read_u32(&shhashent->refcount) > 0);

		pg_atomic_fetch_add_u32(&shhashent->refcount, 1);

		dshash_release_lock(pgStatSharedHash, shhashent);

		shared_ref->shared_stats = shheader;
		shared_ref->shared_entry = shhashent;
	}

	return shared_ref;
}

static void
pgstat_shared_ref_release(PgStatHashKey key, PgStatSharedRef *shared_ref)
{
	Assert(shared_ref == NULL || shared_ref->pending == NULL);

	if (shared_ref && shared_ref->shared_stats)
	{
		Assert(shared_ref->shared_stats->magic == 0xdeadbeef);
		Assert(shared_ref->pending == NULL);

		/*
		 * FIXME: this may be racy.
		 */
		if (pg_atomic_fetch_sub_u32(&shared_ref->shared_entry->refcount, 1) == 1)
		{
			PgStatShmHashEntry *shent;
			dsa_pointer dsap;

			/*
			 * We're the last referrer to this entry, try to drop the shared
			 * entry.
			 */

			/* only dropped entries can reach a 0 refcount */
			Assert(shared_ref->shared_entry->dropped);

			shent = dshash_find(pgStatSharedHash,
								&shared_ref->shared_entry->key,
								true);
			if (!shent)
				elog(PANIC, "could not find just referenced shared stats entry");

			if (pg_atomic_read_u32(&shared_ref->shared_entry->refcount) != 0)
				elog(PANIC, "concurrent access to stats entry during deletion");

			Assert(shared_ref->shared_entry == shent);

			/*
			 * Fetch dsa pointer before deleting entry - that way we can free the
			 * memory after releasing the lock.
			 */
			dsap = shent->body;

			dshash_delete_entry(pgStatSharedHash, shent);

			dsa_free(StatsDSA, dsap);
			shared_ref->shared_stats = NULL;
		}
	}

	if (!pgstat_shared_ref_hash_delete(pgStatSharedRefHash, key))
		elog(PANIC, "something has gone wrong");

	if (shared_ref)
		pfree(shared_ref);
}

bool
pgstat_shared_stat_lock(PgStatSharedRef *shared_ref, bool nowait)
{
	LWLock *lock = &shared_ref->shared_entry->lock;

	if (nowait)
		return LWLockConditionalAcquire(lock, LW_EXCLUSIVE);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	return true;
}

void
pgstat_shared_stat_unlock(PgStatSharedRef *shared_ref)
{
	LWLockRelease(&shared_ref->shared_entry->lock);
}

/*
 * Helper function to fetch and lock shared stats.
 */
PgStatSharedRef *
pgstat_shared_stat_locked(PgStatTypes type, Oid dboid, Oid objoid, bool nowait)
{
	PgStatSharedRef *shared_ref;

	/* find shared table stats entry corresponding to the local entry */
	shared_ref = pgstat_shared_ref_get(type, dboid, objoid, true);

	/* lock the shared entry to protect the content, skip if failed */
	if (!pgstat_shared_stat_lock(shared_ref, nowait))
		return NULL;

	return shared_ref;
}

/*
 * The length of the data portion of a shared memory stats entry (i.e. without
 * transient data such as refcoutns, lwlocks, ...).
 */
static inline size_t
shared_stat_entry_len(PgStatTypes stattype)
{
	size_t		sz = pgstat_types[stattype].shared_data_len;

	AssertArg(stattype <= PGSTAT_TYPE_LAST);
	Assert(sz != 0 && sz < PG_UINT32_MAX);

	return sz;
}

/*
 * Returns a pointer to the data portion of a shared memory stats entry.
 */
static inline void*
shared_stat_entry_data(PgStatTypes stattype, PgStatShm_StatEntryHeader *entry)
{
	size_t		off = pgstat_types[stattype].shared_data_off;

	AssertArg(stattype <= PGSTAT_TYPE_LAST);
	Assert(off != 0 && off < PG_UINT32_MAX);

	return ((char *)(entry)) + off;
}

static bool
pgstat_shared_refs_need_gc(void)
{
	uint64		currage;

	if (!pgStatSharedRefHash)
		return false;

	currage = pg_atomic_read_u64(&StatsShmem->gc_count);

	return pgStatSharedRefAge != currage;
}

static void
pgstat_shared_refs_gc(void)
{
	pgstat_shared_ref_hash_iterator i;
	PgStatSharedRefHashEntry *ent;
	uint64		currage;

	currage = pg_atomic_read_u64(&StatsShmem->gc_count);

	/*
	 * Some entries have been dropped. Invalidate cache pointer to
	 * them.
	 */
	pgstat_shared_ref_hash_start_iterate(pgStatSharedRefHash, &i);
	while ((ent = pgstat_shared_ref_hash_iterate(pgStatSharedRefHash, &i)) != NULL)
	{
		PgStatSharedRef *shared_ref = ent->shared_ref;

		Assert(!shared_ref->shared_stats || shared_ref->shared_stats->magic == 0xdeadbeef);

		/* cannot gc shared ref that has pending data */
		if (shared_ref->pending != NULL)
			continue;

		if (shared_ref->shared_stats && shared_ref->shared_entry->dropped)
			pgstat_shared_ref_release(ent->key, shared_ref);
	}

	pgStatSharedRefAge = currage;
}

/*
 * Returns the appropriate PgStatSharedRef, preparing it to receive pending
 * stats if not already done.
 */
PgStatSharedRef*
pgstat_pending_prepare(PgStatTypes type, Oid dboid, Oid objoid)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_shared_ref_get(type, dboid, objoid, true);

	if (shared_ref->pending == NULL)
	{
		size_t entrysize = pgstat_types[type].pending_size;

		Assert(entrysize != (size_t)-1);

		shared_ref->pending = MemoryContextAllocZero(TopMemoryContext, entrysize);
		dlist_push_tail(&pgStatPending, &shared_ref->pending_node);
	}

	return shared_ref;
}

/*
 * Return an existing stats entry, or NULL.
 *
 * This should only be used for helper function for pgstatfuncs.c - outside of
 * that it shouldn't be needed.
 */
PgStatSharedRef*
pgstat_pending_fetch(PgStatTypes type, Oid dboid, Oid objoid)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_shared_ref_get(type, dboid, objoid, false);

	if (shared_ref == NULL || shared_ref->pending == NULL)
		return NULL;

	return shared_ref;
}

static void
pgstat_pending_delete(PgStatSharedRef *shared_ref)
{
	void *pending_data = shared_ref->pending;

	Assert(pending_data != NULL);

	switch (shared_ref->shared_entry->key.type)
	{
		case PGSTAT_TYPE_TABLE:
			pgstat_delinkstats(((PgStat_TableStatus *) pending_data)->relation);
			break;
		case PGSTAT_TYPE_DB:
		case PGSTAT_TYPE_FUNCTION:
			break;
		default:
			elog(ERROR, "unexpected");
			break;
	}

	pfree(pending_data);
	shared_ref->pending = NULL;

	dlist_delete(&shared_ref->pending_node);
}

/*
 * Release all local references to shared stats entries.
 *
 * When a process exits it cannot do so while still holding references onto
 * stats entries, otherwise the shared stats entries could never be freed.
 */
static void
pgstat_shared_refs_release_all(void)
{
	pgstat_shared_ref_hash_iterator i;
	PgStatSharedRefHashEntry *ent;

	if (pgStatSharedRefHash == NULL)
		return;

	pgstat_shared_ref_hash_start_iterate(pgStatSharedRefHash, &i);

	while ((ent = pgstat_shared_ref_hash_iterate(pgStatSharedRefHash, &i))
		   != NULL)
	{
		Assert(ent->shared_ref != NULL);

		pgstat_shared_ref_release(ent->key, ent->shared_ref);
	}

	Assert(pgStatSharedRefHash->members == 0);
	pgstat_shared_ref_hash_destroy(pgStatSharedRefHash);
	pgStatSharedRefHash = NULL;
}

/*
 * Drop a shared stats entry. The entry must be exclusively locked.
 *
 * This marks the shared entry as dropped. However, the shared hash table
 * entry and the stats entry are only deleted if there are no remaining
 * references.
 *
 * Returns whether the stats data could be freed or not.
 */
static bool
pgstat_drop_stats_entry(dshash_seq_status *hstat)
{
	PgStatShmHashEntry *ent;
	PgStatHashKey key;
	dsa_pointer pdsa;
	bool		did_free;

	ent = dshash_get_current(hstat);
	key = ent->key;
	pdsa = ent->body;

	/*
	 * Signal that the entry is dropped - this will eventually cause other
	 * backends to release their references.
	 */
	Assert(!ent->dropped);
	ent->dropped = true;

	/*
	 * This backend might very well be the only backend holding a
	 * reference. Ensure that we're not preventing it from being cleaned up
	 * till later.
	 *
	 * XXX: don't do this while holding the dshash lock.
	 */
	if (pgStatSharedRefHash)
	{
		PgStatSharedRefHashEntry *shared_ref_entry;

		shared_ref_entry =
			pgstat_shared_ref_hash_lookup(pgStatSharedRefHash, key);

		if (shared_ref_entry && shared_ref_entry->shared_ref)
		{
			Assert(shared_ref_entry->shared_ref->shared_entry == ent);
			pgstat_shared_ref_release(shared_ref_entry->key,
									  shared_ref_entry->shared_ref);
		}
	}

	/*
	 * Now that the entry isn't needed anymore, remove the refcount
	 * representing a valid entry. If that causes the refcount to reach 0 no
	 * other backend can have a reference, so we can free.
	 */
	if (pg_atomic_fetch_sub_u32(&ent->refcount, 1) == 1)
	{
		dshash_delete_current(hstat);
		dsa_free(StatsDSA, pdsa);
		did_free = true;
	}
	else
	{
		did_free = false;
	}

	return did_free;
}

/* ----------
 * collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should pgsstat_oid_destroy the result
 *	when done with it.  (However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static pgstat_oid_hash *
collect_oids(Oid catalogid, AttrNumber anum_oid)
{
	pgstat_oid_hash *rethash;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;

	rethash = pgstat_oid_create(CurrentMemoryContext,
								PGSTAT_TABLE_HASH_SIZE, NULL);

	rel = table_open(catalogid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid			thisoid;
		bool		isnull;
		bool		found;

		thisoid = heap_getattr(tup, anum_oid, RelationGetDescr(rel), &isnull);
		Assert(!isnull);

		CHECK_FOR_INTERRUPTS();

		pgstat_oid_insert(rethash, thisoid, &found);
	}
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, AccessShareLock);

	return rethash;
}

/*
 * Flush out pending stats for database objects (databases, relations,
 * functions).
 */
static bool
pgstat_flush_object_stats(bool nowait)
{
	bool		have_pending = false;
	dlist_node *cur = NULL;

	/*
	 * Need to be a bit careful iterating over the list of pending
	 * entries. Processing a pending entry may queue further pending entries
	 * to the end of the list that we want to process, so a simple iteration
	 * won't do. Further complicating matter is that we want to delete the
	 * current entry in each iteration from the list if we flushed
	 * successfully.
	 *
	 * So we just keep track of the next pointer in each loop iteration.
	 */
	if (!dlist_is_empty(&pgStatPending))
		cur = dlist_head_node(&pgStatPending);

	while (cur)
	{
		PgStatSharedRef *shared_ref = dlist_container(PgStatSharedRef, pending_node, cur);
		bool		remove = false;
		dlist_node *next;

		switch (shared_ref->shared_entry->key.type)
		{
			case PGSTAT_TYPE_TABLE:
				remove = pgstat_flush_table(shared_ref, nowait);
				break;
			case PGSTAT_TYPE_FUNCTION:
				remove = pgstat_flush_function(shared_ref, nowait);
				break;
			case PGSTAT_TYPE_DB:
				remove = pgstat_flush_db(shared_ref, nowait);
				break;
			default:
				elog(ERROR, "unexpected");
				break;
		}

		/* determine next entry, before deleting the pending entry */
		if (dlist_has_next(&pgStatPending, cur))
			next = dlist_next_node(&pgStatPending, cur);
		else
			next = NULL;

		/* if successfully flushed, remove entry */
		if (remove)
			pgstat_pending_delete(shared_ref);
		else
			have_pending = true;

		cur = next;
	}

	Assert(dlist_is_empty(&pgStatPending) == !have_pending);

	return have_pending;
}

/*
 * get_tabstat_stack_level - add a new (sub)transaction stack entry if needed
 */
PgStat_SubXactStatus *
get_tabstat_stack_level(int nest_level)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state == NULL || xact_state->nest_level != nest_level)
	{
		xact_state = (PgStat_SubXactStatus *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(PgStat_SubXactStatus));
		dlist_init(&xact_state->pending_drops);
		xact_state->pending_drops_count = 0;
		xact_state->nest_level = nest_level;
		xact_state->prev = pgStatXactStack;
		xact_state->first = NULL;
		pgStatXactStack = xact_state;
	}
	return xact_state;
}

/*
 * add_tabstat_xact_level - add a new (sub)transaction state record
 */
void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
{
	PgStat_SubXactStatus *xact_state;
	PgStat_TableXactStatus *trans;

	/*
	 * If this is the first rel to be modified at the current nest level, we
	 * first have to push a transaction stack entry.
	 */
	xact_state = get_tabstat_stack_level(nest_level);

	/* Now make a per-table stack entry */
	trans = (PgStat_TableXactStatus *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(PgStat_TableXactStatus));
	trans->nest_level = nest_level;
	trans->upper = pgstat_info->trans;
	trans->parent = pgstat_info;
	trans->next = xact_state->first;
	xact_state->first = trans;
	pgstat_info->trans = trans;
}


/* ----------
 * AtEOXact_PgStat
 *
 *	Called from access/transam/xact.c at top-level transaction commit/abort.
 * ----------
 */
void
AtEOXact_PgStat(bool isCommit, bool parallel)
{
	PgStat_SubXactStatus *xact_state;

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

	/* handle transactional stats information */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);

		/* relations */
		pgstat_eoxact_relations(xact_state, isCommit);

		pgstat_eoxact_stats_drops(xact_state, isCommit);
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/*
 * Perform relation stats specific end-of-sub-transaction work. Helper for
 * AtEOSubXact_PgStat.
 *
 * Transfer transactional insert/update counts into the next higher
 * subtransaction state.
 */
static void
pgstat_eosubxact_drops(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *parent_xact_state;
	dlist_mutable_iter iter;

	if (xact_state->pending_drops_count == 0)
		return;

	parent_xact_state = get_tabstat_stack_level(nestDepth - 1);

	dlist_foreach_modify(iter, &xact_state->pending_drops)
	{
		PgStat_PendingDroppedStatsItem *pending =
			dlist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);

		dlist_delete(&pending->node);
		xact_state->pending_drops_count--;

		if (isCommit)
		{
			dlist_push_tail(&parent_xact_state->pending_drops, &pending->node);
			parent_xact_state->pending_drops_count++;
		}
		else
		{
			pfree(pending);
		}
	}

	Assert(xact_state->pending_drops_count == 0);
}


/* ----------
 * AtEOSubXact_PgStat
 *
 *	Called from access/transam/xact.c at subtransaction commit/abort.
 * ----------
 */
void
AtEOSubXact_PgStat(bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *xact_state;

	/* merge the sub-transaction's transactional stats into the parent */
	xact_state = pgStatXactStack;
	if (xact_state != NULL &&
		xact_state->nest_level >= nestDepth)
	{
		/* delink xact_state from stack immediately to simplify reuse case */
		pgStatXactStack = xact_state->prev;

		/* relations */
		pgstat_eosubxact_relations(xact_state, isCommit, nestDepth);

		pgstat_eosubxact_drops(xact_state, isCommit, nestDepth);

		pfree(xact_state);
	}
}


/*
 * AtPrepare_PgStat
 *		Save the transactional stats state at 2PC transaction prepare.
 *
 * In this phase we just generate 2PC records for all the pending
 * transaction-dependent stats work.
 */
void
AtPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state != NULL)
		AtPrepare_PgStat_Relations(xact_state);

}

/*
 * PostPrepare_PgStat
 *		Clean up after successful PREPARE.
 *
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the activity stats facility immediately, while the effects on
 * live and dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
void
PostPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * We don't bother to free any of the transactional state, since it's all
	 * in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
		PostPrepare_PgStat_Relations(xact_state);

	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}


/* ------------------------------------------------------------
 * Fetching of stats
 *------------------------------------------------------------
 */

static void
pgstat_fetch_prepare(void)
{
	if (stats_snapshot.stats == NULL)
	{
		pgstat_setup_memcxt();

		stats_snapshot.stats = pgstat_snapshot_create(pgStatSnapshotContext,
													  PGSTAT_TABLE_HASH_SIZE,
													  NULL);
	}
}

static void
pgstat_fetch_snapshot_build(void)
{
	dshash_seq_status hstat;
	PgStatShmHashEntry *p;

	pgstat_fetch_prepare();

	Assert(stats_snapshot.stats->members == 0);

	/*
	 * Build snapshot all variable stats.
	 */
	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		bool found;
		PgStatSnapshotEntry *entry = NULL;
		size_t entry_len;
		PgStatShm_StatEntryHeader *stats_data;

		/*
		 * Most stats in other databases cannot be accessed, so we don't need
		 * to snapshot them. But database stats for other databases are
		 * accessible via pg_stat_database.
		 */
		if (p->key.dboid != MyDatabaseId &&
			p->key.dboid != InvalidOid &&
			p->key.type != PGSTAT_TYPE_DB)
			continue;

		if (p->dropped)
			continue;

		Assert(pg_atomic_read_u32(&p->refcount) > 0);

		stats_data = dsa_get_address(StatsDSA, p->body);
		Assert(stats_data);

		entry = pgstat_snapshot_insert(stats_snapshot.stats, p->key, &found);
		Assert(!found);

		entry_len = pgstat_types[p->key.type].shared_size;
		entry->data = MemoryContextAlloc(pgStatSnapshotContext, entry_len);
		memcpy(entry->data,
			   shared_stat_entry_data(p->key.type, stats_data),
			   entry_len);
	}
	dshash_seq_term(&hstat);

	/*
	 * Build snapshot of all global stats.
	 */
	for (int stattype = 0; stattype < PGSTAT_TYPE_LAST; stattype++)
	{
		if (!pgstat_types[stattype].is_global)
		{
			Assert(pgstat_types[stattype].snapshot_cb == NULL);
			continue;
		}

		Assert(pgstat_types[stattype].snapshot_cb != NULL);

		stats_snapshot.global_valid[stattype] = false;

		pgstat_types[stattype].snapshot_cb();

		Assert(!stats_snapshot.global_valid[stattype]);
		stats_snapshot.global_valid[stattype] = true;
	}

	stats_snapshot.mode = STATS_FETCH_CONSISTENCY_SNAPSHOT;
}

void*
pgstat_fetch_entry(PgStatTypes type, Oid dboid, Oid objoid)
{
	PgStatHashKey key;
	PgStatSharedRef *shared_ref;
	void *stats_data;
	size_t data_size;
	size_t data_offset;

	/* should be called from backends */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	pgstat_fetch_prepare();

	AssertArg(type <= PGSTAT_TYPE_LAST);
	AssertArg(!pgstat_types[type].is_global);

	key.type = type;
	key.dboid = dboid;
	key.objoid = objoid;

	/* if we need to build a full snapshot, do so */
	if (stats_snapshot.mode != STATS_FETCH_CONSISTENCY_SNAPSHOT &&
		pgstat_fetch_consistency == STATS_FETCH_CONSISTENCY_SNAPSHOT)
		pgstat_fetch_snapshot_build();

	/* if caching is desired, look up in cache */
	if (pgstat_fetch_consistency > STATS_FETCH_CONSISTENCY_NONE)
	{
		PgStatSnapshotEntry *entry = NULL;

		entry = pgstat_snapshot_lookup(stats_snapshot.stats, key);

		if (entry)
			return entry->data;
	}

	/*
	 * if we built a full snapshot and it's not in stats_snapshot.stats, it
	 * doesn't exist.
	 */
	if (pgstat_fetch_consistency == STATS_FETCH_CONSISTENCY_SNAPSHOT)
		return NULL;

	stats_snapshot.mode = pgstat_fetch_consistency;

	shared_ref = pgstat_shared_ref_get(type, dboid, objoid, false);

	if (shared_ref == NULL || shared_ref->shared_entry->dropped)
	{
		/* FIXME: need to remember that STATS_FETCH_CONSISTENCY_CACHE */
		return NULL;
	}

	/*
	 * FIXME: For STATS_FETCH_CONSISTENCY_NONE, should we instead allocate
	 * stats in calling context?
	 */

	data_size = pgstat_types[type].shared_data_len;
	data_offset = pgstat_types[type].shared_data_off;
	stats_data = MemoryContextAlloc(pgStatSnapshotContext, data_size);
	memcpy(stats_data, ((char*) shared_ref->shared_stats) + data_offset, data_size);

	if (pgstat_fetch_consistency > STATS_FETCH_CONSISTENCY_NONE)
	{
		PgStatSnapshotEntry *entry = NULL;
		bool found;

		entry = pgstat_snapshot_insert(stats_snapshot.stats, key, &found);
		entry->data = stats_data;
	}

	return stats_data;
}

/*
 * ---------
 * pgstat_get_stat_timestamp() -
 *
 *  Returns the last update timstamp of global staticstics.
 */
TimestampTz
pgstat_get_stat_timestamp(void)
{
	return (TimestampTz) pg_atomic_read_u64(&StatsShmem->stats_timestamp);
}


void
pgstat_snapshot_global(PgStatTypes stattype)
{
	AssertArg(stattype <= PGSTAT_TYPE_LAST);
	AssertArg(pgstat_types[stattype].is_global);

	if (pgstat_fetch_consistency == STATS_FETCH_CONSISTENCY_SNAPSHOT)
	{
		if (stats_snapshot.mode != STATS_FETCH_CONSISTENCY_SNAPSHOT)
			pgstat_fetch_snapshot_build();

		Assert(stats_snapshot.global_valid[stattype] == true);
	}
	else if (pgstat_fetch_consistency == STATS_FETCH_CONSISTENCY_NONE ||
		!stats_snapshot.global_valid[stattype])
	{
		if (pgstat_fetch_consistency == STATS_FETCH_CONSISTENCY_NONE)
			stats_snapshot.global_valid[stattype] = false;

		pgstat_types[stattype].snapshot_cb();

		Assert(!stats_snapshot.global_valid[stattype]);
		stats_snapshot.global_valid[stattype] = true;
	}
}

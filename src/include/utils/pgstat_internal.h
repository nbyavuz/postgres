/* ----------
 *	pgstat_internal.h
 *
 *  Definitions for the PostgreSQL activity statistics facility that should
 *  only be needed by files implementing statistics support (rather than ones
 *  reporting stats).
 *
 *	Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 *	src/include/utils/pgstat_internal.h
 * ----------
 */
#ifndef PGSTAT_INTERNAL_H
#define PGSTAT_INTERNAL_H

#include "pgstat.h"
#include "utils/dsa.h"
#include "lib/dshash.h"
#include "access/xact.h"

/*
 * Types to define shared statistics structure.
 *
 * Per-object statistics are stored in the "shared stats" hashtable. That
 * table's entries (PgStatShmHashEntry) contain a pointer to the actual stats
 * data for the object (the size of the stats data varies depending on the
 * type of stats). The table is keyed by PgStatHashKey.
 *
 * Once a backend has a reference to a shared stats entry, it increments the
 * entry's refcount. Even after stats data is dropped (e.g. due to a DROP
 * TABLE), the entry itself can only be deleted once all references have been
 * released.
 *
 * These refcounts, in combination with a backend local hashtable
 * (pgStatSharedRefHash, with entries pointing to PgStatSharedRef) in front of
 * the shared hash table, mean that most stats work can happen without
 * touching the shared hash table, reducing contention.
 *
 * Once there are pending stats updates for a table PgStatSharedRef->pending
 * is allocated to contain a working space for as-of-yet-unapplied stats
 * updates. Once the stats are flushed, PgStatSharedRef->pending is freed.
 *
 * Each stat entry type in the shared hash table has a fixed member
 * PgStat_HashEntryHeader as the first element.
 */

/* The types of statistics entries */
typedef enum PgStatTypes
{
	/* stats with a variable number of entries */
	PGSTAT_TYPE_DB,				/* database-wide statistics */
	PGSTAT_TYPE_TABLE,			/* per-table statistics */
	PGSTAT_TYPE_FUNCTION,		/* per-function statistics */

	/* stats with a constant number of entries */
	PGSTAT_TYPE_ARCHIVER,
	PGSTAT_TYPE_BGWRITER,
	PGSTAT_TYPE_CHECKPOINTER,
	PGSTAT_TYPE_REPLSLOT,
	PGSTAT_TYPE_SLRU,
	PGSTAT_TYPE_WAL,
} PgStatTypes;
#define PGSTAT_TYPE_LAST PGSTAT_TYPE_WAL


/* struct for shared statistics hash entry key. */
typedef struct PgStatHashKey
{
	PgStatTypes type;			/* statistics entry type */
	Oid			dboid;		/* database ID. InvalidOid for shared objects. */
	Oid			objoid;		/* object ID, either table or function. */
} PgStatHashKey;

/* struct for shared statistics hash entry */
typedef struct PgStatShmHashEntry
{
	PgStatHashKey key;			/* hash key */

	/*
	 * If dropped is set, backends need to release their references so that
	 * the memory for the entry can be freed.
	 */
	bool		dropped;

	/*
	 * Refcount managing lifetime of the entry itself (as opposed to the
	 * dshash entry pointing to it). The stats lifetime has to be separate
	 * from the hash table entry lifetime because we allow backends to point
	 * to a stats entry without holding a hash table lock (and some other
	 * reasons).
	 *
	 * As long as the entry is not dropped 1 is added to the refcount
	 * representing that it should not be dropped. In addition each backend
	 * that has a reference to the entry needs to increment the refcount as
	 * long as it does.
	 *
	 * When the refcount reaches 0 the entry needs to be freed.
	 */
	pg_atomic_uint32  refcount;

	LWLock		lock;
	dsa_pointer body;			/* pointer to shared stats in
								 * PgStat_StatEntryHeader */
} PgStatShmHashEntry;

/* ----------
 * PgStatShm_StatEntryHead			common header struct for PgStatShm_Stat*Entry
 * ----------
 */
typedef struct PgStatShm_StatEntryHeader
{
	uint32		magic;				/* just a validity cross-check */
} PgStatShm_StatEntryHeader;

/*
 * A backend local reference to a shared stats entry. As long as at least one
 * such reference exists, the shared stats entry will not be released.
 *
 * If there are pending stats update to the shared stats, these are stored in
 * ->pending.
 */
typedef struct PgStatSharedRef
{
	/*
	 * Pointers to both the hash table entry pgStatSharedHash, and the stats
	 * (as a local pointer, to avoid dsa_get_address()).
	 */
	PgStatShmHashEntry *shared_entry;
	PgStatShm_StatEntryHeader *shared_stats;

	dlist_node	pending_node;	/* membership in pgStatPending list */
	void	   *pending;		/* the pending data itself */
} PgStatSharedRef;



/*
 * Tuple insertion/deletion counts for an open transaction can't be propagated
 * into PgStat_TableStatus counters until we know if it is going to commit
 * or abort.  Hence, we keep these counts in per-subxact structs that live
 * in TopTransactionContext.  This data structure is designed on the assumption
 * that subxacts won't usually modify very many tables.
 *
 * FIXME: Update comment.
 */
typedef struct PgStat_SubXactStatus
{
	int			nest_level;		/* subtransaction nest level */

	dlist_head	pending_drops;
	int			pending_drops_count;

	struct PgStat_SubXactStatus *prev;	/* higher-level subxact if any */
	PgStat_TableXactStatus *first;	/* head of list for this subxact */
} PgStat_SubXactStatus;


/*
 * Metadata for a specific type of statistics.
 */
typedef void (PgStatTypeSnapshotCB)(void);
typedef bool (PgStatTypeFlushCB)(PgStatSharedRef *sr, bool nowait);
typedef struct pgstat_type_info
{
	/*
	 * Is the stats type a global one (of which a precise number exists) or
	 * not (e.g. tables).
	 */
	bool is_global : 1;

	/*
	 * Can stats of this kind be accessed from another database? Determines
	 * whether a stats object gets included in stats snapshots.
	 */
	bool accessed_across_databases : 1;

	/*
	 * The size of an entry in the shared stats hash table (pointed to by
	 * PgStatShmHashEntry->body).
	 */
	uint32 shared_size;

	/*
	 * The offset/size of the statistics inside the shared stats entry. This
	 * is used to e.g. avoid touching lwlocks when serializing / restoring
	 * stats snapshot serialized to / from disk respectively.
	 */
	uint32 shared_data_off;
	uint32 shared_data_len;

	/*
	 * The size of the pending data for this type. E.g. how large
	 * PgStatPendingEntry->pending is. Used for allocations.
	 *
	 * -1 signal that an entry of this type should never have a pending
     * entry.
	 */
	uint32 pending_size;

	/*
	 * For global statistics: Fetch a snapshot of appropriate global stats.
	 */
	PgStatTypeSnapshotCB *snapshot_cb;

	/*
	 * For variable number stats: flush pending stats.
	 */
	PgStatTypeFlushCB *flush_pending_cb;
} pgstat_type_info;


/*
 * List of SLRU names that we keep stats for.  There is no central registry of
 * SLRUs, so we use this fixed list instead.  The "other" entry is used for
 * all SLRUs without an explicit entry (e.g. SLRUs in extensions).
 *
 * This is only defined here so that SLRU_NUM_ELEMENTS is known for later type
 * definitions.
 */
static const char *const slru_names[] = {
	"CommitTs",
	"MultiXactMember",
	"MultiXactOffset",
	"Notify",
	"Serial",
	"Subtrans",
	"Xact",
	"other"						/* has to be last */
};
#define SLRU_NUM_ELEMENTS lengthof(slru_names)


typedef struct StatsShmemStruct
{
	void   *raw_dsa_area;

	/*
	 * Stats for objects for which a variable number exists are kept in this
	 * shared hash table. See comment above PgStatTypes for details.
	 */
	dshash_table_handle hash_handle;	/* shared dbstat hash */

	/*
	 * Whenever the for a dropped stats entry could not be freed (because
	 * backends still have references), this is incremented, causing backends
	 * to run pgstat_lookup_cache_gc(), allowing that memory to be reclaimed.
	 */
	pg_atomic_uint64 gc_count;

	/*
	 * Global stats structs.
	 *
	 * For the various "changecount" members check the definition of struct
	 * PgBackendStatus for some explanation.
	 */
	struct
	{
		PgStat_ArchiverStats stats;
		uint32 changecount;
		PgStat_ArchiverStats reset_offset;	/* protected by StatsLock */
	} archiver;

	struct
	{
		PgStat_BgWriterStats stats;
		uint32 changecount;
		PgStat_BgWriterStats reset_offset;	/* protected by StatsLock */
	} bgwriter;

	struct
	{
		PgStat_CheckPointerStats stats;
		uint32 changecount;
		PgStat_CheckPointerStats reset_offset;	/* protected by StatsLock */
	} checkpointer;

	struct
	{
		LWLock		lock;
		PgStat_ReplSlotStats *stats;
	} replslot;

	struct
	{
		LWLock		lock;
		PgStat_SLRUStats stats[SLRU_NUM_ELEMENTS];
#define SizeOfSlruStats sizeof(PgStat_SLRUStats[SLRU_NUM_ELEMENTS])
	} slru;

	struct
	{
		LWLock		lock;
		PgStat_WalStats stats;
	} wal;

	/* protected by StatsLock */
	pg_atomic_uint64 stats_timestamp;
} StatsShmemStruct;


/* ----------
 * Types and definitions for individual statistic types
 * ----------
 */

typedef struct PgStatShm_StatDBEntry
{
	PgStatShm_StatEntryHeader header;
	PgStat_StatDBEntry stats;
} PgStatShm_StatDBEntry;

typedef struct PgStatShm_StatTabEntry
{
	PgStatShm_StatEntryHeader header;
	PgStat_StatTabEntry stats;
} PgStatShm_StatTabEntry;

typedef struct PgStatShm_StatFuncEntry
{
	PgStatShm_StatEntryHeader header;
	PgStat_StatFuncEntry stats;
} PgStatShm_StatFuncEntry;



/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	/* tuples i/u/d prior to truncate/drop */
	PgStat_Counter inserted_pre_truncdrop;
	PgStat_Counter updated_pre_truncdrop;
	PgStat_Counter deleted_pre_truncdrop;
	Oid			t_id;			/* table's OID */
	bool		t_shared;		/* is it a shared catalog? */
	bool		t_truncdropped;	/* was the relation truncated/dropped? */
} TwoPhasePgStatRecord;



/* ----------
 * Cached statistics snapshot
 * ----------
 */

typedef struct PgStatSnapshot
{
	PgStatsFetchConsistency mode;

	bool global_valid[PGSTAT_TYPE_LAST + 1];

	PgStat_ArchiverStats archiver;

	PgStat_BgWriterStats bgwriter;

	PgStat_CheckPointerStats checkpointer;

	int replslot_count;
	PgStat_ReplSlotStats *replslot;

	PgStat_SLRUStats slru[SLRU_NUM_ELEMENTS];

	PgStat_WalStats wal;

	struct pgstat_snapshot_hash *stats;
} PgStatSnapshot;



extern PgStat_SubXactStatus *get_tabstat_stack_level(int nest_level);
extern void add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level);

extern PgStatSharedRef *pgstat_shared_ref_get(PgStatTypes type,
											  Oid dboid, Oid objoid,
											  bool create);
extern bool pgstat_shared_stat_lock(PgStatSharedRef *shared_ref, bool nowait);
extern void pgstat_shared_stat_unlock(PgStatSharedRef *shared_ref);
extern PgStatSharedRef *pgstat_shared_stat_locked(PgStatTypes type,
												  Oid dboid,
												  Oid objoid,
												  bool nowait);

extern PgStatSharedRef *pgstat_pending_prepare(PgStatTypes type, Oid dboid, Oid objoid);
extern PgStatSharedRef *pgstat_pending_fetch(PgStatTypes type, Oid dboid, Oid objoid);

extern void* pgstat_fetch_entry(PgStatTypes type, Oid dboid, Oid objoid);

extern void pgstat_schedule_drop(PgStatTypes stattype, Oid dboid, Oid objoid);

static inline void pgstat_copy_global_stats(void *dst, void *src, size_t len,
											uint32 *cc);

extern void pgstat_snapshot_global(PgStatTypes stattype);

extern bool walstats_pending(void);

static inline void changecount_before_write(uint32 *cc);
static inline void changecount_after_write(uint32 *cc);
static inline uint32 changecount_before_read(uint32 *cc);
static inline bool changecount_after_read(uint32 *cc, uint32 cc_before);


extern bool pgstat_flush_wal(bool nowait);
extern bool pgstat_flush_slru(bool nowait);

extern void pgstat_update_connstats(bool disconnect);

extern void AtEOXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit);
extern void AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth);
extern void AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state);
extern void PostPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state);


extern const pgstat_type_info pgstat_types[PGSTAT_TYPE_WAL + 1];
extern StatsShmemStruct *StatsShmem;

extern WalUsage prevWalUsage;
extern bool have_slrustats;

/* the current statistics snapshot */
extern PgStatSnapshot stats_snapshot;

extern int	pgStatXactCommit;
extern int	pgStatXactRollback;



/*
 * Helpers for changecount manipulation. See comments around struct
 * PgBackendStatus for details.
 */

static inline void
changecount_before_write(uint32 *cc)
{
	Assert((*cc & 1) == 0);

	START_CRIT_SECTION();
	(*cc)++;
	pg_write_barrier();
}

static inline void
changecount_after_write(uint32 *cc)
{
	Assert((*cc & 1) == 1);

	pg_write_barrier();

	(*cc)++;

	END_CRIT_SECTION();
}

static inline uint32
changecount_before_read(uint32 *cc)
{
	uint32 before_cc = *cc;

	CHECK_FOR_INTERRUPTS();

	pg_read_barrier();

	return before_cc;
}

/*
 * Returns true if the read succeeded, false if it needs to be repeated.
 */
static inline bool
changecount_after_read(uint32 *cc, uint32 before_cc)
{
	uint32 after_cc;

	pg_read_barrier();

	after_cc = *cc;

	/* was a write in progress when we started? */
	if (before_cc & 1)
		return false;

	/* did writes start and complete while we read? */
	return before_cc == after_cc;
}


/*
 * pgstat_copy_global_stats - helper function for functions
 *           pgstat_fetch_stat_*() and pgstat_reset_shared_counters().
 *
 * Copies out the specified memory area following change-count protocol.
 */
static inline void
pgstat_copy_global_stats(void *dst, void *src, size_t len,
						 uint32 *cc)
{
	uint32 cc_before;

	do
	{
		cc_before = changecount_before_read(cc);

		memcpy(dst, src, len);
	}
	while (!changecount_after_read(cc, cc_before));
}


#endif							/* PGSTAT_INTERNAL_H */

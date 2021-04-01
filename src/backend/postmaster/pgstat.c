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
#include "utils/probes.h"
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
 * (pgStatShmLookupCache, with entries pointing to PgStatSharedRef) in front
 * of the shared hash table, mean that most stats work can happen without
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


/* ----------
 * PgStatShm_StatEntryHead			common header struct for PgStatShm_Stat*Entry
 * ----------
 */
typedef struct PgStatShm_StatEntryHeader
{
	uint32		magic;				/* just a validity cross-check */
	LWLock		lock;

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

	/*
	 * If dropped is set, backends need to release their references so that
	 * the memory for the entry can be freed.
	 */
	bool		dropped;
} PgStatShm_StatEntryHeader;


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
	dsa_pointer body;			/* pointer to shared stats in
								 * PgStat_StatEntryHeader */
} PgStatShmHashEntry;

/*
 * A backend local reference to a shared stats entry. As long as at least one
 * such reference exists, the shared stats entry will not be released.
 *
 * If there are pending stats update to the shared stats, these are stored in
 * ->pending.
 */
typedef struct PgStatSharedRef
{
	PgStatHashKey key;

	/*
	 * The shared stats referenced. We store both the dsa pointer and a native
	 * pointer so we can free the shared stats if necessary.
	 *
	 * XXX: Seems better to look up the shared stats entry in that case? Need
	 * that for locking anyway, I think?
	 */
	PgStatShm_StatEntryHeader *shared;	/* address pointer to stats body */

	dlist_node	pending_node;	/* membership in pgStatPending list */
	void	   *pending;		/* the pending data itself */
} PgStatSharedRef;

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

typedef struct PgStat_PendingStatDBEntry
{
	PgStatShm_StatDBEntry *shared;
	PgStat_StatDBEntry pending;
} PgStat_PendingStatDBEntry;


typedef struct PgStat_PendingDroppedStatsItem
{
	PgStat_DroppedStatsItem item;
	dlist_node	node;
} PgStat_PendingDroppedStatsItem;

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
typedef struct pgstat_type_info
{
	/*
	 * Is the stats type a global one (of which a precise number exists) or
	 * not (e.g. tables).
	 */
	bool is_global;

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
} pgstat_type_info;

/* Indexed by PgStatTypes. */
static const pgstat_type_info pgstat_types[];

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


/* ----------
 * Shared memory struct for statistics
 * ----------
 */

StaticAssertDecl(sizeof(TimestampTz) == sizeof(pg_atomic_uint64),
				 "size of pg_atomic_uint64 doesn't match TimestampTz");

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
 * Cached statistics snapshot
 * ----------
 */

typedef struct PgStatSnapshot
{
	PgStatsFetchConsistency mode;

	bool global_valid[PGSTAT_TYPE_LAST];

	PgStat_ArchiverStats archiver;

	PgStat_BgWriterStats bgwriter;

	PgStat_CheckPointerStats checkpointer;

	int replslot_count;
	PgStat_ReplSlotStats *replslot;

	PgStat_SLRUStats slru[SLRU_NUM_ELEMENTS];

	PgStat_WalStats wal;

	pgstat_snapshot_hash *stats;
} PgStatSnapshot;



/* ----------
 * Local function forward declarations
 * ----------
 */

static void pgstat_setup_memcxt(void);
static void pgstat_write_statsfile(void);
static void pgstat_read_statsfile(void);
static void pgstat_shutdown_hook(int code, Datum arg);

static PgStat_SubXactStatus *get_tabstat_stack_level(int nest_level);

static PgStatShm_StatEntryHeader *pgstat_shared_stat_entry_init(PgStatTypes stattype,
																PgStatShmHashEntry *shhashent,
																int init_refcount);
static PgStatSharedRef *pgstat_shared_ref_get(PgStatTypes type,
											   Oid dboid, Oid objoid,
											   bool create);
static void pgstat_shared_ref_release(PgStatSharedRef *shared_ref);
static PgStatShm_StatEntryHeader *pgstat_shared_stat_locked(PgStatTypes type,
															Oid dboid,
															Oid objoid,
															bool nowait);
static bool pgstat_shared_stat_lock(PgStatShm_StatEntryHeader *header, bool nowait);
static inline size_t shared_stat_entry_len(PgStatTypes stattype);
static inline void* shared_stat_entry_data(PgStatTypes stattype, PgStatShm_StatEntryHeader *entry);

static bool pgstat_shared_refs_need_gc(void);
static void pgstat_shared_refs_gc(void);

static void pgstat_shared_refs_release_all(void);

static bool pgstat_drop_stats_entry(dshash_seq_status *hstat);

static PgStatSharedRef *pgstat_pending_prepare(PgStatTypes type, Oid dboid, Oid objoid);
static PgStatSharedRef *pgstat_pending_fetch(PgStatTypes type, Oid dboid, Oid objoid);
static void pgstat_pending_delete(PgStatSharedRef *shared_ref);

static PgStat_StatDBEntry *pgstat_pending_db_prepare(Oid dboid);
static PgStat_TableStatus *pgstat_pending_tab_prepare(Oid rel_id, bool isshared);

static pgstat_oid_hash * collect_oids(Oid catalogid, AttrNumber anum_oid);

static inline void pgstat_copy_global_stats(void *dst, void *src, size_t len,
											uint32 *changecount);

static bool pgstat_flush_object_stats(bool nowait);
static bool pgstat_flush_table(PgStatSharedRef *shared_ref, bool nowait);
static bool pgstat_flush_function(PgStatSharedRef *shared_ref, bool nowait);
static bool pgstat_flush_db(PgStat_PendingStatDBEntry *dbent, Oid dboid, bool nowait);
static bool pgstat_flush_wal(bool nowait);
static bool pgstat_flush_slru(bool nowait);
static void pgstat_update_connstats(bool disconnect);

static inline bool walstats_pending(void);

static inline void changecount_before_write(uint32 *cc);
static inline void changecount_after_write(uint32 *cc);
static inline uint32 changecount_before_read(uint32 *cc);
static inline bool changecount_after_read(uint32 *cc, uint32 cc_before);

static void pgstat_snapshot_archiver(void);
static void pgstat_snapshot_bgwriter(void);
static void pgstat_snapshot_checkpointer(void);
static void pgstat_snapshot_replslot(void);
static void pgstat_snapshot_slru(void);
static void pgstat_snapshot_wal(void);


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
static StatsShmemStruct *StatsShmem = NULL;
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
dlist_head pgStatPending = DLIST_STATIC_INIT(pgStatPending);

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
static PgStatSnapshot stats_snapshot;


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



/* ----------
 * pending stats only modified in this file
 * ----------
 */

static int	pgStatXactCommit = 0;
static int	pgStatXactRollback = 0;

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
static bool have_slrustats = false;

/*
 * WAL usage counters saved from pgWALUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by substracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;

/*
 * As a backend's database doesn't change over time, we don't need to go
 * through pgStatShmLookupCache to find our database. That avoids unnecessary
 * lookups, and makes flushing table stats (which roll over into database
 * stats) easier.
 *
 * The only complication is that we need entries not just for "our" database,
 * but also for oid '0', which tracks shared table stats.
 */
bool havePendingDbStats = false;
static PgStat_PendingStatDBEntry pendingDBStats;
static PgStat_PendingStatDBEntry pendingSharedDBStats;



/* ----------
 * Constants
 * ----------
 */

/* see comments for struct pgstat_type_info */
static const pgstat_type_info pgstat_types[] = {

	/* stats types with a variable number of stats */

	[PGSTAT_TYPE_DB] = {
		.is_global = false,
		.shared_size = sizeof(PgStatShm_StatDBEntry),
		.shared_data_off = offsetof(PgStatShm_StatDBEntry, stats),
		.shared_data_len = sizeof(((PgStatShm_StatDBEntry*) 0)->stats),
		.pending_size = -1, /* PGSTAT_TYPE_DB is never in pending table */
	},

	[PGSTAT_TYPE_TABLE] = {
		.is_global = false,
		.shared_size = sizeof(PgStatShm_StatTabEntry),
		.shared_data_off = offsetof(PgStatShm_StatTabEntry, stats),
		.shared_data_len = sizeof(((PgStatShm_StatTabEntry*) 0)->stats),
		.pending_size = sizeof(PgStat_TableStatus),
	},

	[PGSTAT_TYPE_FUNCTION] = {
		.is_global = false,
		.shared_size = sizeof(PgStatShm_StatFuncEntry),
		.shared_data_off = offsetof(PgStatShm_StatFuncEntry, stats),
		.shared_data_len = sizeof(((PgStatShm_StatFuncEntry*) 0)->stats),
		.pending_size = sizeof(PgStat_BackendFunctionEntry),
	},


	/* global stats */

	[PGSTAT_TYPE_ARCHIVER] = {
		.is_global = true,
		.snapshot_cb = pgstat_snapshot_archiver,
	},

	[PGSTAT_TYPE_BGWRITER] = {
		.is_global = true,
		.snapshot_cb = pgstat_snapshot_bgwriter,
	},

	[PGSTAT_TYPE_CHECKPOINTER] = {
		.is_global = true,
		.snapshot_cb = pgstat_snapshot_checkpointer,
	},

	[PGSTAT_TYPE_REPLSLOT] = {
		.is_global = true,
		.snapshot_cb = pgstat_snapshot_replslot,
	},

	[PGSTAT_TYPE_SLRU] = {
		.is_global = true,
		.snapshot_cb = pgstat_snapshot_slru,
	},

	[PGSTAT_TYPE_WAL] = {
		.is_global = true,
		.snapshot_cb = pgstat_snapshot_wal,
	},

};

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

static void
pgstat_truncdrop_save_counters(PgStat_TableXactStatus *trans, bool is_drop);

static void
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

void
pgstat_drop_relation(Relation rel)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	pgstat_schedule_drop(PGSTAT_TYPE_TABLE,
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
	pgstat_schedule_drop(PGSTAT_TYPE_FUNCTION,
						 MyDatabaseId,
						 proid);
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

			pgstat_shared_ref_release(lohashent->shared_ref);
		}
	}

	shent = dshash_find(pgStatSharedHash, &key, true);
	if (shent)
	{
		PgStatShm_StatEntryHeader *header;
		dsa_pointer pdsa;

		Assert(shent->body != InvalidDsaPointer);
		pdsa = shent->body;
		header = dsa_get_address(StatsDSA, pdsa);

		/*
		 * Signal that the entry is dropped - this will eventually cause other
		 * backends to release their references.
		 */

		if (header->dropped)
			elog(ERROR, "can only drop stats once");
		header->dropped = true;

		if (pg_atomic_fetch_sub_u32(&header->refcount, 1) == 1)
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
		PgStatShm_StatEntryHeader *header;

		header = dsa_get_address(StatsDSA, p->body);

		if (header->dropped)
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
	if (!havePendingDbStats &&
		dlist_is_empty(&pgStatPending) &&
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

/*
 * Copy stats between relations. This is used for things like REINDEX
 * CONCURRENTLY.
 */
void
pgstat_copy_relation_stats(Relation dst, Relation src)
{
	PgStat_StatTabEntry *srcstats;
	PgStatShm_StatTabEntry *dstshstats;

	srcstats = pgstat_fetch_stat_tabentry_extended(src->rd_rel->relisshared,
												   RelationGetRelid(src));
	if (!srcstats)
		return;

	dstshstats = (PgStatShm_StatTabEntry *)
		pgstat_shared_stat_locked(PGSTAT_TYPE_TABLE,
								  dst->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
								  RelationGetRelid(dst),
								  false);
	dstshstats->stats = *srcstats;

	LWLockRelease(&dstshstats->header.lock);
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

		if (header->dropped)
			continue;

		LWLockAcquire(&header->lock, LW_EXCLUSIVE);
		memset(shared_stat_entry_data(p->key.type, header), 0,
			   shared_stat_entry_len(p->key.type));

		if (p->key.type == PGSTAT_TYPE_DB)
		{
			PgStatShm_StatDBEntry *dbstat = (PgStatShm_StatDBEntry *) header;

			dbstat->stats.stat_reset_timestamp = GetCurrentTimestamp();
		}
		LWLockRelease(&header->lock);
	}
	dshash_seq_term(&hstat);
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
	LWLockAcquire(StatsLock, LW_EXCLUSIVE);

	switch (t)
	{
		case RESET_ARCHIVER:
			pgstat_copy_global_stats(&StatsShmem->archiver.reset_offset,
									 &StatsShmem->archiver.stats,
									 sizeof(PgStat_ArchiverStats),
									 &StatsShmem->archiver.changecount);
			StatsShmem->archiver.reset_offset.stat_reset_timestamp = now;
			break;

		case RESET_BGWRITER:
			pgstat_copy_global_stats(&StatsShmem->bgwriter.reset_offset,
									 &StatsShmem->bgwriter.stats,
									 sizeof(PgStat_BgWriterStats),
									 &StatsShmem->bgwriter.changecount);
			pgstat_copy_global_stats(&StatsShmem->checkpointer.reset_offset,
									 &StatsShmem->checkpointer.stats,
									 sizeof(PgStat_CheckPointerStats),
									 &StatsShmem->checkpointer.changecount);
			StatsShmem->bgwriter.reset_offset.stat_reset_timestamp = now;
			break;

		case RESET_WAL:

			/*
			 * Differntly from the two above, WAL statistics has many writer
			 * processes and protected by wal_stats_lock.
			 */
			LWLockAcquire(&StatsShmem->wal.lock, LW_EXCLUSIVE);
			MemSet(&StatsShmem->wal.stats, 0, sizeof(PgStat_WalStats));
			StatsShmem->wal.stats.stat_reset_timestamp = now;
			LWLockRelease(&StatsShmem->wal.lock);
			break;
	}

	LWLockRelease(StatsLock);
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
	PgStatSharedRef *shared_ref;
	PgStatShm_StatEntryHeader *header;
	PgStatShm_StatDBEntry *dbentry;
	PgStatTypes stattype;
	TimestampTz ts = GetCurrentTimestamp();

	dbentry = (PgStatShm_StatDBEntry *)
		pgstat_shared_stat_locked(PGSTAT_TYPE_DB, MyDatabaseId, InvalidOid,
								  false);
	if (!dbentry)
		return;

	/* Set the reset timestamp for the whole database */
	dbentry->stats.stat_reset_timestamp = ts;
	LWLockRelease(&dbentry->header.lock);

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

	shared_ref = pgstat_shared_ref_get(stattype, MyDatabaseId, objoid, false);
	if (!shared_ref || shared_ref->shared->dropped)
		return;
	header = shared_ref->shared;

	LWLockAcquire(&header->lock, LW_EXCLUSIVE);
	memset(shared_stat_entry_data(stattype, header), 0,
		   shared_stat_entry_len(stattype));
	LWLockRelease(&header->lock);
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
		PgStatShm_StatEntryHeader *shent;
		size_t		len;

		CHECK_FOR_INTERRUPTS();

		shent = (PgStatShm_StatEntryHeader *) dsa_get_address(StatsDSA, ps->body);

		/* we may have some "dropped" entries not yet removed, skip them */
		if (shent->dropped)
			continue;

		/* if not dropped the valid-entry refcount should exist */
		Assert(pg_atomic_read_u32(&shent->refcount) > 0);

		fputc('S', fpout);
		rc = fwrite(&ps->key, sizeof(PgStatHashKey), 1, fpout);

		/* Write except the header part of the etnry */
		len = shared_stat_entry_len(ps->key.type);
		rc = fwrite(shared_stat_entry_data(ps->key.type, shent), len, 1, fpout);
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

	chunk = dsa_allocate0(StatsDSA, pgstat_types[stattype].shared_size);
	shheader = dsa_get_address(StatsDSA, chunk);
	shheader->magic = 0xdeadbeef;
	LWLockInitialize(&shheader->lock, LWTRANCHE_STATS);

	pg_atomic_init_u32(&shheader->refcount, init_refcount);

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

	if (!found)
	{
		PgStatSharedRef *shared_ref;

		cache_entry->shared_ref = shared_ref =
			MemoryContextAlloc(pgStatSharedRefContext,
							   sizeof(PgStatSharedRef));
		shared_ref->key = key;
		shared_ref->shared = NULL;
		shared_ref->pending = NULL;
	}
	else if (!cache_entry->shared_ref ||
			 !cache_entry->shared_ref->shared)
	{
		found = false;
	}
	else if (cache_entry->shared_ref)
	{
		PgStatShm_StatEntryHeader *shheader PG_USED_FOR_ASSERTS_ONLY;

		shheader = cache_entry->shared_ref->shared;
		Assert(shheader->magic == 0xdeadbeef);
		/* should have at least our reference */
		Assert(pg_atomic_read_u32(&shheader->refcount) > 0);
	}

	*shared_ref_p = cache_entry->shared_ref;
	return found;
}


/*
 * Get a shared stats reference. If create is true, the shared stats object is
 * created if it does not exist.
 */
static PgStatSharedRef *
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
	{
		Assert(shared_ref->shared);

		return shared_ref;
	}

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
			shared_ref->shared = shheader;
		}
	}
	else
	{
		/*
		 * XXX: In case of a non-existing shared entry, should we delete the
		 * ref again? Probably not worth it, it is likely to soon be created.
		 */
		shfound = false;
	}

	if (shfound)
	{
		shheader = dsa_get_address(StatsDSA, shhashent->body);

		Assert(shheader->magic == 0xdeadbeef);
		Assert(shheader->dropped || pg_atomic_read_u32(&shheader->refcount) > 0);

		pg_atomic_fetch_add_u32(&shheader->refcount, 1);

		dshash_release_lock(pgStatSharedHash, shhashent);

		shared_ref->shared = shheader;
	}

	return shared_ref;
}

static void
pgstat_shared_ref_release(PgStatSharedRef *shared_ref)
{
	Assert(shared_ref->pending == NULL);

	if (shared_ref->shared)
	{
		Assert(shared_ref->shared->magic == 0xdeadbeef);
		Assert(shared_ref->pending == NULL);

		/*
		 * FIXME: this may be racy.
		 */
		if (pg_atomic_fetch_sub_u32(&shared_ref->shared->refcount, 1) == 1)
		{
			PgStatShmHashEntry *shent;
			dsa_pointer dsap;

			/*
			 * We're the last referrer to this entry, try to drop the shared
			 * entry.
			 */

			/* only dropped entries can reach a 0 refcount */
			Assert(shared_ref->shared->dropped);

			shent = dshash_find(pgStatSharedHash, &shared_ref->key, true);

			if (!shent)
				elog(PANIC, "could not find just referenced shared stats entry");
			if (pg_atomic_read_u32(&shared_ref->shared->refcount) != 0)
				elog(PANIC, "concurrent access to stats entry during deletion");

			/*
			 * Fetch dsa pointer before deleting entry - that way we can free the
			 * memory after releasing the lock.
			 */
			dsap = shent->body;

			dshash_delete_entry(pgStatSharedHash, shent);

			dsa_free(StatsDSA, dsap);
			shared_ref->shared = NULL;
		}
	}

	if (!pgstat_shared_ref_hash_delete(pgStatSharedRefHash, shared_ref->key))
		elog(PANIC, "something has gone wrong");

	pfree(shared_ref);
}

static bool
pgstat_shared_stat_lock(PgStatShm_StatEntryHeader *header, bool nowait)
{
	if (nowait)
		return LWLockConditionalAcquire(&header->lock, LW_EXCLUSIVE);

	LWLockAcquire(&header->lock, LW_EXCLUSIVE);
	return true;
}

/*
 * Helper function to fetch and lock shared stats.
 */
static PgStatShm_StatEntryHeader *
pgstat_shared_stat_locked(PgStatTypes type, Oid dboid, Oid objoid, bool nowait)
{
	PgStatSharedRef *shared_ref;

	/* find shared table stats entry corresponding to the local entry */
	shared_ref = pgstat_shared_ref_get(type, dboid, objoid, true);

	/* skip if dshash failed to acquire lock */
	if (shared_ref == NULL)
		return NULL;

	/* lock the shared entry to protect the content, skip if failed */
	if (!pgstat_shared_stat_lock(shared_ref->shared, nowait))
		return NULL;

	return shared_ref->shared;
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
		Assert(!shared_ref->shared || shared_ref->shared->magic == 0xdeadbeef);

		/* cannot gc shared ref that has pending data */
		if (shared_ref->pending != NULL)
			continue;

		if (shared_ref->shared && shared_ref->shared->dropped)
			pgstat_shared_ref_release(shared_ref);
	}

	pgStatSharedRefAge = currage;
}

/*
 * Returns the appropriate PgStatSharedRef, preparing it to receive pending
 * stats if not already done.
 */
static PgStatSharedRef*
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
static PgStatSharedRef*
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

	switch (shared_ref->key.type)
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
 *  Find or create a local PgStat_StatDBEntry entry for dboid.
 */
static PgStat_StatDBEntry *
pgstat_pending_db_prepare(Oid dboid)
{
	PgStat_PendingStatDBEntry *dbent;

	if (dboid == InvalidOid)
		dbent = &pendingSharedDBStats;
	else
	{
		Assert(dboid == MyDatabaseId);
		dbent = &pendingSharedDBStats;
	}

	if (dbent->shared == NULL)
	{
		PgStatSharedRef *shared_ref;

		shared_ref = pgstat_shared_ref_get(PGSTAT_TYPE_DB, dboid, InvalidOid,
										   true);

		dbent->shared = (PgStatShm_StatDBEntry *) shared_ref->shared;
		pg_atomic_fetch_add_u32(&dbent->shared->header.refcount, 1);
	}

	havePendingDbStats = true;

	return &dbent->pending;
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

	shared_ref = pgstat_pending_prepare(PGSTAT_TYPE_TABLE,
										isshared ? InvalidOid : MyDatabaseId,
										rel_id);

	return shared_ref->pending;
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
		PgStatSharedRef *shared_ref = ent->shared_ref;

		Assert(shared_ref != NULL);

		pgstat_shared_ref_release(shared_ref);
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
	PgStatShm_StatEntryHeader *header;
	bool		did_free;

	ent = dshash_get_current(hstat);
	key = ent->key;
	pdsa = ent->body;
	header = dsa_get_address(StatsDSA, pdsa);

	/*
	 * Signal that the entry is dropped - this will eventually cause other
	 * backends to release their references.
	 */
	Assert(!header->dropped);
	header->dropped = true;

	/*
	 * This backend might very well be the only backend holding a
	 * reference. Ensure that we're not preventing it from being cleaned up
	 * till later.
	 *
	 * XXX: don't do this while holding the dshash lock.
	 */
	if (pgStatSharedRefHash)
	{
		PgStatSharedRefHashEntry *ent;

		ent = pgstat_shared_ref_hash_lookup(pgStatSharedRefHash, key);

		if (ent && ent->shared_ref)
			pgstat_shared_ref_release(ent->shared_ref);
	}

	/*
	 * Now that the entry isn't needed anymore, remove the refcount
	 * representing a valid entry. If that causes the refcount to reach 0 no
	 * other backend can have a reference, so we can free.
	 */
	if (pg_atomic_fetch_sub_u32(&header->refcount, 1) == 1)
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


/* ------------------------------------------------------------
 * Functions for flushing the different types of pending statistics
 *------------------------------------------------------------
 */

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

		switch (shared_ref->key.type)
		{
			case PGSTAT_TYPE_TABLE:
				remove = pgstat_flush_table(shared_ref, nowait);
				break;
			case PGSTAT_TYPE_FUNCTION:
				remove = pgstat_flush_function(shared_ref, nowait);
				break;
			case PGSTAT_TYPE_DB:
				/* We don't have that kind of pending entry */
				Assert(false);
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

	have_pending |= pgstat_flush_db(&pendingDBStats, MyDatabaseId, nowait);
	have_pending |= pgstat_flush_db(&pendingSharedDBStats, InvalidOid, nowait);

	if (!have_pending)
		havePendingDbStats = false;

	return have_pending;
}

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

	Assert(shared_ref->key.type == PGSTAT_TYPE_TABLE);
	lstats = (PgStat_TableStatus *) shared_ref->pending;
	dboid = shared_ref->key.dboid;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (memcmp(&lstats->t_counts, &all_zeroes,
			   sizeof(PgStat_TableCounts)) == 0)
	{
		return true;
	}

	shtabstats = (PgStatShm_StatTabEntry *) shared_ref->shared;
	if (!pgstat_shared_stat_lock(shared_ref->shared, nowait))
		return false;			/* failed to acquire lock, skip */

	/* add the values to the shared entry. */
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

	LWLockRelease(&shtabstats->header.lock);

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

	Assert(shared_ref->key.type == PGSTAT_TYPE_FUNCTION);
	localent = (PgStat_BackendFunctionEntry *) shared_ref->pending;

	/* localent always has non-zero content */

	shfuncent = (PgStatShm_StatFuncEntry *) shared_ref->shared;
	Assert(shfuncent);
	if (!pgstat_shared_stat_lock(shared_ref->shared, nowait))
		return false;			/* failed to acquire lock, skip */

	shfuncent->stats.f_numcalls += localent->f_counts.f_numcalls;
	shfuncent->stats.f_total_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_total_time);
	shfuncent->stats.f_self_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_self_time);

	LWLockRelease(&shfuncent->header.lock);

	return true;
}

/*
 * pgstat_flush_db - flush out a local database stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry could not be flushed out.
 */
static bool
pgstat_flush_db(PgStat_PendingStatDBEntry *pendingent, Oid dboid,
						bool nowait)
{
	PgStatShm_StatDBEntry *sharedent;

	if (pendingent->shared == NULL)
		return false;

	if (!pgstat_shared_stat_lock(&pendingent->shared->header, nowait))
		return true;			/* failed to acquire lock, skip */

	sharedent = pendingent->shared;

#define PGSTAT_ACCUM_DBCOUNT(item)		\
	(sharedent)->stats.item += (pendingent)->pending.item

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
	if (OidIsValid(dboid))
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

	LWLockRelease(&sharedent->header.lock);

	memset(pendingent, 0, sizeof(*pendingent));

	return false;
}

/*
 * XXXX: always try to flush WAL stats. We don't want to manipulate another
 * counter during XLogInsert so we don't have an effecient short cut to know
 * whether any counter gets incremented.
 */
static inline bool
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
static bool
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
static bool
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
	PgStatShm_StatDBEntry *dbentry;

	/* can't get here in single user mode */
	Assert(StatsDSA != NULL);
	Assert(IsUnderPostmaster);

	/* FIXME: this didn't use to exist? Why? */
	if (!pgstat_track_counts)
		return;

	/*
	 * End-of-vacuum is reported instantly. Report the start the same way for
	 * consistency. Vacuum doesn't run frequently and is a long-lasting
	 * operation so it doesn't matter if we get blocked here a little.
	 */
	dbentry = (PgStatShm_StatDBEntry *)
		pgstat_shared_stat_locked(PGSTAT_TYPE_DB, dboid, InvalidOid, false);
	if (!dbentry)
		return;
	dbentry->stats.last_autovac_time = GetCurrentTimestamp();
	LWLockRelease(&dbentry->header.lock);
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
	shtabentry = (PgStatShm_StatTabEntry *)
		pgstat_shared_stat_locked(PGSTAT_TYPE_TABLE, dboid, tableoid, false);

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

	LWLockRelease(&shtabentry->header.lock);
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
	tabentry = (PgStatShm_StatTabEntry *)
		pgstat_shared_stat_locked(PGSTAT_TYPE_TABLE, dboid,
								  RelationGetRelid(rel),
								  false);
	/* can't get dropped while accessed */
	Assert(tabentry != NULL);

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
	LWLockRelease(&tabentry->header.lock);
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
	sharedent = (PgStatShm_StatDBEntry *)
		pgstat_shared_stat_locked(PGSTAT_TYPE_DB, dboid, InvalidOid, false);
	sharedent->stats.n_checksum_failures += failurecount;

	LWLockRelease(&sharedent->header.lock);
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

	shared_ref = pgstat_pending_prepare(PGSTAT_TYPE_FUNCTION,
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

	shared_ref = pgstat_pending_fetch(PGSTAT_TYPE_FUNCTION, MyDatabaseId, func_id);

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


/* ----------
 * pgstat_initstats() -
 *
 *	Initialize a relcache entry to count access statistics.
 *	Called whenever a relation is opened.
 *
 *	We assume that a relcache entry's pgstat_info field is zeroed by
 *	relcache.c when the relcache entry is made; thereafter it is long-lived
 *	data.
 * ----------
 */
void
pgstat_initstats(Relation rel)
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

void
pgstat_allocstats(Relation rel)
{
	Assert(rel->pgstat_enabled);
	Assert(rel->pgstat_info == NULL);

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = pgstat_pending_tab_prepare(RelationGetRelid(rel), rel->rd_rel->relisshared);
	/* mark this relation as the owner */

	/* don't allow link a stats to multiple relcache entries */
	Assert(rel->pgstat_info->relation == NULL);
	rel->pgstat_info->relation = rel;
}

/*
 * pgstat_delinkstats() -
 *
 *  Break the mutual link between a relcache entry and a local stats entry.
 *  This must be called always when one end of the link is removed.
 */
void
pgstat_delinkstats(Relation rel)
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

	shared_ref = pgstat_pending_fetch(PGSTAT_TYPE_TABLE, MyDatabaseId, rel_id);
	if (!shared_ref)
		shared_ref = pgstat_pending_fetch(PGSTAT_TYPE_TABLE, InvalidOid, rel_id);

	if (shared_ref)
		return shared_ref->pending;
	return NULL;
}

/*
 * get_tabstat_stack_level - add a new (sub)transaction stack entry if needed
 */
static PgStat_SubXactStatus *
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
static void
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
static void
pgstat_eoxact_relations(PgStat_SubXactStatus *xact_state, bool isCommit)
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

static void
pgstat_eosubxact_relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
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
	{
		PgStat_TableXactStatus *trans;

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
	{
		PgStat_TableXactStatus *trans;

		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			tabstat = trans->parent;
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
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
static void
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

		stats_data = dsa_get_address(StatsDSA, p->body);
		Assert(stats_data);

		if (stats_data->dropped)
			continue;

		Assert(pg_atomic_read_u32(&stats_data->refcount) > 0);

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

static void*
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

	if (shared_ref->shared == NULL || shared_ref->shared->dropped)
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
	memcpy(stats_data, ((char*) shared_ref->shared) + data_offset, data_size);

	if (pgstat_fetch_consistency > STATS_FETCH_CONSISTENCY_NONE)
	{
		PgStatSnapshotEntry *entry = NULL;
		bool found;

		entry = pgstat_snapshot_insert(stats_snapshot.stats, key, &found);
		entry->data = stats_data;
	}

	return stats_data;
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
		pgstat_fetch_entry(PGSTAT_TYPE_DB, dboid, InvalidOid);
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
		pgstat_fetch_entry(PGSTAT_TYPE_TABLE, dboid, reloid);
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
		pgstat_fetch_entry(PGSTAT_TYPE_FUNCTION, MyDatabaseId, func_id);
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


static void
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
	pgstat_snapshot_global(PGSTAT_TYPE_ARCHIVER);

	return &stats_snapshot.archiver;
}

PgStat_BgWriterStats *
pgstat_fetch_stat_bgwriter(void)
{
	pgstat_snapshot_global(PGSTAT_TYPE_BGWRITER);

	return &stats_snapshot.bgwriter;
}


PgStat_CheckPointerStats *
pgstat_fetch_stat_checkpointer(void)
{
	pgstat_snapshot_global(PGSTAT_TYPE_CHECKPOINTER);

	return &stats_snapshot.checkpointer;
}

PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	pgstat_snapshot_global(PGSTAT_TYPE_WAL);

	return &stats_snapshot.wal;
}

PgStat_SLRUStats *
pgstat_fetch_slru(void)
{
	pgstat_snapshot_global(PGSTAT_TYPE_SLRU);

	return stats_snapshot.slru;
}

PgStat_ReplSlotStats *
pgstat_fetch_replslot(int *nslots_p)
{
	pgstat_snapshot_global(PGSTAT_TYPE_REPLSLOT);

	*nslots_p = stats_snapshot.replslot_count;
	return stats_snapshot.replslot;
}

/*-------------------------------------------------------------------------
 *
 * test_aio_fsync.c
 *	  Targeted fault injection for asynchronous checkpoint synchronization.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_aio/test_aio_fsync.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/relation.h"
#include "common/relpath.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/spin.h"
#include "storage/sync.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

typedef struct FsyncTestState
{
	slock_t		lock;			/* protects the mutable fields below */
	ConditionVariable cv;		/* wakes blocked completions */
	FileTag		tag;			/* identifies the targeted sync request */
	bool		enabled;		/* whether tag matching is active */
	bool		wait;			/* whether matching completions should block */
	int			error;			/* error code injected into failures */
	int			failures;		/* number of failures left to inject */
	int			total_attempts; /* number of matching completion attempts */
	int			worker_attempts;	/* matching attempts made by I/O workers */
	char		cleanup;		/* checkpoint cleanup scenario to inject */
	uint32		wait_event;		/* wait event for blocked completions */
	int			total_peak;
	int			transient_peak;
	int			transient_drains;
} FsyncTestState;

static FsyncTestState *fsync_test;

/* Startup has no SQL connection; TAP controls this gate through files. */
static bool datadir_test;
static int	datadir_staged;
static int	datadir_completed;
static int	datadir_reaped;

void		test_aio_fsync_init(void);
void		test_aio_fsync_completion(PgAioHandle *ioh);
extern PGDLLEXPORT void test_aio_fsync_drain(const char *name,
											 const void *private_data, void *arg);
extern PGDLLEXPORT void test_aio_fsync_room(const char *name,
											const void *private_data, void *arg);
extern PGDLLEXPORT void test_aio_datadir(const char *name,
										 const void *private_data, void *arg);

static void
fsync_test_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "test_aio fsync",
					   .size = sizeof(FsyncTestState),
					   .ptr = (void **) &fsync_test);
}

static void
fsync_test_init(void *arg)
{
	SpinLockInit(&fsync_test->lock);
	ConditionVariableInit(&fsync_test->cv);
	fsync_test->wait_event = WaitEventInjectionPointNew("fsync_completion");
#ifdef USE_INJECTION_POINTS
	InjectionPointAttach("sync-before-drain", "test_aio",
						 "test_aio_fsync_drain", NULL, 0);
	InjectionPointAttach("sync-after-retry", "test_aio",
						 "test_aio_fsync_drain", NULL, 0);
	InjectionPointAttach("sync-transient-drain", "test_aio",
						 "test_aio_fsync_drain", NULL, 0);
	InjectionPointAttach("sync-total-room", "test_aio",
						 "test_aio_fsync_room", NULL, 0);
	InjectionPointAttach("sync-transient-room", "test_aio",
						 "test_aio_fsync_room", NULL, 0);
	InjectionPointAttach("datadir-sync-begin", "test_aio",
						 "test_aio_datadir", NULL, 0);
	InjectionPointAttach("datadir-sync-staged", "test_aio",
						 "test_aio_datadir", NULL, 0);
	InjectionPointAttach("datadir-sync-reaped", "test_aio",
						 "test_aio_datadir", NULL, 0);
	InjectionPointAttach("datadir-sync-before-drain", "test_aio",
						 "test_aio_datadir", NULL, 0);
	InjectionPointAttach("datadir-sync-end", "test_aio",
						 "test_aio_datadir", NULL, 0);
#endif
}

static const ShmemCallbacks fsync_test_callbacks = {
	.request_fn = fsync_test_shmem_request,
	.init_fn = fsync_test_init,
};

void
test_aio_fsync_init(void)
{
	RegisterShmemCallbacks(&fsync_test_callbacks);
}

/* Called with the spinlock held.  FileTags can contain padding. */
static bool
fsync_test_matches(const FileTag *tag)
{
	return fsync_test->enabled &&
		tag->handler == fsync_test->tag.handler &&
		tag->forknum == fsync_test->tag.forknum &&
		tag->segno == fsync_test->tag.segno &&
		RelFileLocatorEquals(tag->rlocator, fsync_test->tag.rlocator);
}

/* Change the raw result, rather than raising ERROR in a critical section. */
void
test_aio_fsync_completion(PgAioHandle *ioh)
{
	FileTag		tag = {0};
	bool		wait;

	if (ioh->target == PGAIO_TID_SYNC && datadir_test)
	{
		Assert(AmStartupProcess());
		/* Return one durability error and check startup's LOG-only policy. */
		if (++datadir_completed == 1)
			ioh->result = -EIO;
		return;
	}

	if (ioh->target == PGAIO_TID_SMGR)
	{
		tag.handler = SYNC_HANDLER_MD;
		tag.rlocator = ioh->target_data.smgr.rlocator;
		tag.forknum = ioh->target_data.smgr.forkNum;
		tag.segno = ioh->target_data.smgr.blockNum / RELSEG_SIZE;
	}
	else if (ioh->target == PGAIO_TID_SYNC_FILETAG)
		tag = ioh->target_data.sync_filetag;
	else
		return;

	SpinLockAcquire(&fsync_test->lock);
	if (!fsync_test_matches(&tag))
	{
		SpinLockRelease(&fsync_test->lock);
		return;
	}
	fsync_test->total_attempts++;
	if (AmIoWorkerProcess())
		fsync_test->worker_attempts++;
	if (fsync_test->failures > 0)
	{
		ioh->result = -fsync_test->error;
		fsync_test->failures--;
	}
	wait = fsync_test->wait;
	SpinLockRelease(&fsync_test->lock);

	if (!wait)
		return;

	ConditionVariablePrepareToSleep(&fsync_test->cv);
	for (;;)
	{
		SpinLockAcquire(&fsync_test->lock);
		wait = fsync_test->wait;
		SpinLockRelease(&fsync_test->lock);
		if (!wait)
			break;
		ConditionVariableSleep(&fsync_test->cv, fsync_test->wait_event);
	}
	ConditionVariableCancelSleep();
}

void
test_aio_fsync_drain(const char *name, const void *private_data, void *arg)
{
	InflightSyncEntry *entry = arg;
	char		cleanup;

	if (strcmp(name, "sync-transient-drain") == 0)
	{
		if (entry->close_method != SYNC_CLOSE_TRANSIENT)
			elog(ERROR, "transient pressure selected non-transient fsync");
		SpinLockAcquire(&fsync_test->lock);
		fsync_test->transient_drains++;
		SpinLockRelease(&fsync_test->lock);
		return;
	}

	SpinLockAcquire(&fsync_test->lock);
	cleanup = fsync_test_matches(&entry->tag) ? fsync_test->cleanup : '\0';
	SpinLockRelease(&fsync_test->lock);

	if (cleanup == 'b' && strcmp(name, "sync-before-drain") == 0)
	{
		/* Make the failed result available, but leave it for error cleanup. */
		if (entry->started)
			pgaio_wref_wait(&entry->iow);
		elog(ERROR, "test fsync checkpoint interruption");
	}
	if (cleanup == 'r' && strcmp(name, "sync-after-retry") == 0)
		elog(ERROR, "test fsync retry interruption");
	if (cleanup == 'c' && strcmp(name, "sync-before-drain") == 0)
		RememberSyncRequest(&entry->tag, SYNC_FORGET_REQUEST);
}

PG_FUNCTION_INFO_V1(fsync_test_configure);
Datum
fsync_test_configure(PG_FUNCTION_ARGS)
{
	FileTag		tag = {0};
	Relation	rel;
	char	   *error = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *cleanup = text_to_cstring(PG_GETARG_TEXT_PP(4));
	int			err = 0;

#ifndef USE_INJECTION_POINTS
	elog(ERROR, "injection points not supported");
#endif
	if (strcmp(error, "EIO") == 0)
		err = EIO;
	else if (strcmp(error, "ENOENT") == 0)
		err = ENOENT;
	else if (error[0] != '\0')
		elog(ERROR, "unsupported fsync test error");
	if (cleanup[0] != '\0' && strcmp(cleanup, "before") != 0 &&
		strcmp(cleanup, "retry") != 0 && strcmp(cleanup, "cancel") != 0)
		elog(ERROR, "unsupported fsync test cleanup");
	if (PG_GETARG_INT32(2) < 0 || PG_GETARG_INT64(6) < 0 ||
		PG_GETARG_INT32(7) < SYNC_HANDLER_MD ||
		PG_GETARG_INT32(7) >= SYNC_HANDLER_NONE)
		elog(ERROR, "invalid fsync test configuration");

	tag.handler = PG_GETARG_INT32(7);
	tag.forknum = forkname_to_number(text_to_cstring(PG_GETARG_TEXT_PP(5)));
	tag.segno = PG_GETARG_INT64(6);
	if (tag.handler == SYNC_HANDLER_MD)
	{
		rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
		tag.rlocator = rel->rd_locator;
		relation_close(rel, AccessShareLock);
	}

	SpinLockAcquire(&fsync_test->lock);
	fsync_test->tag = tag;
	fsync_test->enabled = true;
	fsync_test->error = err;
	fsync_test->failures = PG_GETARG_INT32(2);
	fsync_test->wait = PG_GETARG_BOOL(3);
	fsync_test->cleanup = cleanup[0];
	fsync_test->total_attempts = 0;
	fsync_test->worker_attempts = 0;
	fsync_test->total_peak = 0;
	fsync_test->transient_peak = 0;
	fsync_test->transient_drains = 0;
	SpinLockRelease(&fsync_test->lock);
	PG_RETURN_VOID();
}

/* Queue precisely the configured tag without depending on buffer eviction. */
PG_FUNCTION_INFO_V1(fsync_test_request);
Datum
fsync_test_request(PG_FUNCTION_ARGS)
{
	FileTag		tag;

	SpinLockAcquire(&fsync_test->lock);
	tag = fsync_test->tag;
	SpinLockRelease(&fsync_test->lock);
	if (!RegisterSyncRequest(&tag, SYNC_REQUEST, true))
		elog(ERROR, "could not queue test fsync request");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(fsync_test_release);
Datum
fsync_test_release(PG_FUNCTION_ARGS)
{
	SpinLockAcquire(&fsync_test->lock);
	fsync_test->wait = false;
	SpinLockRelease(&fsync_test->lock);
	ConditionVariableBroadcast(&fsync_test->cv);
	PG_RETURN_VOID();
}

/*
 * If true return all matching attempts, otherwise return attempts made by
 * workers.
 */
PG_FUNCTION_INFO_V1(fsync_test_count);
Datum
fsync_test_count(PG_FUNCTION_ARGS)
{
	int			count;

	SpinLockAcquire(&fsync_test->lock);
	count = PG_GETARG_BOOL(0) ?
		fsync_test->worker_attempts : fsync_test->total_attempts;
	SpinLockRelease(&fsync_test->lock);
	PG_RETURN_INT32(count);
}

/* Observe total and transient admission, including retries. */
void
test_aio_fsync_room(const char *name, const void *private_data, void *arg)
{
	int			count = *(int *) arg + 1;
	bool		transient;
	int		   *peak;

	if (strcmp(name, "sync-transient-room") == 0)
		transient = true;
	else if (strcmp(name, "sync-total-room") == 0)
		transient = false;
	else
		elog(ERROR, "unexpected injection point name \"%s\"", name);

	if (count > GetFsyncConcurrencyLimit(transient))
		elog(ERROR, "fsync concurrency limit exceeded");
	SpinLockAcquire(&fsync_test->lock);
	peak = transient ? &fsync_test->transient_peak : &fsync_test->total_peak;
	*peak = Max(*peak, count);
	SpinLockRelease(&fsync_test->lock);
}

PG_FUNCTION_INFO_V1(fsync_test_peak);
Datum
fsync_test_peak(PG_FUNCTION_ARGS)
{
	int			peak;

	SpinLockAcquire(&fsync_test->lock);
	peak = PG_GETARG_BOOL(0) ? fsync_test->transient_peak : fsync_test->total_peak;
	SpinLockRelease(&fsync_test->lock);
	PG_RETURN_INT32(peak);
}

PG_FUNCTION_INFO_V1(fsync_test_limit);
Datum
fsync_test_limit(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(GetFsyncConcurrencyLimit(PG_GETARG_BOOL(0)));
}

PG_FUNCTION_INFO_V1(fsync_test_drain_count);
Datum
fsync_test_drain_count(PG_FUNCTION_ARGS)
{
	int			count;

	SpinLockAcquire(&fsync_test->lock);
	count = fsync_test->transient_drains;
	SpinLockRelease(&fsync_test->lock);
	PG_RETURN_INT32(count);
}

void
test_aio_datadir(const char *name, const void *private_data, void *arg)
{
	if (strcmp(name, "datadir-sync-begin") == 0)
	{
		datadir_test = access("test_aio_datadir", F_OK) == 0;
		datadir_staged = datadir_completed = datadir_reaped = 0;
	}
	if (!datadir_test)
		return;

	if (strcmp(name, "datadir-sync-staged") == 0)
		datadir_staged++;
	else if (strcmp(name, "datadir-sync-reaped") == 0)
	{
		datadir_reaped++;
		elog(LOG, "test datadir synced: %s", (char *) arg);
	}
	else if (strcmp(name, "datadir-sync-before-drain") == 0)
	{
		elog(LOG, "test datadir final drain: %d pending, %d staged, %d completed, %d reaped",
			 *(int *) arg, datadir_staged, datadir_completed, datadir_reaped);
		while (access("test_aio_datadir", F_OK) == 0)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(10000L);
		}
	}
	else if (strcmp(name, "datadir-sync-end") == 0)
	{
		if (datadir_staged != datadir_completed ||
			datadir_staged != datadir_reaped)
			elog(ERROR, "test datadir synchronization was not drained");
		elog(LOG, "test datadir synchronization drained");
		datadir_test = false;
	}
}

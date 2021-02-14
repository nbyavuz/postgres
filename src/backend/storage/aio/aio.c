/*-------------------------------------------------------------------------
 *
 * aio.c
 *	  Asynchronous I/O subsytem.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/aio.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * Big picture changes needed:
 * - backend local recycleable IOs
 * - retries for AIOs that cannot be retried shared (e.g. XLogFileInit())
 * - docs / comments / cleanup
 * - move more of the shared callback logic into bufmgr.c etc
 * - merging of IOs when submitting individual IOs, not when submitting all pending IOs
 * - Shrink size of PgAioInProgress
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "access/xlog.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/squeue32.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/memnodes.h"
#include "pgstat.h"
#include "access/xlog_internal.h"
#include "port/pg_iovec.h"
#include "postmaster/interrupt.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"

#define PGAIO_VERBOSE

/* global list of in-progress IO */
PgAioCtl *aio_ctl;

/* current backend's per-backend state */
PgAioPerBackend *my_aio;
int my_aio_id;

/* FIXME: move into PgAioPerBackend / subsume into ->reaped */
static dlist_head local_recycle_requests;

/* general pgaio helper functions */
static void pgaio_apply_backend_limit(void);
static void pgaio_io_prepare(PgAioInProgress *io, PgAioOp op);
static void pgaio_io_stage(PgAioInProgress *io, PgAioSharedCallback scb);
static void pgaio_bounce_buffer_release_internal(PgAioBounceBuffer *bb, bool holding_lock, bool release_resowner);
static void pgaio_io_ref_internal(PgAioInProgress *io, PgAioIoRef *ref);
static void pgaio_transfer_foreign_to_local(void);
static void pgaio_call_local_callbacks(bool in_error);
static int pgaio_synchronous_submit(void);
static void pgaio_io_wait_ref_int(PgAioIoRef *ref, bool call_shared, bool call_local);
static void pgaio_io_retry_soft_failed(PgAioInProgress *io, uint64 ref_generation);
static void pgaio_wait_for_issued_internal(int n, int fd);

/* printing functions */
static const char *pgaio_io_shared_callback_string(PgAioSharedCallback a);
static const char *pgaio_io_operation_string(PgAioOp op);

/* IO callbacks */
static void pgaio_invalid_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_nop_complete(PgAioInProgress *io);
static void pgaio_nop_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_fsync_raw_complete(PgAioInProgress *io);
static void pgaio_fsync_raw_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_fsync_wal_complete(PgAioInProgress *io);
static void pgaio_fsync_wal_retry(PgAioInProgress *io);
static void pgaio_fsync_wal_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_flush_range_complete(PgAioInProgress *io);
static void pgaio_flush_range_desc(PgAioInProgress *io, StringInfo s);
static void pgaio_flush_range_smgr_retry(PgAioInProgress *io);

static bool pgaio_read_sb_complete(PgAioInProgress *io);
static void pgaio_read_sb_retry(PgAioInProgress *io);
static void pgaio_read_sb_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_read_smgr_complete(PgAioInProgress *io);
static void pgaio_read_smgr_retry(PgAioInProgress *io);
static void pgaio_read_smgr_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_sb_complete(PgAioInProgress *io);
static void pgaio_write_sb_retry(PgAioInProgress *io);
static void pgaio_write_sb_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_smgr_complete(PgAioInProgress *io);
static void pgaio_write_smgr_retry(PgAioInProgress *io);
static void pgaio_write_smgr_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_wal_complete(PgAioInProgress *io);
static void pgaio_write_wal_retry(PgAioInProgress *io);
static void pgaio_write_wal_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_raw_complete(PgAioInProgress *io);
static void pgaio_write_raw_desc(PgAioInProgress *io, StringInfo s);


/*
 * Implementation of different AIO actions.
 *
 * To support EXEC_BACKEND environments, where we cannot rely on callback
 * addresses being equivalent across processes, PgAioInProgress does not point
 * directly to the type's PgAioActionCBs, but contains an index instead.
 */


/*
 * IO completion callback.
 */
typedef bool (*PgAioCompletedCB)(PgAioInProgress *io);

/*
 * IO retry callback.
 */
typedef void (*PgAioRetryCB)(PgAioInProgress *io);

/*
 * IO desc callback.
 */
typedef void (*PgAioDescCB)(PgAioInProgress *io, StringInfo s);

typedef struct PgAioActionCBs
{
	PgAioOp op;
	const char *name;
	PgAioRetryCB retry;
	PgAioCompletedCB complete;
	PgAioDescCB desc;
} PgAioActionCBs;

static const PgAioActionCBs io_action_cbs[] =
{
	[PGAIO_SCB_INVALID] =
	{
		.op = PGAIO_OP_INVALID,
		.name = "invalid",
		.desc = pgaio_invalid_desc,
	},

	[PGAIO_SCB_READ_SB] =
	{
		.op = PGAIO_OP_READ,
		.name = "sb",
		.retry = pgaio_read_sb_retry,
		.complete = pgaio_read_sb_complete,
		.desc = pgaio_read_sb_desc,
	},

	[PGAIO_SCB_READ_SMGR] =
	{
		.op = PGAIO_OP_READ,
		.name = "smgr",
		.retry = pgaio_read_smgr_retry,
		.complete = pgaio_read_smgr_complete,
		.desc = pgaio_read_smgr_desc,
	},

	[PGAIO_SCB_WRITE_SB] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "sb",
		.retry = pgaio_write_sb_retry,
		.complete = pgaio_write_sb_complete,
		.desc = pgaio_write_sb_desc,
	},

	[PGAIO_SCB_WRITE_SMGR] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "smgr",
		.retry = pgaio_write_smgr_retry,
		.complete = pgaio_write_smgr_complete,
		.desc = pgaio_write_smgr_desc,
	},

	[PGAIO_SCB_WRITE_WAL] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "wal",
		.retry = pgaio_write_wal_retry,
		.complete = pgaio_write_wal_complete,
		.desc = pgaio_write_wal_desc,
	},

	[PGAIO_SCB_WRITE_RAW] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "raw",
		.complete = pgaio_write_raw_complete,
		.desc = pgaio_write_raw_desc,
	},

	[PGAIO_SCB_FSYNC_RAW] =
	{
		.op = PGAIO_OP_FSYNC,
		.name = "raw",
		.complete = pgaio_fsync_raw_complete,
		.desc = pgaio_fsync_raw_desc,
	},

	[PGAIO_SCB_FSYNC_WAL] =
	{
		.op = PGAIO_OP_FSYNC,
		.name = "wal",
		.retry = pgaio_fsync_wal_retry,
		.complete = pgaio_fsync_wal_complete,
		.desc = pgaio_fsync_wal_desc,
	},

	[PGAIO_SCB_FLUSH_RANGE_RAW] =
	{
		.op = PGAIO_OP_FLUSH_RANGE,
		.name = "raw",
		.complete = pgaio_flush_range_complete,
		.desc = pgaio_flush_range_desc,
	},

	[PGAIO_SCB_FLUSH_RANGE_SMGR] =
	{
		.op = PGAIO_OP_FLUSH_RANGE,
		.name = "smgr",
		.retry = pgaio_flush_range_smgr_retry,
		.complete = pgaio_flush_range_complete,
		.desc = pgaio_flush_range_desc,
	},

	[PGAIO_SCB_NOP] =
	{
		.op = PGAIO_OP_NOP,
		.name = "nop",
		.complete = pgaio_nop_complete,
		.desc = pgaio_nop_desc,
	},
};

/* GUCs */
int io_method;
int max_aio_in_progress = 32768; /* XXX: Multiple of MaxBackends instead? */
int max_aio_in_flight = 4096;
int max_aio_bounce_buffers = 1024;
int io_max_concurrency = 128;

static int aio_local_callback_depth = 0;

/* Options for io_method. */
const struct config_enum_entry io_method_options[] = {
	{"worker", IOMETHOD_WORKER, false},
#ifdef USE_LIBURING
	{"io_uring", IOMETHOD_LIBURING, false},
#endif
#ifdef USE_POSIX_AIO
	{"posix", IOMETHOD_POSIX, false},
#endif
	{NULL, 0, false}
};

static Size
AioCtlShmemSize(void)
{
	Size		sz;

	/* aio_ctl itself */
	sz = offsetof(PgAioCtl, in_progress_io);

	/* ios */
	sz = add_size(sz, mul_size(max_aio_in_progress, sizeof(PgAioInProgress)));

	return sz;
}

static Size
AioCtlBackendShmemSize(void)
{
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS;

	return mul_size(TotalProcs, sizeof(PgAioPerBackend));
}

static Size
AioBounceShmemSize(void)
{
	Size		sz;

	/* PgAioBounceBuffer itself */
	sz = mul_size(sizeof(PgAioBounceBuffer), max_aio_bounce_buffers);

	/* and the associated buffer */
	sz = add_size(sz,
				  mul_size(BLCKSZ, add_size(max_aio_bounce_buffers, 1)));

	return sz;
}

Size
AioShmemSize(void)
{
	Size		sz = 0;

	sz = add_size(sz, AioCtlShmemSize());
	sz = add_size(sz, AioCtlBackendShmemSize());
	sz = add_size(sz, AioBounceShmemSize());

	sz = add_size(sz, AioWorkerShmemSize());

#ifdef USE_LIBURING
	sz = add_size(sz, AioUringShmemSize());
#endif

#ifdef USE_POSIX_AIO
	sz = add_size(sz, AioPosixShmemSize());
#endif

	return sz;
}

void
AioShmemInit(void)
{
	bool		found;
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS;

	aio_ctl = (PgAioCtl *)
		ShmemInitStruct("PgAio", AioCtlShmemSize(), &found);

	if (!found)
	{
		memset(aio_ctl, 0, AioCtlShmemSize());

		dlist_init(&aio_ctl->unused_ios);
		dlist_init(&aio_ctl->reaped_uncompleted);

		/* Initialize IOs. */
		for (int i = 0; i < max_aio_in_progress; i++)
		{
			PgAioInProgress *io = &aio_ctl->in_progress_io[i];

			ConditionVariableInit(&io->cv);
			dlist_push_tail(&aio_ctl->unused_ios, &io->owner_node);
			io->flags = PGAIOIP_UNUSED;
			io->system_referenced = true;
			io->generation = 1;
			io->bb_idx = PGAIO_BB_INVALID;
			io->merge_with_idx = PGAIO_MERGE_INVALID;
		}

		/* Initialize per-backend memory. */
		aio_ctl->backend_state_count = TotalProcs;
		aio_ctl->backend_state = (PgAioPerBackend *)
			ShmemInitStruct("PgAioBackend", AioCtlBackendShmemSize(), &found);
		memset(aio_ctl->backend_state, 0, AioCtlBackendShmemSize());

		for (int procno = 0; procno < TotalProcs; procno++)
		{
			PgAioPerBackend *bs = &aio_ctl->backend_state[procno];

			dlist_init(&bs->unused);
			dlist_init(&bs->outstanding);
			dlist_init(&bs->pending);
			dlist_init(&bs->issued);
			dlist_init(&bs->issued_abandoned);
			pg_atomic_init_u32(&bs->inflight_count, 0);
			dlist_init(&bs->reaped);

			dlist_init(&bs->foreign_completed);
			SpinLockInit(&bs->foreign_completed_lock);
		}

		/* Initialize bounce buffers. */
		{
			char *p;
			char *blocks;

			dlist_init(&aio_ctl->unused_bounce_buffers);
			aio_ctl->bounce_buffers =
				ShmemInitStruct("PgAioBounceBuffers",
								sizeof(PgAioBounceBuffer) * max_aio_bounce_buffers,
								&found);
			Assert(!found);

			p = ShmemInitStruct("PgAioBounceBufferBlocks",
								BLCKSZ * (max_aio_bounce_buffers + 1),
								&found);
			Assert(!found);
			blocks = (char *) TYPEALIGN(BLCKSZ, (uintptr_t) p);

			for (int i = 0; i < max_aio_bounce_buffers; i++)
			{
				PgAioBounceBuffer *bb = &aio_ctl->bounce_buffers[i];

				bb->buffer = blocks + i * BLCKSZ;
				memset(bb->buffer, 0, BLCKSZ);
				pg_atomic_init_u32(&bb->refcount, 0);
				dlist_push_tail(&aio_ctl->unused_bounce_buffers, &bb->node);
				aio_ctl->unused_bounce_buffers_count++;
			}
		}

		/* Initialize IO-engine specific resources. */
		if (io_method == IOMETHOD_WORKER)
			AioWorkerShmemInit();
#ifdef USE_LIBURING
		else if (io_method == IOMETHOD_LIBURING)
			AioUringShmemInit();
#endif
#ifdef USE_POSIX_AIO
		else if (io_method == IOMETHOD_POSIX)
			AioPosixShmemInit();
#endif
	}
}

void
pgaio_postmaster_init(void)
{
	/* FIXME: should also be allowed to use AIO */
	dlist_init(&local_recycle_requests);

	// XXX: could create a local queue here.
}

void
pgaio_postmaster_child_init_local(void)
{
	if (io_method == IOMETHOD_WORKER)
		;
#ifdef USE_LIBURING
	else if (io_method == IOMETHOD_LIBURING)
		pgaio_uring_postmaster_child_init_local();
#endif
#ifdef USE_POSIX_AIO
	else if (io_method == IOMETHOD_POSIX)
		pgaio_posix_postmaster_child_init_local();
#endif
}

static void
pgaio_postmaster_before_child_exit(int code, Datum arg)
{
	elog(DEBUG2, "aio before shmem exit: start");

	/*
	 * When exitinging in a normal backend there will be no pending IOs due to
	 * pgaio_at_commit()/pgaio_at_abort(). But for aux processes that won't be
	 * the case, so do so explicitly.
	 */
	pgaio_at_abort();


	/*
	 * Need to wait for in-progress IOs initiated by this backend to
	 * finish. Some operating systems, like linux w/ io_uring, cancel IOs that
	 * are still in progress when exiting. Other's don't provide access to the
	 * results of such IOs.
	 */
	pgaio_wait_for_issued_internal(-1, -1);
	pgaio_complete_ios(true);

	elog(DEBUG2, "aio before shmem exit: end");
}

static void
pgaio_postmaster_child_exit(int code, Datum arg)
{
	/* FIXME: handle unused */
	Assert(my_aio->outstanding_count == 0);
	Assert(dlist_is_empty(&my_aio->outstanding));

	Assert(my_aio->pending_count == 0);
	Assert(dlist_is_empty(&my_aio->pending));

	Assert(my_aio->issued_count == 0);
	Assert(dlist_is_empty(&my_aio->issued));

	Assert(my_aio->issued_abandoned_count == 0);
	Assert(dlist_is_empty(&my_aio->issued_abandoned));

	Assert(pg_atomic_read_u32(&my_aio->inflight_count) == 0);

	Assert(dlist_is_empty(&my_aio->reaped));

	Assert(my_aio->local_completed_count == 0);
	Assert(dlist_is_empty(&my_aio->local_completed));

	Assert(my_aio->foreign_completed_count == 0);
	Assert(dlist_is_empty(&my_aio->foreign_completed));

	my_aio = NULL;
}

void
pgaio_postmaster_child_init(void)
{
	Assert(!my_aio);

#ifdef USE_LIBURING
	if (io_method == IOMETHOD_LIBURING)
	{
		/* no locking needed here, only affects this process */
		for (int i = 0; i < aio_ctl->num_contexts; i++)
			io_uring_ring_dontfork(&aio_ctl->contexts[i].io_uring_ring);
	}
#endif

	my_aio_id = MyProc->pgprocno;
	my_aio = &aio_ctl->backend_state[my_aio_id];

	dlist_init(&local_recycle_requests);

	before_shmem_exit(pgaio_postmaster_before_child_exit, 0);
	on_shmem_exit(pgaio_postmaster_child_exit, 0);

	Assert(my_aio->unused_count == 0);
	Assert(my_aio->outstanding_count == 0);
	Assert(my_aio->issued_count == 0);
	Assert(my_aio->issued_abandoned_count == 0);
	Assert(my_aio->pending_count == 0);
	Assert(my_aio->local_completed_count == 0);
	Assert(my_aio->foreign_completed_count == 0);

	/* try to spread out a bit from the start */
	my_aio->last_context = MyProcPid % PGAIO_NUM_CONTEXTS;

	my_aio->executed_total_count = 0;
	my_aio->issued_total_count = 0;
	my_aio->submissions_total_count = 0;
	my_aio->foreign_completed_total_count = 0;
	my_aio->retry_total_count = 0;

#ifdef USE_LIBURING
	my_aio->wr.context = NULL;
	my_aio->wr.aio = NULL;
	my_aio->wr.ref_generation = PG_UINT32_MAX;
#endif
}

void
pgaio_at_abort(void)
{
	/* could have failed within a callback, so reset */
	aio_local_callback_depth = 0;

	pgaio_complete_ios(/* in_error = */ true);

	while (!dlist_is_empty(&my_aio->outstanding))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->outstanding);

		pgaio_io_release(io);
	}

	while (!dlist_is_empty(&my_aio->issued))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->issued);

		pgaio_io_release(io);
	}

	/*
	 * Submit pending requests *after* releasing all IO references. That
	 * ensures that local callback won't get called.
	 */
	pgaio_submit_pending(false);
}

void
pgaio_at_commit(void)
{
	Assert(aio_local_callback_depth == 0);
	Assert(dlist_is_empty(&local_recycle_requests));

	while (!dlist_is_empty(&my_aio->outstanding))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->outstanding);

		elog(WARNING, "leaked outstanding io %zu", io - aio_ctl->in_progress_io);

		pgaio_io_release(io);
	}

	while (!dlist_is_empty(&my_aio->issued))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->issued);

		elog(WARNING, "leaked issued io %zu", io - aio_ctl->in_progress_io);

		pgaio_io_release(io);
	}

	/* see pgaio_at_abort() */
	if (my_aio->pending_count != 0)
	{
		elog(WARNING, "unsubmitted IOs %d", my_aio->pending_count);
		pgaio_submit_pending(false);
	}
}

/* helper pgaio_process_io_completion combining the read/write cases */
static void
pgaio_process_io_rw_completion(PgAioInProgress *myio,
							   bool first,
							   uint32 nbytes,
							   uint64 offset,
							   uint32 *already_done,
							   int result,
							   int *running_result_p,
							   int *new_result,
							   PgAioIPFlags *new_flags)
{
	int running_result = *running_result_p;
	int remaining_bytes;

	Assert(first || *already_done == 0);
	Assert(*already_done < nbytes);

	remaining_bytes = nbytes - *already_done;

	if (result <= 0)
	{
		/* merged request failed, report failure on all */
		*new_result = result;

		if (result == -EAGAIN || result == -EINTR)
			*new_flags |= PGAIOIP_SOFT_FAILURE;
		else
			*new_flags |= PGAIOIP_HARD_FAILURE;

		ereport(DEBUG2,
				errcode_for_file_access(),
				errmsg("aio %zd: failed to %s: %s of %u-%u bytes at offset %llu+%u: %s",
					   myio - aio_ctl->in_progress_io,
					   pgaio_io_operation_string(myio->op),
					   pgaio_io_shared_callback_string(myio->scb),
					   nbytes, *already_done,
					   (long long unsigned) offset, *already_done,
					   strerror(-*new_result)));
	}
	else if (running_result == 0)
	{
		Assert(!first);
		*new_result = -EAGAIN;
		*new_flags |= PGAIOIP_SOFT_FAILURE;
	}
	else if (running_result >= remaining_bytes)
	{
		/* request completely satisfied */
		*already_done += remaining_bytes;
		running_result -= remaining_bytes;
		*new_result = *already_done;
	}
	else
	{
		/*
		 * This is actually pretty common and harmless, happens e.g. when one
		 * parts of the read are in the kernel page cache, but others
		 * aren't. So don't issue WARNING/ERROR, but just retry.
		 *
		 * While it can happen with single BLCKSZ reads (since they're bigger
		 * than typical page sizes), it's made much more likely by us
		 * combining reads.
		 *
		 * For writes it can happen e.g. in case of memory pressure.
		 */

		ereport(DEBUG3,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("aio %zd: partial %s:%s of %u-%u bytes at offset %llu+%u: read only %u of %u",
						myio - aio_ctl->in_progress_io,
						pgaio_io_operation_string(myio->op),
						pgaio_io_shared_callback_string(myio->scb),
						nbytes, *already_done,
						(long long unsigned) offset, *already_done,
						running_result,
						nbytes - *already_done)));

		Assert(running_result < remaining_bytes);
		*already_done += running_result;
		*new_result = *already_done;
		running_result = 0;

		*new_flags |= PGAIOIP_SOFT_FAILURE;
	}

	*running_result_p = running_result;
}

void
pgaio_process_io_completion(PgAioInProgress *io, int result)
{
	int running_result;
	PgAioInProgress *cur = io;
	bool first = true;

	Assert(io->flags & PGAIOIP_INFLIGHT);
	Assert(io->system_referenced);
	Assert(io->result == 0 || io->merge_with_idx == PGAIO_MERGE_INVALID);

	/* very useful for testing the retry logic */
	/*
	 * XXX: a probabilistic mode would be useful, especially if it also
	 * injected occasional EINTR/EAGAINs.
	 */
#if 0
	if (io->op == PGAIO_OP_READ && result > 4096)
		result = 4096;
#endif
#if 0
	if (result > 4096 && (
			io->scb == PGAIO_SCB_WRITE_SMGR
			|| io->scb == PGAIO_SCB_WRITE_SB
			|| io->scb == PGAIO_SCB_WRITE_WAL
			|| io->scb == PGAIO_SCB_WRITE_RAW
			))
		result = 4096;
#endif
#if 0
	if (io->scb == PGAIO_SCB_WRITE_RAW &&
		!(io->flags & PGAIOIP_RETRY))
		result = -EAGAIN;
#endif
#if 0
	if (io->op == PGAIO_OP_FSYNC &&
		!(io->flags & PGAIOIP_RETRY))
		result = -EAGAIN;
#endif

	/* treat 0 length write as ENOSPC */
	if (result == 0 && io->op == PGAIO_OP_WRITE)
		running_result = result = -ENOSPC;
	else
		running_result = result;

	while (cur)
	{
		PgAioInProgress *next;
		PgAioIPFlags new_flags;

		new_flags = cur->flags;

		Assert(new_flags & PGAIOIP_INFLIGHT);
		Assert(cur->system_referenced);
		Assert(cur->op == io->op);
		Assert(cur->scb == io->scb);

		if (cur->merge_with_idx != PGAIO_MERGE_INVALID)
		{
			next = &aio_ctl->in_progress_io[cur->merge_with_idx];
			cur->merge_with_idx = PGAIO_MERGE_INVALID;
		}
		else
			next = NULL;

		switch (cur->op)
		{
			case PGAIO_OP_READ:
				pgaio_process_io_rw_completion(cur,
											   first,
											   cur->op_data.read.nbytes,
											   cur->op_data.read.offset,
											   &cur->op_data.read.already_done,
											   result,
											   &running_result,
											   &cur->result,
											   &new_flags);
				break;

			case PGAIO_OP_WRITE:
				pgaio_process_io_rw_completion(cur,
											   first,
											   cur->op_data.write.nbytes,
											   cur->op_data.write.offset,
											   &cur->op_data.write.already_done,
											   result,
											   &running_result,
											   &cur->result,
											   &new_flags);
				break;

			case PGAIO_OP_FSYNC:
			case PGAIO_OP_FLUSH_RANGE:
			case PGAIO_OP_NOP:
				cur->result = result;
				if (result == -EAGAIN || result == -EINTR)
					new_flags |= PGAIOIP_SOFT_FAILURE;
				else if (result < 0)
					new_flags |= PGAIOIP_HARD_FAILURE;
				break;

			case PGAIO_OP_INVALID:
				pg_unreachable();
				elog(ERROR, "invalid");
				break;
		}

		new_flags &= ~PGAIOIP_INFLIGHT;
		new_flags |= PGAIOIP_REAPED;

		WRITE_ONCE_F(cur->flags) = new_flags;

		dlist_push_tail(&my_aio->reaped, &cur->io_node);

		cur = next;
		first = false;
	}

}

static bool
pgaio_io_call_shared_complete(PgAioInProgress *io)
{
	Assert(io_action_cbs[io->scb].op == io->op);

#ifdef PGAIO_VERBOSE
	if (message_level_is_interesting(DEBUG3))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
		StringInfoData s;

		initStringInfo(&s);

		pgaio_io_print(io, &s);

		ereport(DEBUG3,
				errmsg("completing %s",
					   s.data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s.data);
		MemoryContextSwitchTo(oldcontext);
	}
#endif

	return io_action_cbs[io->scb].complete(io);
}

void
pgaio_complete_ios(bool in_error)
{
	int pending_count_before = my_aio->pending_count;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	/*
	 * Don't want to recurse into proc_exit() or such while calling callbacks
	 * - we need to process the shared (but not local) callbacks.
	 */
	HOLD_INTERRUPTS();

	/* call all callbacks, without holding lock */
	while (!dlist_is_empty(&my_aio->reaped))
	{
		dlist_node *node = dlist_head_node(&my_aio->reaped);
		PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

		Assert(dlist_is_member(&my_aio->reaped, &io->io_node));

		Assert(node != NULL);

		if (!(io->flags & PGAIOIP_SHARED_CALLBACK_CALLED))
		{
			bool finished;

			/*
			 * Set flag before calling callback, otherwise we could easily end
			 * up looping forever.
			 */
			WRITE_ONCE_F(io->flags) |= PGAIOIP_SHARED_CALLBACK_CALLED;

			finished = pgaio_io_call_shared_complete(io);

			dlist_delete_from(&my_aio->reaped, node);

			if (finished)
			{
				/* if a soft failure is done, we can't retry */
				if (io->flags & PGAIOIP_SOFT_FAILURE)
				{
					WRITE_ONCE_F(io->flags) =
						(io->flags & ~PGAIOIP_SOFT_FAILURE) |
						PGAIOIP_HARD_FAILURE;
				}

				dlist_push_tail(&local_recycle_requests, &io->io_node);
			}
			else
			{
				Assert(io->flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_HARD_FAILURE));

				LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
				WRITE_ONCE_F(io->flags) =
					(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
					PGAIOIP_DONE;
				dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
				LWLockRelease(SharedAIOCtlLock);

				/* signal state change */
				if (IsUnderPostmaster)
					ConditionVariableBroadcast(&io->cv);
			}
		}
		else
		{
			Assert(in_error);

			dlist_delete_from(&my_aio->reaped, node);

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			WRITE_ONCE_F(io->flags) =
				(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
				PGAIOIP_DONE |
				PGAIOIP_HARD_FAILURE;
			dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
			LWLockRelease(SharedAIOCtlLock);
		}

#ifdef USE_POSIX_AIO
		if (io_method == IOMETHOD_POSIX)
		{
			/*
			 * The owner may be blocked in pgail_posix_wait_one(), but we
			 * managed to process the completion events.  Wake it up.
			 *
			 * XXX An alternative would be to have multiple completion queues
			 * with locks (ie contexts, just like the uring code), and have
			 * pgaio_posix_wait_one() hold the lock so that other backends
			 * don't finish up processing these.
			 */
			if (io->submitter_id != my_aio_id)
				SetLatch(&ProcGlobal->allProcs[io->submitter_id].procLatch);
		}
#endif
	}

	RESUME_INTERRUPTS();

	/* if any IOs weren't fully done, re-submit them */
	if (pending_count_before != my_aio->pending_count)
		pgaio_submit_pending(false);

	/*
	 * Next, under lock, process all the still pending requests. This entails
	 * releasing the "system" reference on the IO and checking which callbacks
	 * need to be called.
	 */
	START_CRIT_SECTION();

	while (!dlist_is_empty(&local_recycle_requests))
	{
		dlist_mutable_iter iter;
		PgAioInProgress* signal_ios[32];
		int to_signal = 0;

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

		dlist_foreach_modify(iter, &local_recycle_requests)
		{
			PgAioInProgress *cur = dlist_container(PgAioInProgress, io_node, iter.cur);

			dlist_delete_from(&local_recycle_requests, iter.cur);
			signal_ios[to_signal++] = cur;

			Assert(cur->system_referenced);
			Assert(cur->flags & PGAIOIP_REAPED);
			Assert(!(cur->flags & PGAIOIP_DONE));
			Assert(!(cur->flags & PGAIOIP_INFLIGHT));
			Assert(!(cur->flags & (PGAIOIP_SOFT_FAILURE)));
			Assert(cur->merge_with_idx == PGAIO_MERGE_INVALID);

			if (cur->user_referenced)
			{
				cur->system_referenced = false;

				if (cur->owner_id != my_aio_id)
				{
					PgAioPerBackend *other = &aio_ctl->backend_state[cur->owner_id];

					SpinLockAcquire(&other->foreign_completed_lock);

					dlist_push_tail(&other->foreign_completed, &cur->io_node);
					other->foreign_completed_count++;
					other->foreign_completed_total_count++;

					pg_write_barrier();

					WRITE_ONCE_F(cur->flags) =
						(cur->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
						PGAIOIP_DONE |
						PGAIOIP_FOREIGN_DONE;

					SpinLockRelease(&other->foreign_completed_lock);
				}
				else
				{
					WRITE_ONCE_F(cur->flags) =
						(cur->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
						PGAIOIP_DONE;

					dlist_push_tail(&my_aio->local_completed, &cur->io_node);
					my_aio->local_completed_count++;
				}
			}
			else
			{
				PgAioPerBackend *other = &aio_ctl->backend_state[cur->owner_id];

#ifdef PGAIO_VERBOSE
				ereport(DEBUG4,
						errmsg("removing aio %zu/%llu from issued_abandoned complete_ios",
							   cur - aio_ctl->in_progress_io,
							   (long long unsigned) cur->generation),
						errhidecontext(1),
						errhidestmt(1));
#endif

				dlist_delete_from(&other->issued_abandoned, &cur->owner_node);
				Assert(other->issued_abandoned_count > 0);
				other->issued_abandoned_count--;

				cur->generation++;
				pg_write_barrier();

				cur->flags = PGAIOIP_UNUSED;

				if (cur->bb_idx != PGAIO_BB_INVALID)
				{
					pgaio_bounce_buffer_release_internal(&aio_ctl->bounce_buffers[cur->bb_idx],
														 /* holding_lock = */ true,
														 /* release_resowner = */ false);
					cur->bb_idx = PGAIO_BB_INVALID;
				}

				cur->op = PGAIO_OP_INVALID;
				cur->scb = PGAIO_SCB_INVALID;
				cur->owner_id = INVALID_PGPROCNO;
				cur->result = 0;
				cur->system_referenced = true;
				cur->on_completion_local = NULL;

				dlist_push_head(&aio_ctl->unused_ios, &cur->owner_node);
				aio_ctl->used_count--;
			}

			if (to_signal >= lengthof(signal_ios))
				break;
		}
		LWLockRelease(SharedAIOCtlLock);

		if (IsUnderPostmaster)
		{
			for (int i = 0; i < to_signal; i++)
			{
				ConditionVariableBroadcast(&signal_ios[i]->cv);
			}
		}
	}

	END_CRIT_SECTION();
}

/*
 * Broadcast on a set of IOs (and merged IOs).
 *
 * We need to be careful about other backends resetting merge_with_idx.  We
 * still could end up broadcasting on IOs that we don't care about, but that's
 * harmless.
 */
void
pgaio_broadcast_ios(PgAioInProgress **ios, int nios)
{
	for (int i = 0; i < nios; i++)
	{
		PgAioInProgress *cur = ios[i];

		while (true)
		{
			uint32 next_idx;

			ConditionVariableBroadcast(&cur->cv);

			next_idx = cur->merge_with_idx;
			pg_compiler_barrier();
			if (next_idx == PGAIO_MERGE_INVALID)
				break;
			cur = &aio_ctl->in_progress_io[next_idx];
		}
	}
}

static void
pgaio_io_call_local_callback(PgAioInProgress *io, bool in_error)
{
	Assert(!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED));
	Assert(io->user_referenced);

	Assert(my_aio->local_completed_count > 0);
	dlist_delete_from(&my_aio->local_completed, &io->io_node);
	Assert(my_aio->local_completed_count > 0);
	my_aio->local_completed_count--;

	dlist_delete_from(&my_aio->issued, &io->owner_node);
	Assert(my_aio->issued_count > 0);
	my_aio->issued_count--;
	dlist_push_tail(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count++;

	io->flags |= PGAIOIP_LOCAL_CALLBACK_CALLED;

	if (!io->on_completion_local)
		return;

	if (!in_error)
	{
		check_stack_depth();
		aio_local_callback_depth++;
		io->on_completion_local->callback(io->on_completion_local, io);
		Assert(aio_local_callback_depth > 0);
		aio_local_callback_depth--;
	}
}

/*
 * Call all pending local callbacks.
 */
static void
pgaio_call_local_callbacks(bool in_error)
{
	if (my_aio->local_completed_count != 0)
	{
		/*
		 * Don't call local callbacks in a critical section. If a specific IO
		 * is required to finish within the critical section, and that IO uses
		 * a local callback, pgaio_io_wait_ref_int() will call that callback
		 * regardless of the callback depth.
		 */
		if (CritSectionCount != 0)
			return;

		if (aio_local_callback_depth > 0)
			return;

		while (!dlist_is_empty(&my_aio->local_completed))
		{
			dlist_node *node = dlist_pop_head_node(&my_aio->local_completed);
			PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

			pgaio_io_call_local_callback(io, in_error);
		}
	}
}

/*
 * Receive completions in ring.
 */
int
pgaio_drain(PgAioContext *context, bool block, bool call_shared, bool call_local)
{
	int ndrained = 0;

	if (io_method == IOMETHOD_WORKER)
	{
		/*
		 * Worker mode has no completion queue, because the worker processes
		 * all completion work directly.
		 */
	}
#ifdef USE_LIBURING
	else if (io_method == IOMETHOD_LIBURING)
		ndrained = pgaio_uring_drain(context, block, call_shared);
#endif
#ifdef USE_POSIX_AIO
	else if (io_method == IOMETHOD_POSIX)
		ndrained = pgaio_posix_drain(context);
#endif

	if (call_shared)
	{
		pgaio_complete_ios(false);
		pgaio_transfer_foreign_to_local();
	}
	if (call_local)
	{
		Assert(call_shared);
		pgaio_call_local_callbacks(false);
	}
	return ndrained;
}

static void
pgaio_transfer_foreign_to_local(void)
{
	/*
	 * Transfer all the foreign completions into the local queue.
	 */
	if (my_aio->foreign_completed_count != 0)
	{
		SpinLockAcquire(&my_aio->foreign_completed_lock);

		while (!dlist_is_empty(&my_aio->foreign_completed))
		{
			dlist_node *node = dlist_pop_head_node(&my_aio->foreign_completed);
			PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

			Assert(!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED));

			dlist_push_tail(&my_aio->local_completed, &io->io_node);
			io->flags &= ~PGAIOIP_FOREIGN_DONE;
			my_aio->foreign_completed_count--;
			my_aio->local_completed_count++;
		}
		SpinLockRelease(&my_aio->foreign_completed_lock);
	}
}

/*
 * Some AIO modes lack scatter/gather support, which limits I/O combining to
 * contiguous ranges of memory.
 */
static bool
pgaio_can_scatter_gather(void)
{
	if (io_method == IOMETHOD_WORKER)
	{
		/*
		 * We may not have true scatter/gather on this platform (see fallback
		 * emulation in pg_preadv()/pg_pwritev()), but there may still be some
		 * advantage to keeping sequential regions within the same process so
		 * we'll say yes here.
		 */
		return true;
	}
#ifdef USE_LIBURING
	if (io_method == IOMETHOD_LIBURING)
		return true;
#endif
#if defined(HAVE_AIO_READV) && defined(HAVE_AIO_WRITEV)
	if (io_method == IOMETHOD_POSIX)
	{
		/*
		 * aio_readv() and aio_writev() are non-standard extensions found in
		 * FreeBSD 13.
		 */
		return true;
	}
#endif
	return false;
}

static bool
pgaio_can_be_combined(PgAioInProgress *last, PgAioInProgress *cur)
{
	if (last->op != cur->op)
		return false;

	/* could be relaxed, but unlikely to be ever useful? */
	if (last->scb != cur->scb)
		return false;

	if (last->flags & PGAIOIP_RETRY ||
		cur->flags & PGAIOIP_RETRY)
		return false;

	switch (last->op)
	{
		case PGAIO_OP_READ:
			if (last->op_data.read.fd != cur->op_data.read.fd)
				return false;
			if ((last->op_data.read.offset + last->op_data.read.nbytes) != cur->op_data.read.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				((last->op_data.read.bufdata + last->op_data.read.nbytes) != cur->op_data.read.bufdata))
				return false;
			return true;

		case PGAIO_OP_WRITE:
			if (last->op_data.write.fd != cur->op_data.write.fd)
				return false;
			if ((last->op_data.write.offset + last->op_data.write.nbytes) != cur->op_data.write.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				((last->op_data.write.bufdata + last->op_data.write.nbytes) != cur->op_data.write.bufdata))
				return false;
			return true;

		case PGAIO_OP_FSYNC:
		case PGAIO_OP_FLUSH_RANGE:
		case PGAIO_OP_NOP:
			return false;
			break;
		case PGAIO_OP_INVALID:
			elog(ERROR, "unexpected");
			break;
	}

	pg_unreachable();
}

static void
pgaio_io_merge(PgAioInProgress *into, PgAioInProgress *tomerge)
{
	ereport(DEBUG3,
			errmsg("merging %zu to %zu",
				   tomerge - aio_ctl->in_progress_io,
				   into - aio_ctl->in_progress_io),
			errhidestmt(true),
			errhidecontext(true));
	into->merge_with_idx = tomerge - aio_ctl->in_progress_io;
}

static void
pgaio_combine_pending(void)
{
	dlist_iter iter;
	PgAioInProgress *last = NULL;
	int combined = 1;

	Assert(my_aio->pending_count > 1);

	dlist_foreach(iter, &my_aio->pending)
	{
		PgAioInProgress *cur = dlist_container(PgAioInProgress, io_node, iter.cur);

		Assert(cur->merge_with_idx == PGAIO_MERGE_INVALID);

		if (last == NULL)
		{
			last = cur;
			continue;
		}

		if (pgaio_can_be_combined(last, cur))
		{
			combined++;

			pgaio_io_merge(last, cur);
		}
		else
		{
			combined = 1;
		}

		if (combined >= PGAIO_MAX_COMBINE)
		{
			ereport(DEBUG3,
					errmsg("max combine at %d", combined),
					errhidestmt(true),
					errhidecontext(true));
			last = NULL;
			combined = 1;
		}
		else
			last = cur;
	}
}

static int
pgaio_synchronous_submit(void)
{
	int nsubmitted = 0;

	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;

		node = dlist_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);

		pgaio_io_prepare_submit(io, io->ring);
		pgaio_do_synchronously(io);

		++nsubmitted;
	}

	return nsubmitted;
}

static void
pgaio_submit_pending_internal(bool drain, bool call_shared, bool call_local, bool will_wait)
{
	int total_submitted = 0;
	uint32 orig_total;

	if (!aio_ctl || !my_aio)
		return;

	if (my_aio->pending_count == 0)
	{
		Assert(dlist_is_empty(&my_aio->pending));
		return;
	}

	HOLD_INTERRUPTS();

	orig_total = my_aio->pending_count;

#define COMBINE_ENABLED

#ifdef COMBINE_ENABLED
#if 0
	ereport(LOG, errmsg("before combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&my_aio->pending, NULL, offsetof(PgAioInProgress, io_node));
#endif
	if (my_aio->pending_count > 1 && PGAIO_MAX_COMBINE > 1)
		pgaio_combine_pending();

#if 0
	ereport(LOG, errmsg("after combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&my_aio->pending, NULL, offsetof(PgAioInProgress, io_node));
#endif
#endif /* COMBINE_ENABLED */

	/*
	 * FIXME: currently the pgaio_synchronous_submit() path is only compatible
	 * with IOMETHOD_WORKER: There's e.g. a danger we'd wait for io_uring
	 * events, which could stall (or trigger asserts), as
	 * pgaio_io_wait_ref_int() right now has no way of detecting that case.
	 */
	if (io_method != IOMETHOD_WORKER)
		will_wait = false;

	/*
	 * Loop until all pending IOs are submitted. Throttle max in-flight before
	 * calling into the IO implementation specific routine, so this code can
	 * be shared.
	 */
	while (!dlist_is_empty(&my_aio->pending))
	{
		int max_submit;
		int did_submit;
		int inflight_limit;
		int pending_count;

		Assert(my_aio->pending_count > 0);
		pgaio_apply_backend_limit();

		pending_count = my_aio->pending_count;
		Assert(pending_count > 0);
		if (pending_count == 0)
			break;

		inflight_limit = io_max_concurrency - pg_atomic_read_u32(&my_aio->inflight_count);
		max_submit = Min(pending_count, PGAIO_SUBMIT_BATCH_SIZE);
		max_submit = Min(max_submit, inflight_limit);
		Assert(max_submit > 0 && max_submit <= pending_count);

		START_CRIT_SECTION();
		if (my_aio->pending_count == 1 && will_wait)
			did_submit = pgaio_synchronous_submit();
		else if (io_method == IOMETHOD_WORKER)
			did_submit = pgaio_worker_submit(max_submit, drain);
#ifdef USE_LIBURING
		else if (io_method == IOMETHOD_LIBURING)
			did_submit = pgaio_uring_submit(max_submit, drain);
#endif
#ifdef USE_POSIX_AIO
		else if (io_method == IOMETHOD_POSIX)
			did_submit = pgaio_posix_submit(max_submit, drain);
#endif
		else
			elog(ERROR, "unexpected io_method");
		total_submitted += did_submit;
		Assert(did_submit > 0 && did_submit <= max_submit);
		END_CRIT_SECTION();
	}

	my_aio->executed_total_count += orig_total;
	my_aio->issued_total_count += total_submitted;

#ifdef PGAIO_VERBOSE
	ereport(DEBUG3,
			errmsg("submitted %d (orig %d)", total_submitted, orig_total),
			errhidestmt(true),
			errhidecontext(true));
#endif

	RESUME_INTERRUPTS();

	if (call_shared)
		pgaio_complete_ios(false);

	if (call_local)
	{
		pgaio_transfer_foreign_to_local();
		pgaio_call_local_callbacks(/* in_error = */ false);
	}
}

void pg_noinline
pgaio_submit_pending(bool drain)
{
	Assert(my_aio);

	/*
	 * We allow shared callbacks to be called even when not draining, as we
	 * might be forced to drain due to backend IO concurrency limits.
	 */
	pgaio_submit_pending_internal(drain,
								  /* call_shared */ true,
								  /* call_local */ drain,
								  /* will_wait */ false);
}

void
pgaio_closing_possibly_referenced(void)
{
	if (!my_aio)
		return;

	/* the callback could trigger further IO or such ATM */
	pgaio_submit_pending_internal(false,
								  /* call_shared */ false,
								  /* call_local */ false,
								  /* will_wait */ false);
}

void
pgaio_io_prepare_submit(PgAioInProgress *io, uint32 ring)
{
	PgAioInProgress *cur;

	cur = io;

	while (true)
	{
		Assert(cur->flags & PGAIOIP_PENDING);
		Assert(!(cur->flags & PGAIOIP_PREP));
		Assert(!(cur->flags & PGAIOIP_IDLE));
		Assert(my_aio_id == cur->owner_id);

		cur->ring = ring;

		pg_write_barrier();

		WRITE_ONCE_F(cur->flags) =
			(cur->flags & ~PGAIOIP_PENDING) | PGAIOIP_INFLIGHT;

		dlist_delete_from(&my_aio->pending, &cur->io_node);
		my_aio->pending_count--;

		if (cur->user_referenced)
		{
			Assert(my_aio->outstanding_count > 0);
			dlist_delete_from(&my_aio->outstanding, &cur->owner_node);
			my_aio->outstanding_count--;

			dlist_push_tail(&my_aio->issued, &cur->owner_node);
			my_aio->issued_count++;
		}
		else
		{
#ifdef PGAIO_VERBOSE
			ereport(DEBUG4,
					errmsg("putting aio %zu onto issued_abandoned during submit",
						   cur - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			dlist_push_tail(&my_aio->issued_abandoned, &cur->owner_node);
			my_aio->issued_abandoned_count++;
			LWLockRelease(SharedAIOCtlLock);
		}

		ereport(DEBUG5,
				errmsg("readied %zu/%llu for submit",
					   cur - aio_ctl->in_progress_io,
					   (unsigned long long ) cur->generation),
				errhidecontext(1),
				errhidestmt(1));

		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
	}
}

static void
pgaio_apply_backend_limit(void)
{
	uint32 current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);

	while (current_inflight >= io_max_concurrency)
	{
		PgAioInProgress *io;

		/*
		 * XXX: Should we be a bit fairer and check the "oldest" in-flight IO
		 * between issued and issued_abandoned?
		 */

		if (my_aio->issued_count > 0)
		{
			dlist_iter iter;

			Assert(!dlist_is_empty(&my_aio->issued));

			dlist_foreach(iter, &my_aio->issued)
			{
				io = dlist_container(PgAioInProgress, owner_node, iter.cur);

				if (io->flags & PGAIOIP_INFLIGHT)
				{
					PgAioIoRef ref;

					ereport(DEBUG2,
							errmsg("applying per-backend limit to issued IO %zu/%llu (current %d in %d, target %d)",
								   io - aio_ctl->in_progress_io,
								   (unsigned long long) io->generation,
								   my_aio->issued_count + my_aio->issued_abandoned_count,
								   current_inflight,
								   io_max_concurrency),
							errhidestmt(true),
							errhidecontext(true));

					pgaio_io_ref(io, &ref);
					pgaio_io_wait_ref_int(&ref, /* call_shared = */ false, /* call_local = */ false);
					current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);
					break;
				}
			}
		}

		if (current_inflight < io_max_concurrency)
			break;

		if (my_aio->issued_abandoned_count > 0)
		{
			dlist_iter iter;
			PgAioIoRef ref;

			/*
			 * ->issued_abandoned_count is only maintained once shared
			 * callbacks have been invoked. So do so, as otherwise we could
			 * end up looping here endlessly, as those IOs already finished.
			 */
			pgaio_complete_ios(false);
			pgaio_transfer_foreign_to_local();

			io = NULL;

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			dlist_foreach(iter, &my_aio->issued_abandoned)
			{
				io = dlist_container(PgAioInProgress, owner_node, iter.cur);

				if (io->flags & PGAIOIP_INFLIGHT)
				{
					pgaio_io_ref_internal(io, &ref);
					break;
				}
				else
					io = NULL;
			}
			LWLockRelease(SharedAIOCtlLock);

			if (io == NULL)
				continue;

			ereport(DEBUG2,
					errmsg("applying per-backend limit to issued_abandoned IO %zu/%llu (current %d in %d, target %d)",
						   io - aio_ctl->in_progress_io,
						   (unsigned long long) io->generation,
						   my_aio->issued_count + my_aio->issued_abandoned_count,
						   current_inflight,
						   io_max_concurrency),
					errhidestmt(true),
					errhidecontext(true));

			pgaio_io_wait_ref_int(&ref,  /* call_shared = */ false, /* call_local = */ false);
		}

		current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);
	}
}

void
pgaio_io_wait_ref(PgAioIoRef *ref, bool call_local)
{
	pgaio_io_wait_ref_int(ref, /* call_shared = */ true, call_local);
}

static void
pgaio_io_wait_ref_int(PgAioIoRef *ref, bool call_shared, bool call_local)
{
	uint64 ref_generation;
	PgAioInProgress *io;
	uint32 done_flags = PGAIOIP_DONE;
	PgAioIPFlags flags;
	bool am_owner;

	/*
	 * If we just wait for the IO to finish, we can only wait for the
	 * corresponding state.
	 */
	if (!call_shared)
	{
		Assert(!call_local);
		done_flags |= PGAIOIP_REAPED;
	}

	Assert(ref->aio_index < max_aio_in_progress);

	io = &aio_ctl->in_progress_io[ref->aio_index];
	ref_generation = ((uint64) ref->generation_upper) << 32 |
		ref->generation_lower;

	Assert(ref_generation != 0);

	am_owner = io->owner_id == my_aio_id;
	flags = io->flags;
	pg_read_barrier();

	if (io->generation != ref_generation)
		return;

	if (am_owner && (flags & PGAIOIP_PENDING))
	{
		Assert(call_shared);
		pgaio_submit_pending_internal(true, call_shared, call_local,
									  /* will_wait = */ true);
	}

	Assert(!am_owner || !(flags & PGAIOIP_IDLE));

	Assert(!(flags & (PGAIOIP_UNUSED)));

wait_ref_again:

	while (true)
	{
		flags = io->flags;
		pg_read_barrier();

		if (io->generation != ref_generation)
			return;

		if (flags & done_flags)
			goto wait_ref_out;

		Assert(!(flags & (PGAIOIP_UNUSED)));

		if (flags & PGAIOIP_INFLIGHT)
		{
			pgaio_drain(&aio_ctl->contexts[io->ring],
						/* block = */ false,
						call_shared, call_local);

			flags = io->flags;
			pg_read_barrier();

			if (io->generation != ref_generation)
				return;

			if (flags & done_flags)
				goto wait_ref_out;
		}

		if (my_aio->pending_count > 0 && call_shared)
		{
			/*
			 * If we otherwise would have to sleep submit all pending
			 * requests, to avoid others having to wait for us to submit
			 * them. Don't want to do so when not needing to sleep, as
			 * submitting IOs in smaller increments can be less efficient.
			 */
			pgaio_submit_pending_internal(call_shared, call_shared, call_local,
										  /* will_wait = */ false);
		}
		else if (io_method != IOMETHOD_WORKER && (flags & PGAIOIP_INFLIGHT))
		{
			/* note that this is allowed to spuriously return */
			if (io_method == IOMETHOD_WORKER)
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);
#ifdef USE_LIBURING
			else if (io_method == IOMETHOD_LIBURING)
				pgaio_uring_wait_one(&aio_ctl->contexts[io->ring], io, ref_generation,
									 WAIT_EVENT_AIO_IO_COMPLETE_ANY);
#endif
#ifdef USE_POSIX_AIO
			else if (io_method == IOMETHOD_POSIX)
				pgaio_posix_wait_one(io, ref_generation);
#endif
		}
#ifdef USE_POSIX_AIO
		/* XXX untangle this */
		else if (io_method == IOMETHOD_POSIX)
			pgaio_posix_wait_one(io, ref_generation);
#endif
		else
		{
			/* shouldn't be reachable without concurrency */
			Assert(IsUnderPostmaster);

			/* ensure we're going to get woken up */
			if (IsUnderPostmaster)
				ConditionVariablePrepareToSleep(&io->cv);

			flags = io->flags;
			pg_read_barrier();
			if (io->generation == ref_generation && !(flags & done_flags))
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);

			if (IsUnderPostmaster)
				ConditionVariableCancelSleepEx(true);
		}
	}

wait_ref_out:

	flags = io->flags;
	pg_read_barrier();
	if (io->generation != ref_generation)
		return;

	/*
	 * If somebody else is retrying the IO, just wait from scratch.
	 */
	if (!(flags & done_flags))
	{
		Assert(flags & PGAIOIP_RETRY);
		goto wait_ref_again;
	}

	if (!call_shared)
	{
		Assert(flags & (PGAIOIP_REAPED | PGAIOIP_SHARED_CALLBACK_CALLED | PGAIOIP_DONE));
		return;
	}

	Assert(flags & PGAIOIP_DONE);

	/* can retry soft failures, but not hard ones */
	if (unlikely(flags & PGAIOIP_SOFT_FAILURE))
	{
		pgaio_io_retry_soft_failed(io, ref_generation);
		pgaio_io_wait_ref_int(ref, call_shared, call_local);

		return;
	}

	if (am_owner && call_local && !(flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
	{
		if (flags & PGAIOIP_FOREIGN_DONE)
			pgaio_transfer_foreign_to_local();

		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));

		pgaio_io_call_local_callback(io, false);
	}
}

bool
pgaio_io_check_ref(PgAioIoRef *ref)
{
	uint64 ref_generation;
	PgAioInProgress *io;
	PgAioIPFlags flags;
	PgAioContext *context;

	Assert(ref->aio_index < max_aio_in_progress);

	io = &aio_ctl->in_progress_io[ref->aio_index];
	ref_generation = ((uint64) ref->generation_upper) << 32 |
		ref->generation_lower;

	Assert(ref_generation != 0);

	flags = io->flags;
	pg_read_barrier();

	if (io->generation != ref_generation)
		return true;

	if (flags & PGAIOIP_PENDING)
		return false;

	if (flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (flags & PGAIOIP_DONE)
		return true;

	context = &aio_ctl->contexts[io->ring];
	Assert(!(flags & (PGAIOIP_UNUSED)));

	if (io->generation != ref_generation)
		return true;

	if (flags & PGAIOIP_INFLIGHT)
		pgaio_drain(context,
					/* block = */ false,
					/* call_shared = */ true,
					/* call_local = */ false);

	flags = io->flags;
	pg_read_barrier();

	if (io->generation != ref_generation)
		return true;

	if (flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_RETRY))
		return false;

	if (flags & PGAIOIP_DONE)
		return true;

	return false;
}


/*
 */
void
pgaio_io_wait(PgAioInProgress *io)
{
	PgAioIoRef ref;

	Assert(io->user_referenced && io->owner_id == my_aio_id);

	pgaio_io_ref(io, &ref);
	pgaio_io_wait_ref(&ref, /* call_local = */ true);
}

PgAioInProgress *
pgaio_io_get(void)
{
	dlist_node *elem;
	PgAioInProgress *io;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	// FIXME: relax?
	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);

	/* FIXME: wait for an IO to complete if full */

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	while (unlikely(dlist_is_empty(&aio_ctl->unused_ios)))
	{
		LWLockRelease(SharedAIOCtlLock);
		elog(WARNING, "needed to drain while getting IO (used %d inflight %d)",
			 aio_ctl->used_count, pg_atomic_read_u32(&my_aio->inflight_count));

		/*
		 * FIXME: should we wait for IO instead?
		 *
		 * Also, need to protect against too many ios handed out but not used.
		 */
		for (int i = 0; i < aio_ctl->num_contexts; i++)
			pgaio_drain(&aio_ctl->contexts[i],
						/* block = */ true,
						/* call_shared = */ true,
						/* call_local = */ true);

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	}

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);
	aio_ctl->used_count++;

	LWLockRelease(SharedAIOCtlLock);

	io = dlist_container(PgAioInProgress, owner_node, elem);

#ifdef PGAIO_VERBOSE
	ereport(DEBUG3, errmsg("acquired io %zu/%llu",
						   io - aio_ctl->in_progress_io,
						   (long long unsigned) io->generation),
			errhidecontext(1),
			errhidestmt(1));
#endif

	Assert(io->op == PGAIO_OP_INVALID);
	Assert(io->scb == PGAIO_SCB_INVALID);
	Assert(io->flags == PGAIOIP_UNUSED);
	Assert(io->system_referenced);
	Assert(io->on_completion_local == NULL);
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

	io->user_referenced = true;
	io->system_referenced = false;

	WRITE_ONCE_F(io->flags) = PGAIOIP_IDLE;

	io->owner_id = my_aio_id;

	dlist_push_tail(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count++;

	return io;
}

bool
pgaio_io_success(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(io->flags & PGAIOIP_DONE);

	if (io->flags & (PGAIOIP_HARD_FAILURE | PGAIOIP_SOFT_FAILURE))
		return false;

	/* FIXME: is this possible? */
	if (!(io->flags & PGAIOIP_SHARED_CALLBACK_CALLED))
		return false;

	return true;
}

int
pgaio_io_result(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(io->flags & PGAIOIP_DONE);

	/*
	 * FIXME: This will currently not return correct information for partial
	 * writes that were retried.
	 */
	return io->result;
}

bool
pgaio_io_done(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!(io->flags & PGAIOIP_UNUSED));

	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (io->flags & (PGAIOIP_IDLE | PGAIOIP_HARD_FAILURE))
		return true;

	if (io->flags & PGAIOIP_DONE)
	{
		if (io->owner_id == my_aio_id &&
			!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
			return false;
		return true;
	}

	return false;
}

bool
pgaio_io_pending(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!(io->flags & PGAIOIP_UNUSED));

	return io->flags & PGAIOIP_PENDING;
}

uint32
pgaio_io_id(PgAioInProgress *io)
{
	return io - aio_ctl->in_progress_io;
}

uint64
pgaio_io_generation(PgAioInProgress *io)
{
			return io->generation;
}

static void
pgaio_io_ref_internal(PgAioInProgress *io, PgAioIoRef *ref)
{
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_PREP | PGAIOIP_IN_PROGRESS | PGAIOIP_DONE));

	ref->aio_index = io - aio_ctl->in_progress_io;
	ref->generation_upper = (uint32) (io->generation >> 32);
	ref->generation_lower = (uint32) io->generation;
	Assert(io->generation != 0);
}

void
pgaio_io_ref(PgAioInProgress *io, PgAioIoRef *ref)
{
	Assert(io->user_referenced);
	pgaio_io_ref_internal(io, ref);
}

/*
 * Register a completion callback that is executed locally in the backend that
 * initiated the IO, even if the the completion of the IO has been reaped by
 * another process (which executed the shared callback, unlocking buffers
 * etc).  This is mainly useful for AIO using code to promptly react to
 * individual IOs finishing, without having to individually check each of the
 * IOs.
 */
void
pgaio_io_on_completion_local(PgAioInProgress *io, PgAioOnCompletionLocalContext *ocb)
{
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_PENDING));
	Assert(io->on_completion_local == NULL);

	io->on_completion_local = ocb;
}

static void
pgaio_io_retry_common(PgAioInProgress *io)
{
	/* we currently don't merge IOs during retries */
	Assert(!(io->flags & PGAIOIP_DONE));
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

	/*
	 * Need to enforce limit during retries too, otherwise submissions in
	 * other backends could lead us to exhaust resources.
	 */
	pgaio_apply_backend_limit();

#ifdef PGAIO_VERBOSE
	if (message_level_is_interesting(DEBUG2))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
		StringInfoData s;

		initStringInfo(&s);

		pgaio_io_print(io, &s);

		ereport(DEBUG2,
				errmsg("retrying %s",
					   s.data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s.data);
		MemoryContextSwitchTo(oldcontext);
	}
#endif

	my_aio->retry_total_count++;

	START_CRIT_SECTION();
	if (io_method == IOMETHOD_WORKER)
		pgaio_worker_io_retry(io);
#ifdef USE_LIBURING
	else if (io_method == IOMETHOD_LIBURING)
		pgaio_uring_io_retry(io);
#endif
#ifdef USE_POSIX_AIO
	else if (io_method == IOMETHOD_POSIX)
		pgaio_posix_io_retry(io);
#endif
	else
		elog(ERROR, "unexpected io_method");
	END_CRIT_SECTION();
}

static void
pgaio_io_retry_soft_failed(PgAioInProgress *io, uint64 ref_generation)
{
	bool need_retry;
	PgAioIPFlags flags;
	PgAioRetryCB retry_cb = NULL;

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	/* could concurrently have been unset / retried */
	flags = io->flags;

	if (io->generation != ref_generation)
	{
		need_retry = false;
	}
	else if (
		(flags & PGAIOIP_DONE) &&
		flags & (PGAIOIP_HARD_FAILURE | PGAIOIP_SOFT_FAILURE))
	{
		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));
		Assert(!(io->flags & PGAIOIP_HARD_FAILURE));
		Assert(!(io->flags & PGAIOIP_REAPED));
		Assert(io->flags & PGAIOIP_DONE);

		dlist_delete(&io->io_node);

		WRITE_ONCE_F(io->flags) =
			(flags & ~(PGAIOIP_DONE |
					   PGAIOIP_FOREIGN_DONE |
					   PGAIOIP_SHARED_CALLBACK_CALLED |
					   PGAIOIP_LOCAL_CALLBACK_CALLED |
					   PGAIOIP_HARD_FAILURE |
					   PGAIOIP_SOFT_FAILURE)) |
			PGAIOIP_IN_PROGRESS |
			PGAIOIP_RETRY;

		need_retry = true;
	}
	else
	{
		need_retry = false;
	}
	LWLockRelease(SharedAIOCtlLock);

	if (!need_retry)
	{
		ereport(DEBUG2, errmsg("was about to retry %zd/%llu, but somebody else did already",
							   io - aio_ctl->in_progress_io,
							   (long long unsigned) ref_generation),
				errhidestmt(true),
				errhidecontext(true));
		return;
	}

	/* the io is still in-progress */
	Assert(io->system_referenced);

	/*
	 * Only fetch after the above check, otherwise another backend could have
	 * already retried the IO, and subsequently io->scb could have changed.
	 */
	retry_cb = io_action_cbs[io->scb].retry;

	if (!retry_cb)
		elog(PANIC, "non-retryable aio being retried");

	retry_cb(io);

	pgaio_io_retry_common(io);
}

/*
 * Retry failed IO. Needs to have been submitted by this backend, and
 * referenced resources, like fds, still need to be valid.
 */
void
pgaio_io_retry(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!io->system_referenced);
	Assert(io->owner_id == my_aio_id);
	Assert(io->flags & PGAIOIP_DONE);
	Assert(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED);
	Assert(io->flags & PGAIOIP_HARD_FAILURE);

	/*
	 * The IO has completed already, so we need to mark it as in-flight
	 * again.
	 */
	Assert(my_aio->outstanding_count > 0);
	dlist_delete_from(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count--;
	io->system_referenced = true;

	dlist_push_tail(&my_aio->issued, &io->owner_node);
	my_aio->issued_count++;

	WRITE_ONCE_F(io->flags) =
		(io->flags & ~(PGAIOIP_DONE |
					   PGAIOIP_FOREIGN_DONE |
					   PGAIOIP_SHARED_CALLBACK_CALLED |
					   PGAIOIP_LOCAL_CALLBACK_CALLED |
					   PGAIOIP_HARD_FAILURE |
					   PGAIOIP_SOFT_FAILURE)) |
		PGAIOIP_IN_PROGRESS |
		PGAIOIP_RETRY;

	/* can reuse original fd */

	pgaio_io_retry_common(io);
}

/*
 * Can any process perform this IO?
 */
bool
pgaio_io_is_openable(PgAioInProgress *io)
{
	return io_action_cbs[io->scb].retry;
}

/*
 * Make sure the fd is valid in this process.
 */
void
pgaio_io_open(PgAioInProgress *io)
{
	io_action_cbs[io->scb].retry(io);
}

void
pgaio_io_recycle(PgAioInProgress *io)
{
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_DONE));
	Assert(io->user_referenced);
	Assert(io->owner_id == my_aio_id);
	Assert(!io->system_referenced);
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

	if (io->bb_idx != PGAIO_BB_INVALID)
	{
		pgaio_bounce_buffer_release_internal(&aio_ctl->bounce_buffers[io->bb_idx],
											 /* holding_lock = */ false,
											 /* release_resowner = */ false);
		io->bb_idx = PGAIO_BB_INVALID;
	}

	if (io->flags & PGAIOIP_DONE)
	{
		/* request needs to actually be done, including local callbacks */
		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));
		Assert(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED);

		io->generation++;
		pg_write_barrier();

		io->flags &= ~PGAIOIP_DONE;
		io->flags |= PGAIOIP_IDLE;

		io->op = PGAIO_OP_INVALID;
		io->scb = PGAIO_SCB_INVALID;
	}

	io->flags &= ~(PGAIOIP_SHARED_CALLBACK_CALLED |
				   PGAIOIP_LOCAL_CALLBACK_CALLED |
				   PGAIOIP_RETRY |
				   PGAIOIP_HARD_FAILURE |
				   PGAIOIP_SOFT_FAILURE);
	Assert(io->flags == PGAIOIP_IDLE);
	io->result = 0;
	io->on_completion_local = NULL;
}

static void pg_noinline
pgaio_io_prepare(PgAioInProgress *io, PgAioOp op)
{
	/* true for now, but not necessarily in the future */
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);
	Assert(io->op == PGAIO_OP_INVALID);
	Assert(io->scb == PGAIO_SCB_INVALID);

	if (my_aio->pending_count + 1 >= PGAIO_SUBMIT_BATCH_SIZE)
	{
#ifdef PGAIO_VERBOSE
		ereport(DEBUG3,
				errmsg("submitting during prep for %zu due to %u inflight",
					   io - aio_ctl->in_progress_io,
					   my_aio->pending_count),
				errhidecontext(1),
				errhidestmt(1));
#endif
		pgaio_submit_pending(false);
	}

	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);

	/* for this module */
	io->system_referenced = true;
	io->op = op;
	Assert(io->owner_id == my_aio_id);

	WRITE_ONCE_F(io->flags) = PGAIOIP_PREP;
}


static void pg_noinline
pgaio_io_stage(PgAioInProgress *io, PgAioSharedCallback scb)
{
	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);
	Assert(io->flags == PGAIOIP_PREP);
	Assert(io->user_referenced);
	Assert(io->op != PGAIO_OP_INVALID);
	Assert(io->scb == PGAIO_SCB_INVALID);
	Assert(io_action_cbs[scb].op == io->op);

	io->scb = scb;

	WRITE_ONCE_F(io->flags) = (io->flags & ~PGAIOIP_PREP) | PGAIOIP_IN_PROGRESS | PGAIOIP_PENDING;
	dlist_push_tail(&my_aio->pending, &io->io_node);
	my_aio->pending_count++;

#ifdef PGAIO_VERBOSE
	if (message_level_is_interesting(DEBUG3))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
		StringInfoData s;

		/*
		 * This code isn't ever allowed to error out, but the debugging
		 * output is super useful...
		 */
		START_CRIT_SECTION();

		initStringInfo(&s);

		pgaio_io_print(io, &s);

		ereport(DEBUG3,
				errmsg("staged %s",
					   s.data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s.data);
		MemoryContextSwitchTo(oldcontext);

		END_CRIT_SECTION();
	}
#endif
}

void
pgaio_io_release(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(io->owner_id == my_aio_id);

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	io->user_referenced = false;

	if (io->flags & (PGAIOIP_IDLE |
					 PGAIOIP_PREP |
					 PGAIOIP_PENDING |
					 PGAIOIP_LOCAL_CALLBACK_CALLED))
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));

		Assert(my_aio->outstanding_count > 0);
		dlist_delete_from(&my_aio->outstanding, &io->owner_node);
		my_aio->outstanding_count--;

#ifdef PGAIO_VERBOSE
		ereport(DEBUG4, errmsg("releasing plain user reference to %zu",
							   io - aio_ctl->in_progress_io),
				errhidecontext(1),
				errhidestmt(1));
#endif
	}
	else
	{
		dlist_delete_from(&my_aio->issued, &io->owner_node);
		my_aio->issued_count--;

		if (io->system_referenced)
		{
#ifdef PGAIO_VERBOSE
			ereport(DEBUG4, errmsg("putting aio %zu onto issued_abandoned during release",
								   io - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif

			dlist_push_tail(&my_aio->issued_abandoned, &io->owner_node);
			my_aio->issued_abandoned_count++;
		}
		else
		{
			Assert(io->flags & (PGAIOIP_DONE | PGAIOIP_SHARED_CALLBACK_CALLED));

#ifdef PGAIO_VERBOSE
			ereport(DEBUG4, errmsg("not putting aio %zu onto issued_abandoned during release",
								   io - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif
		}
	}

	if (!io->system_referenced)
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));
		Assert(io->flags & (PGAIOIP_IDLE |
							PGAIOIP_PREP |
							PGAIOIP_DONE));

		if (io->flags & PGAIOIP_DONE)
		{
			if (io->flags & PGAIOIP_FOREIGN_DONE)
			{
				SpinLockAcquire(&my_aio->foreign_completed_lock);
				Assert(io->flags & PGAIOIP_FOREIGN_DONE);
				dlist_delete_from(&my_aio->foreign_completed, &io->io_node);
				my_aio->foreign_completed_count--;
				SpinLockRelease(&my_aio->foreign_completed_lock);
			}
			else if (!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
			{
				dlist_delete_from(&my_aio->local_completed, &io->io_node);
				my_aio->local_completed_count--;
				io->on_completion_local = NULL;
			}

		}

		io->generation++;
		pg_write_barrier();

		io->flags = PGAIOIP_UNUSED;
		io->op = PGAIO_OP_INVALID;
		io->scb = PGAIO_SCB_INVALID;
		io->owner_id = INVALID_PGPROCNO;
		io->result = 0;
		io->system_referenced = true;
		io->on_completion_local = NULL;

		Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

		if (io->bb_idx != PGAIO_BB_INVALID)
		{
			pgaio_bounce_buffer_release_internal(&aio_ctl->bounce_buffers[io->bb_idx],
												 /* holding_lock = */ true,
												 /* release_resowner = */ false);
			io->bb_idx = PGAIO_BB_INVALID;
		}

		dlist_push_head(&aio_ctl->unused_ios, &io->owner_node);
		aio_ctl->used_count--;
	}

	LWLockRelease(SharedAIOCtlLock);
}

void
pgaio_print_queues(void)
{
	StringInfoData s;
	uint32 inflight_backend = 0;
	uint32 *inflight_context;

	initStringInfo(&s);

	for (int procno = 0; procno < aio_ctl->backend_state_count; procno++)
	{
		PgAioPerBackend *bs = &aio_ctl->backend_state[procno];

		inflight_backend += pg_atomic_read_u32(&bs->inflight_count);
	}

	inflight_context = palloc0(sizeof(uint32) * aio_ctl->backend_state_count);
	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];

		if (!(io->flags & PGAIOIP_INFLIGHT))
			continue;
		inflight_context[io->ring]++;
	}

	appendStringInfo(&s, "inflight backend: %d", inflight_backend);

#ifdef USE_LIBURING
	for (int contextno = 0; contextno < aio_ctl->num_contexts; contextno++)
	{
		PgAioContext *context = &aio_ctl->contexts[contextno];

		appendStringInfo(&s, "\n\tqueue[%d]: space: %d, ready: %d, we think inflight: %d",
						 contextno,
						 io_uring_sq_space_left(&context->io_uring_ring),
						 io_uring_cq_ready(&context->io_uring_ring),
						 inflight_context[contextno]);
	}
#endif

	ereport(LOG, errmsg_internal("%s", s.data),
			errhidestmt(true),
			errhidecontext(true));
}

static const char *
pgaio_io_operation_string(PgAioOp op)
{
	switch (op)
	{
		case PGAIO_OP_INVALID:
			return "invalid";
		case PGAIO_OP_READ:
			return "read";
		case PGAIO_OP_WRITE:
			return "write";
		case PGAIO_OP_FSYNC:
			return "fsync";
		case PGAIO_OP_FLUSH_RANGE:
			return "flush_range";
		case PGAIO_OP_NOP:
			return "nop";
	}
	return "";
}

static const char *
pgaio_io_shared_callback_string(PgAioSharedCallback a)
{
	return io_action_cbs[a].name;
}

static void
pgaio_io_flag_string(PgAioIPFlags flags, StringInfo s)
{
	bool first = true;

#define STRINGIFY_FLAG(f) if (flags & f) {  appendStringInfoString(s, first ? CppAsString(f) : " | " CppAsString(f)); first = false;}

	STRINGIFY_FLAG(PGAIOIP_UNUSED);
	STRINGIFY_FLAG(PGAIOIP_IDLE);
	STRINGIFY_FLAG(PGAIOIP_PREP);
	STRINGIFY_FLAG(PGAIOIP_IN_PROGRESS);
	STRINGIFY_FLAG(PGAIOIP_PENDING);
	STRINGIFY_FLAG(PGAIOIP_INFLIGHT);
	STRINGIFY_FLAG(PGAIOIP_REAPED);
	STRINGIFY_FLAG(PGAIOIP_SHARED_CALLBACK_CALLED);
	STRINGIFY_FLAG(PGAIOIP_LOCAL_CALLBACK_CALLED);

	STRINGIFY_FLAG(PGAIOIP_DONE);
	STRINGIFY_FLAG(PGAIOIP_FOREIGN_DONE);

	STRINGIFY_FLAG(PGAIOIP_RETRY);
	STRINGIFY_FLAG(PGAIOIP_HARD_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SOFT_FAILURE);

#undef STRINGIFY_FLAG
}

static void
pgaio_io_shared_desc(PgAioInProgress *io, StringInfo s)
{
	io_action_cbs[io->scb].desc(io, s);
}

static void
pgaio_io_print_one(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "aio %zu/%llu: op: %s, scb: %s, result: %d, ring: %d, owner: %d, flags: ",
					 io - aio_ctl->in_progress_io,
					 (long long unsigned) io->generation,
					 pgaio_io_operation_string(io->op),
					 pgaio_io_shared_callback_string(io->scb),
					 io->result,
					 io->ring,
					 io->owner_id);
	pgaio_io_flag_string(io->flags, s);
	appendStringInfo(s, ", user/system_referenced: %d/%d (",
					 io->user_referenced,
					 io->system_referenced);
	pgaio_io_shared_desc(io, s);
	appendStringInfoString(s, ")");
}

void
pgaio_io_ref_print(PgAioIoRef *ref, StringInfo s)
{
	PgAioInProgress *io;

	io = &aio_ctl->in_progress_io[ref->aio_index];

	pgaio_io_print(io, s);
}

void
pgaio_io_print(PgAioInProgress *io, StringInfo s)
{
	bool alloc = false;
	MemoryContext old_context;

	if (s == NULL)
	{
		old_context = MemoryContextSwitchTo(ErrorContext);
		s = makeStringInfo();
		alloc = true;
	}

	pgaio_io_print_one(io, s);

	if (io->merge_with_idx != PGAIO_MERGE_INVALID)
	{
		PgAioInProgress *cur = io;
		int nummerge = 0;

		appendStringInfoString(s, "\n  merge with:");

		while (cur->merge_with_idx != PGAIO_MERGE_INVALID)
		{
			PgAioInProgress *next;

			nummerge++;
			appendStringInfo(s, "\n    %d: ", nummerge);

			next = &aio_ctl->in_progress_io[cur->merge_with_idx];
			pgaio_io_print_one(next, s);

			cur = next;
		}
	}

	if (alloc)
	{
		ereport(LOG,
				errmsg("%s", s->data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s->data);
		pfree(s);
		MemoryContextReset(ErrorContext);
		MemoryContextSwitchTo(old_context);
	}
}

void
pgaio_print_list(dlist_head *head, StringInfo s, size_t offset)
{
	bool alloc = false;
	dlist_iter iter;
	bool first = true;
	MemoryContext old_context;

	if (s == NULL)
	{
		old_context = MemoryContextSwitchTo(ErrorContext);
		s = makeStringInfo();
		alloc = true;
	}

	dlist_foreach(iter, head)
	{
		PgAioInProgress *io = ((PgAioInProgress *) ((char *) (iter.cur) - offset));

		if (!first)
			appendStringInfo(s, "\n");
		first = false;

		pgaio_io_print(io, s);
	}

	if (alloc)
	{
		ereport(LOG,
				errmsg("%s", s->data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s->data);
		pfree(s);
		MemoryContextSwitchTo(old_context);
		MemoryContextReset(ErrorContext);
	}
}

PgAioBounceBuffer *
pgaio_bounce_buffer_get(void)
{
	PgAioBounceBuffer *bb = NULL;

	ResourceOwnerEnlargeAioBB(CurrentResourceOwner);

	while (true)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		if (!dlist_is_empty(&aio_ctl->unused_bounce_buffers))
		{
			dlist_node *node = dlist_pop_head_node(&aio_ctl->unused_bounce_buffers);

			aio_ctl->unused_bounce_buffers_count--;
			bb = dlist_container(PgAioBounceBuffer, node, node);

			Assert(pg_atomic_read_u32(&bb->refcount) == 0);
		}
		LWLockRelease(SharedAIOCtlLock);

		if (!bb)
		{
			elog(WARNING, "needed to drain while getting BB (inflight %d)",
				 pg_atomic_read_u32(&my_aio->inflight_count));

			for (int i = 0; i < aio_ctl->num_contexts; i++)
				pgaio_drain(&aio_ctl->contexts[i],
							/* block = */ true,
							/* call_shared = */ true,
							/* call_local = */ true);
		}
		else
			break;
	}

	pg_atomic_write_u32(&bb->refcount, 1);

	ResourceOwnerRememberAioBB(CurrentResourceOwner, bb);

	return bb;
}

static void
pgaio_bounce_buffer_release_internal(PgAioBounceBuffer *bb, bool holding_lock, bool release_resowner)
{
	Assert(holding_lock == LWLockHeldByMe(SharedAIOCtlLock));
	Assert(bb != NULL);

	if (release_resowner)
		ResourceOwnerForgetAioBB(CurrentResourceOwner, bb);

	if (pg_atomic_sub_fetch_u32(&bb->refcount, 1) != 0)
		return;

	if (!holding_lock)
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	dlist_push_tail(&aio_ctl->unused_bounce_buffers, &bb->node);
	aio_ctl->unused_bounce_buffers_count++;
	if (!holding_lock)
		LWLockRelease(SharedAIOCtlLock);
}

void
pgaio_bounce_buffer_release(PgAioBounceBuffer *bb)
{
	pgaio_bounce_buffer_release_internal(bb,
										 /* holding_lock = */ false,
										 /* release_resowner */ true);
}

char *
pgaio_bounce_buffer_buffer(PgAioBounceBuffer *bb)
{
	return bb->buffer;
}

void
pgaio_assoc_bounce_buffer(PgAioInProgress *io, PgAioBounceBuffer *bb)
{
	Assert(bb != NULL);
	Assert(io->bb_idx == PGAIO_BB_INVALID);
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);
	Assert(pg_atomic_read_u32(&bb->refcount) > 0);

	io->bb_idx = bb - aio_ctl->bounce_buffers;
	pg_atomic_fetch_add_u32(&bb->refcount, 1);
}


/* --------------------------------------------------------------------------------
 * IO initialization routines for the basic IO types (see PgAioOp).
 * --------------------------------------------------------------------------------
 */


void
pgaio_io_prep_read(PgAioInProgress *io, int fd, char *bufdata, uint64 offset, uint32 nbytes)
{
	Assert(io->op == PGAIO_OP_READ);
	Assert(io->flags == PGAIOIP_PREP);
	Assert(ShmemAddrIsValid(bufdata));

	io->op_data.read.fd = fd;
	io->op_data.read.offset = offset;
	io->op_data.read.nbytes = nbytes;
	io->op_data.read.bufdata = bufdata;
	io->op_data.read.already_done = 0;
}

void
pgaio_io_prep_write(PgAioInProgress *io, int fd, char *bufdata, uint64 offset, uint32 nbytes)
{
	Assert(io->op == PGAIO_OP_WRITE);
	Assert(io->flags == PGAIOIP_PREP);
	Assert(ShmemAddrIsValid(bufdata));

	io->op_data.write.fd = fd;
	io->op_data.write.offset = offset;
	io->op_data.write.nbytes = nbytes;
	io->op_data.write.bufdata = bufdata;
	io->op_data.write.already_done = 0;
}

void
pgaio_io_prep_fsync(PgAioInProgress *io, int fd, bool datasync)
{
	Assert(io->op == PGAIO_OP_FSYNC);
	Assert(io->flags == PGAIOIP_PREP);

	io->op_data.fsync.fd = fd;
	io->op_data.fsync.datasync = datasync;
}

void
pgaio_io_prep_flush_range(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes)
{
	Assert(io->op == PGAIO_OP_FLUSH_RANGE);
	Assert(io->flags == PGAIOIP_PREP);

	io->op_data.flush_range.fd = fd;
	io->op_data.flush_range.offset = offset;
	io->op_data.flush_range.nbytes = nbytes;
}

void
pgaio_io_prep_nop(PgAioInProgress *io)
{
	Assert(io->op == PGAIO_OP_WRITE);
	Assert(io->flags == PGAIOIP_PREP);
}

/* --------------------------------------------------------------------------------
 * IO start routines (see PgAioSharedCallback for a list)
 * --------------------------------------------------------------------------------
 */

void
pgaio_io_start_read_sb(PgAioInProgress *io, struct SMgrRelationData* smgr, ForkNumber forknum,
					   BlockNumber blocknum, char *bufdata, int buffid, int mode)
{
	Assert(BufferIsValid(buffid));

	pgaio_io_prepare(io, PGAIO_OP_READ);

	smgrstartread(io, smgr, forknum, blocknum, bufdata);

	io->scb_data.read_sb.buffid = buffid;
	io->scb_data.read_sb.backend = smgr->smgr_rnode.backend;
	io->scb_data.read_sb.mode = mode;

	ReadBufferPrepRead(io, buffid);

	pgaio_io_stage(io, PGAIO_SCB_READ_SB);
}

void
pgaio_io_start_read_smgr(PgAioInProgress *io, struct SMgrRelationData* smgr, ForkNumber forknum,
						 BlockNumber blocknum, char *bufdata)
{
	pgaio_io_prepare(io, PGAIO_OP_READ);

	smgrstartread(io, smgr, forknum, blocknum, bufdata);

	io->scb_data.read_smgr.tag = (AioBufferTag){
		.rnode = smgr->smgr_rnode,
		.forkNum = forknum,
		.blockNum = blocknum
	};

	pgaio_io_stage(io, PGAIO_SCB_READ_SMGR);
}

void
pgaio_io_start_write_sb(PgAioInProgress *io,
						struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
						char *bufdata, int buffid, bool skipFsync, bool release_lock)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	smgrstartwrite(io, smgr, forknum, blocknum, bufdata, skipFsync);

	io->scb_data.write_sb.buffid = buffid;
	io->scb_data.write_sb.backend = smgr->smgr_rnode.backend;
	io->scb_data.write_sb.release_lock = release_lock;

	ReadBufferPrepWrite(io, buffid, release_lock);

	pgaio_io_stage(io, PGAIO_SCB_WRITE_SB);
}

void
pgaio_io_start_write_smgr(PgAioInProgress *io,
						  struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
						  char *bufdata, bool skipFsync)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	smgrstartwrite(io, smgr, forknum, blocknum, bufdata, skipFsync);

	io->scb_data.write_smgr.tag = (AioBufferTag){
		.rnode = smgr->smgr_rnode,
		.forkNum = forknum,
		.blockNum = blocknum
	};

	pgaio_io_stage(io, PGAIO_SCB_WRITE_SMGR);
}

void
pgaio_io_start_write_wal(PgAioInProgress *io, int fd, uint32 offset, uint32 nbytes, char *bufdata, uint32 write_no)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	pgaio_io_prep_write(io, fd, bufdata, offset, nbytes);

	io->scb_data.write_wal.write_no = write_no;

	pgaio_io_stage(io, PGAIO_SCB_WRITE_WAL);
}

void
pgaio_io_start_write_raw(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes, char *bufdata)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	pgaio_io_prep_write(io, fd, bufdata, offset, nbytes);

	pgaio_io_stage(io, PGAIO_SCB_WRITE_RAW);
}

void
pgaio_io_start_fsync_raw(PgAioInProgress *io, int fd, bool datasync)
{
	pgaio_io_prepare(io, PGAIO_OP_FSYNC);

	pgaio_io_prep_fsync(io, fd, datasync);

	pgaio_io_stage(io, PGAIO_SCB_FSYNC_RAW);
}

void
pgaio_io_start_fsync_wal(PgAioInProgress *io, int fd, bool datasync_only, uint32 flush_no)
{
	pgaio_io_prepare(io, PGAIO_OP_FSYNC);

	pgaio_io_prep_fsync(io, fd, datasync_only);

	io->scb_data.fsync_wal.flush_no = flush_no;

	pgaio_io_stage(io, PGAIO_SCB_FSYNC_WAL);
}

void
pgaio_io_start_flush_range_raw(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes)
{
	pgaio_io_prepare(io, PGAIO_OP_FLUSH_RANGE);

	pgaio_io_prep_flush_range(io, fd, offset, nbytes);

	pgaio_io_stage(io, PGAIO_SCB_FLUSH_RANGE_RAW);
}

BlockNumber
pgaio_io_start_flush_range_smgr(PgAioInProgress *io, struct SMgrRelationData* smgr, ForkNumber forknum,
								BlockNumber blocknum, BlockNumber nblocks)
{
	BlockNumber ret;

	pgaio_io_prepare(io, PGAIO_OP_FLUSH_RANGE);

	ret = smgrstartwriteback(io, smgr, forknum, blocknum, nblocks);

	if (ret != InvalidBlockNumber)
	{
		io->scb_data.flush_range_smgr.tag = (AioBufferTag){
			.rnode = smgr->smgr_rnode,
			.forkNum = forknum,
			.blockNum = blocknum
		};
		pgaio_io_stage(io, PGAIO_SCB_FLUSH_RANGE_SMGR);
	}
	else
	{
		Assert(io->op == PGAIO_OP_FLUSH_RANGE);
		Assert(io->owner_id == my_aio_id);
		Assert(io->flags == PGAIOIP_PREP);
		Assert(io->system_referenced);

		io->op = PGAIO_OP_INVALID;
		io->system_referenced = false;

		WRITE_ONCE_F(io->flags) = PGAIOIP_IDLE;
	}

	return ret;
}

void
pgaio_io_start_nop(PgAioInProgress *io)
{
	pgaio_io_prepare(io, PGAIO_OP_NOP);

	pgaio_io_prep_nop(io);

	pgaio_io_stage(io, PGAIO_SCB_NOP);
}


/* --------------------------------------------------------------------------------
 * shared IO implementation (see PgAioSharedCallback for a list)
 * --------------------------------------------------------------------------------
 */

static void
pgaio_invalid_desc(PgAioInProgress *io, StringInfo s)
{
}

static bool
pgaio_nop_complete(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed nop");
#endif

	return true;
}

static void
pgaio_nop_desc(PgAioInProgress *io, StringInfo s)
{
}

static bool
pgaio_fsync_raw_complete(PgAioInProgress *io)
{
	/* can't retry automatically, don't know which file etc */
	return true;
}

static void
pgaio_fsync_raw_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, datasync: %d",
					 io->op_data.fsync.fd,
					 io->op_data.fsync.datasync);
}

static void
pgaio_fsync_wal_retry(PgAioInProgress *io)
{
	io->op_data.fsync.fd = XLogFileForFlushNo(io->scb_data.fsync_wal.flush_no);
}

static bool
pgaio_fsync_wal_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (io->result != 0)
		elog(PANIC, "fsync_wal needs better error handling");

	XLogFlushComplete(io, io->scb_data.fsync_wal.flush_no);
	return true;
}

static void
pgaio_fsync_wal_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "flush_no: %d, fd: %d, datasync: %d",
					 io->scb_data.fsync_wal.flush_no,
					 io->op_data.fsync.fd,
					 io->op_data.fsync.datasync);
}

static bool
pgaio_flush_range_complete(PgAioInProgress *io)
{
	return true;
}

static void
pgaio_flush_range_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %llu",
					 io->op_data.flush_range.fd,
					 (unsigned long long) io->op_data.flush_range.offset,
					 (unsigned long long) io->op_data.flush_range.nbytes);
}

static void
pgaio_flush_range_smgr_retry(PgAioInProgress *io)
{
	uint32 off;
	AioBufferTag *tag = &io->scb_data.flush_range_smgr.tag;
	SMgrRelation reln = smgropen(tag->rnode.node,
								 tag->rnode.backend);

	io->op_data.flush_range.fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
	Assert(off == io->op_data.flush_range.offset);
}

static void
pgaio_read_sb_retry(PgAioInProgress *io)
{
	BufferDesc *bufHdr = NULL;
	bool		islocal;
	BufferTag	tag;
	Buffer		buffid = io->scb_data.read_sb.buffid;
	SMgrRelation reln;
	uint32		off;

	islocal = BufferIsLocal(buffid);

	if (islocal)
		bufHdr = GetLocalBufferDescriptor(-buffid - 1);
	else
		bufHdr = GetBufferDescriptor(buffid - 1);

	tag = bufHdr->tag;

	reln = smgropen(tag.rnode, io->scb_data.read_sb.backend);
	io->op_data.read.fd = smgrfd(reln, tag.forkNum, tag.blockNum, &off);

	Assert(off == io->op_data.read.offset);
}

static bool
pgaio_read_sb_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	ReadBufferCompleteRead(io->scb_data.read_sb.buffid,
						   io->op_data.read.bufdata,
						   io->scb_data.read_sb.mode,
						   io->flags & PGAIOIP_HARD_FAILURE);
	return true;
}

static void
pgaio_read_sb_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, mode: %d, offset: %llu, nbytes: %u, already_done: %u, buffid: %u",
					 io->op_data.read.fd,
					 io->scb_data.read_sb.mode,
					 (long long unsigned) io->op_data.read.offset,
					 io->op_data.read.nbytes,
					 io->op_data.read.already_done,
					 io->scb_data.read_sb.buffid);
}

static void
pgaio_read_smgr_retry(PgAioInProgress *io)
{
	uint32 off;
	AioBufferTag *tag = &io->scb_data.read_smgr.tag;
	SMgrRelation reln = smgropen(tag->rnode.node,
								 tag->rnode.backend);

	io->op_data.read.fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
	Assert(off == io->op_data.read.offset);
}

static bool
pgaio_read_smgr_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	ReadBufferCompleteRawRead(&io->scb_data.read_smgr.tag,
							  io->op_data.read.bufdata,
							  io->flags & PGAIOIP_HARD_FAILURE);

	return true;
}

static void
pgaio_read_smgr_desc(PgAioInProgress *io, StringInfo s)
{
	/* FIXME: proper path, critical section safe */
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, rel: %u, buf: %p",
					 io->op_data.read.fd,
					 (long long unsigned) io->op_data.read.offset,
					 io->op_data.read.nbytes,
					 io->op_data.read.already_done,
					 io->scb_data.read_smgr.tag.rnode.node.relNode,
					 io->op_data.read.bufdata);
}

static void
pgaio_write_sb_retry(PgAioInProgress *io)
{
	BufferDesc *bufHdr = NULL;
	bool		islocal;
	BufferTag	tag;
	Buffer		buffid = io->scb_data.read_sb.buffid;
	SMgrRelation reln;
	uint32		off;

	islocal = BufferIsLocal(buffid);

	if (islocal)
		bufHdr = GetLocalBufferDescriptor(-buffid - 1);
	else
		bufHdr = GetBufferDescriptor(buffid - 1);

	tag = bufHdr->tag;
	reln = smgropen(tag.rnode, io->scb_data.read_sb.backend);
	io->op_data.write.fd = smgrfd(reln, tag.forkNum, tag.blockNum, &off);

	Assert(off == io->op_data.write.offset);
}

static bool
pgaio_write_sb_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	ReadBufferCompleteWrite(io->scb_data.write_sb.buffid,
							io->scb_data.write_sb.release_lock,
							io->flags & PGAIOIP_HARD_FAILURE);
	return true;
}

static void
pgaio_write_sb_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, release_lock: %d, buffid: %u",
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->scb_data.write_sb.release_lock,
					 io->scb_data.write_sb.buffid);
}


static void
pgaio_write_smgr_retry(PgAioInProgress *io)
{
	uint32 off;
	AioBufferTag *tag = &io->scb_data.write_smgr.tag;
	SMgrRelation reln = smgropen(tag->rnode.node,
								 tag->rnode.backend);

	io->op_data.read.fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
	Assert(off == io->op_data.read.offset);
}

static bool
pgaio_write_smgr_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (!(io->flags & PGAIOIP_HARD_FAILURE))
		Assert(io->op_data.write.already_done == BLCKSZ);

	return true;
}

static void
pgaio_write_smgr_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, data: %p",
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->op_data.write.bufdata);
}


static void
pgaio_write_wal_retry(PgAioInProgress *io)
{
	io->op_data.write.fd = XLogFileForWriteNo(io->scb_data.write_wal.write_no);
}

static bool
pgaio_write_wal_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (io->flags & PGAIOIP_HARD_FAILURE)
	{
		if (io->result <= 0)
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log file: %s",
							strerror(-io->result))));
		}
		else
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log file: wrote only %d of %d bytes",
							io->result, (io->op_data.write.nbytes - io->op_data.write.already_done))));
		}
	}


	XLogWriteComplete(io, io->scb_data.write_wal.write_no);

	return true;
}

static void
pgaio_write_wal_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "write_no: %d, fd: %d, offset: %llu, nbytes: %u, already_done: %u, bufdata: %p",
					 io->scb_data.write_wal.write_no,
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->op_data.write.bufdata);
}

static bool
pgaio_write_raw_complete(PgAioInProgress *io)
{
	/* can't retry automatically, don't know which file etc */
	return true;
}

static void
pgaio_write_raw_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, bufdata: %p",
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->op_data.write.bufdata);
}

/*
 * Extract iov_base and iov_len from a single IO.
 */
static void
pgaio_fill_one_iov(struct iovec *iov, const PgAioInProgress *io, bool first)
{
	switch (io->op)
	{
		case PGAIO_OP_WRITE:
			Assert(first || io->op_data.write.already_done == 0);
			iov->iov_base = io->op_data.write.bufdata + io->op_data.write.already_done;
			iov->iov_len = io->op_data.write.nbytes - io->op_data.write.already_done;
			break;
		case PGAIO_OP_READ:
			Assert(first || io->op_data.read.already_done == 0);
			iov->iov_base = io->op_data.read.bufdata + io->op_data.read.already_done;
			iov->iov_len = io->op_data.read.nbytes - io->op_data.read.already_done;
			break;
		default:
			elog(ERROR, "unexpected IO type while populating iovec");
	}
}

/*
 * Populate an array of iovec objects with the address ranges from a chain of
 * merged IOs.  Return the number of iovecs (which may be smaller than the
 * number of IOs).
 */
int
pgaio_fill_iov(struct iovec *iovs, const PgAioInProgress *io)
{
	struct iovec *iov;

	/* Fill in the first one. */
	iov = &iovs[0];
	pgaio_fill_one_iov(iov, io, true);

	/*
	 * We have a chain of IOs that were linked together because they access
	 * contiguous regions of a file.  As a micro-optimization we'll also
	 * consolidate iovecs that access contiguous memory.
	 */
	while (io->merge_with_idx != PGAIO_MERGE_INVALID)
	{
		struct iovec *next = iov + 1;

		io = &aio_ctl->in_progress_io[io->merge_with_idx];

		pgaio_fill_one_iov(next, io, false);
		if ((char *) iov->iov_base + iov->iov_len == next->iov_base)
			iov->iov_len += next->iov_len;
		else
			++iov;
	}

	return iov + 1 - iovs;
}

/*
 * Run an IO with a traditional blocking system call.  This is used by worker
 * mode to simulate AIO in worker processes, but it can also be used in other
 * modes if there's only a single IO to be submitted and we know we'll wait
 * for it anyway, saving some overheads.  XXX make that statement true
 */
void
pgaio_do_synchronously(PgAioInProgress *io)
{
	ssize_t result = 0;
	struct iovec iov[IOV_MAX];
	int iovcnt = 0;

	Assert(io->flags & PGAIOIP_INFLIGHT);

	/* Perform IO. */
	switch (io->op)
	{
		case PGAIO_OP_READ:
			iovcnt = pgaio_fill_iov(iov, io);
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
			result = pg_preadv(io->op_data.read.fd, iov, iovcnt,
							   io->op_data.read.offset + io->op_data.read.already_done);
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_WRITE:
			iovcnt = pgaio_fill_iov(iov, io);
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
			result = pg_pwritev(io->op_data.write.fd, iov, iovcnt,
								io->op_data.write.offset + io->op_data.write.already_done);
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_FSYNC:
			pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
			if (io->op_data.fsync.datasync)
				result = pg_fdatasync(io->op_data.fsync.fd);
			else
				result = pg_fsync(io->op_data.fsync.fd);
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_FLUSH_RANGE:
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_FLUSH);
			pg_flush_data(io->op_data.flush_range.fd,
						  io->op_data.flush_range.offset,
						  io->op_data.flush_range.nbytes);
			/* never errors */
			/* XXX previously we would PANIC on errors here */
			result = 0;
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_NOP:
			result = 0;
			break;
		case PGAIO_OP_INVALID:
			result = -1;
			errno = EOPNOTSUPP;
			break;
	}

	pgaio_process_io_completion(io, result < 0 ? -errno : result);
}

static bool
pgaio_io_matches_fd(PgAioInProgress *io, int fd)
{
#ifdef USE_POSIX_AIO
	/* XXX review sanity of this */
	/* We only want to match IOs that were submitted by this process. */
	if (io_method != IOMETHOD_POSIX || io->submitter_id != my_aio_id)
		return false;
#endif

	switch (io->op)
	{
		case PGAIO_OP_READ:
			return fd == io->op_data.read.fd;
		case PGAIO_OP_WRITE:
			return fd == io->op_data.write.fd;
		case PGAIO_OP_FSYNC:
			return fd == io->op_data.fsync.fd;
		case PGAIO_OP_FLUSH_RANGE:
			return fd == io->op_data.flush_range.fd;
		default:
			return false;
	}
}

/*
 * Workhorse routine to wait for all IOs issued by this process to complete.
 * If n >= 0, then only wait for at most n IOs.  If fd is >= 0, then only wait
 * for IOs on the given descriptor.
 */
static void
pgaio_wait_for_issued_internal(int n, int fd)
{
	dlist_iter iter;

	dlist_foreach(iter, &my_aio->issued)
	{
		PgAioInProgress *io = dlist_container(PgAioInProgress, owner_node, iter.cur);

		if (io->flags & PGAIOIP_INFLIGHT &&
			(fd < 0 || pgaio_io_matches_fd(io, fd)))
		{
			PgAioIoRef ref;

			pgaio_io_ref_internal(io, &ref);
			pgaio_io_print(io, NULL);
			pgaio_io_wait_ref(&ref, false);
			if (n > 0 && --n == 0)
				return;
		}
	}

	/* XXX explain dirty read of issued_abandoned */
	while (!dlist_is_empty(&my_aio->issued_abandoned))
	{
		PgAioInProgress *io = NULL;
		PgAioIoRef ref;

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		dlist_foreach(iter, &my_aio->issued_abandoned)
		{
			io = dlist_container(PgAioInProgress, owner_node, iter.cur);

			if (io->flags & PGAIOIP_INFLIGHT &&
				(fd < 0 || pgaio_io_matches_fd(io, fd)))
			{
				pgaio_io_ref_internal(io, &ref);
				break;
			}
			else
				io = NULL;
		}
		LWLockRelease(SharedAIOCtlLock);

		if (!io)
			break;

		pgaio_io_print(io, NULL);
		pgaio_io_wait_ref(&ref, false);
		pgaio_complete_ios(false);
		pgaio_transfer_foreign_to_local();
		if (n > 0 && --n == 0)
			return;
	}
}

/*
 * Should be called before closing any fd that might have asynchronous I/O
 * operations.  Since some POSIX AIO implementations cancel IOs (or worse,
 * confuse files) when the fd is concurrently closed, we'll default to waiting
 * for anything in flight on this fd to complete first, except on systems where
 * we're sure that isn't necessary.
 */
void
pgaio_closing_fd(int fd)
{
#ifdef USE_POSIX_AIO
#if !defined(__freebsd__)
	if (io_method == IOMETHOD_POSIX)
		pgaio_wait_for_issued_internal(-1, fd);
#endif
#endif
}

/* --------------------------------------------------------------------------------
 * SQL interface functions
 * --------------------------------------------------------------------------------
 */

Datum
pg_stat_get_aio_backends(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_AIO_BACKEND_COLS	13

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (int i = 0; i < aio_ctl->backend_state_count; i++)
	{
		PgAioPerBackend *bs = &aio_ctl->backend_state[i];
		Datum		values[PG_STAT_GET_AIO_BACKEND_COLS];
		bool		nulls[PG_STAT_GET_AIO_BACKEND_COLS];
		int			pid = ProcGlobal->allProcs[i].pid;

		if (pid == 0)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[ 0] = Int32GetDatum(pid);
		values[ 1] = Int64GetDatum(bs->executed_total_count);
		values[ 2] = Int64GetDatum(bs->issued_total_count);
		values[ 3] = Int64GetDatum(bs->submissions_total_count);
		values[ 4] = Int64GetDatum(bs->foreign_completed_total_count);
		values[ 5] = Int64GetDatum(bs->retry_total_count);
		values[ 6] = Int64GetDatum(pg_atomic_read_u32(&bs->inflight_count));
		values[ 7] = Int32GetDatum(bs->unused_count);
		values[ 8] = Int32GetDatum(bs->outstanding_count);
		values[ 9] = Int32GetDatum(bs->pending_count);
		values[10] = Int32GetDatum(bs->local_completed_count);
		values[11] = Int32GetDatum(bs->foreign_completed_count);
		values[12] = Int32GetDatum(bs->last_context);


		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

Datum
pg_stat_get_aios(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_AIOS_COLS	10

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	StringInfoData tmps;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	initStringInfo(&tmps);

	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *raw_io = &aio_ctl->in_progress_io[i];
		PgAioInProgress io_copy;
		Datum		values[PG_STAT_GET_AIOS_COLS];
		bool		nulls[PG_STAT_GET_AIOS_COLS];
		uint64 ref_generation;
		PgAioInProgressFlags flags;

		flags = raw_io->flags;
		if (flags & PGAIOIP_UNUSED)
			continue;

		ref_generation = raw_io->generation;

		pg_read_barrier();
		io_copy = *raw_io;
		pg_read_barrier();

		flags = raw_io->flags;
		pg_read_barrier();

		if (raw_io->generation != ref_generation)
			continue;

		if (flags & PGAIOIP_UNUSED)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[ 0] = Int32GetDatum(i);
		values[ 1] = Int64GetDatum(io_copy.generation);
		values[ 2] = PointerGetDatum(cstring_to_text(pgaio_io_operation_string(io_copy.op)));
		values[ 3] = PointerGetDatum(cstring_to_text(pgaio_io_shared_callback_string(io_copy.scb)));

		pgaio_io_flag_string(flags, &tmps);
		values[ 4] = PointerGetDatum(cstring_to_text(tmps.data));
		resetStringInfo(&tmps);

		values[ 5] = Int32GetDatum(io_copy.ring);

		if (io_copy.owner_id != INVALID_PGPROCNO)
			values[ 6] = Int32GetDatum(ProcGlobal->allProcs[io_copy.owner_id].pid);
		else
			nulls[ 6] = true;

		if (io_copy.merge_with_idx != PGAIO_MERGE_INVALID)
			values[ 7] = Int32GetDatum(io_copy.merge_with_idx);
		else
			nulls[ 7] = true;

		values[ 8] = Int32GetDatum(io_copy.result);

		pgaio_io_shared_desc(&io_copy, &tmps);
		values[ 9] = PointerGetDatum(cstring_to_text(tmps.data));
		resetStringInfo(&tmps);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

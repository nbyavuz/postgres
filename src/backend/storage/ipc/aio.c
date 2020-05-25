/*
 *
 * Should-be state:
 *
 * - PG IOs acquired using dedicated lock
 * - PG IOs get queued in local submission queues
 * - PG IOs converted into uring IOs, submitted to shared ring, under
 *   submission lock
 * - uring completions are reaped under completion lock, PG IOs extracted,
 *   uring IOs are recycled
 * - PG IO completions are run
 */
#include "postgres.h"

#include <fcntl.h>
#include <liburing.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/memnodes.h"
#include "pgstat.h"
#include "storage/aio.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/memutils.h"


#define PGAIO_VERBOSE

#define PGAIO_SUBMIT_BATCH_SIZE 256
#define PGAIO_BACKPRESSURE_LIMIT 1024
#define PGAIO_BACKPRESSURE_BLEAT (PGAIO_BACKPRESSURE_LIMIT * 2)
#define PGAIO_MAX_LOCAL_REAPED 128
#define PGAIO_MAX_COMBINE 16

typedef enum PgAioAction
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_INVALID = 0,

	PGAIO_NOP,
	PGAIO_FSYNC,
	PGAIO_FLUSH_RANGE,

	PGAIO_READ_BUFFER,
	PGAIO_WRITE_BUFFER,
	PGAIO_WRITE_WAL,
} PgAioAction;

typedef enum PgAioInProgressFlags
{
	/* request in the ->unused list */
	PGAIOIP_IDLE = 1 << 0,

	/* request used, may also be in other states */
	PGAIOIP_IN_USE = 1 << 1,

	/* owned solely by user */
	PGAIOIP_ONLY_USER = 1 << 2,

	/* request in kernel */
	PGAIOIP_INFLIGHT = 1 << 3,

	/* completion callback was called */
	PGAIOIP_CALLBACK_CALLED = 1 << 4,

	/* IO is merged with others */
	PGAIOIP_MERGE = 1 << 5,

	PGAIOIP_RETRY = 1 << 6,

	/* request failed completely */
	PGAIOIP_HARD_FAILURE = 1 << 7,
	/* request failed partly, e.g. a short write */
	PGAIOIP_SOFT_FAILURE = 1 << 8,

	PGAIOIP_SHARED_UNREAPED = 1 << 9,

} PgAioInProgressFlags;

/* IO completion callback */
typedef bool (*PgAioCompletedCB)(PgAioInProgress *io);

typedef uint16 PgAioIPFlags;

struct PgAioInProgress
{
	dlist_node issuer_node;
	dlist_node system_node;

	ConditionVariable cv;

	PgAioIPFlags flags;

	/* which AIO ring is this entry active for */
	uint8 ring;

	/* PgAioAction, indexes PgAioCompletionCallbacks */
	PgAioAction type;

	/* index into allProcs, or PG_UINT32_MAX for process local IO */
	uint32 initiatorProcIndex;

	/* the IOs result, depends on operation. E.g. the length of a read */
	int32 result;

	bool user_referenced;
	bool system_referenced;
	bool local_pending;

	pg_atomic_uint32 extra_refs;

	PgAioBounceBuffer *bb;

	PgAioInProgress *merge_with;

	/*
	 * NB: Note that fds in here may *not* be relied upon for re-issuing
	 * requests (e.g. for partial reads/writes) - the fd might be from another
	 * process, or closed since. That's not a problem for IOs waiting to be
	 * issued only because the queue is flushed when closing an fd.
	 */
	union {
		struct
		{
			int fd;
			bool barrier;
			bool datasync;
		} fsync;

		struct
		{
			int fd;
			off_t nbytes;
			off_t offset;
		} flush_range;

		struct
		{
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			char *bufdata;
			int fd;
			Buffer buf;
			AioBufferTag tag;
			int mode;
			struct iovec iovec[PGAIO_MAX_COMBINE];
		} read_buffer;

		struct
		{
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			char *bufdata;
			int fd;
			Buffer buf;
			AioBufferTag tag;
			struct iovec iovec[PGAIO_MAX_COMBINE];
		} write_buffer;

		struct
		{
			int fd;
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			char *bufdata;
			bool no_reorder;
			struct iovec iovec[PGAIO_MAX_COMBINE];
		} write_wal;
	} d;
};

/* typedef in header */
struct PgAioBounceBuffer
{
	union
	{
		char buffer[BLCKSZ];
		dlist_node node;
	} d;
};

typedef struct PgAioCtl
{
	/* FIXME: this needs to be partitioned */
	/* PgAioInProgress that are not used */
	dlist_head unused_ios;

	dlist_head reaped_uncompleted;

	/* FIXME: this should be per ring */
	/*
	 * PgAioInProgress that are issued to the ringbuffer, and have not yet
	 * been processed (but they may have completed without us processing the
	 * completions).
	 */
	pg_atomic_uint32 inflight;

	/*
	 * FIXME: there should be multiple rings, at least one for data integrity
	 * writes, allowing efficient interleaving with WAL writes, and one for
	 * the rest. But likely a small number of non integrity rings too.
	 *
	 * It could also make sense to have a / a few rings for specific purposes
	 * like prefetching, and vacuum too. Configuring different depths could be
	 * a nice tool to manage maximum overall system impact (in particular by
	 * limiting queue/inflight operations size).
	 */
	struct io_uring shared_ring;

	/*
	 * Number of PgAioInProgressIOs that are in use. This includes pending
	 * requests, as well as requests actually issues to the queue.
	 */
	pg_atomic_uint32 outstanding;

	dlist_head bounce_buffers;

	PgAioInProgress in_progress_io[FLEXIBLE_ARRAY_MEMBER];
} PgAioCtl;

struct io_uring local_ring;

/* general pgaio helper functions */
static void pgaio_complete_ios(bool in_error);
static void pgaio_recycle_completed(void);
static void pgaio_backpressure(struct io_uring *ring, const char *loc);
static void pgaio_prepare_io(PgAioInProgress *io, PgAioAction action);
static void pgaio_finish_io(PgAioInProgress *io);
static void pgaio_bounce_buffer_release_locked(PgAioInProgress *io);

/* io_uring related functions */
static int pgaio_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe);
static void pgaio_complete_cqes(struct io_uring *ring,
								struct io_uring_cqe **cqes, int ready);

static int __sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
								unsigned flags, sigset_t *sig);

/* io completions */
static bool pgaio_complete_nop(PgAioInProgress *io);
static bool pgaio_complete_fsync(PgAioInProgress *io);
static bool pgaio_complete_flush_range(PgAioInProgress *io);
static bool pgaio_complete_read_buffer(PgAioInProgress *io);
static bool pgaio_complete_write_buffer(PgAioInProgress *io);
static bool pgaio_complete_write_wal(PgAioInProgress *io);

static int reopen_buffered(const AioBufferTag *tag);

static MemoryContext aio_retry_context;

/*
 * To support EXEC_BACKEND environments, where we cannot rely on callback
 * addresses being equivalent across processes, completion actions are just
 * indices into a process local array of callbacks, indexed by the type of
 * action.  Also makes the shared memory entries a bit smaller, but that's not
 * a huge win.
 */
static const PgAioCompletedCB completion_callbacks[] =
{
	[PGAIO_NOP] = pgaio_complete_nop,
	[PGAIO_FSYNC] = pgaio_complete_fsync,
	[PGAIO_FLUSH_RANGE] = pgaio_complete_flush_range,
	[PGAIO_READ_BUFFER] = pgaio_complete_read_buffer,
	[PGAIO_WRITE_BUFFER] = pgaio_complete_write_buffer,
	[PGAIO_WRITE_WAL] = pgaio_complete_write_wal,
};


/* (future) GUC controlling global MAX number of in-progress IO entries */
extern int max_aio_in_progress;
int max_aio_in_progress = 4096;

/* global list of in-progress IO */
static PgAioCtl *aio_ctl;

/*
 * Requests waiting to be issued to the kernel. They are submitted to the
 * kernel in batches, for efficiency (including allowing for better queue
 * processing).
 */
static int num_local_pending_requests = 0;

static dlist_head local_pending_requests;
static dlist_head local_recycle_requests;
static dlist_head local_outstanding_requests;

/*
 * Requests completions received from the kernel. These are in global
 * variables so we can continue processing, if a completion callback fails.
 */
static int num_local_reaped = 0;
static struct io_uring_cqe *local_reaped_cqes[PGAIO_MAX_LOCAL_REAPED];
dlist_head local_reaped_ios;


static Size AioCtlShmemSize(void)
{
	return add_size(mul_size(max_aio_in_progress, sizeof(PgAioInProgress)),
					offsetof(PgAioCtl, in_progress_io));
}

static Size AioBounceShmemSize(void)
{
	return add_size(BLCKSZ /* alignment padding */,
					mul_size(BLCKSZ, max_aio_in_progress));
}

Size
AioShmemSize(void)
{
	return add_size(AioCtlShmemSize(), AioBounceShmemSize());
}

void
AioShmemInit(void)
{
	bool		found;

	aio_ctl = (PgAioCtl *)
		ShmemInitStruct("PgAio", AioCtlShmemSize(), &found);

	if (!found)
	{
		memset(aio_ctl, 0, AioCtlShmemSize());

		dlist_init(&aio_ctl->unused_ios);
		dlist_init(&aio_ctl->reaped_uncompleted);
		pg_atomic_init_u32(&aio_ctl->inflight, 0);
		pg_atomic_init_u32(&aio_ctl->outstanding, 0);

		for (int i = 0; i < max_aio_in_progress; i++)
		{
			PgAioInProgress *io = &aio_ctl->in_progress_io[i];

			ConditionVariableInit(&io->cv);
			dlist_push_tail(&aio_ctl->unused_ios,
							&io->system_node);
			io->flags = PGAIOIP_IDLE;
			io->system_referenced = true;
		}

		{
			char *p;
			PgAioBounceBuffer *buffers;

			dlist_init(&aio_ctl->bounce_buffers);
			p = ShmemInitStruct("PgAioBounceBuffers", AioBounceShmemSize(), &found);
			Assert(!found);
			buffers = (PgAioBounceBuffer *) TYPEALIGN(BLCKSZ, (uintptr_t) p);

			for (int i = 0; i < max_aio_in_progress; i++)
			{
				PgAioBounceBuffer *bb = &buffers[i];

				memset(bb, 0, BLCKSZ);
				dlist_push_tail(&aio_ctl->bounce_buffers, &bb->d.node);
			}
		}

		{
			int ret;

			ret = io_uring_queue_init(512, &aio_ctl->shared_ring, 0);
			if (ret < 0)
				elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
		}

	}
}

void
pgaio_postmaster_init(void)
{
	dlist_init(&local_pending_requests);
	num_local_pending_requests = 0;
	num_local_reaped = 0;
	dlist_init(&local_recycle_requests);

	dlist_init(&local_outstanding_requests);

	// XXX: could create a local queue here.

	/*
	 * Need to be allowed to re-open files during retries. Those can happen,
	 * e.g. when fsyncing WAL, within a critical section. Reopening files
	 * currently requires memory. So create a context with small reservation
	 * that's allowed to be used within a critical section.
	 */
	aio_retry_context = AllocSetContextCreate(TopMemoryContext,
											  "aio retry context",
											  1024,
											  1024,
											  1024);
	MemoryContextAllowInCriticalSection(aio_retry_context, true);
}

void
pgaio_postmaster_child_init(void)
{
	/* no locking needed here, only affects this process */
	io_uring_ring_dontfork(&aio_ctl->shared_ring);
	dlist_init(&local_pending_requests);
	num_local_pending_requests = 0;
	num_local_reaped = 0;

	dlist_init(&local_recycle_requests);
	dlist_init(&local_outstanding_requests);

	/*
	 *
	 */
	{
		int ret;

		ret = io_uring_queue_init(32, &local_ring, 0);
		if (ret < 0)
		{
			elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
		}
	}
}

void
pgaio_at_abort(void)
{
	pgaio_recycle_completed();

	if (num_local_reaped > 0)
	{
		elog(LOG, "at abort with %d pending", num_local_reaped);

		pgaio_complete_ios(/* in_error = */ true);
	}

	pgaio_submit_pending(true);

	while (!dlist_is_empty(&local_outstanding_requests))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, issuer_node, &local_outstanding_requests);

		pgaio_wait_for_io(io, true);
		pgaio_release(io);
	}
}

void
pgaio_at_commit(void)
{
	Assert(dlist_is_empty(&local_recycle_requests));

	while (!dlist_is_empty(&local_outstanding_requests))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, issuer_node, &local_outstanding_requests);

		elog(WARNING, "leaked io %zu", io - aio_ctl->in_progress_io);
		pgaio_wait_for_io(io, true);
		pgaio_release(io);
	}
}

static int
pgaio_split_complete(PgAioInProgress *io)
{
	int orig_result = io->result;
	int running_result = orig_result;
	PgAioInProgress *cur = io;
	PgAioInProgress *last = NULL;
	int extracted = 0;

	while (cur)
	{
		PgAioInProgress *next = cur->merge_with;

		Assert(!(cur->flags & PGAIOIP_CALLBACK_CALLED));
		Assert(cur->merge_with || cur != io);

		cur->merge_with = NULL;
		cur->flags &= ~PGAIOIP_MERGE;

		if (cur != io)
			cur->flags &= ~PGAIOIP_INFLIGHT;

		switch (cur->type)
		{
			case PGAIO_READ_BUFFER:
				Assert(cur->d.read_buffer.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.read_buffer.nbytes)
				{
					cur->result = cur->d.read_buffer.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.read_buffer.nbytes)
				{
					cur->result = running_result;
					running_result = 0;
				}

				break;

			case PGAIO_WRITE_BUFFER:
				Assert(cur->d.write_buffer.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.write_buffer.nbytes)
				{
					cur->result = cur->d.write_buffer.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.write_buffer.nbytes)
				{
					cur->result = running_result;
					running_result = 0;
				}
				break;

			case PGAIO_WRITE_WAL:
				Assert(cur->d.write_wal.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.write_wal.nbytes)
				{
					cur->result = cur->d.write_wal.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.write_wal.nbytes)
				{
					cur->result = running_result;
					running_result = 0;
				}
				break;

			default:
				elog(PANIC, "merge for %d not supported yet", cur->type);
		}

		if (last)
		{
			num_local_reaped++;
			dlist_insert_after(&last->system_node, &cur->system_node);
			extracted++;
		}

		last = cur;
		cur = next;
	}

	return extracted;
}

static void  __attribute__((noinline))
pgaio_complete_ios(bool in_error)
{
	int num_pending_before = num_local_pending_requests;
	dlist_mutable_iter iter;
	int extracted = 0;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	/* "unmerge" merged IOs, so they can be treated uniformly */
	START_CRIT_SECTION();
	dlist_foreach_modify(iter, &local_reaped_ios)
	{
		PgAioInProgress *io = dlist_container(PgAioInProgress, system_node, iter.cur);

		if (io->flags & PGAIOIP_MERGE)
		{
			extracted += pgaio_split_complete(io);
		}
	}

	if (extracted)
		pg_atomic_fetch_sub_u32(&aio_ctl->inflight, extracted);

	END_CRIT_SECTION();

	/* call all callbacks, without holding lock */
	while (!dlist_is_empty(&local_reaped_ios))
	{
		dlist_node *node = dlist_head_node(&local_reaped_ios);
		PgAioInProgress *io = dlist_container(PgAioInProgress, system_node, node);

		Assert(node != NULL);


		if (!(io->flags & PGAIOIP_CALLBACK_CALLED))
		{
			PgAioCompletedCB cb;
			bool finished;

			*(volatile PgAioIPFlags*) &io->flags |= PGAIOIP_CALLBACK_CALLED;

			cb = completion_callbacks[io->type];
			finished = cb(io);

			num_local_reaped--;
			dlist_delete(node);

			if (finished)
			{
				dlist_push_tail(&local_recycle_requests, &io->system_node);
			}
			else
			{
				LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);
				io->flags |= PGAIOIP_SHARED_UNREAPED;
				dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->system_node);
				LWLockRelease(SharedAIOCompletionLock);
			}

			/* signal state change */
			if (IsUnderPostmaster)
				ConditionVariableBroadcast(&io->cv);
		}
		else
		{

			Assert(in_error);

			num_local_reaped--;
			dlist_delete(node);

			LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);
			io->flags |= PGAIOIP_HARD_FAILURE | PGAIOIP_SHARED_UNREAPED;
			dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->system_node);
			LWLockRelease(SharedAIOCompletionLock);
		}
	}

	Assert(num_local_reaped == 0);

	pgaio_recycle_completed();

	/* if any IOs weren't fully done, re-submit them */
	if (num_pending_before != num_local_pending_requests)
		pgaio_submit_pending(false);
}

static void
pgaio_recycle_completed(void)
{
	while (!dlist_is_empty(&local_recycle_requests))
	{
		dlist_mutable_iter iter;
		PgAioInProgress* signal_ios[32];
		int to_signal = 0;

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

		dlist_foreach_modify(iter, &local_recycle_requests)
		{
			PgAioInProgress *cur = dlist_container(PgAioInProgress, system_node, iter.cur);

			dlist_delete_from(&local_recycle_requests, iter.cur);
			signal_ios[to_signal++] = cur;

			Assert(cur->system_referenced);
			Assert(!(cur->flags & PGAIOIP_INFLIGHT));
			Assert(!(cur->flags & PGAIOIP_MERGE));
			Assert(cur->merge_with == NULL);
			Assert(cur->flags & PGAIOIP_IN_USE);

			if (cur->user_referenced)
			{
				cur->system_referenced = false;
				*(volatile PgAioIPFlags*) &cur->flags = (cur->flags & ~PGAIOIP_IN_USE) | PGAIOIP_ONLY_USER;
			}
			else
			{
				*(volatile PgAioIPFlags*) &cur->flags = PGAIOIP_IDLE;

				pgaio_bounce_buffer_release_locked(cur);

				cur->type = 0;
				cur->initiatorProcIndex = INVALID_PGPROCNO;
				cur->result = 0;
				cur->system_referenced = true;

				dlist_push_tail(&aio_ctl->unused_ios, &cur->system_node);

				pg_atomic_write_u32(&aio_ctl->outstanding, pg_atomic_read_u32(&aio_ctl->outstanding) - 1);
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
}

/*
 * This checks if there are completions to be processed, before unlocking the
 * ring.
 */
static int  __attribute__((noinline))
pgaio_drain_and_unlock(struct io_uring *ring)
{
	int processed = 0;

	Assert(LWLockHeldByMe(SharedAIOCompletionLock));

	START_CRIT_SECTION();

	if (io_uring_cq_ready(ring))
	{
		processed = num_local_reaped =
			io_uring_peek_batch_cqe(ring,
									local_reaped_cqes,
									PGAIO_MAX_LOCAL_REAPED);

		pgaio_complete_cqes(ring,
							local_reaped_cqes,
							num_local_reaped);
	}

	LWLockRelease(SharedAIOCompletionLock);

	pg_atomic_fetch_sub_u32(&aio_ctl->inflight, processed);

	END_CRIT_SECTION();

	if (num_local_reaped > 0)
		pgaio_complete_ios(false);

	return processed;
}

static int  __attribute__((noinline))
pgaio_drain(struct io_uring *ring, bool already_locked)
{
	bool lock_held = already_locked;
	int total = 0;

	while (true)
	{
		uint32 ready = io_uring_cq_ready(ring);
		uint32 processed;

		if (ready == 0)
			break;

		if (!lock_held)
		{
			LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);
			lock_held = true;
		}

		processed = pgaio_drain_and_unlock(ring);
		lock_held = false;
		total += processed;

		if (processed >= ready)
			break;
	}

	if (already_locked && !lock_held)
		LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);

	return total;
}

void
pgaio_drain_shared(void)
{
	pgaio_drain(&aio_ctl->shared_ring, false);
}

static bool
pgaio_can_be_combined(PgAioInProgress *last, PgAioInProgress *cur)
{
	if (last->type != cur->type)
		return false;

	if (last->flags & PGAIOIP_RETRY ||
		cur->flags & PGAIOIP_RETRY)
		return false;

	switch (last->type)
	{
		case PGAIO_INVALID:
			elog(ERROR, "unexpected");
			break;

		case PGAIO_READ_BUFFER:
			if (last->d.read_buffer.fd != cur->d.read_buffer.fd)
				return false;
			if ((last->d.read_buffer.offset + last->d.read_buffer.nbytes) != cur->d.read_buffer.offset)
				return false;
			if (last->d.read_buffer.mode != cur->d.read_buffer.mode)
				return false;
			if (last->d.read_buffer.already_done != 0 || cur->d.read_buffer.already_done != 0)
				return false;

			return true;
		case PGAIO_NOP:
		case PGAIO_FLUSH_RANGE:
		case PGAIO_FSYNC:
		case PGAIO_WRITE_BUFFER:
			if (last->d.write_buffer.fd != cur->d.write_buffer.fd)
				return false;
			if ((last->d.write_buffer.offset + last->d.write_buffer.nbytes) != cur->d.write_buffer.offset)
				return false;
			if (last->d.write_buffer.already_done != 0 || cur->d.write_buffer.already_done != 0)
				return false;
			return true;

		case PGAIO_WRITE_WAL:
			if (last->d.write_wal.fd != cur->d.write_wal.fd)
				return false;
			if ((last->d.write_wal.offset + last->d.write_wal.nbytes) != cur->d.write_wal.offset)
				return false;
			if (last->d.write_wal.already_done != 0 || cur->d.write_wal.already_done != 0)
				return false;
			if (last->d.write_wal.no_reorder || cur->d.write_wal.no_reorder)
				return false;
			return true;
	}

	pg_unreachable();
}

static void
pgaio_io_merge(PgAioInProgress *into, PgAioInProgress *tomerge)
{
	elog(DEBUG3, "merging %zu to %zu",
		 tomerge - aio_ctl->in_progress_io,
		 into - aio_ctl->in_progress_io);

	into->merge_with = tomerge;
	into->flags |= PGAIOIP_MERGE;
}

static void
pgaio_combine_pending(void)
{
	dlist_mutable_iter iter;
	PgAioInProgress *last = NULL;
	int combined = 1;

	Assert(num_local_pending_requests > 1);

	dlist_foreach_modify(iter, &local_pending_requests)
	{
		PgAioInProgress *cur = dlist_container(PgAioInProgress, system_node, iter.cur);

		/* can happen when failing partway through io submission */
		if (cur->merge_with)
		{
			elog(DEBUG1, "already merged request (%zu), giving up on merging",
				 cur - aio_ctl->in_progress_io);
			return;
		}

		Assert(cur->merge_with == NULL);
		Assert(!(cur->flags & PGAIOIP_MERGE));

		if (last == NULL)
		{
			last = cur;
			continue;
		}

		if (pgaio_can_be_combined(last, cur))
		{
			combined++;

			dlist_delete_from(&local_pending_requests, &cur->system_node);
			cur->local_pending = false;

			num_local_pending_requests--;

			pgaio_io_merge(last, cur);
		}
		else
		{
			combined = 1;
		}

		if (combined >= PGAIO_MAX_COMBINE)
		{
			elog(DEBUG3, "max combine at %d", combined);
			last = NULL;
			combined = 1;
		}
		else
			last = cur;
	}
}

void  __attribute__((noinline))
pgaio_submit_pending(bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	struct io_uring_sqe *sqe[PGAIO_SUBMIT_BATCH_SIZE];
	int total_submitted = 0;
	uint32 orig_total = num_local_pending_requests;

	/* FIXME: */
	if (!aio_ctl || dlist_is_empty(&local_pending_requests))
	{
		Assert(num_local_pending_requests == 0);
		return;
	}

#define COMBINE_ENABLED

#ifdef COMBINE_ENABLED
#if 0
	ereport(LOG, errmsg("before combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&local_pending_requests, NULL, offsetof(PgAioInProgress, system_node));
#endif
	if (num_local_pending_requests > 1)
		pgaio_combine_pending();

#if 0
	ereport(LOG, errmsg("after combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&local_pending_requests, NULL, offsetof(PgAioInProgress, system_node));
#endif
#endif

	while (!dlist_is_empty(&local_pending_requests))
	{
		int nios = 0;
		int nsubmit = Min(num_local_pending_requests, PGAIO_SUBMIT_BATCH_SIZE);

		Assert(nsubmit != 0 && nsubmit <= num_local_pending_requests);
		LWLockAcquire(SharedAIOSubmissionLock, LW_EXCLUSIVE);

		for (int i = 0; i < nsubmit; i++)
		{
			dlist_node *node;
			PgAioInProgress *io;
			int inflight_add;

			sqe[nios] = io_uring_get_sqe(&aio_ctl->shared_ring);

			if (!sqe[nios])
				break;

			node = dlist_pop_head_node(&local_pending_requests);

			io = dlist_container(PgAioInProgress, system_node, node);

			Assert(io->local_pending);

			ios[nios] = io;

			inflight_add = pgaio_sq_from_io(ios[nios], sqe[nios]);
			Assert(ios[nios]->flags & PGAIOIP_IN_USE);
			ios[nios]->flags |= PGAIOIP_INFLIGHT;

			/* FIXME: do outside of lwlocked region */
			pg_atomic_add_fetch_u32(&aio_ctl->inflight, inflight_add);

			io->local_pending = false;
			nios++;
			num_local_pending_requests--;
			total_submitted++;
		}

		if (nios > 0)
		{
			int ret;
	again:
			pgstat_report_wait_start(WAIT_EVENT_AIO_SUBMIT);
			ret = io_uring_submit(&aio_ctl->shared_ring);
			pgstat_report_wait_end();

			if (ret == -EINTR)
				goto again;

			if (ret < 0)
				elog(PANIC, "failed: %d/%s",
					 ret, strerror(-ret));
		}

		LWLockRelease(SharedAIOSubmissionLock);

		/* check if there are completions we could process */
		if (drain)
			pgaio_drain(&aio_ctl->shared_ring, false);
	}

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "submitted %d (orig %d)", total_submitted, orig_total);
#endif

	pgaio_backpressure(&aio_ctl->shared_ring, "submit_pending");
}

static void  __attribute__((noinline))
pgaio_backpressure(struct io_uring *ring, const char *loc)
{
	if (pg_atomic_read_u32(&aio_ctl->inflight) > PGAIO_BACKPRESSURE_LIMIT &&
		io_uring_cq_ready(ring) > 0)
	{
		int total;
		int cqr_before = io_uring_cq_ready(ring);
		int inflight_before = pg_atomic_read_u32(&aio_ctl->inflight);
		int outstanding_before = pg_atomic_read_u32(&aio_ctl->outstanding);

		total = pgaio_drain(ring, false);

		elog(DEBUG1, "backpressure drain at %s: cqr before/after: %d/%d, inflight b/a: %d/%d, outstanding b/a: %d/%d, processed %d",
			 loc,
			 cqr_before, io_uring_cq_ready(ring),
			 inflight_before, pg_atomic_read_u32(&aio_ctl->inflight),
			 outstanding_before, pg_atomic_read_u32(&aio_ctl->outstanding),
			 total);

	}

	// FIXME: may busy wait
	if (pg_atomic_read_u32(&aio_ctl->inflight) > PGAIO_BACKPRESSURE_LIMIT)
	{
		for (int i = 0; i < 1; i++)
		{
			int ret;
			int waitfor;
			int cqr_before;
			int cqr_after;
			int inflight_before = pg_atomic_read_u32(&aio_ctl->inflight);
			int outstanding_before = pg_atomic_read_u32(&aio_ctl->outstanding);

			/*
			 * FIXME: This code likely has a race condition, where the queue
			 * might be emptied after our check, but before we wait for events
			 * to be completed.  Using a lock around this could fix that, but
			 * we dont want that (both because others should be able to add
			 * requests, and because we'd rather have the kernel wake everyone
			 * up, if there's some readiness - it's quite likely multiple
			 * backends may wait for the same IO).
			 *
			 * Possible fix: While holding lock, register for CV for one of
			 * the inflight requests. Then, using appropriate sigmask'ery,
			 * wait until either that request is processed by somebody else,
			 * or a new completion is ready. The latter is much more likely.
			 */
			cqr_before = io_uring_cq_ready(ring);

			//waitfor = inflight_before - PGAIO_BACKPRESSURE_LIMIT;
			waitfor = 1;

			pgstat_report_wait_start(WAIT_EVENT_AIO_BACKPRESSURE);
			ret = __sys_io_uring_enter(ring->ring_fd,
									   0, waitfor,
									   IORING_ENTER_GETEVENTS, NULL);
			pgstat_report_wait_end();
			if (ret < 0 && errno != EINTR)
				elog(WARNING, "enter failed: %d/%s", ret, strerror(-ret));

			cqr_after = io_uring_cq_ready(ring);
#if 1
			elog(DEBUG2, "backpressure wait at %s waited for %d, "
				 "for inflight b/a: %d/%d, outstanding b/a: %d/%d, "
				 "cqr before %d after %d "
				 "space left: %d, sq ready: %d",
				 loc, waitfor,
				 inflight_before, pg_atomic_read_u32(&aio_ctl->inflight),
				 outstanding_before, pg_atomic_read_u32(&aio_ctl->outstanding),
				 cqr_before, io_uring_cq_ready(ring),
				 io_uring_sq_space_left(ring),
				 io_uring_sq_ready(ring));
#endif
			if (cqr_after)
				pgaio_drain(ring, false);
		}
	}

	if (pg_atomic_read_u32(&aio_ctl->outstanding) > PGAIO_BACKPRESSURE_BLEAT)
	{
		elog(DEBUG1, "something's up: %d outstanding! inflight: %d, cq ready: %u sq space left: %d, sq ready: %d",
			 pg_atomic_read_u32(&aio_ctl->outstanding),
			 pg_atomic_read_u32(&aio_ctl->inflight),
			 io_uring_cq_ready(ring),
			 io_uring_sq_space_left(ring),
			 io_uring_sq_ready(ring));
	}
}

void
pgaio_wait_for_io(PgAioInProgress *io, bool holding_reference)
{
	PgAioIPFlags init_flags;
	PgAioIPFlags flags;
	bool increased_refcount = false;
	uint32 done_flags =
		PGAIOIP_ONLY_USER |
		PGAIOIP_IDLE |
		PGAIOIP_SOFT_FAILURE |
		PGAIOIP_HARD_FAILURE;

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "waiting for %zu",
		 io - aio_ctl->in_progress_io);
#endif

again:

	if (!holding_reference)
	{
		/*
		 * If we're not possibly trying to reuse the IO, it's sufficient to
		 * wait for the callback to have been called. But if the backend might
		 * want to recycle the IO, that's not good enough.
		 */
		done_flags |= PGAIOIP_CALLBACK_CALLED;

		init_flags = flags = *(volatile PgAioIPFlags*) &io->flags;

		if (init_flags & (PGAIOIP_IN_USE))
		{
			pg_atomic_fetch_add_u32(&io->extra_refs, 1);
			increased_refcount = true;
			HOLD_INTERRUPTS();
		}
		else
		{
			Assert(init_flags & done_flags);
		}

		if (!increased_refcount)
			goto out;
	}
	else
	{
		init_flags = flags = *(volatile PgAioIPFlags*) &io->flags;
		Assert(io->user_referenced);
	}

	/*
	 * When holding a reference the IO can't go idle. And if we're not, we
	 * should have exited above.
	 */
	Assert(!(init_flags & PGAIOIP_IDLE));
	Assert(init_flags & (PGAIOIP_IN_USE | PGAIOIP_ONLY_USER));

	if (init_flags & PGAIOIP_ONLY_USER)
	{
		Assert(holding_reference);
		goto out;
	}

#if 0
	if (!(init_flags & PGAIOIP_INFLIGHT) &&
		!(init_flags & PGAIOIP_DONE))
	{
		pgaio_submit_pending(false);
	}
#else
	if (!IsUnderPostmaster || io->initiatorProcIndex == MyProc->pgprocno)
	{
		pgaio_submit_pending(false);
	}
#endif

	while (true)
	{
		flags = *(volatile PgAioIPFlags*) &io->flags;

		if (flags & done_flags)
			break;

		pgaio_drain(&aio_ctl->shared_ring, false);

		flags = *(volatile PgAioIPFlags*) &io->flags;

		if (flags & done_flags)
			break;

		if (flags & PGAIOIP_INFLIGHT)
		{
			int ret;

			/* ensure we're going to get woken up */
			if (IsUnderPostmaster)
			{
				ConditionVariablePrepareToSleep(&io->cv);
				PG_SETMASK(&BlockSig);
				ResetLatch(MyLatch);
			}

			flags = *(volatile PgAioIPFlags*) &io->flags;
			if (!(flags & PGAIOIP_INFLIGHT))
			{
				PG_SETMASK(&UnBlockSig);
				continue;
			}

#ifdef PGAIO_VERBOSE
			elog(DEBUG3, "sys enter %zu",
				 io - aio_ctl->in_progress_io);
#endif

			/* wait for one io to be completed */
			errno = 0;
			pgstat_report_wait_start(WAIT_EVENT_AIO_IO_COMPLETE_ANY);
			ret = __sys_io_uring_enter(aio_ctl->shared_ring.ring_fd,
									   0, 1,
									   IORING_ENTER_GETEVENTS, &UnBlockSig);
			pgstat_report_wait_end();
			PG_SETMASK(&UnBlockSig);

			if (ret < 0 && errno == EINTR)
			{
				elog(DEBUG3, "got interrupted");
			}
			else if (ret != 0)
				elog(WARNING, "unexpected: %d/%s: %m", ret, strerror(-ret));

			pgaio_drain(&aio_ctl->shared_ring, false);

			if (IsUnderPostmaster)
				ConditionVariableCancelSleep();
		}
		else
		{
			/* shouldn't be reachable without concurrency */
			Assert(IsUnderPostmaster);

			/* ensure we're going to get woken up */
			if (IsUnderPostmaster)
				ConditionVariablePrepareToSleep(&io->cv);

			flags = *(volatile PgAioIPFlags*) &io->flags;
			if (!(flags & done_flags))
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);

			if (IsUnderPostmaster)
				ConditionVariableCancelSleep();
		}
	}

	flags = *(volatile PgAioIPFlags*) &io->flags;
	Assert(flags & done_flags);

out:
	if (flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_SOFT_FAILURE))
	{
		/* can retry soft failures, but not hard ones */
		/* FIXME: limit number of soft retries */
		if (flags & PGAIOIP_SOFT_FAILURE)
		{
			ereport(LOG, errmsg("retrying IO %zd",
								 io - aio_ctl->in_progress_io),
					errhidestmt(true),
					errhidecontext(true));
			pgaio_io_print(io, NULL);

			pgaio_io_retry(io);

			if (increased_refcount)
			{
				pg_atomic_fetch_sub_u32(&io->extra_refs, 1);
				ConditionVariableBroadcast(&io->cv);
				RESUME_INTERRUPTS();
				increased_refcount = false;
			}

			goto again;
		}
		else
		{
			elog(WARNING, "request %zd failed permanently",
				 io - aio_ctl->in_progress_io);
		}
	}

	if (increased_refcount)
	{
		pg_atomic_fetch_sub_u32(&io->extra_refs, 1);
		ConditionVariableBroadcast(&io->cv);
		RESUME_INTERRUPTS();
	}
}

static void
pgaio_io_wait_extra_refs(PgAioInProgress *io)
{
	while (pg_atomic_read_u32(&io->extra_refs) > 0)
	{
		elog(WARNING, "waiting for extra refs");
		ConditionVariablePrepareToSleep(&io->cv);
		if (pg_atomic_read_u32(&io->extra_refs) == 0)
		{
			ConditionVariableCancelSleep();
			break;
		}
		ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_REFS);
	}
}

PgAioInProgress *
pgaio_io_get(void)
{
	dlist_node *elem;
	PgAioInProgress *io;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	// FIXME: relax?
	Assert(num_local_pending_requests < PGAIO_SUBMIT_BATCH_SIZE);

	/* FIXME: wait for an IO to complete if full */

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	while (unlikely(dlist_is_empty(&aio_ctl->unused_ios)))
	{
		LWLockRelease(SharedAIOCtlLock);
		elog(DEBUG1, "needed to drain while getting IO (outstanding %d inflight %d)",
			 pg_atomic_read_u32(&aio_ctl->outstanding), pg_atomic_read_u32(&aio_ctl->inflight));
		pgaio_drain(&aio_ctl->shared_ring, false);

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	}

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);
	pg_atomic_write_u32(&aio_ctl->outstanding, pg_atomic_read_u32(&aio_ctl->outstanding) + 1);

	LWLockRelease(SharedAIOCtlLock);

	io = dlist_container(PgAioInProgress, system_node, elem);

	Assert(!(io->flags & PGAIOIP_IN_USE));
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->system_referenced);
	io->user_referenced = true;
	io->system_referenced = false;

	*(volatile PgAioIPFlags*) &io->flags = (io->flags & ~PGAIOIP_IDLE) | PGAIOIP_ONLY_USER;

	pgaio_io_wait_extra_refs(io);
	Assert(pg_atomic_read_u32(&io->extra_refs) == 0);

	if (IsUnderPostmaster)
		io->initiatorProcIndex = MyProc->pgprocno;

	dlist_push_tail(&local_outstanding_requests, &io->issuer_node);

	return io;
}

bool
pgaio_io_success(PgAioInProgress *io)
{
	Assert(io->user_referenced);

	Assert(!(io->flags & PGAIOIP_IN_USE));

	if (io->flags & (PGAIOIP_HARD_FAILURE | PGAIOIP_SOFT_FAILURE))
		return false;

	if (!(io->flags & PGAIOIP_CALLBACK_CALLED))
		return false;

	return true;
}

void
pgaio_io_retry(PgAioInProgress *io)
{
	bool retryable = false;


	switch (io->type)
	{
		case PGAIO_READ_BUFFER:
			retryable = true;
			io->d.read_buffer.fd = reopen_buffered(&io->d.write_buffer.tag);
			break;

		case PGAIO_WRITE_BUFFER:
			retryable = true;
			io->d.write_buffer.fd = reopen_buffered(&io->d.write_buffer.tag);
			break;

		default:
			return;
			break;
	}

	if (retryable)
	{
		io->flags &= ~(PGAIOIP_CALLBACK_CALLED | PGAIOIP_HARD_FAILURE | PGAIOIP_SOFT_FAILURE);
		io->flags |= PGAIOIP_IN_USE;
		io->flags |= PGAIOIP_RETRY;
		io->local_pending = true;

		if (io->flags & PGAIOIP_SHARED_UNREAPED)
		{
			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			io->flags &= ~PGAIOIP_SHARED_UNREAPED;
			dlist_delete(&io->system_node);
			LWLockRelease(SharedAIOCtlLock);
		}

		dlist_push_tail(&local_pending_requests, &io->system_node);
		num_local_pending_requests++;

		pgaio_submit_pending(true);
	}
}

extern void
pgaio_io_recycle(PgAioInProgress *io)
{
	uint32 init_flags = *(volatile PgAioIPFlags*) &io->flags;

	Assert(init_flags & PGAIOIP_ONLY_USER);
	Assert(io->user_referenced);
	Assert(io->merge_with == NULL);

	pgaio_io_wait_extra_refs(io);

	if (io->bb)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		pgaio_bounce_buffer_release_locked(io);
		LWLockRelease(SharedAIOCtlLock);
	}

	io->flags &= ~(PGAIOIP_CALLBACK_CALLED | PGAIOIP_RETRY | PGAIOIP_HARD_FAILURE | PGAIOIP_SOFT_FAILURE);
	io->result = 0;
}

static void  __attribute__((noinline))
pgaio_prepare_io(PgAioInProgress *io, PgAioAction action)
{
	/* true for now, but not necessarily in the future */
	Assert((io->flags & (PGAIOIP_ONLY_USER | PGAIOIP_MERGE)) == io->flags);
	Assert(io->user_referenced);
	Assert(!io->local_pending);
	Assert(io->merge_with == NULL);

	Assert(num_local_pending_requests < PGAIO_SUBMIT_BATCH_SIZE);

	*(volatile PgAioIPFlags*) &io->flags = (io->flags & ~PGAIOIP_ONLY_USER) | PGAIOIP_IN_USE;
	/* for this module */
	io->system_referenced = true;
	io->type = action;
	if (IsUnderPostmaster)
		io->initiatorProcIndex = MyProc->pgprocno;
	io->local_pending = true;

	// FIXME: should this be done in end_get_io?
	dlist_push_tail(&local_pending_requests,
					&io->system_node);
	num_local_pending_requests++;
}

static void  __attribute__((noinline))
pgaio_finish_io(PgAioInProgress *io)
{
	if (num_local_pending_requests >= PGAIO_SUBMIT_BATCH_SIZE)
		pgaio_submit_pending(true);
	else
		pgaio_backpressure(&aio_ctl->shared_ring, "get_io");
}


void
pgaio_release(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!IsUnderPostmaster || io->initiatorProcIndex == MyProc->pgprocno);

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	io->user_referenced = false;

	dlist_delete_from(&local_outstanding_requests, &io->issuer_node);

	if (!io->system_referenced)
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));
		Assert(!(io->flags & PGAIOIP_MERGE));
		Assert(io->flags & PGAIOIP_CALLBACK_CALLED ||
			   io->flags & PGAIOIP_ONLY_USER);

		*(volatile PgAioIPFlags*) &io->flags =
			(io->flags & ~(PGAIOIP_IN_USE | PGAIOIP_CALLBACK_CALLED | PGAIOIP_ONLY_USER | PGAIOIP_RETRY)) | PGAIOIP_IDLE;

		io->type = 0;
		io->initiatorProcIndex = INVALID_PGPROCNO;
		io->result = 0;
		io->system_referenced = true;
		Assert(io->merge_with == NULL);

		/* could do this earlier or conditionally */
		pgaio_bounce_buffer_release_locked(io);

		pg_atomic_write_u32(&aio_ctl->outstanding, pg_atomic_read_u32(&aio_ctl->outstanding) - 1);
		dlist_push_tail(&aio_ctl->unused_ios, &io->system_node);
	}

	LWLockRelease(SharedAIOCtlLock);
}

void
pgaio_print_queues(void)
{
	ereport(LOG,
			errmsg("shared queue: space: %d ready: %d, we think: %d inflight",
				   io_uring_sq_space_left(&aio_ctl->shared_ring),
				   io_uring_cq_ready(&aio_ctl->shared_ring),
				   pg_atomic_read_u32(&aio_ctl->inflight)),
			errhidestmt(true),
			errhidecontext(true)
		);
}

static const char *
pgaio_io_action_string(PgAioAction a)
{
	switch(a)
	{
		case PGAIO_INVALID:
			return "invalid";
		case PGAIO_NOP:
			return "nop";
		case PGAIO_FLUSH_RANGE:
			return "flush_range";
		case PGAIO_FSYNC:
			return "fsync";
		case PGAIO_READ_BUFFER:
			return "read_buffer";
		case PGAIO_WRITE_BUFFER:
			return "write_buffer";
		case PGAIO_WRITE_WAL:
			return "write_wal";
	}

	pg_unreachable();
}


static void
pgaio_io_flag_string(PgAioIPFlags flags, StringInfo s)
{
	bool first = true;

#define STRINGIFY_FLAG(f) if (flags & f) {  appendStringInfoString(s, first ? CppAsString(f) : " | " CppAsString(f)); first = false;}

	STRINGIFY_FLAG(PGAIOIP_IDLE);
	STRINGIFY_FLAG(PGAIOIP_IN_USE);
	STRINGIFY_FLAG(PGAIOIP_ONLY_USER);
	STRINGIFY_FLAG(PGAIOIP_INFLIGHT);
	STRINGIFY_FLAG(PGAIOIP_CALLBACK_CALLED);
	STRINGIFY_FLAG(PGAIOIP_MERGE);
	STRINGIFY_FLAG(PGAIOIP_RETRY);
	STRINGIFY_FLAG(PGAIOIP_HARD_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SOFT_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SHARED_UNREAPED);

#undef STRINGIFY_FLAG
}

static void
pgaio_io_print_one(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "aio %zu: ring: %d, init: %d, flags: ",
					 io - aio_ctl->in_progress_io,
					 io->ring,
					 io->initiatorProcIndex);
	pgaio_io_flag_string(io->flags, s);
	appendStringInfo(s, ", result: %d, user/system_referenced: %d/%d, action: %s",
					 io->result,
					 io->user_referenced,
					 io->system_referenced,
					 pgaio_io_action_string(io->type));

	switch (io->type)
	{
		case PGAIO_FSYNC:
			appendStringInfo(s, " (fd: %d, datasync: %d, barrier: %d)",
							 io->d.fsync.fd,
							 io->d.fsync.datasync,
							 io->d.fsync.barrier);
			break;
		case PGAIO_FLUSH_RANGE:
			appendStringInfo(s, " (fd: %d, offset: %llu, nbytes: %llu)",
							 io->d.flush_range.fd,
							 (unsigned long long) io->d.flush_range.offset,
							 (unsigned long long) io->d.flush_range.nbytes);
			break;
		case PGAIO_READ_BUFFER:
			appendStringInfo(s, " (fd: %d, mode: %d, offset: %d, nbytes: %d, already_done: %d, buf/data: %d/%p)",
							 io->d.read_buffer.fd,
							 io->d.read_buffer.mode,
							 io->d.read_buffer.offset,
							 io->d.read_buffer.nbytes,
							 io->d.read_buffer.already_done,
							 io->d.read_buffer.buf,
							 io->d.read_buffer.bufdata);
			break;
		case PGAIO_WRITE_BUFFER:
			appendStringInfo(s, " (fd: %d, offset: %d, nbytes: %d, already_done: %d, buf/data: %d/%p)",
							 io->d.write_buffer.fd,
							 io->d.write_buffer.offset,
							 io->d.write_buffer.nbytes,
							 io->d.write_buffer.already_done,
							 io->d.read_buffer.buf,
							 io->d.write_buffer.bufdata);
			break;
		case PGAIO_WRITE_WAL:
			appendStringInfo(s, " (fd: %d, offset: %d, nbytes: %d, already_done: %d, bufdata: %p, no-reorder: %d)",
							 io->d.write_wal.fd,
							 (int) io->d.write_wal.offset,
							 (int) io->d.write_wal.nbytes,
							 io->d.write_wal.already_done,
							 io->d.write_wal.bufdata,
							 io->d.write_wal.no_reorder);
			break;
		default:
			break;
	}

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

	{
		PgAioInProgress *cur = io;
		int nummerge = 0;

		if (cur->merge_with)
			appendStringInfoString(s, "\n  merge with:");

		while (cur->merge_with)
		{
			nummerge++;
			appendStringInfo(s, "\n    %d: ", nummerge);
			pgaio_io_print_one(cur->merge_with, s);

			cur = cur->merge_with;
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

	while (true)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		if (!dlist_is_empty(&aio_ctl->bounce_buffers))
		{
			dlist_node *node = dlist_pop_head_node(&aio_ctl->bounce_buffers);

			bb = dlist_container(PgAioBounceBuffer, d.node, node);
		}
		LWLockRelease(SharedAIOCtlLock);

		if (!bb)
			pgaio_drain(&aio_ctl->shared_ring, false);
		else
			break;
	}

	return bb;
}

static void
pgaio_bounce_buffer_release_locked(PgAioInProgress *io)
{
	Assert(LWLockHeldByMe(SharedAIOCtlLock));

	if (!io->bb)
		return;

	dlist_push_tail(&aio_ctl->bounce_buffers, &io->bb->d.node);
	io->bb = NULL;
}

char *
pgaio_bounce_buffer_buffer(PgAioBounceBuffer *bb)
{
	return bb->d.buffer;
}

void
pgaio_assoc_bounce_buffer(PgAioInProgress *io, PgAioBounceBuffer *bb)
{
	Assert(bb != NULL);
	Assert(io->bb == NULL);
	Assert(!(io->flags & PGAIOIP_IDLE));
	Assert(io->flags & PGAIOIP_ONLY_USER);
	Assert(!(io->flags & PGAIOIP_INFLIGHT));

	io->bb = bb;
}

/* --------------------------------------------------------------------------------
 * io_uring related code
 * --------------------------------------------------------------------------------
 */

static void __attribute__((noinline))
pgaio_complete_cqes(struct io_uring *ring, struct io_uring_cqe **cqes, int ready)
{
	int consumed = 0;

	Assert(LWLockHeldByMe(SharedAIOCompletionLock));

	for (int i = 0; i < ready; i++)
	{
		struct io_uring_cqe *cqe = cqes[i];
		PgAioInProgress *io;

		io = io_uring_cqe_get_data(cqe);
		Assert(io != NULL);
		Assert(io->flags & PGAIOIP_INFLIGHT);
		Assert(io->flags & PGAIOIP_IN_USE);
		Assert(!(io->flags == PGAIOIP_IDLE));

		*(volatile PgAioIPFlags*) &io->flags = (io->flags & ~PGAIOIP_INFLIGHT);
		io->result = cqe->res;

		dlist_push_tail(&local_reaped_ios, &io->system_node);

		if (cqe->res < 0)
		{
			elog(WARNING, "cqe: u: %p s: %d/%s f: %u",
				 io_uring_cqe_get_data(cqe),
				 cqe->res,
				 cqe->res < 0 ? strerror(-cqe->res) : "",
				 cqe->flags);
		}

		io_uring_cqe_seen(ring, cqe);
		consumed++;
	}
}

/*
 * FIXME: These need to be deduplicated.
 */
static int
prep_read_buffer_iov(PgAioInProgress *io, struct io_uring_sqe *sqe)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.read_buffer.offset;
	int	niov = 0;
	struct iovec *iov = io->d.read_buffer.iovec;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_buffer.already_done;
		iov->iov_base = cur->d.read_buffer.bufdata + cur->d.read_buffer.already_done;
		iov->iov_len = cur->d.read_buffer.nbytes - cur->d.read_buffer.already_done;

		niov++;
		iov++;

		if (niov > 0)
			cur->flags |= PGAIOIP_INFLIGHT;

		cur = cur->merge_with;
	}

	io_uring_prep_readv(sqe,
						io->d.read_buffer.fd,
						io->d.read_buffer.iovec,
						niov,
						offset);

	return niov;
}

static int
prep_write_buffer_iov(PgAioInProgress *io, struct io_uring_sqe *sqe)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.write_buffer.offset;
	int	niov = 0;
	struct iovec *iov = io->d.write_buffer.iovec;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_buffer.already_done;
		iov->iov_base = cur->d.write_buffer.bufdata + cur->d.write_buffer.already_done;
		iov->iov_len = cur->d.write_buffer.nbytes - cur->d.write_buffer.already_done;

		niov++;
		iov++;

		if (niov > 0)
			cur->flags |= PGAIOIP_INFLIGHT;

		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						io->d.write_buffer.fd,
						io->d.write_buffer.iovec,
						niov,
						offset);
	return niov;
}

static int
prep_write_wal_iov(PgAioInProgress *io, struct io_uring_sqe *sqe)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.write_wal.offset;
	int	niov = 0;
	struct iovec *iov = io->d.write_wal.iovec;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_wal.already_done;
		iov->iov_base = cur->d.write_wal.bufdata + cur->d.write_wal.already_done;
		iov->iov_len = cur->d.write_wal.nbytes - cur->d.write_wal.already_done;

		niov++;
		iov++;

		if (niov > 0)
			cur->flags |= PGAIOIP_INFLIGHT;

		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						io->d.write_wal.fd,
						io->d.write_wal.iovec,
						niov,
						offset);
	return niov;
}

static int
pgaio_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe)
{
	int submitted = 1;

	switch (io->type)
	{
		case PGAIO_FSYNC:
			io_uring_prep_fsync(sqe,
								io->d.fsync.fd,
								io->d.fsync.datasync ? IORING_FSYNC_DATASYNC : 0);
			if (io->d.fsync.barrier)
				sqe->flags |= IOSQE_IO_DRAIN;
			break;

		case PGAIO_READ_BUFFER:
			submitted = prep_read_buffer_iov(io, sqe);
			//sqe->flags |= IOSQE_ASYNC;
			break;

		case PGAIO_WRITE_BUFFER:
			submitted = prep_write_buffer_iov(io, sqe);
			break;

		case PGAIO_FLUSH_RANGE:
			io_uring_prep_rw(IORING_OP_SYNC_FILE_RANGE,
							 sqe,
							 io->d.flush_range.fd,
							 NULL,
							 io->d.flush_range.nbytes,
							 io->d.flush_range.offset);
			sqe->sync_range_flags = SYNC_FILE_RANGE_WRITE;
			break;

		case PGAIO_WRITE_WAL:
			submitted = prep_write_wal_iov(io, sqe);
			if (io->d.write_wal.no_reorder)
				sqe->flags = IOSQE_IO_DRAIN;
			break;

		case PGAIO_NOP:
			elog(ERROR, "not yet");
			break;

		case PGAIO_INVALID:
			elog(ERROR, "invalid");
	}

	io_uring_sqe_set_data(sqe, io);

	return submitted;
}

static int
__sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
			 unsigned flags, sigset_t *sig)
{

# ifndef __NR_io_uring_enter
#  define __NR_io_uring_enter		426
# endif

	return syscall(__NR_io_uring_enter, fd, to_submit, min_complete,
			flags, sig, _NSIG / 8);
}



/* --------------------------------------------------------------------------------
 * Code dealing with specific IO types
 * --------------------------------------------------------------------------------
 */

void
pgaio_start_flush_range(PgAioInProgress *io, int fd, off_t offset, off_t nbytes)
{
	pgaio_prepare_io(io, PGAIO_FLUSH_RANGE);

	io->d.flush_range.fd = fd;
	io->d.flush_range.offset = offset;
	io->d.flush_range.nbytes = nbytes;

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_flush_range %zu: %d, %llu, %llu",
		 io - aio_ctl->in_progress_io,
		 fd, (unsigned long long) offset, (unsigned long long) nbytes);
#endif
}

void
pgaio_start_read_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes, char *bufdata, int buffno, int mode)
{
	pgaio_prepare_io(io, PGAIO_READ_BUFFER);

	io->d.read_buffer.buf = buffno;
	io->d.read_buffer.mode = mode;
	io->d.read_buffer.fd = fd;
	io->d.read_buffer.offset = offset;
	io->d.read_buffer.nbytes = nbytes;
	io->d.read_buffer.bufdata = bufdata;
	io->d.read_buffer.already_done = 0;
	memcpy(&io->d.read_buffer.tag, tag, sizeof(io->d.read_buffer.tag));

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_buffer_read %zu: "
		 "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 buffno,
		 bufdata);
#endif
}

void
pgaio_start_write_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes, char *bufdata, int buffno)
{
	pgaio_prepare_io(io, PGAIO_WRITE_BUFFER);

	io->d.write_buffer.buf = buffno;
	io->d.write_buffer.fd = fd;
	io->d.write_buffer.offset = offset;
	io->d.write_buffer.nbytes = nbytes;
	io->d.write_buffer.bufdata = bufdata;
	io->d.write_buffer.already_done = 0;
	memcpy(&io->d.write_buffer.tag, tag, sizeof(io->d.write_buffer.tag));

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_buffer_write %zu: "
		 "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 buffno,
		 bufdata);
#endif
}

void
pgaio_start_write_wal(PgAioInProgress *io, int fd, uint32 offset, uint32 nbytes, char *bufdata, bool no_reorder)
{
	pgaio_prepare_io(io, PGAIO_WRITE_WAL);

	io->d.write_wal.fd = fd;
	io->d.write_wal.no_reorder = no_reorder;
	io->d.write_wal.offset = offset;
	io->d.write_wal.nbytes = nbytes;
	io->d.write_wal.bufdata = bufdata;
	io->d.write_wal.already_done = 0;

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_write_wal %zu:"
		 "fd %d, off: %llu, bytes: %llu, no_reorder: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 no_reorder,
		 bufdata);
#endif
}

void
pgaio_start_nop(PgAioInProgress *io)
{
	pgaio_prepare_io(io, PGAIO_NOP);
	pgaio_finish_io(io);
}

void
pgaio_start_fsync(PgAioInProgress *io, int fd, bool barrier)
{
	pgaio_prepare_io(io, PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.barrier = barrier;
	io->d.fsync.datasync = false;
	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_fsync %zu:"
		 "fd %d, is_barrier: %d, is_datasync: %d",
		 io - aio_ctl->in_progress_io,
		 fd,
		 barrier,
		 false);
#endif
}

void
pgaio_start_fdatasync(PgAioInProgress *io, int fd, bool barrier)
{
	pgaio_prepare_io(io, PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.barrier = barrier;
	io->d.fsync.datasync = true;
	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_fsync %zu:"
		 "fd %d, is_barrier: %d, is_datasync: %d",
		 io - aio_ctl->in_progress_io,
		 fd,
		 barrier,
		 true);
#endif
}

static bool
pgaio_complete_nop(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed nop");
#endif

	return true;
}

static bool
pgaio_complete_fsync(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed fsync: %zu",
		 io - aio_ctl->in_progress_io);
#endif
	if (io->result != 0)
		elog(PANIC, "fsync needs better error handling");

	return true;
}

static bool
pgaio_complete_flush_range(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed flush_range: %zu, %s",
		 io - aio_ctl->in_progress_io,
		 io->result < 0 ? strerror(-io->result) : "ok");
#endif

	return true;
}

static int
reopen_buffered(const AioBufferTag *tag)
{
	uint32 off;
	SMgrRelation reln = smgropen(tag->rnode.node, tag->rnode.backend);

	return smgrfd(reln, tag->forkNum, tag->blockNum, &off);
}

static bool
pgaio_complete_read_buffer(PgAioInProgress *io)
{
	Buffer		buffer = io->d.read_buffer.buf;

	bool		call_completion;
	bool		failed;
	bool		done;

	/* great for torturing error handling */
#if 0
	if (io->result > 4096)
		io->result = 4096;
#endif

#ifdef PGAIO_VERBOSE
	elog(io->flags & PGAIOIP_RETRY ? DEBUG1 : DEBUG3,
		 "completed read_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.read_buffer.buf);
#endif

	if (io->result != (io->d.read_buffer.nbytes - io->d.read_buffer.already_done))
	{
		MemoryContext old_context = MemoryContextSwitchTo(aio_retry_context);

		failed = true;

		pgaio_io_print(io, NULL);

		if (io->result < 0)
		{
			io->flags |= PGAIOIP_HARD_FAILURE;

			if (io->result == EAGAIN || io->result == EINTR)
			{
				elog(PANIC, "need to implement retries for failed requests");
			}
			else
			{
				ereport(WARNING,
						errcode_for_file_access(),
						errmsg("could not read block %u in file \"%s\": %s",
							   io->d.read_buffer.tag.blockNum,
							   relpath(io->d.read_buffer.tag.rnode,
									   io->d.read_buffer.tag.forkNum),
							   strerror(-io->result)));
			}

			call_completion = true;
			done = true;
		}
		else
		{
			io->flags |= PGAIOIP_SOFT_FAILURE;
			call_completion = false;
			done = false;
			io->d.read_buffer.already_done += io->result;

			/*
			 * This is actually pretty common and harmless, happens when part
			 * of the block is in the kernel page cache, but the other
			 * isn't. So don't issue WARNING/ERROR, but just retry.
			 *
			 * While it can happen with single BLCKSZ reads (since they're
			 * bigger than typical page sizes), it's made much more likely by
			 * us combining reads.
			 *
			 * XXX: Should we handle repeated failures for the same blocks
			 * differently?
			 */
			ereport(DEBUG1,
					errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("%zd: could not read block %u in file \"%s\": read only %d of %d bytes (init: %d, cur: %d)",
						   io - aio_ctl->in_progress_io,
						   io->d.read_buffer.tag.blockNum,
						   relpath(io->d.read_buffer.tag.rnode,
								   io->d.read_buffer.tag.forkNum),
						   io->result, BLCKSZ,
						   io->initiatorProcIndex, MyProc ? MyProc->pgprocno : INVALID_PGPROCNO));
		}

		MemoryContextSwitchTo(old_context);
		MemoryContextReset(aio_retry_context);
	}
	else
	{
		io->d.read_buffer.already_done += io->result;
		Assert(io->d.read_buffer.already_done == BLCKSZ);

		call_completion = true;
		failed = false;
		done = true;
	}

	if (call_completion && BufferIsValid(buffer))
		ReadBufferCompleteRead(buffer, io->d.read_buffer.mode, failed);

	return done;
}

static bool
pgaio_complete_write_buffer(PgAioInProgress *io)
{
	Buffer		buffer = io->d.write_buffer.buf;

	bool		call_completion;
	bool		failed;
	bool		done;

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed write_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.write_buffer.buf);
#endif

	if (io->result != (io->d.write_buffer.nbytes - io->d.write_buffer.already_done))
	{
		MemoryContext old_context = MemoryContextSwitchTo(aio_retry_context);

		failed = true;

		pgaio_io_print(io, NULL);


		if (io->result < 0)
		{
			failed = true;

			io->flags |= PGAIOIP_HARD_FAILURE;

			if (io->result == EAGAIN || io->result == EINTR)
			{
				elog(PANIC, "need to implement retries for transiently failed requests");
			}
			else
			{
				ereport(WARNING,
						errcode_for_file_access(),
						errmsg("could not write block %u in file \"%s\": %s",
							   io->d.write_buffer.tag.blockNum,
							   relpath(io->d.write_buffer.tag.rnode,
									   io->d.write_buffer.tag.forkNum),
							   strerror(-io->result)),
						errhint("Check free disk space."));
			}

			call_completion = true;
			done = true;
		}
		else
		{
			io->flags |= PGAIOIP_SOFT_FAILURE;
			io->d.write_buffer.already_done += io->result;

			call_completion = false;
			done = false;

			ereport(WARNING,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes (init: %d, cur: %d)",
							io->d.write_buffer.tag.blockNum,
							relpath(io->d.write_buffer.tag.rnode,
									io->d.write_buffer.tag.forkNum),
							io->result, (io->d.write_buffer.nbytes - io->d.write_buffer.already_done),
							io->initiatorProcIndex, MyProc ? MyProc->pgprocno : INVALID_PGPROCNO)));
		}

		MemoryContextSwitchTo(old_context);
		MemoryContextReset(aio_retry_context);
	}
	else
	{
		io->d.write_buffer.already_done += io->result;
		Assert(io->d.write_buffer.already_done == BLCKSZ);

		call_completion = true;
		failed = false;
		done = true;
	}

	if (call_completion && BufferIsValid(buffer))
		ReadBufferCompleteWrite(buffer, failed);

	return done;
}

static bool
pgaio_complete_write_wal(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed write_wal: %zu, %d/%s",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok");
#endif

	if (io->result < 0)
	{
		if (io->result == EAGAIN || io->result == EINTR)
		{
			elog(WARNING, "need to implement retries");
		}

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: %s",
						strerror(-io->result))));
	}
	else if (io->result != (io->d.write_wal.nbytes - io->d.write_wal.already_done))
	{
		/* FIXME: implement retries for short writes */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: wrote only %d of %d bytes",
						io->result, (io->d.write_wal.nbytes - io->d.write_wal.already_done))));
	}

	/* FIXME: update xlog.c state */

	return true;
}

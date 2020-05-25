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
#include "miscadmin.h"
#include "nodes/memnodes.h"
#include "storage/aio.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "libpq/pqsignal.h"


#define PGAIO_VERBOSE

#define PGAIO_SUBMIT_BATCH_SIZE 32
#define PGAIO_BACKPRESSURE_LIMIT 500
#define PGAIO_MAX_LOCAL_REAPED 32

typedef enum PgAioAction
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_INVALID = 0,

	PGAIO_NOP,
	PGAIO_FLUSH_RANGE,
	PGAIO_FSYNC,

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
	/* request in kernel */
	PGAIOIP_INFLIGHT = 1 << 2,
	/* completion callback was called */
	PGAIOIP_DONE = 1 << 3,
} PgAioInProgressFlags;

/* IO completion callback */
typedef bool (*PgAioCompletedCB)(PgAioInProgress *io);

struct PgAioInProgress
{
	dlist_node node;

	ConditionVariable cv;

	uint8 flags;

	/* PgAioAction, indexes PgAioCompletionCallbacks */
	PgAioAction type;

	/* which AIO ring is this entry active for */
	uint8 ring;

	/* index into allProcs, or PG_UINT32_MAX for process local IO */
	uint32 initiatorProcIndex;

	/* the IOs result, depends on operation. E.g. the length of a read */
	int32 result;

	uint32 refcount;

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
			Buffer buf;
			int mode;
			int fd;
			int already_done;
			off_t offset;
			struct iovec iovec;
		} read_buffer;

		struct
		{
			Buffer buf;
			int fd;
			int already_done;
			off_t offset;
			struct iovec iovec;
		} write_buffer;

		struct
		{
			int fd;
			int already_done;
			bool no_reorder;
			off_t offset;
			struct iovec iovec;
		} write_wal;
	} d;
};


typedef struct PgAioCtl
{
	/* FIXME: this needs to be partitioned */
	/* PgAioInProgress that are not used */
	dlist_head unused_ios;

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

	PgAioInProgress in_progress_io[FLEXIBLE_ARRAY_MEMBER];
} PgAioCtl;


/* general pgaio helper functions */
static void pgaio_complete_ios(bool in_error);
static void pgaio_backpressure(struct io_uring *ring, const char *loc);
static PgAioInProgress* pgaio_start_get_io(PgAioAction action);
static void pgaio_end_get_io(void);

/* io_uring related functions */
static void pgaio_put_io_locked(PgAioInProgress *io);
static void pgaio_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe);
static void pgaio_complete_cqes(struct io_uring *ring, PgAioInProgress **ios,
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
int max_aio_in_progress = 2048;

/* global list of in-progress IO */
static PgAioCtl *aio_ctl;

/*
 * Requests waiting to be issued to the kernel. They are submitted to the
 * kernel in batches, for efficiency (including allowing for better queue
 * processing).
 */
static int num_local_pending_requests = 0;
static dlist_head local_pending_requests;

/*
 * Requests completions received from the kernel. These are in global
 * variables so we can continue processing, if a completion callback fails.
 */
static int num_local_reaped = 0;
static struct io_uring_cqe *local_reaped_cqes[PGAIO_MAX_LOCAL_REAPED];
static PgAioInProgress *local_reaped_ios[PGAIO_MAX_LOCAL_REAPED];



Size
AioShmemSize(void)
{
	return add_size(mul_size(max_aio_in_progress, sizeof(PgAioInProgress)),
					offsetof(PgAioCtl, in_progress_io));
}

void
AioShmemInit(void)
{
	bool		found;

	aio_ctl = (PgAioCtl *)
		ShmemInitStruct("PgAio", AioShmemSize(), &found);

	if (!found)
	{
		memset(aio_ctl, 0, AioShmemSize());

		dlist_init(&aio_ctl->unused_ios);
		pg_atomic_init_u32(&aio_ctl->inflight, 0);
		pg_atomic_init_u32(&aio_ctl->outstanding, 0);

		for (int i = 0; i < max_aio_in_progress; i++)
		{
			PgAioInProgress *io = &aio_ctl->in_progress_io[i];

			ConditionVariableInit(&io->cv);
			dlist_push_head(&aio_ctl->unused_ios,
							&io->node);
			io->flags = PGAIOIP_IDLE;
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

	// XXX: could create a local queue here.
}

void
pgaio_postmaster_child_init(void)
{
	/* no locking needed here, only affects this process */
	io_uring_ring_dontfork(&aio_ctl->shared_ring);
	dlist_init(&local_pending_requests);
	num_local_pending_requests = 0;
	num_local_reaped = 0;

	// XXX: could create a local queue here.
}

void
pgaio_at_abort(void)
{
	if (num_local_reaped > 0)
	{
		elog(LOG, "at abort with %d pending", num_local_reaped);

		pgaio_complete_ios(/* in_error = */ true);
	}

	pgaio_submit_pending(true);
}

static void  __attribute__((noinline))
pgaio_complete_ios(bool in_error)
{
	int num_complete = 0;
	int num_reissued = 0;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	/* call all callbacks, without holding lock */
	for (int i = 0; i < num_local_reaped; i++)
	{
		PgAioInProgress *io = local_reaped_ios[i];

		Assert(io->flags & PGAIOIP_IN_USE);

		if (!(io->flags & PGAIOIP_DONE))
		{
			PgAioCompletedCB cb;
			bool done;

			cb = completion_callbacks[io->type];
			done = cb(io);

			if (done)
			{
				io->flags |= PGAIOIP_DONE;
				num_complete++;
			}
			else
			{
				num_reissued++;
			}

			/* signal state change */
			ConditionVariableBroadcast(&io->cv);
		}
		else
		{
			Assert(in_error);
		}
	}

	START_CRIT_SECTION();

	/* and recycle io entries */
	if (num_complete > 0)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		for (int i = 0; i < num_local_reaped; i++)
		{
			PgAioInProgress *io = local_reaped_ios[i];

			if (io->flags & PGAIOIP_DONE)
				pgaio_put_io_locked(io);
		}
		LWLockRelease(SharedAIOCtlLock);
	}

	num_local_reaped = 0;

	END_CRIT_SECTION();

	/* if any IOs weren't fully done, re-submit them */
	if (num_reissued)
		pgaio_submit_pending(false);
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
	Assert(num_local_reaped == 0);

	START_CRIT_SECTION();

	if (io_uring_cq_ready(ring))
	{
		processed = num_local_reaped =
			io_uring_peek_batch_cqe(ring,
									local_reaped_cqes,
									PGAIO_MAX_LOCAL_REAPED);

		pgaio_complete_cqes(ring,
							local_reaped_ios,
							local_reaped_cqes,
							num_local_reaped);
	}

	LWLockRelease(SharedAIOCompletionLock);

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


void  __attribute__((noinline))
pgaio_drain_outstanding(void)
{
	uint32 inflight;

	Assert(num_local_reaped == 0);
	Assert(!LWLockHeldByMe(SharedAIOCompletionLock));

	pgaio_submit_pending(false);
	pgaio_drain(&aio_ctl->shared_ring, false);

	inflight = pg_atomic_read_u32(&aio_ctl->inflight);

	while (inflight > 0)
	{
		uint32 ready;

		LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);

		inflight = pg_atomic_read_u32(&aio_ctl->inflight);
		ready = io_uring_cq_ready(&aio_ctl->shared_ring);

		if (inflight > 0 &&	ready == 0)
		{
			int ret = __sys_io_uring_enter(aio_ctl->shared_ring.ring_fd,
										   0, 1,
										   IORING_ENTER_GETEVENTS, NULL);
			if (ret != 0 && errno != EINTR)
				elog(WARNING, "unexpected: %d/%s: %m", ret, strerror(-ret));

			pgaio_drain(&aio_ctl->shared_ring, true);
		}

		if (io_uring_cq_ready(&aio_ctl->shared_ring))
			pgaio_drain(&aio_ctl->shared_ring, true);

		LWLockRelease(SharedAIOCompletionLock);
	}
}

void  __attribute__((noinline))
pgaio_submit_pending(bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	struct io_uring_sqe *sqe[PGAIO_SUBMIT_BATCH_SIZE];
	int total_submitted = 0;

	/* FIXME: */
	if (!aio_ctl || dlist_is_empty(&local_pending_requests))
	{
		Assert(num_local_pending_requests == 0);
		return;
	}

	while (!dlist_is_empty(&local_pending_requests))
	{
		int nios = 0;
		int nsubmit = Min(num_local_pending_requests, PGAIO_SUBMIT_BATCH_SIZE);

		Assert(nsubmit != 0 && nsubmit <= num_local_pending_requests);
		LWLockAcquire(SharedAIOSubmissionLock, LW_EXCLUSIVE);

		for (int i = 0; i < nsubmit; i++)
		{
			dlist_node *node;

			sqe[nios] = io_uring_get_sqe(&aio_ctl->shared_ring);

			if (!sqe[nios])
				break;

			node = dlist_pop_head_node(&local_pending_requests);
			ios[nios] = dlist_container(PgAioInProgress, node, node);

			pgaio_sq_from_io(ios[nios], sqe[nios]);
			Assert(ios[nios]->flags & PGAIOIP_IN_USE);
			ios[nios]->flags |= PGAIOIP_INFLIGHT;

			pg_atomic_add_fetch_u32(&aio_ctl->inflight, 1);

			nios++;
			num_local_pending_requests--;
			total_submitted++;
		}

		if (nios > 0)
		{
			int ret;
	again:
			ret = io_uring_submit(&aio_ctl->shared_ring);

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
	elog(DEBUG2, "submitted %d", total_submitted);
#endif

	pgaio_backpressure(&aio_ctl->shared_ring, "submit_pending");
}

static void  __attribute__((noinline))
pgaio_backpressure(struct io_uring *ring, const char *loc)
{
	if (pg_atomic_read_u32(&aio_ctl->outstanding) > PGAIO_BACKPRESSURE_LIMIT &&
		io_uring_cq_ready(ring) > 0)
	{
		int total;
		int cqr_before = io_uring_cq_ready(ring);
		int inflight_before = pg_atomic_read_u32(&aio_ctl->inflight);
		int outstanding_before = pg_atomic_read_u32(&aio_ctl->outstanding);

		total = pgaio_drain(ring, false);

		elog(DEBUG3, "backpressure drain at %s: cqr before/after: %d/%d, inflight b/a: %d/%d, outstanding b/a: %d/%d, processed %d",
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

			waitfor = 1;
			ret = __sys_io_uring_enter(ring->ring_fd,
									   0, waitfor,
									   IORING_ENTER_GETEVENTS, NULL);
			if (ret < 0 && errno != EINTR)
				elog(WARNING, "enter failed: %d/%s", ret, strerror(-ret));

			cqr_after = io_uring_cq_ready(ring);
#if 1
			elog(DEBUG3, "backpressure wait at %s waited for %d, "
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

	if (pg_atomic_read_u32(&aio_ctl->outstanding) > 1024)
	{
		elog(WARNING, "something's up: %d outstanding! cq ready: %u sq space left: %d, sq ready: %d",
			 pg_atomic_read_u32(&aio_ctl->outstanding),
			 io_uring_cq_ready(ring),
			 io_uring_sq_space_left(ring),
			 io_uring_sq_ready(ring));
	}
}

void
pgaio_wait_for_io(PgAioInProgress *io)
{
	uint8 init_flags;

#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "waiting for %zu",
		 io - aio_ctl->in_progress_io);
#endif

	init_flags = *(volatile uint8*) &io->flags;

	if (!(init_flags & PGAIOIP_INFLIGHT) &&
		!(init_flags & (PGAIOIP_DONE | PGAIOIP_IDLE)))
	{
		pgaio_submit_pending(false);
	}

	while (true)
	{
		if (io->flags & (PGAIOIP_DONE | PGAIOIP_IDLE))
			break;

		pgaio_drain(&aio_ctl->shared_ring, false);

		if (io->flags & (PGAIOIP_DONE | PGAIOIP_IDLE))
			break;

		if (io->flags & PGAIOIP_INFLIGHT)
		{
			int ret;

			/* ensure we're going to get woken up */
			if (IsUnderPostmaster)
				ConditionVariablePrepareToSleep(&io->cv);
			PG_SETMASK(&BlockSig);
			ResetLatch(MyLatch);

			if (!(io->flags & PGAIOIP_INFLIGHT))
			{
				PG_SETMASK(&UnBlockSig);
				continue;
			}

#ifdef PGAIO_VERBOSE
			elog(DEBUG2, "sys enter %zu",
				 io - aio_ctl->in_progress_io);
#endif

			/* wait for one io to be completed */
			errno = 0;
			ret = __sys_io_uring_enter(aio_ctl->shared_ring.ring_fd,
									   0, 1,
									   IORING_ENTER_GETEVENTS, &UnBlockSig);
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

			if (!(io->flags & (PGAIOIP_DONE | PGAIOIP_IDLE)))
				ConditionVariableSleep(&io->cv, 0);

			if (IsUnderPostmaster)
				ConditionVariableCancelSleep();
		}
	}
}

static PgAioInProgress*  __attribute__((noinline))
pgaio_start_get_io(PgAioAction action)
{
	dlist_node *elem;
	PgAioInProgress *io;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));
	Assert(num_local_pending_requests < PGAIO_SUBMIT_BATCH_SIZE);

	/* FIXME: wait for an IO to complete if full */

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	while (unlikely(dlist_is_empty(&aio_ctl->unused_ios)))
	{
		LWLockRelease(SharedAIOCtlLock);
		elog(LOG, "needed to drain while getting IO");
		pgaio_drain(&aio_ctl->shared_ring, false);

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	}

	START_CRIT_SECTION();

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);
	pg_atomic_write_u32(&aio_ctl->outstanding, pg_atomic_read_u32(&aio_ctl->outstanding) + 1);

	LWLockRelease(SharedAIOCtlLock);

	io = dlist_container(PgAioInProgress, node, elem);

	Assert(!(io->flags & PGAIOIP_IN_USE));
	Assert(io->flags == PGAIOIP_IDLE);

	io->flags &= ~PGAIOIP_IDLE;
	io->flags |= PGAIOIP_IN_USE;
	io->refcount = 2; // one for this module, one for the user */
	io->type = action;
	io->initiatorProcIndex = MyProc->pgprocno;

	// FIXME: should this be done in end_get_io?
	dlist_push_tail(&local_pending_requests,
					&io->node);
	num_local_pending_requests++;

	return io;
}

static void  __attribute__((noinline))
pgaio_end_get_io(void)
{
	END_CRIT_SECTION();

	//pgaio_drain(&aio_ctl->shared_ring, false);

	if (num_local_pending_requests >= PGAIO_SUBMIT_BATCH_SIZE)
		pgaio_submit_pending(true);
	else
		pgaio_backpressure(&aio_ctl->shared_ring, "get_io");
}

static void __attribute__((noinline))
pgaio_put_io_locked(PgAioInProgress *io)
{
	Assert(LWLockHeldByMe(SharedAIOCtlLock));

	Assert(io->refcount > 0);
	io->refcount--;
	if (io->refcount > 0)
		return;

	Assert(io->flags & PGAIOIP_DONE);

	io->flags &= ~(PGAIOIP_IN_USE|PGAIOIP_DONE);
	io->flags |= PGAIOIP_IDLE;
	io->type = 0;
	io->initiatorProcIndex = INVALID_PGPROCNO;

	//pg_atomic_fetch_sub_u32(&aio_ctl->outstanding, 1);
	pg_atomic_write_u32(&aio_ctl->outstanding, pg_atomic_read_u32(&aio_ctl->outstanding) - 1);
	dlist_push_tail(&aio_ctl->unused_ios,
					&io->node);
}

void
pgaio_release(PgAioInProgress *io)
{
	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	pgaio_put_io_locked(io);
	LWLockRelease(SharedAIOCtlLock);
}

/* --------------------------------------------------------------------------------
 * io_uring related code
 * --------------------------------------------------------------------------------
 */

static void __attribute__((noinline))
pgaio_complete_cqes(struct io_uring *ring, PgAioInProgress **ios, struct io_uring_cqe **cqes, int ready)
{
	Assert(LWLockHeldByMe(SharedAIOCompletionLock));

	for (int i = 0; i < ready; i++)
	{
		struct io_uring_cqe *cqe = cqes[i];
		PgAioInProgress *io;

		ios[i] = io = io_uring_cqe_get_data(cqe);
		Assert(ios[i] != NULL);
		ios[i]->result = cqe->res;

		Assert(io->flags & PGAIOIP_INFLIGHT);
		Assert(io->flags & PGAIOIP_IN_USE);
		Assert(!(io->flags == PGAIOIP_IDLE));
		io->flags &= ~PGAIOIP_INFLIGHT;

		if (cqe->res < 0)
		{
			elog(WARNING, "cqe: u: %p s: %d/%s f: %u",
				 io_uring_cqe_get_data(cqe),
				 cqe->res,
				 cqe->res < 0 ? strerror(-cqe->res) : "",
				 cqe->flags);
		}

		io_uring_cqe_seen(ring, cqe);
		pg_atomic_sub_fetch_u32(&aio_ctl->inflight, 1);
	}
}


static void
pgaio_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe)
{
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
			io_uring_prep_readv(sqe,
								io->d.read_buffer.fd,
								&io->d.read_buffer.iovec,
								1,
								io->d.read_buffer.offset + io->d.read_buffer.already_done);
			//sqe->flags |= IOSQE_ASYNC;
			break;
		case PGAIO_WRITE_BUFFER:
			io_uring_prep_writev(sqe,
								 io->d.write_wal.fd,
								 &io->d.write_wal.iovec,
								 1,
								 io->d.write_wal.offset);
			if (io->d.write_wal.no_reorder)
				sqe->flags = IOSQE_IO_DRAIN;
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
			io_uring_prep_writev(sqe,
								 io->d.write_wal.fd,
								 &io->d.write_wal.iovec,
								 1,
								 io->d.write_wal.offset);
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

PgAioInProgress *
pgaio_start_flush_range(int fd, off_t offset, off_t nbytes)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_FLUSH_RANGE);

	io->d.flush_range.fd = fd;
	io->d.flush_range.offset = offset;
	io->d.flush_range.nbytes = nbytes;

	pgaio_end_get_io();

#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "start_flush_range %zu: %d, %llu, %llu",
		 io - aio_ctl->in_progress_io,
		 fd, (unsigned long long) offset, (unsigned long long) nbytes);
#endif

	return io;
}


PgAioInProgress *
pgaio_start_read_buffer(int fd, off_t offset, off_t nbytes, char* data, int buffno, int mode)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_READ_BUFFER);

	io->d.read_buffer.buf = buffno;
	io->d.read_buffer.mode = mode;
	io->d.read_buffer.fd = fd;
	io->d.read_buffer.offset = offset;
	io->d.read_buffer.already_done = 0;
	io->d.read_buffer.iovec.iov_base = data;
	io->d.read_buffer.iovec.iov_len = nbytes;

	{
		bool		localBuffer = BufferIsLocal(buffno);
		BufferDesc *bufHdr;

		if (localBuffer)
			bufHdr = GetLocalBufferDescriptor(-buffno - 1);
		else
			bufHdr = GetBufferDescriptor(buffno - 1);
		bufHdr->io_in_progress = io;
	}

	pgaio_end_get_io();

#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "start_buffer_read %zu:"
		 "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 buffno,
		 data);
#endif

	return io;
}

PgAioInProgress *
pgaio_start_write_buffer(int fd, off_t offset, off_t nbytes, char* data, int buffno)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_WRITE_BUFFER);

	io->d.write_buffer.buf = buffno;
	io->d.write_buffer.fd = fd;
	io->d.write_buffer.offset = offset;
	io->d.write_buffer.already_done = 0;
	io->d.write_buffer.iovec.iov_base = data;
	io->d.write_buffer.iovec.iov_len = nbytes;

	pgaio_end_get_io();

#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "start_buffer_write %zu:"
		 "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 buffno,
		 data);
#endif

	return io;
}

PgAioInProgress *
pgaio_start_write_wal(int fd, off_t offset, off_t nbytes, char *data, bool no_reorder)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_WRITE_WAL);

	io->d.write_wal.fd = fd;
	io->d.write_wal.offset = offset;
	io->d.write_wal.already_done = 0;
	io->d.write_wal.no_reorder = no_reorder;
	io->d.write_wal.iovec.iov_base = data;
	io->d.write_wal.iovec.iov_len = nbytes;

	pgaio_end_get_io();

#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "start_write_wal %zu:"
		 "fd %d, off: %llu, bytes: %llu, no_reorder: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 no_reorder,
		 data);
#endif

	return io;
}

PgAioInProgress *
pgaio_start_nop(void)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_NOP);
	pgaio_end_get_io();

	return io;
}

PgAioInProgress *
pgaio_start_fdatasync(int fd, bool barrier)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.barrier = barrier;
	io->d.fsync.datasync = true;
	pgaio_end_get_io();

	return io;
}

static bool
pgaio_complete_nop(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "completed nop");
#endif

	return true;
}

static bool
pgaio_complete_fsync(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "completed fsync");
#endif
	if (io->result != 0)
		elog(PANIC, "fsync needs better error handling");

	return true;
}

static bool
pgaio_complete_flush_range(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "completed flush_range: %zu, %s",
		 io - aio_ctl->in_progress_io,
		 io->result < 0 ? strerror(-io->result) : "ok");
#endif

	return true;
}

static int
reopen_buffered(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blockNum, uint32 *off)
{
	SMgrRelation reln = smgropen(rnode, backend);

	return smgrfd(reln, forkNum, blockNum, off);
}

static bool
pgaio_complete_read_buffer(PgAioInProgress *io)
{
	bool		failed = false;
	Buffer		buffer = io->d.read_buffer.buf;

#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "completed read_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.read_buffer.buf);
#endif

	if (io->result != io->d.read_buffer.iovec.iov_len)
	{
		bool		localBuffer = BufferIsLocal(buffer);
		BufferDesc *bufHdr;
		RelFileNode rnode;
		ForkNumber forkNum;
		BlockNumber blockNum;

		if (localBuffer)
			bufHdr = GetLocalBufferDescriptor(-buffer - 1);
		else
			bufHdr = GetBufferDescriptor(buffer - 1);

		rnode = bufHdr->tag.rnode;
		forkNum = bufHdr->tag.forkNum;
		blockNum = bufHdr->tag.blockNum;

		if (io->result < 0)
		{
			failed = true;

			if (io->result == EAGAIN || io->result == EINTR)
			{
				int fd;
				uint32 off;

				elog(WARNING, "need to implement retries");

				fd = reopen_buffered(rnode, InvalidBackendId /* FIXME */, forkNum, blockNum, &off);
			}
			else
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not read block %u in file \"%s\": %s",
								blockNum,
								relpathperm(rnode, forkNum),
								strerror(-io->result))));
			}
		}
		else
		{
			int fd;
			uint32 off;
			bool oldAllowInCrit = CurrentMemoryContext->allowInCritSection;

			//CurrentMemoryContext->allowInCritSection = true;

			failed = true;
			ereport(DEBUG1,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in file \"%s\": read only %d of %d bytes (init: %d, cur: %d)",
							blockNum,
							relpathperm(rnode, forkNum),
							io->result, BLCKSZ,
							io->initiatorProcIndex, MyProc->pgprocno)));

			fd = reopen_buffered(rnode, InvalidBackendId /* FIXME */, forkNum, blockNum, &off);
			io->d.read_buffer.fd = fd;
			io->d.read_buffer.already_done = io->result;
			io->d.read_buffer.iovec.iov_base = (char * ) io->d.read_buffer.iovec.iov_base + io->result;
			io->d.read_buffer.iovec.iov_len -= io->result;

			dlist_push_tail(&local_pending_requests,
							&io->node);
			num_local_pending_requests++;

			CurrentMemoryContext->allowInCritSection = oldAllowInCrit;

			return false;
		}
	}

	ReadBufferCompleteRead(io->d.read_buffer.buf, io->d.read_buffer.mode, failed);

	return true;
}

static bool
pgaio_complete_write_buffer(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "completed write_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.write_buffer.buf);
#endif

	return true;
}

static bool
pgaio_complete_write_wal(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG2, "completed write_wal: %zu, %d/%s, offset: %d, nbytes: %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 (int)io->d.write_wal.offset,
		 (int) io->d.write_wal.iovec.iov_len
		);
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
	else if (io->result != io->d.write_wal.iovec.iov_len)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: wroteonly %d of %d bytes",
						io->result, (int) io->d.write_wal.iovec.iov_len)));
	}

	/* FIXME: update xlog.c state */

	return true;
}

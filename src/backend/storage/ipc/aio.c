#include "postgres.h"

#include <fcntl.h>
#include <liburing.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "lib/ilist.h"
#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"


#define PGAIO_VERBOSE

#define PGAIO_SUBMIT_BATCH_SIZE 16
#define PGAIO_BACKPRESSURE_LIMIT 500
#define PGAIO_MAX_LOCAL_REAPED 32

typedef enum PgAioAction
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_INVALID = 0,

	PGAIO_NOP,
	PGAIO_FLUSH_RANGE,
	PGAIO_READ_BUFFER,
	PGAIO_WRITE_BUFFER
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
typedef void (*PgAioCompletedCB)(PgAioInProgress *io);

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
			off_t nbytes;
			off_t offset;
		} flush_range;

		struct
		{
			Buffer buf;
			int fd;
			int already_done;
			off_t offset;
			struct iovec iovec;
		} read_buffer;

		struct
		{
			Buffer buf;
			struct iovec iovec;
		} write_buffer;
	} d;
};

/* general pgaio helper functions */
static void pgaio_complete_ios(bool in_error);
static void pgaio_backpressure(struct io_uring *ring, const char *loc);
static PgAioInProgress* pgaio_start_get_io(PgAioAction action);
static void pgaio_end_get_io(void);

/* io completions */
/* FIXME: parts of these probably don't belong here */
static void pgaio_complete_nop(PgAioInProgress *io);
static void pgaio_complete_flush_range(PgAioInProgress *io);
static void pgaio_complete_read_buffer(PgAioInProgress *io);
static void pgaio_complete_write_buffer(PgAioInProgress *io);

/* io_uring related functions */
static void pgaio_put_io_locked(PgAioInProgress *io);
static void pgaio_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe);
static void pgaio_complete_cqes(struct io_uring *ring, PgAioInProgress **ios,
								struct io_uring_cqe **cqes, int ready);

static int __sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
								unsigned flags, sigset_t *sig);


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
	dlist_head inflight;

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
	int32 outstanding;

	PgAioInProgress in_progress_io[FLEXIBLE_ARRAY_MEMBER];
} PgAioCtl;



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
	[PGAIO_FLUSH_RANGE] = pgaio_complete_flush_range,
	[PGAIO_READ_BUFFER] = pgaio_complete_read_buffer,
	[PGAIO_WRITE_BUFFER] = pgaio_complete_write_buffer,
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
int num_local_pending_requests = 0;
static dlist_head local_pending_requests;

/*
 * Requests completions received from the kernel. These are in global
 * variables so we can continue processing, if a completion callback fails.
 */
int num_local_reaped = 0;
struct io_uring_cqe *local_reaped_cqes[PGAIO_MAX_LOCAL_REAPED];
PgAioInProgress *local_reaped_ios[PGAIO_MAX_LOCAL_REAPED];



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
		dlist_init(&aio_ctl->inflight);

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

	pgaio_submit_pending();
}



PgAioInProgress *
pgaio_start_flush_range(int fd, off_t offset, off_t nbytes)
{
	PgAioInProgress *io;
	//struct io_uring_sqe *sqe;

	io = pgaio_start_get_io(PGAIO_FLUSH_RANGE);

	io->d.flush_range.fd = fd;
	io->d.flush_range.offset = offset;
	io->d.flush_range.nbytes = nbytes;

	pgaio_end_get_io();

#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "start_flush_range %zu: %d, %llu, %llu",
		 io - aio_ctl->in_progress_io,
		 fd, (unsigned long long) offset, (unsigned long long) nbytes);
#endif

	return io;
}


PgAioInProgress *
pgaio_start_buffer_read(int fd, off_t offset, off_t nbytes, char* data, int buffno)
{
	PgAioInProgress *io;
	//struct io_uring_sqe *sqe;

	io = pgaio_start_get_io(PGAIO_READ_BUFFER);

	io->d.read_buffer.buf = buffno;
	io->d.read_buffer.fd = fd;
	io->d.read_buffer.offset = offset;
	io->d.read_buffer.already_done = 0;
	io->d.read_buffer.iovec.iov_base = data;
	io->d.read_buffer.iovec.iov_len = nbytes;

	pgaio_end_get_io();

#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "start_buffer_read %zu:"
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
pgaio_start_nop(void)
{
	PgAioInProgress *io;

	io = pgaio_start_get_io(PGAIO_NOP);
	pgaio_end_get_io();

	return io;
}


static void
pgaio_complete_ios(bool in_error)
{
	Assert(!LWLockHeldByMe(SharedAIOLock));

	/* call all callbacks, without holding lock */
	for (int i = 0; i < num_local_reaped; i++)
	{
		PgAioInProgress *io = local_reaped_ios[i];

		Assert(io->flags & PGAIOIP_IN_USE);

		if (!(io->flags & PGAIOIP_DONE))
		{
			PgAioCompletedCB cb;

			cb = completion_callbacks[io->type];
			cb(io);

			io->flags |= PGAIOIP_DONE;

			/* signal state change */
			ConditionVariableBroadcast(&io->cv);
		}
		else
		{
			Assert(in_error);
		}
	}

	/* and recycle io entries */
	START_CRIT_SECTION();
	LWLockAcquire(SharedAIOLock, LW_EXCLUSIVE);
	for (int i = 0; i < num_local_reaped; i++)
		pgaio_put_io_locked(local_reaped_ios[i]);
	num_local_reaped = 0;
	LWLockRelease(SharedAIOLock);
	END_CRIT_SECTION();
}

/*
 * This checks if there are completions to be processed, before unlocking the
 * ring.
 */
static int
pgaio_drain_and_unlock(struct io_uring *ring)
{
	Assert(LWLockHeldByMe(SharedAIOLock));
	Assert(num_local_reaped == 0);

	START_CRIT_SECTION();

	if (io_uring_cq_ready(ring))
	{
		num_local_reaped =
			io_uring_peek_batch_cqe(ring,
									local_reaped_cqes,
									PGAIO_MAX_LOCAL_REAPED);

		pgaio_complete_cqes(ring,
							local_reaped_ios,
							local_reaped_cqes,
							num_local_reaped);
	}

	LWLockRelease(SharedAIOLock);

	END_CRIT_SECTION();

	if (num_local_reaped > 0)
		pgaio_complete_ios(false);

	return num_local_reaped;
}

static void
pgaio_drain(struct io_uring *ring, bool already_locked)
{
	bool lock_held = already_locked;

	while (true)
	{
		uint32 ready = io_uring_cq_ready(ring);
		uint32 processed;

		if (ready == 0)
			break;

		if (!lock_held)
		{
			LWLockAcquire(SharedAIOLock, LW_EXCLUSIVE);
			lock_held = true;
		}

		processed = pgaio_drain_and_unlock(ring);
		lock_held = false;

		if (processed >= ready)
			break;
	}

	if (already_locked && !lock_held)
		LWLockAcquire(SharedAIOLock, LW_EXCLUSIVE);
}

void
pgaio_drain_shared(void)
{
	pgaio_drain(&aio_ctl->shared_ring, false);
}

void
pgaio_drain_outstanding(void)
{
	int outstanding;

	Assert(!LWLockHeldByMe(SharedAIOLock));

	pgaio_submit_pending();
	pgaio_drain(&aio_ctl->shared_ring, false);

	outstanding = aio_ctl->outstanding;

	while (outstanding > 0)
	{
		LWLockAcquire(SharedAIOLock, LW_EXCLUSIVE);

		outstanding = aio_ctl->outstanding;

		if (!dlist_is_empty(&aio_ctl->inflight) &&
			io_uring_cq_ready(&aio_ctl->shared_ring) == 0)
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

		LWLockRelease(SharedAIOLock);
	}
}

void
pgaio_submit_pending(void)
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
		LWLockAcquire(SharedAIOLock, LW_EXCLUSIVE);

		for (int i = 0; i < nsubmit; i++)
		{
			dlist_node *node;

			sqe[nios] = io_uring_get_sqe(&aio_ctl->shared_ring);

			if (!sqe[nios])
				break;

			node = dlist_pop_head_node(&local_pending_requests);
			ios[nios] = dlist_container(PgAioInProgress, node,node);

			pgaio_sq_from_io(ios[nios], sqe[nios]);
			Assert(ios[nios]->flags & PGAIOIP_IN_USE);
			ios[nios]->flags |= PGAIOIP_INFLIGHT;

			dlist_push_head(&aio_ctl->inflight,
							&ios[nios]->node);

			nios++;
			num_local_pending_requests--;
			total_submitted++;
		}

		if (nios > 0)
		{
			int ret = io_uring_submit(&aio_ctl->shared_ring);

	again:
			if (ret == -EINTR)
				goto again;

			if (ret < 0)
				elog(PANIC, "failed: %d/%s",
					 ret, strerror(-ret));
		}

		/* while still holding the lock, extract all CQs we can */
		pgaio_drain_and_unlock(&aio_ctl->shared_ring);
	}

#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "submitted %d", total_submitted);
#endif

	pgaio_backpressure(&aio_ctl->shared_ring, "submit_pending");
}

static void
pgaio_backpressure(struct io_uring *ring, const char *loc)
{
	if (aio_ctl->outstanding > PGAIO_BACKPRESSURE_LIMIT)
	{
		for (int i = 0; i < 1; i++)
		{
			int ret;
			int waitfor;
			int cqr_before;
			int cqr_after;

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
			elog(DEBUG1, "nonlock at %s for depth %d waited for %d got %d "
				 "cqr before %d after %d "
				 "space left: %d, sq ready: %d",
				 loc,
				 aio_ctl->outstanding, waitfor, ret, cqr_before,
				 io_uring_cq_ready(ring),
				 io_uring_sq_space_left(ring),
				 io_uring_sq_ready(ring));
#endif
			if (cqr_after)
				pgaio_drain(ring, false);
		}
	}

	if (aio_ctl->outstanding > 1024)
	{
		elog(WARNING, "something's up: %d outstanding! cq ready: %u sq space left: %d, sq ready: %d",
			 aio_ctl->outstanding,
			 io_uring_cq_ready(ring),
			 io_uring_sq_space_left(ring),
			 io_uring_sq_ready(ring));
	}
}

static PgAioInProgress*
pgaio_start_get_io(PgAioAction action)
{
	dlist_node *elem;
	PgAioInProgress *io;

	Assert(!LWLockHeldByMe(SharedAIOLock));
	Assert(num_local_pending_requests < PGAIO_SUBMIT_BATCH_SIZE);

	/* FIXME: wait for an IO to complete if full */

	LWLockAcquire(SharedAIOLock, LW_EXCLUSIVE);

	while (dlist_is_empty(&aio_ctl->unused_ios))
	{
		pgaio_drain(&aio_ctl->shared_ring, true);
		Assert(LWLockHeldByMe(SharedAIOLock));
	}

	START_CRIT_SECTION();

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);

	io = dlist_container(PgAioInProgress, node, elem);

	Assert(!(io->flags & PGAIOIP_IN_USE));
	Assert(io->flags == PGAIOIP_IDLE);

	io->flags &= ~PGAIOIP_IDLE;
	io->flags |= PGAIOIP_IN_USE;
	io->type = action;
	io->initiatorProcIndex = MyProc->pgprocno;

	aio_ctl->outstanding++;

	dlist_push_tail(&local_pending_requests,
					&io->node);
	num_local_pending_requests++;

	END_CRIT_SECTION();

	return io;
}

static void
pgaio_end_get_io(void)
{
	Assert(LWLockHeldByMe(SharedAIOLock));

	pgaio_drain_and_unlock(&aio_ctl->shared_ring);

	if (num_local_pending_requests >= PGAIO_SUBMIT_BATCH_SIZE)
		pgaio_submit_pending();
	else
		pgaio_backpressure(&aio_ctl->shared_ring, "get_io");
}

static void
pgaio_put_io_locked(PgAioInProgress *io)
{
	Assert(LWLockHeldByMe(SharedAIOLock));
	Assert(io->flags & PGAIOIP_DONE);

	io->flags &= ~(PGAIOIP_IN_USE|PGAIOIP_DONE);
	io->flags |= PGAIOIP_IDLE;
	io->type = 0;
	io->initiatorProcIndex = INVALID_PGPROCNO;

	aio_ctl->outstanding--;
	dlist_push_head(&aio_ctl->unused_ios,
					&io->node);
}

static void
pgaio_complete_nop(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "completed nop");
#endif
}

static void
pgaio_complete_flush_range(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "completed flush_range: %zu, %s",
		 io - aio_ctl->in_progress_io,
		 io->result < 0 ? strerror(-io->result) : "ok");
#endif
}

static void
pgaio_complete_read_buffer(PgAioInProgress *io)
{
	BufferDesc *bufHdr = GetBufferDescriptor(io->d.read_buffer.buf - 1);
	uint32		buf_state;
	bool		failed = false;
	RelFileNode rnode = bufHdr->tag.rnode;
	BlockNumber forkNum = bufHdr->tag.forkNum;
	BlockNumber blockNum = bufHdr->tag.blockNum;

#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "completed read_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.read_buffer.buf);
#endif

	if (io->result < 0)
	{
		if (io->result == EAGAIN || io->result == EINTR)
		{
			elog(WARNING, "need to implement retries");
		}

		failed = true;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not read block %u in file \"%s\": %s",
						blockNum,
						relpathperm(rnode, forkNum),
						strerror(-io->result))));
	}
	else if (io->result != BLCKSZ)
	{
		failed = true;
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
						blockNum,
						relpathperm(rnode, forkNum),
						io->result, BLCKSZ)));
	}

	/* FIXME: needs to be in bufmgr.c */
	{
		Block		bufBlock;
		RelFileNode rnode = bufHdr->tag.rnode;
		BlockNumber forkNum = bufHdr->tag.forkNum;
		BlockNumber blockNum = bufHdr->tag.blockNum;

		bufBlock = BufferGetBlock(io->d.read_buffer.buf);

		/* check for garbage data */
		if (!PageIsVerified((Page) bufBlock, blockNum))
		{
			failed = true;
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid page in block %u of relation %s",
							blockNum,
							relpathperm(rnode, forkNum))));
		}
	}

	buf_state = LockBufHdr(bufHdr);

	Assert(buf_state & BM_IO_IN_PROGRESS);

	buf_state &= ~(BM_IO_IN_PROGRESS | BM_IO_ERROR);
	if (failed)
		buf_state |= BM_IO_ERROR;
	else
		buf_state |= BM_VALID;

	buf_state -= BUF_REFCOUNT_ONE;

	UnlockBufHdr(bufHdr, buf_state);

	ConditionVariableBroadcast(BufferDescriptorGetIOCV(bufHdr));
}

static void
pgaio_complete_write_buffer(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG1, "completed write_buffer");
#endif
}


/* --------------------------------------------------------------------------------
 * io_uring related code
 * --------------------------------------------------------------------------------
 */

static void
pgaio_complete_cqes(struct io_uring *ring, PgAioInProgress **ios, struct io_uring_cqe **cqes, int ready)
{
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

		/* delete from PgAioCtl->inflight */
		dlist_delete(&io->node);
	}
	//io_uring_cq_advance(ring, ready);
}


static void
pgaio_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe)
{
	switch (io->type)
	{
		case PGAIO_READ_BUFFER:
			io_uring_prep_readv(sqe,
								io->d.read_buffer.fd,
								&io->d.read_buffer.iovec,
								1,
								io->d.read_buffer.offset);
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
		case PGAIO_WRITE_BUFFER:
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

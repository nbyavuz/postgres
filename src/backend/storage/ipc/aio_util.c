#include "postgres.h"

#include "storage/aio.h"
#include "miscadmin.h"

/* typedef is in header */
typedef struct oustanding_write
{
	PgAioInProgress *aio;
	bool in_progress;
	void *private_data;
} outstanding_write;

struct pg_streaming_write
{
	uint32 iodepth;

	uint32 head;
	uint32 inflight;

	void *private_data;
	//WritebackContext *wb_context;

	pg_streaming_write_completed on_completion;

	outstanding_write outstanding_writes[];
};

pg_streaming_write*
pg_streaming_write_alloc(uint32 iodepth, void *private_data, pg_streaming_write_completed on_completion)
{
	pg_streaming_write *psw;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);

	psw = palloc0(offsetof(pg_streaming_write, outstanding_writes)
				 + sizeof(outstanding_write) * iodepth);

	psw->iodepth = iodepth;
	psw->private_data = private_data;
	psw->on_completion = on_completion;

	return psw;
}

PgAioInProgress *
pg_streaming_write_get_io(pg_streaming_write *pgsw)
{
	uint32		off = (pgsw->head) % pgsw->iodepth;
	outstanding_write *this_write = &pgsw->outstanding_writes[off];

	if (this_write->in_progress)
	{
		pg_streaming_write_wait(pgsw, 1);
		pgaio_io_recycle(this_write->aio);
	}

	if (!this_write->aio)
	{
		this_write->aio = pgaio_io_get();
	}
	else
	{
		pgaio_io_recycle(this_write->aio);
	}

	Assert(!this_write->in_progress);

	return this_write->aio;
}

uint32
pg_streaming_write_inflight(pg_streaming_write *pgsw)
{
	return pgsw->inflight;
}

void
pg_streaming_write_write(pg_streaming_write *pgsw, PgAioInProgress *io, void *private_data)
{
	uint32		off = (pgsw->head++) % pgsw->iodepth;
	outstanding_write *this_write = &pgsw->outstanding_writes[off];

	Assert(this_write->aio && this_write->aio == io);
	Assert(!this_write->in_progress);

	this_write->private_data = private_data;
	this_write->in_progress = true;

	pgsw->inflight++;

	/*
	 * XXX: It'd make sense to trigger io submission more often.
	 */
}

void
pg_streaming_write_wait(pg_streaming_write *pgsw, uint32 wait_for)
{
	Assert(wait_for <= pgsw->iodepth);
	bool waited = false;

	for (uint32 i = 0; i < pgsw->iodepth; i++, wait_for--)
	{
		uint32 off = (i + pgsw->head) % pgsw->iodepth;
		outstanding_write *this_write = &pgsw->outstanding_writes[off];

		if (!this_write->in_progress)
			continue;

		if (wait_for > 0)
		{
			pgaio_wait_for_io(this_write->aio, true);
			waited = true;
		}
		else if (!pgaio_io_done(this_write->aio))
			break;

		pgaio_io_recycle(this_write->aio);
		this_write->in_progress = false;
		pgsw->inflight--;

		pgsw->on_completion(pgsw->private_data, this_write->private_data);
		this_write->private_data = NULL;
	}
}

void
pg_streaming_write_wait_all(pg_streaming_write *pgsw)
{
	if (pgsw->inflight > 0)
		pg_streaming_write_wait(pgsw, pgsw->iodepth);
	Assert(pgsw->inflight == 0);
}

void
pg_streaming_write_free(pg_streaming_write *pgsw)
{
	for (uint32 off = 0; off < pgsw->iodepth; off++)
	{
		outstanding_write *this_write = &pgsw->outstanding_writes[off];

		Assert(!this_write->in_progress);
		if (this_write->aio)
			pgaio_release(this_write->aio);
		this_write->aio = NULL;
	}

	pfree(pgsw);
}


typedef struct OutstandingRead
{
	PgAioInProgress *aio;
	bool in_progress;
	bool done;
	uintptr_t read_private;
} OutstandingRead;

struct PgStreamingRead
{
	uint32 iodepth;
	uintptr_t pgsr_private;
	PgStreamingReadDetermineNextCB determine_next_cb;
	PgStreamingReadRelease release_cb;

	uint32 current_window;

	uint32 end_at;
	uint32 scan_at;
	uint32 prefetch_at;

	OutstandingRead outstanding_reads[];
};

PgStreamingRead *
pg_streaming_read_alloc(uint32 iodepth, uintptr_t pgsr_private,
						PgStreamingReadDetermineNextCB determine_next_cb,
						PgStreamingReadRelease release_cb)
{
	PgStreamingRead *pgsr;

	pgsr = palloc0(offsetof(PgStreamingRead, outstanding_reads) +
				   sizeof(OutstandingRead) * iodepth);

	pgsr->iodepth = iodepth;
	pgsr->pgsr_private = pgsr_private;
	pgsr->end_at = -1;
	pgsr->determine_next_cb = determine_next_cb;
	pgsr->release_cb = release_cb;

	return pgsr;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	for (int i = 0; i < pgsr->iodepth; i++)
	{
		OutstandingRead *this_read = &pgsr->outstanding_reads[i];

		if (this_read->in_progress)
		{
			pgaio_wait_for_io(this_read->aio, true);
			this_read->in_progress = false;
			this_read->done = true;
		}

		if (this_read->done)
			pgsr->release_cb(pgsr->pgsr_private, this_read->read_private);

		if (this_read->aio)
		{
			pgaio_release(this_read->aio);
			this_read->aio = NULL;
		}
	}
}

static void
pg_streaming_read_prefetch_one(PgStreamingRead *pgsr)
{
	uint32 off = (pgsr->prefetch_at) % pgsr->iodepth;
	OutstandingRead *this_read = &pgsr->outstanding_reads[off];
	PgStreamingReadNextStatus status;

	Assert(!this_read->in_progress);
	Assert(!this_read->done);

	if (this_read->aio == NULL)
	{
		this_read->aio = pgaio_io_get();
	}
	else
	{
		pgaio_io_recycle(this_read->aio);
	}

#if 0
	ereport(DEBUG2,
			(errmsg("fetching into slot %d", off),
			 errhidestmt(true),
			 errhidecontext(true)));
#endif

	status = pgsr->determine_next_cb(pgsr->pgsr_private, this_read->aio, &this_read->read_private);

	if (status == PGSR_NEXT_END)
	{
		pgsr->end_at = pgsr->prefetch_at;
	}
	else if (status == PGSR_NEXT_NO_IO)
	{
		Assert(this_read->read_private != 0);
		this_read->done = true;
		this_read->in_progress = false;
		pgsr->prefetch_at++;
	}
	else
	{
		Assert(this_read->read_private != 0);
		this_read->done = false;
		this_read->in_progress = true;
		pgsr->prefetch_at++;
	}
}

static void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	uint32 min_issue;
	uint32 pending = 0;

	if (pgsr->end_at != -1)
		return;

	if (pgsr->current_window < pgsr->iodepth)
	{
		if (pgsr->current_window == 0)
			pgsr->current_window = 4;
		else
			pgsr->current_window *= 2;

		if (pgsr->current_window > pgsr->iodepth)
			pgsr->current_window = pgsr->iodepth;

		min_issue = 1;
	}
	else
	{
		min_issue = Min(pgsr->iodepth, 8);
	}

	if (pgsr->scan_at + pgsr->current_window < pgsr->prefetch_at + min_issue)
		return;

#if 0
	ereport(DEBUG2,
			errmsg("checking prefetch at scan_at: %d, prefetch_at: %d, window: %d, fetching %d",
				   pgsr->scan_at, pgsr->prefetch_at, pgsr->current_window,
				   (pgsr->scan_at + pgsr->current_window) - pgsr->prefetch_at),
			errhidestmt(true),
			errhidecontext(true));
#endif

	while(pgsr->prefetch_at < pgsr->scan_at + pgsr->current_window)
	{
		pg_streaming_read_prefetch_one(pgsr);
		pending++;

		if (pending >= 16)
		{
			pgaio_submit_pending(true);
			pending = 0;
		}

		if (pgsr->end_at != -1)
			return;
	}

	if (pending > 0)
		pgaio_submit_pending(true);
}

uintptr_t
pg_streaming_read_get_next(PgStreamingRead *pgsr)
{
	uint32 off = (pgsr->scan_at) % pgsr->iodepth;
	OutstandingRead *this_read = &pgsr->outstanding_reads[off];

	if (pgsr->scan_at == pgsr->end_at)
		return 0;

	//if (pgsr->prefetch_at == 0)
	pg_streaming_read_prefetch(pgsr);

	if (pgsr->scan_at == pgsr->end_at)
		return 0;

	Assert(pgsr->scan_at < pgsr->prefetch_at);
	Assert(this_read->aio);
	Assert(this_read->done || this_read->in_progress);

	if (!this_read->done)
	{
		pgaio_wait_for_io(this_read->aio, true);
		Assert(pgaio_io_success(this_read->aio));
		this_read->in_progress = false;
		this_read->done = true;
	}

	pgsr->scan_at++;
	this_read->done = false;

	//pg_streaming_read_prefetch(pgsr);

	return this_read->read_private;
}

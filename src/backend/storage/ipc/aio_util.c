#include "postgres.h"

#include "storage/aio.h"

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
	}

	if (!this_write->aio)
	{
		this_write->aio = pgaio_io_get();
	}
	else
	{
		pgaio_io_recycle(this_write->aio);
	}

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
pg_streaming_write_wait(pg_streaming_write *pgsw, uint32 count)
{
	Assert(count <= pgsw->iodepth);

	for (uint32 i = 0; i < count; i++)
	{
		uint32 off = (i + pgsw->head) % pgsw->iodepth;
		outstanding_write *this_write = &pgsw->outstanding_writes[off];

		if (!this_write->in_progress)
			continue;

		pgaio_wait_for_io(this_write->aio, true);
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


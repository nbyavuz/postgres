/*-------------------------------------------------------------------------
 *
 * aio.h
 *	  aio
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/aio.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AIO_H
#define AIO_H

#include "common/relpath.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilenode.h"

typedef struct PgAioInProgress PgAioInProgress;

/* initialization */
extern void pgaio_postmaster_init(void);
extern Size AioShmemSize(void);
extern void AioShmemInit(void);
extern void pgaio_postmaster_child_init(void);

extern void pgaio_at_abort(void);

/*
 * XXX: Add flags to the initiation functions that govern:
 * - whether PgAioInProgress will be released once the IO has successfully
 *   finished, even if done by another backend (e.g. useful when prefetching,
 *   without wanting to look at the concrete buffer, or when doing buffer
 *   writeback). In particular this'd also cause the buffer pin to be released
 *   upon completion.
 * - whether a failed request needs to be seen by the issuing backend. That's
 *   not needed e.g. for prefetches, but is for buffer writes by checkpointer.
 * - whether pending requests might be issued if sensible, or whether that's
 *   not allowed.
 *
 *
 * FIXME: Add indicator about which IO channel needs to be used (important
 * e.g. for buffer writes interleaved with WAL writes, for queue depth
 * management of checkpointer, for readahead)
 */
extern PgAioInProgress *pgaio_start_flush_range(int fd, off_t offset, off_t nbytes);
extern PgAioInProgress *pgaio_start_nop(void);
extern PgAioInProgress *pgaio_start_fsync(int fd);

struct BufferDesc;
extern PgAioInProgress *pgaio_start_read_buffer(int fd, off_t offset, off_t nbytes,
												char *data, int buffno, int mode);
extern PgAioInProgress *pgaio_start_write_buffer(int fd, off_t offset, off_t nbytes,
												char *data, int buffno);
extern PgAioInProgress *pgaio_start_write_wal(int fd, off_t offset, off_t nbytes,
											  char *data, bool no_reorder);

extern void pgaio_submit_pending(void);

extern void pgaio_drain_shared(void);
extern void pgaio_drain_outstanding(void);

#endif							/* AIO_H */

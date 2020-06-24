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


extern PgAioInProgress *pgaio_start_flush_range(int fd, off_t offset, off_t nbytes);
extern PgAioInProgress *pgaio_start_nop(void);
extern PgAioInProgress *pgaio_start_fsync(int fd);

struct BufferDesc;
extern PgAioInProgress *pgaio_start_buffer_read(int fd, off_t offset, off_t nbytes,
												char *data, int buffno);

extern void pgaio_submit_pending(void);

extern void pgaio_drain_shared(void);
extern void pgaio_drain_outstanding(void);

#endif							/* AIO_H */

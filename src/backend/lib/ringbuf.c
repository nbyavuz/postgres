/*-------------------------------------------------------------------------
 *
 * ringbuf.c

 *	  Single producer, multiple consumer ringbuffer where consumption is
 *	  obstruction-free (i.e. no progress guarantee, but a consumer that is
 *	  stopped will not block progress).
 *
 * Implemented by essentially using an optimistic lock on the read side.
 *
 * XXX: It'd be nice if we could modify this so there's variants for push/pop
 * that work for different concurrency scenarios. E.g. having spsc_push(),
 * spmc_push(), ... - that'd avoid having to use different interfaces for
 * different needs.
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/ringbuf.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/ringbuf.h"
#include "storage/proc.h"

static inline uint32
ringbuf_backendid(ringbuf *rb, uint32 pos)
{
	return pos & 0xffff0000;
}

uint32
ringbuf_elements(ringbuf *rb)
{
	uint32 read_off = ringbuf_pos(rb, pg_atomic_read_u32(&rb->read_state));
	uint32 write_off = ringbuf_pos(rb, rb->write_off);

	/* not wrapped around */
	if (read_off <= write_off)
	{
		return write_off - read_off;
	}

	/* wrapped around */
	return (rb->size - read_off) + write_off;
}

size_t
ringbuf_size(size_t nelems)
{
	Assert(nelems <= 0x0000FFFF);
	return sizeof(ringbuf) + sizeof(void *) * nelems;
}

/*
 * Memory needs to be externally allocated and be at least
 * ringbuf_size(nelems) large.
 */
ringbuf *
ringbuf_create(void *target, size_t nelems)
{
	ringbuf *rb = (ringbuf *) target;

	Assert(nelems <= 0x0000FFFF);

	memset(target, 0, ringbuf_size(nelems));

	rb->size = nelems;
	pg_atomic_init_u32(&rb->read_state, 0);
	rb->write_off = 0;

	return rb;
}

bool
ringbuf_push(ringbuf *rb, void *data)
{
	uint32 read_off = pg_atomic_read_u32(&rb->read_state);

	/*
	 * Check if full - can be outdated, but that's ok. New readers are just
	 * going to further consume elements, never cause the buffer to become
	 * full.
	 */
	if (ringbuf_pos(rb, read_off)
		== ringbuf_pos(rb, ringbuf_advance_pos(rb, rb->write_off)))
	{
		return false;
	}

	rb->elements[ringbuf_pos(rb, rb->write_off)] = data;

	/*
	 * The write adding the data needs to be visible before the corresponding
	 * increase of write_off is visible.
	 */
	pg_write_barrier();

	rb->write_off = ringbuf_advance_pos(rb, rb->write_off);

	return true;
}


bool
ringbuf_pop(ringbuf *rb, void **data)
{
	void *ret;
	uint32 mybackend = MyProc->backendId;

	Assert((mybackend & 0x0000ffff) == mybackend);

	while (true)
	{
		uint32 read_state = pg_atomic_read_u32(&rb->read_state);
		uint32 read_off = ringbuf_pos(rb, read_state);
		uint32 old_read_state = read_state;

		/* check if empty - can be outdated, but that's ok */
		if (read_off == ringbuf_pos(rb, rb->write_off))
			return false;

		/*
		 * Add our backend id to the position, to detect wrap around.
		 * XXX
		 *
		 * XXX: Skip if the ID already is ours. That's probably likely enough
		 * to warrant the additional branch.
		 */
		read_state = (read_state & 0x0000ffff) | mybackend << 16;

		/*
		 * Mix the reader position into the current read_off, otherwise
		 * unchanged. If the offset changed since, retry from start.
		 *
		 * NB: This also serves as the read barrier pairing with the write
		 * barrier in ringbuf_push().
		 */
		if (!pg_atomic_compare_exchange_u32(&rb->read_state, &old_read_state,
											read_state))
			continue;
		old_read_state = read_state; /* with backend id mixed in */

		/* finally read the data */
		ret = rb->elements[read_off];

		/* compute next offset */
		read_state = ringbuf_advance_pos(rb, read_state);

		if (pg_atomic_compare_exchange_u32(&rb->read_state, &old_read_state,
										   read_state))
			break;
	}

	*data = ret;

	return true;
}

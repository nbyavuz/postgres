/*
 * ringbuf.h
 *
 * Single writer.multiple reader lockless & obstruction free ringbuffer.
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * src/include/lib/ringbuf.h
 */
#ifndef RINGBUF_H
#define RINGBUF_H

#include "port/atomics.h"

typedef struct ringbuf
{
	uint32 size;

	/* 16 bit reader id, 16 bit offset */
	/* XXX: probably should be on separate cachelines */
	pg_atomic_uint32 read_state;
	uint32_t write_off;

	void *elements[FLEXIBLE_ARRAY_MEMBER];
} ringbuf;

size_t ringbuf_size(size_t nelems);

ringbuf *ringbuf_create(void *target, size_t nelems);

static inline uint32
ringbuf_pos(ringbuf *rb, uint32 pos)
{
	/*
	 * XXX: replacing rb->size with a bitmask op would avoid expensive
	 * divisions. Requiring a pow2 size seems ok.
	 */
	return (pos & 0x0000ffff) % rb->size;
}

/*
 * Compute the new offset, slightly complicated by the fact that we only want
 * to modify the lower 16 bits.
 */
static inline uint32
ringbuf_advance_pos(ringbuf *rb, uint32 pos)
{
	return ((ringbuf_pos(rb, pos) + 1) & 0x0000FFFF) | (pos & 0xFFFF0000);
}

static inline bool
ringbuf_empty(ringbuf *rb)
{
	uint32 read_state = pg_atomic_read_u32(&rb->read_state);

	return ringbuf_pos(rb, read_state) == ringbuf_pos(rb, rb->write_off);
}

static inline bool
ringbuf_full(ringbuf *rb)
{
	uint32 read_state = pg_atomic_read_u32(&rb->read_state);

	return ringbuf_pos(rb, read_state) ==
		ringbuf_pos(rb, ringbuf_advance_pos(rb, rb->write_off));
}

uint32 ringbuf_elements(ringbuf *rb);
bool ringbuf_push(ringbuf *rb, void *data);
bool ringbuf_pop(ringbuf *rb, void **data);

#endif

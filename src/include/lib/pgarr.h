/*-------------------------------------------------------------------------
 *
 * pgarr.h
 *	  array helpers
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/lib/pgarr.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGARR_H
#define PGARR_H

#define PGARR_STRUCT_NAME(elty) \
	CppConcat(ArrayOf, elty)

#define PGARR_STRUCT_BODY(elty) \
	uint32 size; \
	uint32 capacity; \
	elty *elementsp;
/* inline elements, if present, follow MAXALIGned */

/*
 * Define one type - not intended to be used externally - to allow to compute
 * the size of the struct etc.
 */
typedef struct PgArrBase
{
	PGARR_STRUCT_BODY(void)
} PgArrBase;


/*
 *
 */
#define PGARR_DEFINE_TYPE(elty) \
	typedef struct PGARR_STRUCT_NAME(elty) \
	{ \
		PGARR_STRUCT_BODY(elty) \
	} \
	PGARR_STRUCT_NAME(elty)

/*
 *
 */
#define PGARR_UNNAMED(elty) \
	struct \
	{ \
		PGARR_STRUCT_BODY(elty) \
	} \

/*
 *
 */
#define PGARR(elty) \
	struct PGARR_STRUCT_NAME(elty)

/*
 *
 */
#define pgarr_capacity(arr) \
	((arr)->capacity)

/*
 *
 */
static inline uint32
pgarr_helper_size(const void *arr)
{
	const PgArrBase *base = (PgArrBase *) arr;

	if (!base)
		return 0;
	else
		return base->size;
}

#define pgarr_size(arr) \
	pgarr_helper_size(arr)

#define pgarr_empty(arr) \
	(pgarr_helper_size(arr) == 0)

/*
 *
 */
#define pgarr_helper_inline_ptr(data) \
	( \
		(void *) ( \
			(char *) data + \
			MAXALIGN(sizeof(PgArrBase)) \
		) \
	)

#define PGARR_DEFAULT_ALLOC 8
#define PGARR_INLINE_LIMIT 128

static inline void *
pgarr_helper_alloc_inline(size_t elems_size,
						  uint32 capacity)
{
	size_t head_sz = MAXALIGN(sizeof(PgArrBase));
	size_t elements_sz = elems_size * capacity;
	void *data = palloc(head_sz + elements_sz);
	PgArrBase *base = (PgArrBase *) data;

	base->size = 0;
	base->capacity = capacity;
	base->elementsp = pgarr_helper_inline_ptr(data);

	return data;
}

static inline void *
pgarr_helper_alloc_outline(size_t elems_size,
						   uint32 capacity)
{
	size_t head_sz = MAXALIGN(sizeof(PgArrBase));
	size_t elements_sz = elems_size * capacity;
	void *data_head = palloc(head_sz);
	void *data_data = palloc(elements_sz);

	PgArrBase *base = (PgArrBase *) data_head;

	base->size = 0;
	base->capacity = capacity;
	base->elementsp = data_data;

	return data_head;
}

static inline void *
pgarr_helper_alloc(size_t elems_size,
				   uint32 capacity)
{
	if (MAXALIGN(sizeof(PgArrBase)) + elems_size * capacity <= PGARR_INLINE_LIMIT)
		return pgarr_helper_alloc_inline(elems_size,
										 capacity);
	else
		return pgarr_helper_alloc_outline(elems_size,
										  capacity);
}

/*
 * Allocate an array for type `element_tp`.
 *
 * Initially there is the capacity to hold PGARR_DEFAULT_ALLOC elements.
 */
#define pgarr_alloc(element_tp) \
	(PGARR(element_tp) *) pgarr_helper_alloc(\
		sizeof(element_tp), \
		PGARR_DEFAULT_ALLOC)

/*
 * Allocate an array for type `element_tp`, with the capacity to hold
 * `capacity` elements without reallocation.
 *
 * Whether the resulting array will consist out of one or two allocations is
 * determined whether the resulting array needs more than PGARR_INLINE_LIMIT
 * space.
 */
#define pgarr_alloc_capacity(element_tp, capacity) \
	(PGARR(element_tp) *) pgarr_helper_alloc(\
		sizeof(element_tp), \
		capacity)

/*
 * Allocate an array for type `element_tp`, with the capacity to hold
 * `capacity` elements without reallocation.
 *
 * The resulting array will always consist out of a single allocation, based
 * on the assumption that `capacity` will never be exceeded - otherwise space
 * is permanently wasted for `capacity` elements.
 */
#define pgarr_alloc_ro(element_tp, capacity) \
	PGARR(element_tp) *pgarr_helper_alloc_inline(\
		sizeof(element_tp), \
		capacity)

extern void pgarr_realloc(void *arr,
						  size_t elems_size,
						  uint64 newcapacity);

static inline void *
pgarr_helper_at(size_t elems_off, size_t elems_size, void *arr, size_t at)
{
	size_t nmemb PG_USED_FOR_ASSERTS_ONLY = *(size_t*) arr;
	char *elems = (*(char **) arr) + elems_off;

	Assert(at < nmemb);

	return elems + elems_size * at;
}

//#undef HAVE_TYPEOF

#ifdef HAVE_TYPEOF

/*
 * Return pointer to element at position `at`. Note that the pointer is only
 * guaranteed to be valid as long as long as the size of the array is not
 * changed.
 */
#define pgarr_at(arr, at) \
	({ \
		typeof(arr) _arr = (arr); \
		typeof(at) _at = (at); \
		Assert(_at < _arr->size); \
		Assert(_arr->size <= _arr->capacity); \
		&(_arr->elementsp)[_at]; \
	})
#else /* HAVE_TYPEOF */

/*
 * Return pointer to element at position `at`. Note that the pointer is only
 * guaranteed to be valid as long as long as the size of the array is not
 * changed.
 */
#define pgarr_at(arr, at) \
	&((arr)->elementsp)[(at)]

#endif /* HAVE_TYPEOF */

static inline void
pgarr_helper_reserve_realloc(void *arr,
							 size_t elems_size,
							 uint64 add)
{
	PgArrBase *base = (PgArrBase *) arr;
	uint64 capacity_new;
	uint64 capacity_required;


	/*
	 * If the new size is bigger than the max, bail out.
	 *
	 * FIXME: define better handling
	 */
	if (add >= PG_UINT32_MAX)
		abort();
	capacity_required = (uint64) base->size + add;

	if (capacity_required > PG_UINT32_MAX)
		abort();

	/* double capacity until the additial elements fit */
	capacity_new = Max((uint64) base->size * 2, 8);
	while (capacity_new < capacity_required)
		capacity_new *= 2;

	/*
	 * By doubling we might have exceeded PG_UINT32_MAX, even if the required
	 * elements are <= PG_UINT32_MAX;
	 */
	if (capacity_new > PG_UINT32_MAX)
		capacity_new = PG_UINT32_MAX;

	pgarr_realloc(arr, elems_size, capacity_new);
}

static inline void
pgarr_helper_append(void *arr,
					 size_t elems_size)
{
	PgArrBase *base = (PgArrBase *) arr;

	if (unlikely(base->size + 1 >= base->capacity))
		pgarr_helper_reserve_realloc(arr, elems_size, 1);

	Assert(base->size + 1 < base->capacity);
}

static inline void
pgarr_helper_reserve(void *arr,
					 size_t elems_size,
					 size_t add)
{
	PgArrBase *base = (PgArrBase *) arr;

	if (unlikely(((uint64) base->size + add) > (uint64) base->capacity))
		pgarr_helper_reserve_realloc(arr, elems_size, add);
}

/*
 * Append element to array. The array's is grow in necessary (i.e. this
 * may trigger dynamic allocations).
 *
 * XXX: It'd be much nicer if we could avoid needing the element type here,
 * but without either
 * - relying on typeof(), to make a local copy of the array variable that can
 *   be referenced multiple times
 * - requiring that newel be of a form that allows to take its address - so we
 *   can memcpy it into place
 * I don't see a way to avoid multiple-evaluation hazards.
 */
#define pgarr_append(elty, arr, newel) \
	do { \
		PGARR_UNNAMED(elty) *_arr; \
		AssertVariableIsOfType(*(arr)->elementsp, elty); \
		\
		_arr = (void *) (arr); \
		\
		pgarr_helper_append(_arr, sizeof(elty)); \
		\
		_arr->elementsp[_arr->size++] = (newel); \
	} while (0)

/*
 * Append element to array, after previously having ensured enough space is
 * available using pgarr_reserve(arr, add).
 *
 * When appending multiple array elements, this is considerably cheaper than
 * pgarr_append(), as the required memory is allocated upfront.
 */
#define pgarr_append_reserved(elty, arr, newel) \
	do { \
		PGARR_UNNAMED(elty) *_arr; \
		AssertVariableIsOfType(*(arr)->elementsp, elty); \
		\
		_arr = (void *) arr; \
		\
		Assert(_arr->size < _arr->capacity); \
		\
		_arr->elementsp[_arr->size++] = newel; \
	} while (0)


static inline void *
pgarr_helper_clone(const void *srcarr, size_t elem_size)
{
	PgArrBase *srcbase = (PgArrBase *) srcarr;
	void *newarr;
	PgArrBase *newbase;

	newarr = pgarr_helper_alloc(
		elem_size,
		srcbase->size);
	newbase = (PgArrBase *) newarr;

	/* copy over elements */
	memcpy(newbase->elementsp,
		   srcbase->elementsp,
		   elem_size * srcbase->size);
	newbase->size = srcbase->size;

	return newbase;
}

#define pgarr_clone_unnamed(elty, srcarr) \
	(AssertVariableIsOfTypeMacro(*(srcarr)->elementsp, elty), \
	 PGARR_UNNAMED(elty)* pgarr_helper_clone(srcarr, sizeof(elty)))

#define pgarr_clone(elty, srcarr) \
	(AssertVariableIsOfTypeMacro(*(srcarr)->elementsp, elty), \
	 PGARR(elty)* pgarr_helper_clone(srcarr, sizeof(elty)))

#define pgarr_clone_raw(srcarr) \
	pgarr_helper_clone(srcarr, sizeof(*(srcarr)->elementsp))

#define pgarr_set_all(arr, n, val) \
	do { \
		PgArrBase *_arr = (PgArrBase *) (arr); \
		uint32 _n = (n); \
		\
		_arr->size = 0; \
		pgarr_helper_reserve(_arr, sizeof(*(arr)->elementsp), _n); \
		memset(_arr->elementsp, val, \
			   sizeof(*(arr)->elementsp) * _n); \
		_arr->size = n; \
	} while (0)


#define pgarr_copy(elty, dstarr, srcarr) \
	do { \
		PGARR_UNNAMED(elty)* _dstarr = (void *) dstarr; \
		PGARR_UNNAMED(elty)* _srcarr = (void *) srcarr; \
		\
		AssertVariableIsOfType(*(srcarr)->elementsp, elty); \
		AssertVariableIsOfType(*(dstarr)->elementsp, elty); \
		\
		_dstarr->size = 0; \
		pgarr_helper_reserve(_dstarr, sizeof(*dstarr->elementsp), _srcarr->size); \
		\
		for (int i = 0; i < _srcarr->size; i++) \
		{ \
			_dstarr->elementsp[i] = _srcarr->elementsp[i]; \
		} \
		_dstarr->size = _srcarr->size; \
	} while (0)


#define pgarr_data(arr) \
	(arr)->elementsp

typedef char* charstar;
typedef const char * constcharstar;

PGARR_DEFINE_TYPE(int);
PGARR_DEFINE_TYPE(bool);
PGARR_DEFINE_TYPE(Oid);
PGARR_DEFINE_TYPE(charstar);
PGARR_DEFINE_TYPE(constcharstar);

#endif /* PGARR_H */

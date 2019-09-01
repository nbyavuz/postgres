#include "postgres.h"

#include "lib/pgarr.h"


void
pgarr_realloc(void *arr,
			  size_t elems_size,
			  uint64 newcapacity)
{
	PgArrBase *base = (PgArrBase *) arr;

	Assert(newcapacity < PG_UINT32_MAX);

	if (base->size > newcapacity)
		base->size = newcapacity;

	if (base->elementsp == NULL)
	{
		base->elementsp = palloc(elems_size * newcapacity);
	}
	else if (base->elementsp == pgarr_helper_inline_ptr(arr))
	{
		void *newelementsp = palloc(elems_size * newcapacity);
		memcpy(newelementsp,
			   base->elementsp,
			   base->size * elems_size);
	}
	else
	{
		base->elementsp = repalloc(base->elementsp,
								   elems_size * newcapacity);
	}

	base->capacity = newcapacity;
}

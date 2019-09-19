/*-------------------------------------------------------------------------
 *
 * stringinfo.c
 *
 * StringInfo provides an indefinitely-extensible string data type.  It can be
 * used to buffer either ordinary C strings (null-terminated text) or
 * arbitrary binary data.  All storage is allocated with palloc() (falling
 * back to malloc in frontend code).
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	  src/common/stringinfo.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef FRONTEND

#include "postgres.h"
#include "utils/memutils.h"

#else

#include "postgres_fe.h"

/* It's possible we could use a different value for this in frontend code */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

#endif

#include "lib/stringinfo.h"


/*
 * makeStringInfo
 *
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
StringInfo
makeStringInfo(void)
{
	StringInfo	res;

	res = (StringInfo) palloc(sizeof(StringInfoData));

	initStringInfo(res);

	return res;
}

/*
 * initStringInfo
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initStringInfo(StringInfo str)
{
	int			size = 1024;	/* initial default buffer size */

	str->data = (char *) palloc(size);
	str->maxlen = size;
	resetStringInfo(str);
}

/*
 * resetStringInfo
 *
 * Reset the StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 */
void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

/*
 * appendStringInfo
 *
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void
appendStringInfo(StringInfo str, const char *fmt,...)
{
	int			save_errno = errno;

	for (;;)
	{
		va_list		args;
		int			needed;

		/* Try to format the data. */
		errno = save_errno;
		va_start(args, fmt);
		needed = appendStringInfoVA(str, fmt, args);
		va_end(args);

		if (needed == 0)
			break;				/* success */

		/* Increase the buffer size and try again. */
		enlargeStringInfo(str, needed);
	}
}

/*
 * appendStringInfoVA
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to enlargeStringInfo() before trying again; see
 * appendStringInfo for standard usage pattern.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 *
 * XXX This API is ugly, but there seems no alternative given the C spec's
 * restrictions on what can portably be done with va_list arguments: you have
 * to redo va_start before you can rescan the argument list, and we can't do
 * that from here.
 */
int
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
{
	int			avail;
	size_t		nprinted;

	Assert(str != NULL);

	/*
	 * If there's hardly any space, don't bother trying, just fail to make the
	 * caller enlarge the buffer first.  We have to guess at how much to
	 * enlarge, since we're skipping the formatting work.
	 */
	avail = str->maxlen - str->len;
	if (avail < 16)
		return 32;

	nprinted = pvsnprintf(str->data + str->len, (size_t) avail, fmt, args);

	if (nprinted < (size_t) avail)
	{
		/* Success.  Note nprinted does not include trailing null. */
		str->len += (int) nprinted;
		return 0;
	}

	/* Restore the trailing null so that str is unmodified. */
	str->data[str->len] = '\0';

	/*
	 * Return pvsnprintf's estimate of the space needed.  (Although this is
	 * given as a size_t, we know it will fit in int because it's not more
	 * than MaxAllocSize.)
	 */
	return (int) nprinted;
}

/*
 * appendStringInfoSpaces
 *
 * Append the specified number of spaces to a buffer.
 */
void
appendStringInfoSpaces(StringInfo str, int count)
{
	if (count > 0)
	{
		/* Make more room if needed */
		enlargeStringInfo(str, count);

		/* OK, append the spaces */
		while (--count >= 0)
			str->data[str->len++] = ' ';
		str->data[str->len] = '\0';
	}
}

/*
 * appendBinaryStringInfoNT
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Does not ensure a trailing null-byte exists.
 */
void
appendBinaryStringInfoNT(StringInfo str, const char *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
}

/*
 * enlargeStringInfoImpl
 *
 * Make enough space for 'needed' more bytes ('needed' does not include the
 * terminating null). This is not for external consumption, it's only to be
 * called by enlargeStringInfo() when more space is actually needed (including
 * when we'd overflow the maximum size).
 *
 * As this normally shouldn't be the common case, mark as noinline, to avoid
 * including the function into the fastpath.
 */
pg_noinline void
enlargeStringInfoImpl(StringInfo str, int needed)
{
	int			newlen;

	/*
	 * Guard against out-of-range "needed" values.  Without this, we can get
	 * an overflow or infinite loop in the following.
	 */
#ifndef FRONTEND
	if (unlikely(needed < 0))				/* should not happen */
		elog(ERROR, "invalid string enlargement request size: %d", needed);
	if (unlikely(((Size) needed) >= (MaxAllocSize - (Size) str->len)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of memory"),
				 errdetail("Cannot enlarge string buffer containing %d bytes by %d more bytes.",
						   str->len, needed)));
#else
	if (unlikely(needed < 0))				/* should not happen */
	{
		fprintf(stderr, "invalid string enlargement request size: %d", needed);
		exit(EXIT_FAILURE);
	}
	if (unlikely(((Size) needed) >= (MaxAllocSize - (Size) str->len)))
	{
		fprintf(stderr,
				_("out of memory\n\nCannot enlarge string buffer containing %d bytes by %d more bytes."),
				str->len, needed);
		exit(EXIT_FAILURE);
	}
#endif

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= MaxAllocSize */

	/* should only be called when needed */
	Assert(needed > str->maxlen);

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	str->data = (char *) repalloc(str->data, newlen);

	str->maxlen = newlen;
}

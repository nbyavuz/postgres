/*-------------------------------------------------------------------------
 *
 * stringinfo.h
 *	  Declarations/definitions for "StringInfo" functions.
 *
 * StringInfo provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with palloc().
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/lib/stringinfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STRINGINFO_H
#define STRINGINFO_H


#include "common/int.h"
#include "common/string.h"
#include "common/shortest_dec.h"


/*-------------------------
 * StringInfoData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with palloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *		cursor	is initialized to zero by makeStringInfo or initStringInfo,
 *				but is not otherwise touched by the stringinfo.c routines.
 *				Some routines use it to scan through a StringInfo.
 *-------------------------
 */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;


/*------------------------
 * There are two ways to create a StringInfo object initially:
 *
 * StringInfo stringptr = makeStringInfo();
 *		Both the StringInfoData and the data buffer are palloc'd.
 *
 * StringInfoData string;
 * initStringInfo(&string);
 *		The data buffer is palloc'd but the StringInfoData is just local.
 *		This is the easiest approach for a StringInfo object that will
 *		only live as long as the current routine.
 *
 * To destroy a StringInfo, pfree() the data buffer, and then pfree() the
 * StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using StringInfo, and then
 * release the StringInfoData but return the data string itself to their
 * caller.  At that point the data string looks like a plain palloc'd
 * string.
 *-------------------------
 */

/*------------------------
 * makeStringInfo
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
extern StringInfo makeStringInfo(void);

/*------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void initStringInfo(StringInfo str);

/*------------------------
 * resetStringInfo
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 */
extern void resetStringInfo(StringInfo str);

/*------------------------
 * resetStringInfo
 *
 * Actually enlarge the string, only to be called by enlargeStringInfo().
 */
extern void enlargeStringInfoImpl(StringInfo str, int needed);

/*------------------------
 * enlargeStringInfo
 * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
static inline void
enlargeStringInfo(StringInfo str, int datalen)
{
	int res;

	if (unlikely(pg_add_s32_overflow(str->len, datalen, &res)) ||
		unlikely(res >= str->maxlen))
		enlargeStringInfoImpl(str, datalen);
}

/*------------------------
 * appendBinaryStringInfo
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Ensures that a trailing null byte is present.
 */
static inline void
appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data.  (Some callers are dealing with text but call this because
	 * their input isn't null-terminated.)
	 */
	str->data[str->len] = '\0';
}

/*------------------------
 * appendBinaryStringInfoNT
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Does not ensure a trailing null-byte exists.
 */
extern void appendBinaryStringInfoNT(StringInfo str,
									 const char *data, int datalen);

/*------------------------
 * appendStringInfo
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void appendStringInfo(StringInfo str, const char *fmt,...) pg_attribute_printf(2, 3);

/*------------------------
 * appendStringInfoVA
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to enlargeStringInfo() before trying again; see
 * appendStringInfo for standard usage pattern.
 */
extern int	appendStringInfoVA(StringInfo str, const char *fmt, va_list args) pg_attribute_printf(2, 0);

/*------------------------
 * appendStringInfoString
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
static inline void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, strlen(s));
}

/*------------------------
 * appendStringInfoChar
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
static inline void
appendStringInfoChar(StringInfo str, char ch)
{
	/* Make more room if needed */
	enlargeStringInfo(str, 1);

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}
/* backward compat for external code */
#define appendStringInfoCharMacro appendStringInfoChar

/*------------------------
 * appendStringInfoSpaces
 * Append a given number of spaces to str.
 */
extern void appendStringInfoSpaces(StringInfo str, int count);


static inline void
appendStringInfoInt32(StringInfo str, int32 val)
{
	char *after;

	/* Make more room if needed */
	enlargeStringInfo(str, MAXINT32LEN);

	after = pg_int32tostr_nn(str->data + str->len, val);
	str->len = after - str->data;
	str->data[str->len] = '\0';
}

static inline void
appendStringInfoInt64(StringInfo str, uint64 val)
{
	char *after;

	/* Make more room if needed */
	enlargeStringInfo(str, MAXINT64LEN);

	after = pg_int64tostr_nn(str->data + str->len, val);
	str->len = after - str->data;
	str->data[str->len] = '\0';
}

static inline void
appendStringInfoUInt32(StringInfo str, uint32 val)
{
	char *after;

	/* Make more room if needed */
	enlargeStringInfo(str, MAXINT32LEN);

	after = pg_uint32tostr_nn(str->data + str->len, val);
	str->len = after - str->data;
	str->data[str->len] = '\0';
}

static inline void
appendStringInfoUInt64(StringInfo str, uint64 val)
{
	char *after;

	/* Make more room if needed */
	enlargeStringInfo(str, MAXINT64LEN);

	after = pg_uint64tostr_nn(str->data + str->len, val);
	str->len = after - str->data;
	str->data[str->len] = '\0';
}

static inline void
appendStringInfoFloat(StringInfo str, float val)
{
	int len;

	/* Make more room if needed */
	enlargeStringInfo(str, FLOAT_SHORTEST_DECIMAL_LEN);

	len = float_to_shortest_decimal_buf(val, str->data + str->len);
	str->len += len;
	Assert(str->data[str->len] == '\0');
}

static inline void
appendStringInfoDouble(StringInfo str, double val)
{
	int len;

	/* Make more room if needed */
	enlargeStringInfo(str, DOUBLE_SHORTEST_DECIMAL_LEN);

	len = double_to_shortest_decimal_buf(val, str->data + str->len);
	str->len += len;
	Assert(str->data[str->len] == '\0');
}


#endif							/* STRINGINFO_H */

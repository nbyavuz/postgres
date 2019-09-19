/*
 *	string.h
 *		string handling helpers
 *
 *	Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/include/common/string.h
 */
#ifndef COMMON_STRING_H
#define COMMON_STRING_H

extern bool pg_str_endswith(const char *str, const char *end);
extern int	strtoint(const char *pg_restrict str, char **pg_restrict endptr,
					 int base);
extern void pg_clean_ascii(char *str);
extern int	pg_strip_crlf(char *str);

#define MAXINT16LEN		sizeof(CppAsString(PG_INT16_MIN))
#define MAXINT32LEN		sizeof(CppAsString(PG_INT32_MIN))
#define MAXINT64LEN		sizeof(CppAsString(PG_INT64_MIN))

extern char *pg_int16tostr_nn(char *str, int16 value);
extern char *pg_uint16tostr_nn(char *str, int16 value);
extern char *pg_int32tostr_nn(char *str, int32 value);
extern char *pg_uint32tostr_nn(char *str, uint32 value);
extern char *pg_int32tostr_nn_zeropad(char *str, int32 value, int32 minwidth);
extern char *pg_int64tostr_nn(char *str, int64 value);
extern char *pg_uint64tostr_nn(char *str, uint64 value);

/*
 * pg_intNNtostr: converts a signed NN-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINTNNLEN bytes, counting a leading sign and trailing NUL).
 */

static inline char *
pg_int16tostr(char *str, int16 value)
{
	char *end =	pg_int16tostr_nn(str, value);

	*end = '\0';

	return end;
}

static inline char *
pg_int32tostr(char *str, int32 value)
{
	char *end =	pg_int32tostr_nn(str, value);

	*end = '\0';

	return end;
}

static inline char *
pg_int64tostr(char *str, int64 value)
{
	char *end =	pg_int64tostr_nn(str, value);

	*end = '\0';

	return end;
}


/*
 * pg_uintNNtostr: converts a un signed NN-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINTNNLEN bytes, counting the trailing NUL).
 */

static inline char *
pg_uint16tostr(char *str, uint16 value)
{
	char *end =	pg_uint16tostr_nn(str, value);

	*end = '\0';

	return end;
}

static inline char *
pg_uint32tostr(char *str, uint32 value)
{
	char *end =	pg_uint32tostr_nn(str, value);

	*end = '\0';

	return end;
}

static inline char *
pg_uint64tostr(char *str, uint64 value)
{
	char *end =	pg_uint64tostr_nn(str, value);

	*end = '\0';

	return end;
}

#endif							/* COMMON_STRING_H */

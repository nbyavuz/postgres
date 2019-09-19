/*-------------------------------------------------------------------------
 *
 * string.c
 *		string handling helpers
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/string.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/string.h"


/*
 * Returns whether the string `str' has the postfix `end'.
 */
bool
pg_str_endswith(const char *str, const char *end)
{
	size_t		slen = strlen(str);
	size_t		elen = strlen(end);

	/* can't be a postfix if longer */
	if (elen > slen)
		return false;

	/* compare the end of the strings */
	str += slen - elen;
	return strcmp(str, end) == 0;
}


/*
 * strtoint --- just like strtol, but returns int not long
 */
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
	long		val;

	val = strtol(str, endptr, base);
	if (val != (int) val)
		errno = ERANGE;
	return (int) val;
}


/*
 * pg_clean_ascii -- Replace any non-ASCII chars with a '?' char
 *
 * Modifies the string passed in which must be '\0'-terminated.
 *
 * This function exists specifically to deal with filtering out
 * non-ASCII characters in a few places where the client can provide an almost
 * arbitrary string (and it isn't checked to ensure it's a valid username or
 * database name or similar) and we don't want to have control characters or other
 * things ending up in the log file where server admins might end up with a
 * messed up terminal when looking at them.
 *
 * In general, this function should NOT be used- instead, consider how to handle
 * the string without needing to filter out the non-ASCII characters.
 *
 * Ultimately, we'd like to improve the situation to not require stripping out
 * all non-ASCII but perform more intelligent filtering which would allow UTF or
 * similar, but it's unclear exactly what we should allow, so stick to ASCII only
 * for now.
 */
void
pg_clean_ascii(char *str)
{
	/* Only allow clean ASCII chars in the string */
	char	   *p;

	for (p = str; *p != '\0'; p++)
	{
		if (*p < 32 || *p > 126)
			*p = '?';
	}
}


/*
 * pg_strip_crlf -- Remove any trailing newline and carriage return
 *
 * Removes any trailing newline and carriage return characters (\r on
 * Windows) in the input string, zero-terminating it.
 *
 * The passed in string must be zero-terminated.  This function returns
 * the new length of the string.
 */
int
pg_strip_crlf(char *str)
{
	int			len = strlen(str);

	while (len > 0 && (str[len - 1] == '\n' ||
					   str[len - 1] == '\r'))
		str[--len] = '\0';

	return len;
}

/* see pg_int32tostr_nn */
char *
pg_int16tostr_nn(char *str, int16 value)
{
	/* It doesn't seem worth implementing this separately. */
	return pg_int32tostr_nn(str, (int32) value);
}

/*
 * pg_int32tostr_nn
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_int32tostr_nn(str, a);
 *	*str++ = ' ';
 *	str = pg_int32tostr_nn(str, b);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char *
pg_int32tostr_nn(char *str, int32 value)
{
	char	   *start;
	char	   *end;

	/*
	 * Handle negative numbers in a special way.  We can't just write a '-'
	 * prefix and reverse the sign as that would overflow for INT32_MIN.
	 */
	if (value < 0)
	{
		*str++ = '-';

		/* Mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int32		oldval = value;
			int32		remainder;

			value /= 10;
			remainder = oldval - value * 10;
			/* As above, we expect remainder to be negative. */
			*str++ = '0' - remainder;
		} while (value != 0);
	}
	else
	{
		/* Mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int32		oldval = value;
			int32		remainder;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + remainder;
		} while (value != 0);
	}

	/* Remember the end+1 and back up 'str' to the last character. */
	end = str--;

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;

		*start++ = *str;
		*str-- = swap;
	}

	return end;
}

/* see pg_int32tostr_nn, except for unsigned */
char *
pg_uint32tostr_nn(char *str, uint32 value)
{
	char	   *start;
	char	   *end;

	/* Mark the position we must reverse the string from. */
	start = str;

	/* Compute the result string backwards. */
	do
	{
		uint32		oldval = value;
		uint32		remainder;

		value /= 10;
		remainder = oldval - value * 10;
		*str++ = '0' + remainder;
	} while (value != 0);

	/* Remember the end+1 and back up 'str' to the last character. */
	end = str--;

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;

		*start++ = *str;
		*str-- = swap;
	}

	return end;
}

/* see pg_int32tostr_nn */
char *
pg_int64tostr_nn(char *str, int64 value)
{
	char	   *start;
	char	   *end;

	/*
	 * Handle negative numbers in a special way.  We can't just write a '-'
	 * prefix and reverse the sign as that would overflow for INT64_MIN.
	 */
	if (value < 0)
	{
		*str++ = '-';

		/* Mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int64		oldval = value;
			int64		remainder;

			value /= 10;
			remainder = oldval - value * 10;
			/* As above, we expect remainder to be negative. */
			*str++ = '0' - remainder;
		} while (value != 0);
	}
	else
	{
		/* Mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int64		oldval = value;
			int64		remainder;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + remainder;
		} while (value != 0);
	}

	/* Remember the end+1 and back up 'str' to the last character. */
	end = str--;

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;

		*start++ = *str;
		*str-- = swap;
	}

	return end;
}

/* see pg_int64tostr_nn, except for unsigned */
char *
pg_uint64tostr_nn(char *str, uint64 value)
{
	char	   *start;
	char	   *end;

	/* Mark the position we must reverse the string from. */
	start = str;

	/* Compute the result string backwards. */
	do
	{
		uint64		oldval = value;
		uint64		remainder;

		value /= 10;
		remainder = oldval - value * 10;
		*str++ = '0' + remainder;
	} while (value != 0);

	/* Remember the end+1 and back up 'str' to the last character. */
	end = str--;

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;

		*start++ = *str;
		*str-- = swap;
	}

	return end;
}

/*
 * pg_int32tostr_nn_zeropad
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *		'minwidth' specifies the minimum width of the result; any extra space
 *		is filled up by prefixing the number with zeros.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_int32tostr_nn_zeropad(str, hours, 2);
 *	*str++ = ':';
 *	str = pg_int32tostr_nn_zeropad(str, mins, 2);
 *	*str++ = ':';
 *	str = pg_int32tostr_nn_zeropad(str, secs, 2);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char *
pg_int32tostr_nn_zeropad(char *str, int32 value, int32 minwidth)
{
	char	   *start = str;
	char	   *end = &str[minwidth];
	int32		num = value;

	Assert(minwidth > 0);

	/*
	 * Handle negative numbers in a special way.  We can't just write a '-'
	 * prefix and reverse the sign as that would overflow for INT32_MIN.
	 */
	if (num < 0)
	{
		*start++ = '-';
		minwidth--;

		/*
		 * Build the number starting at the last digit.  Here remainder will
		 * be a negative number, so we must reverse the sign before adding '0'
		 * in order to get the correct ASCII digit.
		 */
		while (minwidth--)
		{
			int32		oldval = num;
			int32		remainder;

			num /= 10;
			remainder = oldval - num * 10;
			start[minwidth] = '0' - remainder;
		}
	}
	else
	{
		/* Build the number starting at the last digit */
		while (minwidth--)
		{
			int32		oldval = num;
			int32		remainder;

			num /= 10;
			remainder = oldval - num * 10;
			start[minwidth] = '0' + remainder;
		}
	}

	/*
	 * If minwidth was not high enough to fit the number then num won't have
	 * been divided down to zero.  We punt the problem to pg_int32tostr_nn(), which
	 * will generate a correct answer in the minimum valid width.
	 */
	if (num != 0)
		return pg_int32tostr_nn(str, value);

	/* Otherwise, return last output character + 1 */
	return end;
}

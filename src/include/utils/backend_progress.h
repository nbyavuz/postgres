/* ----------
 *	backend_progress.h
 *
 *	Command progress reporting infrastructure.
 *
 *	Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 *	src/include/utils/backend_progress.h
 * ----------
 */
#ifndef BACKEND_PROGRESS_H
#define BACKEND_PROGRESS_H


/* ----------
 * Command type for progress reporting purposes
 * ----------
 */
typedef enum ProgressCommandType
{
	PROGRESS_COMMAND_INVALID,
	PROGRESS_COMMAND_VACUUM,
	PROGRESS_COMMAND_ANALYZE,
	PROGRESS_COMMAND_CLUSTER,
	PROGRESS_COMMAND_CREATE_INDEX,
	PROGRESS_COMMAND_BASEBACKUP,
	PROGRESS_COMMAND_COPY
} ProgressCommandType;

#define PGSTAT_NUM_PROGRESS_PARAM	20


extern void pgstat_progress_start_command(ProgressCommandType cmdtype,
										  Oid relid);
extern void pgstat_progress_update_param(int index, int64 val);
extern void pgstat_progress_update_multi_param(int nparam, const int *index,
											   const int64 *val);
extern void pgstat_progress_end_command(void);


#endif /* BACKEND_PROGRESS_H */

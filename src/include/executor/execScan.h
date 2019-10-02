/*-------------------------------------------------------------------------
 *
 * execScan.h
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXEC_SCAN_H
#define EXEC_SCAN_H

#include "executor/execdesc.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/lockoptions.h"
#include "nodes/parsenodes.h"
#include "utils/memutils.h"

typedef void *(*ExecScanInitMtd) (ScanState *node);
typedef TupleTableSlot *(*ExecScanAccessMtd) (ScanState *node);
typedef bool (*ExecScanRecheckMtd) (ScanState *node, TupleTableSlot *slot);

extern TupleTableSlot *ExecScanEPQ(ScanState *node,
								   ExecScanInitMtd initMtd,
								   ExecScanAccessMtd accessMtd,
								   ExecScanRecheckMtd recheckMtd);
extern void ExecScanReScan(ScanState *node);


#define SCAN_FUNC_NAME(a,b) CppConcat(a,b)

/*
 * Generate functions suitable for PlanState.ExecProcNode, with custom
 * implementations for each combination of (qual/no qual, project/no project)
 * and one separate implementation for EPQ. The latter probably isn't in
 * enough hot paths to warrant having a separate implementation for each node.
 *
 * Normally the init_func (which may be NULL), next_func, recheck_func will be
 * static functions in the same translation unit that
 * INSTANTIATE_SCAN_FUNCTIONS() is used in, which allows the compiler to
 * optimize ExecScanNormal() for the specific case (e.g. removing branches,
 * and avoiding re-doing work for each next_func() call after which quals
 * fail).
 */
#define INSTANTIATE_SCAN_FUNCTIONS(name, init_func, next_func, recheck_func) \
	static TupleTableSlot * SCAN_FUNC_NAME(name, Plain)(PlanState *pstate) \
	{ \
		return ExecScanNormal((ScanState *) pstate, (ExecScanInitMtd) init_func, \
							  (ExecScanAccessMtd) next_func, false, false); \
	} \
	\
	static TupleTableSlot * SCAN_FUNC_NAME(name, Qual)(PlanState *pstate) \
	{ \
		return ExecScanNormal((ScanState *) pstate, (ExecScanInitMtd) init_func, \
							  (ExecScanAccessMtd) next_func, true, false); \
	} \
	\
	static TupleTableSlot * SCAN_FUNC_NAME(name, Proj)(PlanState *pstate) \
	{ \
		return ExecScanNormal((ScanState *) pstate, (ExecScanInitMtd) init_func,\
							  (ExecScanAccessMtd) next_func, false, true); \
	} \
	\
	static TupleTableSlot * SCAN_FUNC_NAME(name, QualProj)(PlanState *pstate) \
	{ \
		return ExecScanNormal((ScanState *) pstate, (ExecScanInitMtd) init_func, \
							  (ExecScanAccessMtd) next_func, true, true); \
	} \
	static TupleTableSlot * SCAN_FUNC_NAME(name, EPQ)(PlanState *pstate) \
	{ \
		return ExecScanEPQ((ScanState *) pstate, (ExecScanInitMtd) init_func, \
						   (ExecScanAccessMtd) next_func, \
						   (ExecScanRecheckMtd) recheck_func); \
	}

/*
 * Choose which scan function is appropriate to use. This needs to be done
 * after quals and projects are initialized - before that we don't know if
 * projects are going to be performed.
 */
#define CHOOSE_SCAN_FUNCTION(scan, estate, prefix) \
	do \
	{ \
		ScanState *choose_scanstate = scan; \
		EState *choose_estate = estate; \
		\
		if (choose_estate->es_epq_active != NULL) \
			choose_scanstate->ps.ExecProcNode = SCAN_FUNC_NAME(prefix, EPQ); \
		else if (!choose_scanstate->ps.qual && !choose_scanstate->ps.ps_ProjInfo) \
			choose_scanstate->ps.ExecProcNode = SCAN_FUNC_NAME(prefix, Plain); \
		else if (choose_scanstate->ps.qual && !choose_scanstate->ps.ps_ProjInfo) \
			choose_scanstate->ps.ExecProcNode = SCAN_FUNC_NAME(prefix, Qual); \
		else if (!choose_scanstate->ps.qual && choose_scanstate->ps.ps_ProjInfo) \
			choose_scanstate->ps.ExecProcNode = SCAN_FUNC_NAME(prefix, Proj); \
		else if (choose_scanstate->ps.qual && choose_scanstate->ps.ps_ProjInfo) \
			choose_scanstate->ps.ExecProcNode = SCAN_FUNC_NAME(prefix, QualProj); \
		else \
			Assert(false); \
	} while (0)


static pg_attribute_always_inline TupleTableSlot *
ExecScanNormal(ScanState *node,
			   ExecScanInitMtd initMtd,
			   ExecScanAccessMtd accessMtd,
			   bool do_qual,
			   bool do_proj)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	ExprState  *qual;
	ProjectionInfo *projInfo;

	if (initMtd)
		(*initMtd)(node);

	/*
	 * If we have neither a qual to check nor a projection to do, just skip
	 * all the overhead and return the raw scan tuple.
	 */
	if (!do_qual && !do_proj)
	{
		ResetExprContext(econtext);

		CHECK_FOR_INTERRUPTS();

		return (*accessMtd)(node);
	}

	if (do_qual)
		qual = node->ps.qual;
	else
		Assert(node->ps.qual == NULL);

	if (do_proj)
		projInfo = node->ps.ps_ProjInfo;
	else
		Assert(node->ps.ps_ProjInfo == NULL);

	for (;;)
	{
		TupleTableSlot *slot;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Reset per-tuple memory context to free any expression evaluation
		 * storage allocated in the previous tuple cycle.
		 */
		ResetExprContext(econtext);

		slot = (*accessMtd)(node);

		/*
		 * if the slot returned by the accessMtd contains NULL, then it means
		 * there is nothing more to scan so we just return an empty slot,
		 * being careful to use the projection result slot so it has correct
		 * tupleDesc.
		 */
		if (unlikely(TupIsNull(slot)))
		{
			if (do_proj)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			else
				return slot;
		}

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		if (!do_qual || ExecQual(qual, econtext))
		{
			/*
			 * Found a satisfactory scan tuple.
			 */

			if (do_proj)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it.
				 */
				return ExecProject(projInfo);
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so try again.
		 */
	}
}

#endif /* EXEC_SCAN_H */

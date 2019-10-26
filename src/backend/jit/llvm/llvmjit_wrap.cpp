/*-------------------------------------------------------------------------
 *
 * llvmjit_wrap.cpp
 *	  Parts of the LLVM interface not (yet) exposed to C.
 *
 * Copyright (c) 2016-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/llvm/llvmjit_wrap.cpp
 *
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "postgres.h"
}

/* Avoid macro clash with LLVM's C++ headers */
#undef Min

#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/Host.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <llvm-c/Transforms/PassManagerBuilder.h>

#include "jit/llvmjit.h"


/*
 * C-API extensions.
 */
#if defined(HAVE_DECL_LLVMGETHOSTCPUNAME) && !HAVE_DECL_LLVMGETHOSTCPUNAME
char *LLVMGetHostCPUName(void) {
	return strdup(llvm::sys::getHostCPUName().data());
}
#endif


#if defined(HAVE_DECL_LLVMGETHOSTCPUFEATURES) && !HAVE_DECL_LLVMGETHOSTCPUFEATURES
char *LLVMGetHostCPUFeatures(void) {
	llvm::SubtargetFeatures Features;
	llvm::StringMap<bool> HostFeatures;

	if (llvm::sys::getHostCPUFeatures(HostFeatures))
		for (auto &F : HostFeatures)
			Features.AddFeature(F.first(), F.second);

	return strdup(Features.getString().c_str());
}
#endif


#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm-c/Target.h"
#include "llvm-c/TargetMachine.h"
#include "llvm/Target/TargetMachine.h"


static inline llvm::PassManagerBuilder *
unwrap(LLVMPassManagerBuilderRef P) {
    return reinterpret_cast<llvm::PassManagerBuilder*>(P);
}

static inline LLVMPassManagerBuilderRef
wrap(llvm::PassManagerBuilder *P) {
	return reinterpret_cast<LLVMPassManagerBuilderRef>(P);
}

static inline llvm::TargetLibraryInfoImpl *
unwrap(LLVMTargetLibraryInfoRef P) {
	return reinterpret_cast<llvm::TargetLibraryInfoImpl*>(P);
}

static inline LLVMTargetLibraryInfoRef
wrap(const llvm::TargetLibraryInfoImpl *P) {
	llvm::TargetLibraryInfoImpl *X = const_cast<llvm::TargetLibraryInfoImpl*>(P);
	return reinterpret_cast<LLVMTargetLibraryInfoRef>(X);
}

static llvm::TargetMachine *unwrap(LLVMTargetMachineRef P) {
	return reinterpret_cast<llvm::TargetMachine *>(P);
}

static inline LLVMTargetMachineRef
wrap(const llvm::TargetMachine *P) {
	return reinterpret_cast<LLVMTargetMachineRef>(const_cast<llvm::TargetMachine *>(P));
}

void
LLVMPassManagerBuilderSetMergeFunctions(
	struct LLVMOpaquePassManagerBuilder *PMBR,
	bool value)
{
    unwrap(PMBR)->MergeFunctions = value;
}

LLVMTargetLibraryInfoRef
LLVMGetTargetLibraryInfo(LLVMTargetMachineRef T)
{
	llvm::TargetLibraryInfoImpl *TLI =
		new llvm::TargetLibraryInfoImpl(unwrap(T)->getTargetTriple());

	return wrap(TLI);
}

void
LLVMPassManagerBuilderUseLibraryInfo(LLVMPassManagerBuilderRef PMBR,
									 LLVMTargetLibraryInfoRef TLI) {
	unwrap(PMBR)->LibraryInfo = unwrap(TLI);
}

#include <llvm/Support/Timer.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/IR/PassTimingInfo.h>
#include <llvm/ADT/Statistic.h>
#include <llvm/Support/JSON.h>

void
LLVMEnableStatistics()
{
	llvm::TimePassesIsEnabled = true;
	llvm::EnableStatistics(false /* print at shutdown */);
}

void
LLVMPrintAllTimers(bool clear)
{
	std::string s;
	llvm::raw_string_ostream o(s);

	llvm::TimerGroup::printAll(o);
	if (clear)
		llvm::TimerGroup::clearAll();

	llvm::PrintStatistics(o);
	if (clear)
		llvm::ResetStatistics();

	ereport(LOG, (errmsg("statistics: %s", s.c_str())));
}

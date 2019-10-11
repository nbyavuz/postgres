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

#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/Host.h>

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

/* Avoid macro clash with LLVM's C++ headers */
#undef Min

#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm-c/Transforms/PassManagerBuilder.h"
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

void
LLVMPassManagerBuilderSetMergeFunctions(
	struct LLVMOpaquePassManagerBuilder *PMBR,
	bool value)
{
#if LLVM_VERSION_MAJOR >= 7
	unwrap(PMBR)->MergeFunctions = true;;
#endif
}

#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/nodes
#
# IDENTIFICATION
#    src/backend/nodes/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/nodes
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = nodeFuncs.o nodes.o list.o bitmapset.o tidbitmap.o \
       copyfuncs_new.o equalfuncs_new.o extensible.o \
       makefuncs.o nodeinfo_data.o outfuncs_new.o \
       readfuncs_new.o print.o params.o value.o

# node metadata generation
GENNODE_FILES = nodes/primnodes.h \
		nodes/pathnodes.h \
		nodes/plannodes.h \
		nodes/execnodes.h \
		nodes/memnodes.h \
		nodes/value.h \
		nodes/pg_list.h \
		nodes/extensible.h \
		nodes/parsenodes.h \
		nodes/replnodes.h \
		nodes/supportnodes.h \
		nodes/value.h \
		utils/rel.h

all: generated-node-data

# FIXME: If we want to support doing this during cross compilation,
# this'd need to be done using the host compiler
gennodes.o: override CFLAGS += $(LLVM_CFLAGS)
gennodes.o: override CPPFLAGS += $(LLVM_CPPFLAGS)
gennodes: override LDFLAGS += -lclang

gennodes: | submake-libpgport

generated-node-data: $(top_srcdir)/src/backend/nodes/nodeinfo_data.c

# metadata generation depends on the to be the processed headers
$(top_srcdir)/src/backend/nodes/nodeinfo_data.c: \
	$(addprefix $(top_srcdir)/src/include/, $(GENNODE_FILES))

# But also on some other headers
$(top_srcdir)/src/backend/nodes/nodeinfo_data.c: \
	$(top_srcdir)/src/backend/nodes/gennodes.c \
	$(top_srcdir)/src/include/nodes/nodeinfo.h

# Order only dependency on gennodes, so gennodes only needs to get
# only be built (and thus the LLVM dependency is only required in that
# case) when the node metadata is out of date.
#
# FIXME: proper error message when LLVM isn't available
#
# XXX: It'd be nicer if we'd had some more granular check whether this
# needs to be rebuilt, obviously there's plenty changes that wouldn't
# matter.
$(top_srcdir)/src/backend/nodes/nodeinfo_data.c: | gennodes
	./gennodes \
		--llvm-config $(LLVM_CONFIG) \
		--output $(top_srcdir)/src/backend/nodes/nodeinfo_data.c \
		$(GENNODE_FILES) \
		-- \
		$(CPPFLAGS) -Wno-ignored-attributes

include $(top_srcdir)/src/backend/common.mk

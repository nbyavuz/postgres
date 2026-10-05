#-------------------------------------------------------------------------
#
# Makefile for src
#
# Copyright (c) 1994, Regents of the University of California
#
# src/Makefile
#
#-------------------------------------------------------------------------

subdir = src
top_builddir = ..
include Makefile.global

SUBDIRS = \
	common \
	port \
	timezone \
	backend \
	backend/utils/mb/conversion_procs \
	backend/snowball \
	include \
	interfaces \
	backend/replication/libpqwalreceiver \
	backend/replication/pgoutput \
	backend/replication/pgrepack \
	fe_utils \
	bin \
	pl \
	makefiles \
	test/regress \
	test/isolation \
	test/perl

ifeq ($(with_llvm), yes)
SUBDIRS += backend/jit/llvm
endif

# There are too many interdependencies between the subdirectories, so
# don't attempt parallel make here.
.NOTPARALLEL:

$(recurse)

install: install-local

install-local: installdirs-local
	$(INSTALL_DATA) Makefile.global '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.global'
	$(INSTALL_DATA) Makefile.port '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.port'
	$(INSTALL_DATA) $(srcdir)/Makefile.shlib '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.shlib'
	$(INSTALL_DATA) $(srcdir)/nls-global.mk '$(DESTDIR)$(pgxsdir)/$(subdir)/nls-global.mk'
	$(INSTALL_DATA) $(srcdir)/nls-common.json '$(DESTDIR)$(pgxsdir)/$(subdir)/nls-common.json'
	$(INSTALL_DATA) $(srcdir)/tools/po/read-nls.pl '$(DESTDIR)$(pgxsdir)/$(subdir)/tools/po/read-nls.pl'

installdirs: installdirs-local

installdirs-local:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)/tools/po'

uninstall: uninstall-local

uninstall-local:
	rm -f $(addprefix '$(DESTDIR)$(pgxsdir)/$(subdir)'/, Makefile.global Makefile.port Makefile.shlib nls-global.mk nls-common.json tools/po/read-nls.pl)

clean:
	$(MAKE) -C test $@
	$(MAKE) -C tutorial NO_PGXS=1 $@
	$(MAKE) -C test/isolation $@
	$(MAKE) -C tools/pg_bsd_indent $@

distclean:
	$(MAKE) -C test $@
	$(MAKE) -C tutorial NO_PGXS=1 $@
	$(MAKE) -C test/isolation $@
	$(MAKE) -C tools/pg_bsd_indent $@
	rm -f Makefile.port Makefile.global


.PHONY: install-local installdirs-local uninstall-local

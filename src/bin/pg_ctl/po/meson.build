# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_ctl': {
  'directory': 'src/bin/pg_ctl',
  'sources': pg_ctl_sources + files(
    '../../../common/controldata_utils.c', '../../../common/exec.c',
    '../../../common/fe_memutils.c', '../../../common/wait_error.c', '../../../port/path.c'),
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_ctl-' + pg_version_major.to_string())]
endif

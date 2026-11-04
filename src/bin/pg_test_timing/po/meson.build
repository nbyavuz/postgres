# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_test_timing': {
  'directory': 'src/bin/pg_test_timing',
  'sources': pg_test_timing_sources + files('../../../common/fe_memutils.c'),
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_test_timing-' + pg_version_major.to_string())]
endif

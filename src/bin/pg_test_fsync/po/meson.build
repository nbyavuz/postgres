# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_test_fsync': {
  'directory': 'src/bin/pg_test_fsync',
  'sources': test_fsync_sources + files('../../../common/logging.c', '../../../common/fe_memutils.c'),
  'keywords': nls_frontend_keywords + ['die'],
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_test_fsync-' + pg_version_major.to_string())]
endif

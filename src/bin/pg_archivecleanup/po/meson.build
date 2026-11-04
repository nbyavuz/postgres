# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_archivecleanup': {
  'directory': 'src/bin/pg_archivecleanup',
  'sources': pg_archivecleanup_sources + files('../../../common/logging.c', '../../../common/fe_memutils.c'),
  'keywords': nls_frontend_keywords,
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_archivecleanup-' + pg_version_major.to_string())]
endif

# Copyright (c) 2024-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_walsummary': {
  'directory': 'src/bin/pg_walsummary',
  'sources': pg_walsummary_sources + files('../../../common/logging.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c', '../../../fe_utils/option_utils.c'),
  'keywords': nls_frontend_keywords,
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_walsummary-' + pg_version_major.to_string())]
endif

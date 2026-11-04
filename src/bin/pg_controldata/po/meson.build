# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_controldata': {
  'directory': 'src/bin/pg_controldata',
  'sources': pg_controldata_sources + files('../../../common/controldata_utils.c', '../../../common/fe_memutils.c'),
  'keywords': nls_frontend_keywords,
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_controldata-' + pg_version_major.to_string())]
endif

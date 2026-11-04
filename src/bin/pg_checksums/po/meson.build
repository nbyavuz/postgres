# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_checksums': {
  'directory': 'src/bin/pg_checksums',
  'sources': pg_checksums_sources + files(
    '../../../common/logging.c', '../../../common/controldata_utils.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c',
    '../../../fe_utils/option_utils.c', '../../../fe_utils/version.c'),
  'keywords': nls_frontend_keywords,
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_checksums-' + pg_version_major.to_string())]
endif

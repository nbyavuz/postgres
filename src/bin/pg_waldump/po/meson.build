# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_waldump': {
  'directory': 'src/bin/pg_waldump',
  'sources': pg_waldump_sources + files('../../../common/logging.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c'),
  'keywords': nls_frontend_keywords + ['report_invalid_record:2'],
  'flags': nls_frontend_flags + ['report_invalid_record:2:c-format'],
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_waldump-' + pg_version_major.to_string())]
endif

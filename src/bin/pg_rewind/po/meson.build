# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_rewind': {
  'directory': 'src/bin/pg_rewind',
  'sources': pg_rewind_sources + files(
    '../../../common/logging.c', '../../../common/controldata_utils.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c',
    '../../../common/percentrepl.c', '../../../common/restricted_token.c',
    '../../../fe_utils/archive.c', '../../../fe_utils/option_utils.c',
    '../../../fe_utils/recovery_gen.c', '../../../fe_utils/string_utils.c'),
  'keywords': nls_frontend_keywords + ['report_invalid_record:2'],
  'flags': nls_frontend_flags + ['report_invalid_record:2:c-format'],
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_rewind-' + pg_version_major.to_string())]
endif

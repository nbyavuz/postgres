# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_amcheck': {
  'directory': 'src/bin/pg_amcheck',
  'sources': pg_amcheck_sources + files(
    '../../../common/logging.c', '../../../common/fe_memutils.c',
    '../../../common/file_utils.c', '../../../common/username.c',
    '../../../fe_utils/cancel.c', '../../../fe_utils/connect_utils.c',
    '../../../fe_utils/option_utils.c', '../../../fe_utils/parallel_slot.c',
    '../../../fe_utils/query_utils.c', '../../../fe_utils/string_utils.c'),
  'keywords': nls_frontend_keywords + ['log_no_match'],
  'flags': nls_frontend_flags + ['log_no_match:1:c-format'],
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_amcheck-' + pg_version_major.to_string())]
endif

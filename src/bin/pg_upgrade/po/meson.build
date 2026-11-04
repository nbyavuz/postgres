# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_upgrade': {
  'directory': 'src/bin/pg_upgrade',
  'sources': pg_upgrade_sources + files(
    '../../../common/fe_memutils.c', '../../../common/file_utils.c',
    '../../../common/restricted_token.c', '../../../common/username.c',
    '../../../fe_utils/option_utils.c', '../../../fe_utils/string_utils.c', '../../../fe_utils/version.c'),
  'keywords': ['pg_fatal', 'pg_log:2', 'prep_status', 'prep_status_progress', 'report_status:2'],
  'flags': ['pg_fatal:1:c-format', 'pg_log:2:c-format', 'prep_status:1:c-format',
    'prep_status_progress:1:c-format', 'report_status:2:c-format'],
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_upgrade-' + pg_version_major.to_string())]
endif

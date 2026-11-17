# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_upgrade-' + pg_version_major.to_string())]

nls_catalogs += {'pg_upgrade': {
  'sources': pg_upgrade_nls_sources + [
    fe_memutils_c,
    file_utils_c,
    restricted_token_c,
    username_c,
    option_utils_c,
    string_utils_c,
    version_c,
  ],
  'keywords': [
    'pg_fatal', 'pg_log:2', 'prep_status', 'prep_status_progress', 'report_status:2',
  ],
  'flags': [
    'pg_fatal:1:c-format', 'pg_log:2:c-format', 'prep_status:1:c-format',
    'prep_status_progress:1:c-format', 'report_status:2:c-format',
  ],
  'po_dir': meson.current_source_dir(),
}}

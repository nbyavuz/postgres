# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_dump-' + pg_version_major.to_string())]

nls_catalogs += {'pg_dump': {
  'sources': pg_dump_nls_sources + nls_frontend_sources + [
    exec_c,
    fe_memutils_c,
    file_utils_c,
    wait_error_c,
    option_utils_c,
    string_utils_c,
  ] + files('../../../common/compression.c'),
  'keywords': nls_frontend_triggers + [
    'simple_prompt', 'ExecuteSqlCommand:3', 'warn_or_exit_horribly:2',
  ],
  'flags': nls_frontend_flags + ['warn_or_exit_horribly:2:c-format'],
  'po_dir': meson.current_source_dir(),
}}

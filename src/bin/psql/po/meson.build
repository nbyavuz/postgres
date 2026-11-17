# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('psql-' + pg_version_major.to_string())]

nls_catalogs += {'psql': {
  'sources': psql_nls_sources + [psqlscan] + nls_frontend_sources + [
    exec_c,
    fe_memutils_c,
    username_c,
    wait_error_c,
    cancel_c,
    string_utils_c,
  ] + files('../../../fe_utils/print.c'),
  'scanners': [psqlscanslash, psqlscan],
  'keywords': nls_frontend_triggers + [
    'HELP0', 'HELPN', 'N_', 'addQueryComment:2',
    'simple_prompt', 'simple_prompt_extended',
  ],
  'flags': nls_frontend_flags + ['HELPN:1:c-format'],
  'po_dir': meson.current_source_dir(),
}}

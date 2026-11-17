# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('initdb-' + pg_version_major.to_string())]

nls_catalogs += {'initdb': {
  'sources': initdb_nls_sources + nls_frontend_sources + [
    exec_c,
    fe_memutils_c,
    file_utils_c,
    restricted_token_c,
    username_c,
    wait_error_c,
    option_utils_c,
    string_utils_c,
  ] + files(
    '../../../common/pgfnames.c',
    '../../../common/rmtree.c',
    '../../../port/dirmod.c',
  ),
  'keywords': nls_frontend_triggers + ['simple_prompt'],
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

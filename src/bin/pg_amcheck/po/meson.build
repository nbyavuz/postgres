# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_amcheck-' + pg_version_major.to_string())]

nls_catalogs += {'pg_amcheck': {
  'sources': pg_amcheck_nls_sources + nls_frontend_sources + [
    fe_memutils_c,
    file_utils_c,
    username_c,
    cancel_c,
    option_utils_c,
    string_utils_c,
  ] + files(
    '../../../fe_utils/connect_utils.c',
    '../../../fe_utils/parallel_slot.c',
    '../../../fe_utils/query_utils.c',
  ),
  'keywords': nls_frontend_triggers + ['log_no_match'],
  'flags': nls_frontend_flags + ['log_no_match:1:c-format'],
  'po_dir': meson.current_source_dir(),
}}

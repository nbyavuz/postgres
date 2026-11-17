# Copyright (c) 2024-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_walsummary-' + pg_version_major.to_string())]

nls_catalogs += {'pg_walsummary': {
  'sources': pg_walsummary_nls_sources + nls_frontend_sources + [
    fe_memutils_c,
    file_utils_c,
    option_utils_c,
  ],
  'keywords': nls_frontend_triggers,
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

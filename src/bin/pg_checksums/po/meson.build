# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_checksums-' + pg_version_major.to_string())]

nls_catalogs += {'pg_checksums': {
  'sources': pg_checksums_nls_sources + nls_frontend_sources + [
    controldata_utils_c,
    fe_memutils_c,
    file_utils_c,
    option_utils_c,
    version_c,
  ],
  'keywords': nls_frontend_triggers,
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

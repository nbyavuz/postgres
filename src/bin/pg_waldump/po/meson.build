# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_waldump-' + pg_version_major.to_string())]

nls_catalogs += {'pg_waldump': {
  'sources': pg_waldump_nls_sources + nls_frontend_sources + [
    fe_memutils_c,
    file_utils_c,
  ],
  'keywords': nls_frontend_triggers + ['report_invalid_record:2'],
  'flags': nls_frontend_flags + ['report_invalid_record:2:c-format'],
  'po_dir': meson.current_source_dir(),
}}

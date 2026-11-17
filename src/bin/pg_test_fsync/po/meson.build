# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_test_fsync-' + pg_version_major.to_string())]

nls_catalogs += {'pg_test_fsync': {
  'sources': pg_test_fsync_nls_sources + nls_frontend_sources + fe_memutils_c,
  'keywords': nls_frontend_triggers + ['die'],
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

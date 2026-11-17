# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_test_timing-' + pg_version_major.to_string())]

nls_catalogs += {'pg_test_timing': {
  'sources': pg_test_timing_nls_sources + fe_memutils_c,
  'po_dir': meson.current_source_dir(),
}}

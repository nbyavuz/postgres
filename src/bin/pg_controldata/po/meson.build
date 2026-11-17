# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_controldata-' + pg_version_major.to_string())]

nls_catalogs += {'pg_controldata': {
  'sources': pg_controldata_nls_sources + controldata_utils_c + fe_memutils_c,
  'keywords': nls_frontend_triggers,
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

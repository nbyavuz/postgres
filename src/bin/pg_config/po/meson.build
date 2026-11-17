# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_config-' + pg_version_major.to_string())]

nls_catalogs += {'pg_config': {
  'sources': pg_config_nls_sources + [config_info_sources, exec_c, fe_memutils_c],
  'po_dir': meson.current_source_dir(),
}}

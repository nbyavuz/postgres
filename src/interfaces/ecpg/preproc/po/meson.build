# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('ecpg-' + pg_version_major.to_string())]

nls_catalogs += {'ecpg': {
  'sources': ecpg_nls_sources,
  'scanners': [pgc],
  'keywords': ['mmerror:3', 'mmfatal:2'],
  'flags': ['mmerror:3:c-format', 'mmfatal:2:c-format'],
  'po_dir': meson.current_source_dir(),
}}

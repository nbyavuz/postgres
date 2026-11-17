# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('ecpglib' + '6' + '-' + pg_version_major.to_string())]

nls_catalogs += {'ecpglib': {
  'sources': ecpglib_nls_sources,
  'keywords': ['ecpg_gettext'],
  'flags': ['ecpg_gettext:1:pass-c-format'],
  'po_dir': meson.current_source_dir(),
}}

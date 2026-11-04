# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'ecpglib': {
  'directory': 'src/interfaces/ecpg/ecpglib',
  'sources': ecpglib_sources,
  'keywords': ['ecpg_gettext'],
  'flags': ['ecpg_gettext:1:pass-c-format'],
}}

if libintl.found()
  nls_targets += [i18n.gettext('ecpglib' + '6' + '-' + pg_version_major.to_string())]
endif

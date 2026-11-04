# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'ecpg': {
  'directory': 'src/interfaces/ecpg/preproc',
  'sources': ecpg_sources + [preproc_y] + files('../pgc.l'),
  'keywords': ['mmerror:3', 'mmfatal:2'],
  'flags': ['mmerror:3:c-format', 'mmfatal:2:c-format'],
}}

if libintl.found()
  nls_targets += [i18n.gettext('ecpg-' + pg_version_major.to_string())]
endif

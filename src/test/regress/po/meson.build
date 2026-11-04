# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'postgresql-regress': {
  'directory': 'src/test/regress',
  'sources': files('../regress.c'),
  'keywords': nls_backend_keywords,
  'flags': nls_backend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('postgresql-regress-' + pg_version_major.to_string())]
endif

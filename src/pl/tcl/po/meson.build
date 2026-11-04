# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pltcl': {
  'directory': 'src/pl/tcl',
  'sources': pltcl_sources,
  'keywords': nls_backend_keywords,
  'flags': nls_backend_flags,
}}

if libintl.found() and tcl_dep.found()
  nls_targets += [i18n.gettext('pltcl-' + pg_version_major.to_string())]
endif

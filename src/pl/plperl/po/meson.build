# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'plperl': {
  'directory': 'src/pl/plperl',
  # XS actions can be extracted as C without requiring the Perl development kit.
  'sources': plperl_sources + files('../SPI.xs', '../Util.xs'),
  'keywords': nls_backend_keywords,
  'flags': nls_backend_flags,
}}

if libintl.found() and perl_dep.found()
  nls_targets += [i18n.gettext('plperl-' + pg_version_major.to_string())]
endif

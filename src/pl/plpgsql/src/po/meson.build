# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'plpgsql': {
  'directory': 'src/pl/plpgsql/src',
  'sources': plpgsql_sources + files('../pl_gram.y'),
  'keywords': nls_backend_keywords + ['yyerror:4', 'plpgsql_yyerror:4'],
  'flags': nls_backend_flags,
}}

if libintl.found()
  nls_targets += [i18n.gettext('plpgsql-' + pg_version_major.to_string())]
endif

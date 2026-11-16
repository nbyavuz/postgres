# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('plpgsql-' + pg_version_major.to_string())]

nls_catalogs += {'plpgsql': {
  'sources': plpgsql_nls_sources,
  'keywords': nls_backend_triggers + [
    'yyerror:4',
    'plpgsql_yyerror:4'
  ],
  'flags': nls_backend_flags,
  'po_dir': meson.current_source_dir(),
}}

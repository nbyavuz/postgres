# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'plpython': {
  'directory': 'src/pl/plpython',
  'sources': plpython_sources,
  'keywords': nls_backend_keywords + [
    'PLy_elog:2', 'PLy_exception_set:2', 'PLy_exception_set_plural:2,3'],
  'flags': nls_backend_flags + [
    'PLy_elog:2:c-format', 'PLy_exception_set:2:c-format',
    'PLy_exception_set_plural:2:c-format', 'PLy_exception_set_plural:3:c-format'],
}}

if libintl.found() and python3_dep.found()
  nls_targets += [i18n.gettext('plpython-' + pg_version_major.to_string())]
endif

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('plpython-' + pg_version_major.to_string())]

nls_catalogs += {'plpython': {
  'sources': plpython_nls_sources,
  'keywords': nls_backend_triggers + [
    'PLy_elog:2',
    'PLy_exception_set:2',
    'PLy_exception_set_plural:2,3',
  ],
  'flags': nls_backend_flags + [
    'PLy_elog:2:c-format',
    'PLy_exception_set:2:c-format',
    'PLy_exception_set_plural:2:c-format',
    'PLy_exception_set_plural:3:c-format',
  ],
  'po_dir': meson.current_source_dir(),
}}

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_ctl-' + pg_version_major.to_string())]

nls_catalogs += {'pg_ctl': {
  'sources': pg_ctl_nls_sources + [
    controldata_utils_c,
    exec_c,
    fe_memutils_c,
    wait_error_c,
  ] + files('../../../port/path.c'),
  'po_dir': meson.current_source_dir(),
}}

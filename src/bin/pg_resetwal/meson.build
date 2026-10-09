# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_resetwal_sources = files(
  'pg_resetwal.c',
)

pg_resetwal_po_sources = pg_resetwal_sources
if host_system == 'windows'
  pg_resetwal_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_resetwal',
    '--FILEDESC', 'pg_resetwal - reset PostgreSQL WAL log'])
endif

pg_resetwal = executable('pg_resetwal',
  pg_resetwal_sources,
  dependencies: [frontend_code],
  kwargs: default_bin_args,
)
bin_targets += pg_resetwal

tests += {
  'name': 'pg_resetwal',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
      't/002_corrupted.pl',
    ],
  },
}

po_catalogs += {'src/bin/pg_resetwal': {
  'name': 'pg_resetwal', 'frontend': true,
  'sources': pg_resetwal_po_sources +
    common_logging_source + common_controldata_utils_source + common_fe_memutils_source +
    common_file_utils_source + common_restricted_token_source +
    fe_utils_option_utils_source + fe_utils_version_source,
}}

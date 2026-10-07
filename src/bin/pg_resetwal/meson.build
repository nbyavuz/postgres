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
  'sources': pg_resetwal_po_sources + files(
    '../../common/logging.c', '../../common/controldata_utils.c', '../../common/fe_memutils.c',
    '../../common/file_utils.c', '../../common/restricted_token.c',
    '../../fe_utils/option_utils.c', '../../fe_utils/version.c'),
}}

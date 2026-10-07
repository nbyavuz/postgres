# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_checksums_sources = files(
  'pg_checksums.c',
)

pg_checksums_po_sources = pg_checksums_sources
if host_system == 'windows'
  pg_checksums_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_checksums',
    '--FILEDESC', 'pg_checksums - verify data checksums in an offline cluster',])
endif

pg_checksums = executable('pg_checksums',
  pg_checksums_sources,
  include_directories: [timezone_inc],
  dependencies: [frontend_code],
  kwargs: default_bin_args,
)
bin_targets += pg_checksums

tests += {
  'name': 'pg_checksums',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
      't/002_actions.pl',
    ],
  },
}

po_catalogs += {'src/bin/pg_checksums': {
  'name': 'pg_checksums', 'frontend': true,
  'sources': pg_checksums_po_sources + files(
    '../../common/logging.c', '../../common/controldata_utils.c', '../../common/fe_memutils.c',
    '../../common/file_utils.c', '../../fe_utils/option_utils.c', '../../fe_utils/version.c'),
}}

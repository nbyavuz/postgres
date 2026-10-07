# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_amcheck_sources = files(
  'pg_amcheck.c',
)

pg_amcheck_po_sources = pg_amcheck_sources
if host_system == 'windows'
  pg_amcheck_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_amcheck',
    '--FILEDESC', 'pg_amcheck - detect corruption within database relations',])
endif

pg_amcheck = executable('pg_amcheck',
  pg_amcheck_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
bin_targets += pg_amcheck

tests += {
  'name': 'pg_amcheck',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
      't/002_nonesuch.pl',
      't/003_check.pl',
      't/004_verify_heapam.pl',
      't/005_opclass_damage.pl',
    ],
  },
}

po_catalogs += {'src/bin/pg_amcheck': {
  'name': 'pg_amcheck', 'frontend': true,
  'sources': pg_amcheck_po_sources + files(
    '../../common/logging.c', '../../common/fe_memutils.c', '../../common/file_utils.c',
    '../../common/username.c', '../../fe_utils/cancel.c', '../../fe_utils/connect_utils.c',
    '../../fe_utils/option_utils.c', '../../fe_utils/parallel_slot.c',
    '../../fe_utils/query_utils.c', '../../fe_utils/string_utils.c'),
  'keywords': ['log_no_match'], 'flags': ['log_no_match:1:c-format'],
}}

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
  'sources': pg_amcheck_po_sources +
    common_logging_source + common_fe_memutils_source + common_file_utils_source +
    common_username_source + fe_utils_cancel_source + fe_utils_connect_utils_source +
    fe_utils_option_utils_source + fe_utils_parallel_slot_source +
    fe_utils_query_utils_source + fe_utils_string_utils_source,
  'keywords': ['log_no_match'], 'flags': ['log_no_match:1:c-format'],
}}

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_upgrade_sources = files(
  'check.c',
  'controldata.c',
  'dump.c',
  'exec.c',
  'file.c',
  'function.c',
  'info.c',
  'multixact_read_v18.c',
  'multixact_rewrite.c',
  'option.c',
  'parallel.c',
  'pg_upgrade.c',
  'relfilenumber.c',
  'server.c',
  'slru_io.c',
  'tablespace.c',
  'task.c',
  'util.c',
  'version.c',
)

pg_upgrade_po_sources = pg_upgrade_sources
if host_system == 'windows'
  pg_upgrade_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_upgrade',
    '--FILEDESC', 'pg_upgrade - an in-place binary upgrade utility'])
endif

pg_upgrade = executable('pg_upgrade',
  pg_upgrade_sources,
  c_pch: pch_postgres_fe_h,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
bin_targets += pg_upgrade

test_ext_sources = files(
    '../../test/modules/test_extensions/test_ext.c'
)

if host_system == 'windows'
  test_ext_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_ext',
    '--FILEDESC', 'test_ext - test C extension for pg_upgrade',])
endif

test_ext = shared_module('test_ext',
  test_ext_sources,
  kwargs: pg_test_mod_args,
)

tests += {
  'name': 'pg_upgrade',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'env': {
      'with_icu': icu.found() ? 'yes' : 'no',
      'TEST_EXT_LIB': test_ext.full_path(),
    },
    'tests': [
      't/001_basic.pl',
      't/002_pg_upgrade.pl',
      't/003_logical_slots.pl',
      't/004_subscription.pl',
      't/005_char_signedness.pl',
      't/006_transfer_modes.pl',
      't/007_multixact_conversion.pl',
      't/008_extension_control_path.pl',
    ],
    'deps': [test_ext],
    'test_kwargs': {'priority': 40}, # pg_upgrade tests are slow
  },
}

po_catalogs += {'src/bin/pg_upgrade': {
  'name': 'pg_upgrade',
  'sources': pg_upgrade_po_sources + files(
    '../../common/fe_memutils.c', '../../common/file_utils.c', '../../common/restricted_token.c',
    '../../common/username.c', '../../fe_utils/option_utils.c',
    '../../fe_utils/string_utils.c', '../../fe_utils/version.c'),
  'keywords': ['pg_fatal', 'pg_log:2', 'prep_status', 'prep_status_progress', 'report_status:2'],
  'flags': ['pg_fatal:1:c-format', 'pg_log:2:c-format', 'prep_status:1:c-format',
            'prep_status_progress:1:c-format', 'report_status:2:c-format'],
}}

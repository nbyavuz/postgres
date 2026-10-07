# Copyright (c) 2022-2026, PostgreSQL Global Development Group

scripts_common_sources = files('common.c')
scripts_po_sources = scripts_common_sources
scripts_common = static_library('libscripts_common',
  scripts_common_sources,
  dependencies: [frontend_code, libpq],
  kwargs: internal_lib_args,
)

binaries = [
  'createdb',
  'dropdb',
  'createuser',
  'dropuser',
  'clusterdb',
  'reindexdb',
  'pg_isready',
]

foreach binary : binaries
  binary_sources = files('@0@.c'.format(binary))
  scripts_po_sources += binary_sources

  if host_system == 'windows'
    binary_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
      '--NAME', binary,
      '--FILEDESC', '@0@ - PostgreSQL utility'.format(binary),])
  endif

  binary = executable(binary,
    binary_sources,
    link_with: [scripts_common],
    dependencies: [frontend_code, libpq],
    kwargs: default_bin_args,
  )
  bin_targets += binary
endforeach

vacuuming_common_sources = scripts_common_sources + files('vacuuming.c')
scripts_po_sources += vacuuming_common_sources
vacuuming_common = static_library('libvacuuming_common',
  vacuuming_common_sources,
  dependencies: [frontend_code, libpq],
  kwargs: internal_lib_args,
)

binaries = [
  'vacuumdb',
]
foreach binary : binaries
  binary_sources = files('@0@.c'.format(binary))
  scripts_po_sources += binary_sources

  if host_system == 'windows'
    binary_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
      '--NAME', binary,
      '--FILEDESC', '@0@ - PostgreSQL utility'.format(binary),])
  endif

  binary = executable(binary,
    binary_sources,
    link_with: [vacuuming_common],
    dependencies: [frontend_code, libpq],
    kwargs: default_bin_args,
  )
  bin_targets += binary
endforeach

tests += {
  'name': 'scripts',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'env': {'with_icu': icu.found() ? 'yes' : 'no'},
    'tests': [
      't/010_clusterdb.pl',
      't/011_clusterdb_all.pl',
      't/020_createdb.pl',
      't/040_createuser.pl',
      't/050_dropdb.pl',
      't/070_dropuser.pl',
      't/080_pg_isready.pl',
      't/090_reindexdb.pl',
      't/091_reindexdb_all.pl',
      't/100_vacuumdb.pl',
      't/101_vacuumdb_all.pl',
      't/102_vacuumdb_stages.pl',
      't/200_connstr.pl',
    ],
  },
}

po_catalogs += {'src/bin/scripts': {
  'name': 'pgscripts', 'frontend': true,
  'sources': scripts_po_sources + files(
    '../../common/logging.c', '../../common/fe_memutils.c', '../../common/file_utils.c',
    '../../common/username.c', '../../fe_utils/cancel.c', '../../fe_utils/connect_utils.c',
    '../../fe_utils/option_utils.c', '../../fe_utils/parallel_slot.c', '../../fe_utils/print.c',
    '../../fe_utils/query_utils.c', '../../fe_utils/string_utils.c'),
  'keywords': ['simple_prompt', 'yesno_prompt'],
}}

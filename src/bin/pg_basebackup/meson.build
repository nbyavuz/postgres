# Copyright (c) 2022-2026, PostgreSQL Global Development Group

common_sources = files(
  'astreamer_inject.c',
  'receivelog.c',
  'streamutil.c',
  'walmethods.c',
)

pg_basebackup_deps = [frontend_code, libpq, lz4, zlib, zstd]
pg_basebackup_common = static_library('libpg_basebackup_common',
  common_sources,
  dependencies: pg_basebackup_deps,
  kwargs: internal_lib_args,
)

pg_basebackup_sources = files(
  'pg_basebackup.c',
)
pg_basebackup_po_sources = common_sources + pg_basebackup_sources

if host_system == 'windows'
  pg_basebackup_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_basebackup',
    '--FILEDESC', 'pg_basebackup - streaming WAL and backup receivers',])
endif

pg_basebackup = executable('pg_basebackup',
  pg_basebackup_sources,
  link_with: [pg_basebackup_common],
  dependencies: pg_basebackup_deps,
  kwargs: default_bin_args,
)
bin_targets += pg_basebackup


pg_createsubscriber_sources = files(
  'pg_createsubscriber.c'
)
pg_basebackup_po_sources += pg_createsubscriber_sources

if host_system == 'windows'
  pg_createsubscriber_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
	'--NAME', 'pg_createsubscriber',
	'--FILEDESC', 'pg_createsubscriber - create a new logical replica from a standby server',])
endif

pg_createsubscriber = executable('pg_createsubscriber',
  pg_createsubscriber_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
bin_targets += pg_createsubscriber


pg_receivewal_sources = files(
  'pg_receivewal.c',
)
pg_basebackup_po_sources += pg_receivewal_sources

if host_system == 'windows'
  pg_receivewal_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_receivewal',
    '--FILEDESC', 'pg_receivewal - streaming WAL and backup receivers',])
endif

pg_receivewal = executable('pg_receivewal',
  pg_receivewal_sources,
  link_with: [pg_basebackup_common],
  dependencies: pg_basebackup_deps,
  kwargs: default_bin_args,
)
bin_targets += pg_receivewal


pg_recvlogical_sources = files(
  'pg_recvlogical.c',
)
pg_basebackup_po_sources += pg_recvlogical_sources

if host_system == 'windows'
  pg_recvlogical_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_recvlogical',
    '--FILEDESC', 'pg_recvlogical - streaming WAL and backup receivers',])
endif

pg_recvlogical = executable('pg_recvlogical',
  pg_recvlogical_sources,
  link_with: [pg_basebackup_common],
  dependencies: pg_basebackup_deps,
  kwargs: default_bin_args,
)
bin_targets += pg_recvlogical

tests += {
  'name': 'pg_basebackup',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'env': {'GZIP_PROGRAM': gzip.found() ? gzip.full_path() : '',
            'TAR': tar.found() ? tar.full_path() : '',
            'LZ4': program_lz4.found() ? program_lz4.full_path() : '',
    },
    'tests': [
      't/010_pg_basebackup.pl',
      't/011_in_place_tablespace.pl',
      't/020_pg_receivewal.pl',
      't/030_pg_recvlogical.pl',
      't/040_pg_createsubscriber.pl',
    ],
  },
}

po_catalogs += {'src/bin/pg_basebackup': {
  'name': 'pg_basebackup', 'frontend': true,
  'sources': pg_basebackup_po_sources + files(
    '../../common/logging.c', '../../common/compression.c', '../../common/controldata_utils.c',
    '../../common/fe_memutils.c', '../../common/file_utils.c', '../../common/restricted_token.c',
    '../../fe_utils/astreamer_file.c', '../../fe_utils/astreamer_gzip.c',
    '../../fe_utils/astreamer_lz4.c', '../../fe_utils/astreamer_tar.c',
    '../../fe_utils/astreamer_zstd.c', '../../fe_utils/option_utils.c',
    '../../fe_utils/recovery_gen.c', '../../fe_utils/string_utils.c', '../../fe_utils/version.c'),
  'keywords': ['simple_prompt'],
}}

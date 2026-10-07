# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_verifybackup_sources = files(
  'astreamer_verify.c',
  'pg_verifybackup.c'
)

pg_verifybackup_po_sources = pg_verifybackup_sources
if host_system == 'windows'
  pg_verifybackup_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_verifybackup',
    '--FILEDESC', 'pg_verifybackup - verify a backup against using a backup manifest'])
endif

pg_verifybackup = executable('pg_verifybackup',
  pg_verifybackup_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
bin_targets += pg_verifybackup

tests += {
  'name': 'pg_verifybackup',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'env': {'GZIP_PROGRAM': gzip.found() ? gzip.full_path() : '',
            'TAR': tar.found() ? tar.full_path() : '',
            'LZ4': program_lz4.found() ? program_lz4.full_path() : '',
            'ZSTD': program_zstd.found() ? program_zstd.full_path() : ''},
    'tests': [
      't/001_basic.pl',
      't/002_algorithm.pl',
      't/003_corruption.pl',
      't/004_options.pl',
      't/005_bad_manifest.pl',
      't/006_encoding.pl',
      't/007_wal.pl',
      't/008_untar.pl',
      't/009_extract.pl',
      't/010_client_untar.pl',
    ],
  },
}

po_catalogs += {'src/bin/pg_verifybackup': {
  'name': 'pg_verifybackup', 'frontend': true,
  'sources': pg_verifybackup_po_sources + files(
    '../../common/logging.c', '../../common/controldata_utils.c',
    '../../common/cryptohash.c', '../../common/cryptohash_openssl.c',
    '../../common/fe_memutils.c', '../../common/jsonapi.c', '../../common/parse_manifest.c',
    '../../fe_utils/astreamer_gzip.c', '../../fe_utils/astreamer_lz4.c',
    '../../fe_utils/astreamer_tar.c', '../../fe_utils/astreamer_zstd.c'),
  'keywords': ['error_cb:2', 'json_manifest_parse_failure:2', 'json_token_error:2',
               'report_backup_error:2', 'report_fatal_error'],
  'flags': ['error_cb:2:c-format', 'json_token_error:2:c-format',
            'report_backup_error:2:c-format', 'report_fatal_error:1:c-format'],
}}

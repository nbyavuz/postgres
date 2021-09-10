common_sources = files(
  'receivelog.c',
  'bbstreamer_file.c',
  'bbstreamer_inject.c',
  'bbstreamer_tar.c',
  'streamutil.c',
  'walmethods.c',
)

pg_basebackup_common = static_library('pg_basebackup_common',
  common_sources,
  dependencies: [frontend_code, libpq, zlib, lz4],
  kwargs: internal_lib_args,
)

executable('pg_basebackup',
  'pg_basebackup.c',
  link_with: [pg_basebackup_common],
  dependencies: [frontend_code, libpq, zlib, lz4],
  kwargs: default_bin_args,
)

executable('pg_receivewal',
  'pg_receivewal.c',
  link_with: [pg_basebackup_common],
  dependencies: [frontend_code, libpq, zlib, lz4],
  kwargs: default_bin_args,
)

executable('pg_recvlogical',
  'pg_recvlogical.c',
  link_with: [pg_basebackup_common],
  dependencies: [frontend_code, libpq, zlib, lz4],
  kwargs: default_bin_args,
)

tap_tests += {
  'name' : 'pg_basebackup',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'env': {'GZIP_PROGRAM': gzip.path(), 'TAR': tar.path()},
  'tests': [
    't/010_pg_basebackup.pl',
    't/020_pg_receivewal.pl',
    't/030_pg_recvlogical.pl',
  ]
}

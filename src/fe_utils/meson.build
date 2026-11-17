# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# Shared with message catalogs in other directories.
cancel_c = files('cancel.c')
option_utils_c = files('option_utils.c')
string_utils_c = files('string_utils.c')
version_c = files('version.c')

fe_utils_sources = [
  cancel_c,
  option_utils_c,
  string_utils_c,
  version_c,
] + files(
  'archive.c',
  'astreamer_file.c',
  'astreamer_gzip.c',
  'astreamer_lz4.c',
  'astreamer_tar.c',
  'astreamer_zstd.c',
  'conditional.c',
  'connect_utils.c',
  'mbprint.c',
  'parallel_slot.c',
  'print.c',
  'query_utils.c',
  'recovery_gen.c',
  'simple_list.c',
)

psqlscan = custom_target('psqlscan',
  input: 'psqlscan.l',
  output: 'psqlscan.c',
  command: [flex_cmd, '--no-backup', '--', '-Cfe', '-p', '-p'],
)
generated_sources += psqlscan
fe_utils_sources += psqlscan

fe_utils = static_library('libpgfeutils',
  fe_utils_sources,
  c_pch: pch_postgres_fe_h,
  include_directories: [postgres_inc, libpq_inc],
  c_args: host_system == 'windows' ? ['-DFD_SETSIZE=1024'] : [],
  dependencies: frontend_common_code,
  kwargs: default_lib_args + {
            'install': install_internal_static_lib,
          },
)

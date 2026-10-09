# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# Shared by compilation and per-program message catalog extraction.
fe_utils_archive_source = files('archive.c')
fe_utils_astreamer_file_source = files('astreamer_file.c')
fe_utils_astreamer_gzip_source = files('astreamer_gzip.c')
fe_utils_astreamer_lz4_source = files('astreamer_lz4.c')
fe_utils_astreamer_tar_source = files('astreamer_tar.c')
fe_utils_astreamer_zstd_source = files('astreamer_zstd.c')
fe_utils_cancel_source = files('cancel.c')
fe_utils_connect_utils_source = files('connect_utils.c')
fe_utils_option_utils_source = files('option_utils.c')
fe_utils_parallel_slot_source = files('parallel_slot.c')
fe_utils_print_source = files('print.c')
fe_utils_query_utils_source = files('query_utils.c')
fe_utils_recovery_gen_source = files('recovery_gen.c')
fe_utils_string_utils_source = files('string_utils.c')
fe_utils_version_source = files('version.c')

fe_utils_sources = [
  fe_utils_archive_source,
  fe_utils_astreamer_file_source,
  fe_utils_astreamer_gzip_source,
  fe_utils_astreamer_lz4_source,
  fe_utils_astreamer_tar_source,
  fe_utils_astreamer_zstd_source,
  fe_utils_cancel_source,
  fe_utils_connect_utils_source,
  fe_utils_option_utils_source,
  fe_utils_parallel_slot_source,
  fe_utils_print_source,
  fe_utils_query_utils_source,
  fe_utils_recovery_gen_source,
  fe_utils_string_utils_source,
  fe_utils_version_source,
] + files(
  'conditional.c',
  'mbprint.c',
  'simple_list.c',
)

psqlscan_input = files('psqlscan.l')
psqlscan = custom_target('psqlscan',
  input: psqlscan_input,
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

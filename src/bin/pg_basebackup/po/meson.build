# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_basebackup-' + pg_version_major.to_string())]

nls_catalogs += {'pg_basebackup': {
  'sources': pg_basebackup_nls_sources + nls_frontend_sources + [
    controldata_utils_c,
    fe_memutils_c,
    file_utils_c,
    restricted_token_c,
    option_utils_c,
    string_utils_c,
    version_c,
  ] + files(
    '../../../common/compression.c',
    '../../../fe_utils/astreamer_file.c',
    '../../../fe_utils/astreamer_gzip.c',
    '../../../fe_utils/astreamer_lz4.c',
    '../../../fe_utils/astreamer_tar.c',
    '../../../fe_utils/astreamer_zstd.c',
    '../../../fe_utils/recovery_gen.c',
  ),
  'keywords': nls_frontend_triggers + ['simple_prompt'],
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

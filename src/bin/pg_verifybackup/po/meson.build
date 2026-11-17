# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_verifybackup-' + pg_version_major.to_string())]

nls_catalogs += {'pg_verifybackup': {
  'sources': pg_verifybackup_nls_sources + nls_frontend_sources + [
    controldata_utils_c,
    cryptohash_c,
    cryptohash_openssl_c,
    fe_memutils_c,
  ] + files(
    '../../../common/jsonapi.c',
    '../../../common/parse_manifest.c',
    '../../../fe_utils/astreamer_gzip.c',
    '../../../fe_utils/astreamer_lz4.c',
    '../../../fe_utils/astreamer_tar.c',
    '../../../fe_utils/astreamer_zstd.c',
  ),
  'keywords': nls_frontend_triggers + [
    'json_token_error:2', 'json_manifest_parse_failure:2', 'error_cb:2',
    'report_backup_error:2', 'report_fatal_error',
  ],
  'flags': nls_frontend_flags + [
    'json_token_error:2:c-format', 'error_cb:2:c-format',
    'report_backup_error:2:c-format', 'report_fatal_error:1:c-format',
  ],
  'po_dir': meson.current_source_dir(),
}}

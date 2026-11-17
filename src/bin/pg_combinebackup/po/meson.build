# Copyright (c) 2023-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_combinebackup-' + pg_version_major.to_string())]

nls_catalogs += {'pg_combinebackup': {
  'sources': pg_combinebackup_nls_sources + nls_frontend_sources + [
    controldata_utils_c,
    cryptohash_c,
    cryptohash_openssl_c,
    fe_memutils_c,
    file_utils_c,
    option_utils_c,
    version_c,
  ] + files(
    '../../../common/jsonapi.c',
    '../../../common/parse_manifest.c',
  ),
  'keywords': nls_frontend_triggers + [
    'json_token_error:2', 'json_manifest_parse_failure:2', 'error_cb:2',
  ],
  'flags': nls_frontend_flags + ['json_token_error:2:c-format', 'error_cb:2:c-format'],
  'po_dir': meson.current_source_dir(),
}}

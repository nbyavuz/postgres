# Copyright (c) 2023-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_combinebackup': {
  'directory': 'src/bin/pg_combinebackup',
  'sources': pg_combinebackup_sources + files(
    '../../../common/logging.c', '../../../common/controldata_utils.c',
    '../../../common/cryptohash.c', '../../../common/cryptohash_openssl.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c',
    '../../../common/jsonapi.c', '../../../common/parse_manifest.c',
    '../../../fe_utils/option_utils.c', '../../../fe_utils/version.c'),
  'keywords': nls_frontend_keywords + ['json_token_error:2', 'json_manifest_parse_failure:2', 'error_cb:2'],
  'flags': nls_frontend_flags + ['json_token_error:2:c-format', 'error_cb:2:c-format'],
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_combinebackup-' + pg_version_major.to_string())]
endif

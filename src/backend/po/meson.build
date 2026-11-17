# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('postgres-' + pg_version_major.to_string())]

nls_catalogs += {'postgres': {
  'sources': postgres_nls_sources + common_nls_sources + pgport_sources +
    pgport_windows_sources + timezone_sources + [
      cryptohash_c,
      cryptohash_openssl_c,
      guc_tables,
    ] + files(
      '../../common/hmac.c',
      '../../common/hmac_openssl.c',
      '../../include/postmaster/proctypelist.h',
    ),
  'scanners': backend_nls_scanners,
  'keywords': nls_backend_triggers + [
    'GUC_check_errmsg', 'GUC_check_errdetail', 'GUC_check_errhint', 'write_stderr',
    'yyerror', 'jsonpath_yyerror:4', 'parser_yyerror', 'replication_yyerror:3',
    'scanner_yyerror', 'syncrep_yyerror:4', 'report_invalid_record:2',
    'ereport_startup_progress', 'json_token_error:2',
    'json_manifest_parse_failure:2', 'error_cb:2',
  ],
  'flags': nls_backend_flags + [
    'GUC_check_errmsg:1:c-format', 'GUC_check_errdetail:1:c-format',
    'GUC_check_errhint:1:c-format',
    'write_stderr:1:c-format', 'report_invalid_record:2:c-format',
    'ereport_startup_progress:1:c-format',
    'json_token_error:2:c-format', 'error_cb:2:c-format',
  ],
  'po_dir': meson.current_source_dir(),
}}

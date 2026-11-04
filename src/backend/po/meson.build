# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'postgres': {
  'directory': 'src/backend',
  'sources': backend_sources + backend_nls_sources + common_nls_sources + pgport_nls_sources +
    parser_sources + boot_parser_sources + repl_parser_sources +
    dict_snowball_sources + libpqwalreceiver_sources + pgoutput_sources + pgrepack_sources +
    [guc_tables, jsonpath_scan, jsonpath_gram, guc_scan] + files(
      '../bootstrap/bootparse.y', '../bootstrap/bootscanner.l',
      '../parser/gram.y', '../parser/scan.l',
      '../replication/repl_gram.y', '../replication/repl_scanner.l',
      '../replication/syncrep_gram.y', '../replication/syncrep_scanner.l',
      '../utils/adt/jsonpath_gram.y', '../utils/adt/jsonpath_scan.l',
      '../utils/misc/guc-file.l', '../../include/postmaster/proctypelist.h',
      '../regex/regc_pg_locale.c', '../utils/adt/like_match.c', '../utils/adt/levenshtein.c'),
  'keywords': nls_backend_keywords + [
    'GUC_check_errmsg', 'GUC_check_errdetail', 'GUC_check_errhint', 'write_stderr',
    'yyerror', 'jsonpath_yyerror:4', 'parser_yyerror', 'replication_yyerror:3',
    'scanner_yyerror', 'syncrep_yyerror:4', 'report_invalid_record:2',
    'ereport_startup_progress', 'json_token_error:2', 'json_manifest_parse_failure:2', 'error_cb:2'],
  'flags': nls_backend_flags + [
    'GUC_check_errmsg:1:c-format', 'GUC_check_errdetail:1:c-format',
    'GUC_check_errhint:1:c-format', 'write_stderr:1:c-format',
    'report_invalid_record:2:c-format', 'ereport_startup_progress:1:c-format',
    'json_token_error:2:c-format', 'error_cb:2:c-format'],
}}

if libintl.found()
  nls_targets += [i18n.gettext('postgres-' + pg_version_major.to_string())]
endif

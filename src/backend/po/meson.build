# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('postgres-' + pg_version_major.to_string())]

# Extraction includes generated C files even when they are normally included
# by another C file rather than compiled independently (notably GUC metadata).
# Pass the actual targets so a fresh build needs no server compilation first.
nls_catalogs += {
  'name': 'postgres',
  'directory': meson.current_source_dir() / '..',
  'generated': [
    backend_scanner, backend_parser, bootscanner, bootparse,
    repl_scanner, repl_gram, syncrep_scanner, syncrep_gram,
    jsonpath_scan, jsonpath_gram, guc_scan,
    guc_tables, generated_nodes, fmgrtab_target, wait_event_target,
  ],
}

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('psql-' + pg_version_major.to_string())]

nls_catalogs += {
  'name': 'psql',
  'directory': meson.current_source_dir() / '..',
  'generated': [psqlscanslash, psqlscan, tabcomplete, sql_help],
}

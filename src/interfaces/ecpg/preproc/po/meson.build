# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('ecpg-' + pg_version_major.to_string())]

nls_catalogs += {
  'name': 'ecpg',
  'directory': meson.current_source_dir() / '..',
  'generated': [pgc, preproc],
}

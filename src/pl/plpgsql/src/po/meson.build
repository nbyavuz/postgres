# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('plpgsql-' + pg_version_major.to_string())]

nls_catalogs += {
  'name': 'plpgsql',
  'directory': meson.current_source_dir() / '..',
  'generated': [pl_gram],
}

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('plperl-' + pg_version_major.to_string())]

nls_catalogs += {
  'name': 'plperl',
  'directory': meson.current_source_dir() / '..',
  'generated': plperl_nls_sources,
}

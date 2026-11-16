# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pltcl-' + pg_version_major.to_string())]

nls_catalogs += {'pltcl': {
  'sources': pltcl_nls_sources,
  'keywords': nls_backend_triggers,
  'flags': nls_backend_flags,
  'po_dir': meson.current_source_dir(),
}}

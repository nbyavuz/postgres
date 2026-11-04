# Copyright (c) 2026, PostgreSQL Global Development Group

# Catalog maintenance is independent of ENABLE_NLS and of optional modules.
# xgettext is not required for an ordinary build or installation.
xgettext = find_program('xgettext', required: false, native: true)
nls_tool = files('../nls.py')
nls_pots = []

foreach name, catalog : nls_catalogs
  if not xgettext.found()
    continue
  endif
  options = []
  foreach keyword : catalog.get('keywords', [])
    options += '--keyword=' + keyword
  endforeach
  foreach flag : catalog.get('flags', [])
    options += '--flag=' + flag
  endforeach
  pot = custom_target(name + '-catalog',
    input: catalog['sources'],
    depend_files: files(meson.project_source_root() / catalog['directory'] / 'po/meson.build'),
    output: name + '.pot',
    command: [python, nls_tool, 'extract',
      '--xgettext', xgettext,
      '--source-root', meson.project_source_root(),
      '--build-root', meson.project_build_root(),
      '--catalog-dir', catalog['directory'],
      '--catalog', name, '--version', pg_version_major.to_string(),
      '--output', '@OUTPUT@', options, '@INPUT@'],
    # Keep gettext diagnostics and ordering independent of the user's locale.
    env: {'LC_ALL': 'C'},
    build_by_default: false,
  )
  nls_pots += pot
  alias_target(name + '-init-po', pot)
endforeach

if xgettext.found()
  alias_target('init-po', nls_pots)
endif

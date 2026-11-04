# Copyright (c) 2026, PostgreSQL Global Development Group

# Catalog maintenance is independent of ENABLE_NLS and of optional modules.
# Neither gettext program is required for an ordinary build or installation.
xgettext = find_program('xgettext', required: false, native: true)
msgmerge = find_program('msgmerge', required: false, native: true)
nls_tool = files('../nls.py')
nls_pots = []
nls_updates = []
nls_languages = []
nls_compendia = {}

# Reading every LINGUAS here makes language additions trigger reconfiguration.
foreach name, catalog : nls_catalogs
  directory = meson.project_source_root() / catalog['directory'] / 'po'
  foreach line : fs.read(directory / 'LINGUAS').split('\n')
    foreach language : line.split('#')[0].split()
      po = files(directory / (language + '.po'))
      nls_compendia += {language: nls_compendia.get(language, []) + po}
      if language not in nls_languages
        nls_languages += language
      endif
    endforeach
  endforeach
endforeach

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

  if not msgmerge.found()
    continue
  endif
  updates = []
  foreach language : nls_languages
    inputs = [pot] + nls_compendia[language]
    options = []
    po = meson.project_source_root() / catalog['directory'] / 'po' / (language + '.po')
    if fs.is_file(po)
      inputs += files(po)
      options += ['--po', po]
    endif
    foreach compendium : nls_compendia[language]
      options += ['--compendium', compendium]
    endforeach
    updates += custom_target(name + '-' + language + '-update-po',
      input: inputs,
      output: name + '-' + language + '.po.new',
      command: [python, nls_tool, 'merge', '--msgmerge', msgmerge,
        '--language', language, '--pot', '@INPUT0@',
        '--output', '@OUTPUT@', options],
      build_by_default: false,
    )
  endforeach
  nls_updates += updates
  if updates.length() > 0
    alias_target(name + '-update-po', updates)
  endif
endforeach

if xgettext.found()
  alias_target('init-po', nls_pots)
  if msgmerge.found() and nls_updates.length() > 0
    alias_target('update-po', nls_updates)
  endif
endif

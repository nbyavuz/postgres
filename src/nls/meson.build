# Copyright (c) 2026, PostgreSQL Global Development Group

# Each nls_catalogs entry supplies sources, a po_dir, and optional gettext
# keywords and flags.  Sources reuse the compilation lists, with explicit
# additions for shared code, included C fragments, and optional implementations.
# scanners identifies Flex outputs whose skeleton code must be omitted.
# Catalog-specific source lists avoid generating unrelated headers and Windows
# resources.

if not libintl.found()
  subdir_done()
endif

# These tools are needed by translators, not by normal builds and installs.
xgettext = find_program('xgettext', required: false, native: true)
if not xgettext.found()
  subdir_done()
endif

init_po_targets = []
gen_pot = files('gen_pot.py')

foreach name, catalog : nls_catalogs
  gettext_args = ['--keyword=_', '--flag=_:1:pass-c-format']
  foreach keyword : catalog.get('keywords', [])
    gettext_args += '--keyword=' + keyword
  endforeach
  foreach flag : catalog.get('flags', [])
    gettext_args += '--flag=' + flag
  endforeach
  foreach scanner : catalog.get('scanners', [])
    gettext_args += ['--scanner', scanner]
  endforeach

  pot = custom_target(name + '-pot-file',
    input: catalog['sources'],
    output: name + '.pot',
    command: [
      python, gen_pot,
      '--xgettext', xgettext,
      '--catalog', name,
      '--version', pg_version_major.to_string(),
      '--source-root', meson.project_source_root(),
      '--build-root', meson.project_build_root(),
      '--output', '@OUTPUT@',
      gettext_args, '--', '@INPUT@',
    ],
    # Besides fixing gettext's locale, env makes Meson use its command wrapper,
    # avoiding the shell's command-line length limit for the backend's inputs.
    env: {'LC_ALL': 'C'},
    build_by_default: false,
  )
  alias_target(name + '-init-po', pot)
  init_po_targets += pot
endforeach

alias_target('init-po', init_po_targets)

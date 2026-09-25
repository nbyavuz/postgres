# Copyright (c) 2026, PostgreSQL Global Development Group

init_po_targets = []
update_po_targets = []
clean_po_targets = []
maintain_po = files('../maintain-po.py')

foreach catalog : nls_catalogs
  # These are explicit maintainer operations, not part of an ordinary build.
  # Always run the helper so newly added backend sources and compendium
  # catalogs are noticed without a configure-time directory scan.  Use custom
  # targets rather than aliases of run targets to support Meson 0.57.2.
  command = [
    python, maintain_po,
    '--source-root', meson.project_source_root(),
    '--build-root', meson.project_build_root(),
    '--catalog-dir', catalog['directory'],
    '--version', pg_version_major.to_string(),
  ]
  generated = catalog.get('generated', [])
  generated_paths = []
  foreach target : generated
    foreach output : target.to_list()
      generated_paths += output.full_path()
    endforeach
  endforeach
  init_po = custom_target(catalog['name'] + '-init-po.stamp',
    output: catalog['name'] + '-init-po.stamp',
    command: command + ['init-po', '--stamp', '@OUTPUT@',
                        '--generated', generated_paths],
    depends: generated,
    build_always_stale: true,
    build_by_default: false,
  )
  alias_target(catalog['name'] + '-init-po', init_po)
  init_po_targets += init_po
  # Share extraction with init-po.  Besides supporting a fresh build, this
  # prevents two writers of the same template when both targets are requested.
  update_po = custom_target(catalog['name'] + '-update-po.stamp',
    output: catalog['name'] + '-update-po.stamp',
    command: command + ['update-po', '--skip-extraction', '--stamp', '@OUTPUT@'],
    depends: init_po,
    build_always_stale: true,
    build_by_default: false,
  )
  alias_target(catalog['name'] + '-update-po', update_po)
  update_po_targets += update_po

  # Maintenance outputs include a runtime-selected set of languages.  Provide
  # explicit cleanup for these side effects, which Ninja's clean cannot see.
  # There are deliberately no dependencies on extraction or gettext tools.
  clean_po = custom_target(catalog['name'] + '-clean-po.stamp',
    output: catalog['name'] + '-clean-po.stamp',
    command: command + ['clean-po', '--stamp', '@OUTPUT@'],
    build_always_stale: true,
    build_by_default: false,
  )
  alias_target(catalog['name'] + '-clean-po', clean_po)
  clean_po_targets += clean_po
endforeach

if nls_catalogs.length() > 0
  alias_target('init-po', init_po_targets)
  alias_target('update-po', update_po_targets)
  alias_target('clean-po', clean_po_targets)
endif

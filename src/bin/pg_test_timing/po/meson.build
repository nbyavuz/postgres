# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if libintl.found()
  nls_targets += [i18n.gettext('pg_test_timing-' + pg_version_major.to_string())]
endif

if xgettext.found()
  pg_test_timing_pot = custom_target(po_name + '.pot',
    command: [po_template_command, '--xgettext', xgettext, '@INPUT@'],
    kwargs: po_template_kwargs,
  )
  po_templates += pg_test_timing_pot
  alias_target(po_name + '-init-po', pg_test_timing_pot)
endif

local_languages = fs.read('LINGUAS').split()
pg_test_timing_po_new = []
foreach lang, catalogs : po_catalogs_by_language
  # For languages not maintained here, merge the template against itself so
  # its header is used even when a compendium supplies a translation.
  local_po = meson.current_source_dir() / (lang + '.po')
  maintained = lang in local_languages
  definition = maintained ? files(local_po) : pg_test_timing_pot

  compendia = []
  foreach po_file : catalogs
    if not maintained or po_file != local_po
      compendia += ['--compendium', files(po_file)]
    endif
  endforeach

  pg_test_timing_po_new += custom_target('pg_test_timing-' + lang + '.po.new',
    input: pg_test_timing_pot,
    output: lang + '.po.new',
    command: [msgmerge, '--no-wrap', '--previous', '--sort-by-file',
      '--lang=' + lang, definition, '@INPUT@', compendia, '-o', '@OUTPUT@'],
    build_by_default: false,
  )
endforeach
if pg_test_timing_po_new.length() > 0
  alias_target('pg_test_timing-update-po', pg_test_timing_po_new)
endif

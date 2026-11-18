# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if libintl.found()
  nls_targets += [i18n.gettext('pg_test_timing-' + pg_version_major.to_string())]
endif

# Extracting messages needs xgettext on the build machine, even if the
# executable itself is built without NLS.
xgettext = find_program('xgettext', native: true, required: false)
if xgettext.found()
  pg_test_timing_pot = custom_target('pg_test_timing.pot',
    input: pg_test_timing_nls_sources,
    output: 'pg_test_timing.pot',
    command: [python, files('../../../tools/generate_pot.py'),
      '--xgettext', xgettext, '--srcdir', meson.current_source_dir() / '..',
      '--catalog', 'pg_test_timing', '--version', pg_version_major.to_string(),
      '--output', '@OUTPUT@', '@INPUT@'],
    build_by_default: false,
  )
  alias_target('pg_test_timing-init-po', pg_test_timing_pot)
endif

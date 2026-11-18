# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if libintl.found()
  nls_targets += [i18n.gettext('ecpglib' + '6' + '-' + pg_version_major.to_string())]
endif

xgettext = find_program('xgettext', native: true, required: false)
if xgettext.found()
  po_template = custom_target(po_name + '.pot',
    command: [po_template_command, '--xgettext', xgettext, '@INPUT@'],
    kwargs: po_template_kwargs,
  )
  po_templates += po_template
  alias_target(po_name + '-init-po', po_template)
endif

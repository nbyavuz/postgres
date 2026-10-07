# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if libintl.found()
  nls_targets += [i18n.gettext('ecpglib' + '6' + '-' + pg_version_major.to_string())]
endif

po_template = custom_target(po_name + '-template', kwargs: po_template_kwargs)
po_templates += po_template
alias_target(po_name + '-init-po', po_template)
po_merges = []
foreach language, kwargs : po_merge_kwargs
  po_merges += custom_target(po_name + '-' + language + '-merge',
    input: po_template, kwargs: kwargs)
endforeach
po_updates += po_merges
alias_target(po_name + '-update-po', [po_template] + po_merges)

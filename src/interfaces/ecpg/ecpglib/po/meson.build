# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if libintl.found()
  nls_targets += [i18n.gettext('ecpglib' + '6' + '-' + pg_version_major.to_string())]
endif

if xgettext.found()
  po_template = custom_target(po_name + '.pot', kwargs: po_template_kwargs)
endif
po_merges = {}
foreach lang, kwargs : po_merge_kwargs
  po_merges += {lang: custom_target(po_name + '-' + lang + '.po.new',
    input: po_template, kwargs: kwargs)}
endforeach

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pgscripts-' + pg_version_major.to_string())]

nls_catalogs += {'pgscripts': {
  'sources': pgscripts_nls_sources + nls_frontend_sources + [
    fe_memutils_c,
    file_utils_c,
    username_c,
    cancel_c,
    option_utils_c,
    string_utils_c,
  ] + files(
    '../../../fe_utils/connect_utils.c',
    '../../../fe_utils/parallel_slot.c',
    '../../../fe_utils/print.c',
    '../../../fe_utils/query_utils.c',
  ),
  'keywords': nls_frontend_triggers + ['simple_prompt', 'yesno_prompt'],
  'flags': nls_frontend_flags,
  'po_dir': meson.current_source_dir(),
}}

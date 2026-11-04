# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pgscripts': {
  'directory': 'src/bin/scripts',
  'sources': scripts_nls_sources + files(
    '../../../common/logging.c', '../../../common/fe_memutils.c',
    '../../../common/file_utils.c', '../../../common/username.c',
    '../../../fe_utils/parallel_slot.c', '../../../fe_utils/cancel.c',
    '../../../fe_utils/print.c', '../../../fe_utils/connect_utils.c',
    '../../../fe_utils/option_utils.c', '../../../fe_utils/query_utils.c', '../../../fe_utils/string_utils.c'),
  'keywords': nls_frontend_keywords + ['simple_prompt', 'yesno_prompt'],
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pgscripts-' + pg_version_major.to_string())]
endif

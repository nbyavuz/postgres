# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'initdb': {
  'directory': 'src/bin/initdb',
  'sources': initdb_sources + files(
    '../../../common/logging.c', '../../../common/exec.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c',
    '../../../common/pgfnames.c', '../../../common/restricted_token.c',
    '../../../common/rmtree.c', '../../../common/username.c',
    '../../../common/wait_error.c', '../../../fe_utils/option_utils.c',
    '../../../fe_utils/string_utils.c', '../../../port/dirmod.c'),
  'keywords': nls_frontend_keywords + ['simple_prompt'],
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('initdb-' + pg_version_major.to_string())]
endif

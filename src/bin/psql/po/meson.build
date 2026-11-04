# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'psql': {
  'directory': 'src/bin/psql',
  'sources': psql_sources + [psqlscan] + files(
    '../psqlscanslash.l', '../../../fe_utils/psqlscan.l',
    '../../../common/logging.c', '../../../common/exec.c',
    '../../../common/fe_memutils.c', '../../../common/username.c',
    '../../../common/wait_error.c', '../../../fe_utils/cancel.c',
    '../../../fe_utils/print.c', '../../../fe_utils/string_utils.c'),
  'keywords': nls_frontend_keywords + [
    'HELP0', 'HELPN', 'N_', 'addQueryComment:2', 'simple_prompt',
    'simple_prompt_extended'],
  'flags': nls_frontend_flags + ['HELPN:1:c-format'],
}}

if libintl.found()
  nls_targets += [i18n.gettext('psql-' + pg_version_major.to_string())]
endif

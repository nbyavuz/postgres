# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_dump': {
  'directory': 'src/bin/pg_dump',
  'sources': pg_dump_common_sources + pg_dump_sources + pg_dumpall_sources + pg_restore_sources + files(
    '../parallel.h', '../pg_backup_utils.h', '../../../common/logging.c',
    '../../../common/compression.c', '../../../common/exec.c',
    '../../../common/fe_memutils.c', '../../../common/file_utils.c',
    '../../../common/wait_error.c', '../../../fe_utils/option_utils.c', '../../../fe_utils/string_utils.c'),
  'keywords': nls_frontend_keywords + ['simple_prompt', 'ExecuteSqlCommand:3', 'warn_or_exit_horribly:2'],
  'flags': nls_frontend_flags + ['warn_or_exit_horribly:2:c-format'],
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_dump-' + pg_version_major.to_string())]
endif

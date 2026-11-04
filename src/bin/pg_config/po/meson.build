# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_config': {
  'directory': 'src/bin/pg_config',
  'sources': pg_config_sources + files(
    '../../../common/config_info.c', '../../../common/exec.c', '../../../common/fe_memutils.c'),
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_config-' + pg_version_major.to_string())]
endif

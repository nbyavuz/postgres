# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'pg_basebackup': {
  'directory': 'src/bin/pg_basebackup',
  'sources': common_sources + pg_basebackup_sources + pg_createsubscriber_sources +
    pg_receivewal_sources + pg_recvlogical_sources + files(
    '../../../common/logging.c', '../../../common/compression.c',
    '../../../common/controldata_utils.c', '../../../common/fe_memutils.c',
    '../../../common/file_utils.c', '../../../common/restricted_token.c',
    '../../../fe_utils/astreamer_file.c', '../../../fe_utils/astreamer_gzip.c',
    '../../../fe_utils/astreamer_lz4.c', '../../../fe_utils/astreamer_tar.c',
    '../../../fe_utils/astreamer_zstd.c', '../../../fe_utils/option_utils.c',
    '../../../fe_utils/recovery_gen.c', '../../../fe_utils/string_utils.c',
    '../../../fe_utils/version.c'),
  'keywords': nls_frontend_keywords + ['simple_prompt'],
  'flags': nls_frontend_flags,
}}
if libintl.found()
  nls_targets += [i18n.gettext('pg_basebackup-' + pg_version_major.to_string())]
endif

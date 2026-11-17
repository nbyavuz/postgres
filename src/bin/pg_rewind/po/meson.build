# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_targets += [i18n.gettext('pg_rewind-' + pg_version_major.to_string())]

nls_catalogs += {'pg_rewind': {
  'sources': pg_rewind_nls_sources + nls_frontend_sources + [
    controldata_utils_c,
    fe_memutils_c,
    file_utils_c,
    restricted_token_c,
    option_utils_c,
    string_utils_c,
  ] + files(
    '../../../common/percentrepl.c',
    '../../../fe_utils/archive.c',
    '../../../fe_utils/recovery_gen.c',
  ),
  'keywords': nls_frontend_triggers + ['report_invalid_record:2'],
  'flags': nls_frontend_flags + ['report_invalid_record:2:c-format'],
  'po_dir': meson.current_source_dir(),
}}

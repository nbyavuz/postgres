# Copyright (c) 2022-2026, PostgreSQL Global Development Group

nls_catalogs += {'libpq': {
  'directory': 'src/interfaces/libpq',
  'sources': libpq_nls_sources + files('../../libpq-oauth/oauth-curl.c', '../../libpq-oauth/oauth-utils.c'),
  'keywords': ['actx_error:2', 'libpq_append_conn_error:2', 'libpq_append_error:2',
    'libpq_gettext', 'libpq_ngettext:1,2', 'oauth_json_set_error:2',
    'oauth_parse_set_error:2', 'pqInternalNotice:2'],
  'flags': ['actx_error:2:c-format', 'libpq_append_conn_error:2:c-format',
    'libpq_append_error:2:c-format', 'libpq_gettext:1:pass-c-format',
    'libpq_ngettext:1:pass-c-format', 'libpq_ngettext:2:pass-c-format',
    'oauth_json_set_error:2:c-format', 'oauth_parse_set_error:2:c-format',
    'pqInternalNotice:2:c-format'],
}}

if libintl.found()
  nls_targets += [i18n.gettext('libpq' + '5' + '-' + pg_version_major.to_string())]
endif

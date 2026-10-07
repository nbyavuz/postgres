# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'auth-oauth.c',
  'auth-sasl.c',
  'auth-scram.c',
  'auth.c',
  'be-fsstubs.c',
  'be-secure-common.c',
  'be-secure.c',
  'crypt.c',
  'hba.c',
  'ifaddr.c',
  'pqcomm.c',
  'pqformat.c',
  'pqmq.c',
  'pqsignal.c',
)

backend_ssl_sources = files('be-secure-openssl.c')
backend_gss_sources = files('be-secure-gssapi.c', 'be-gssapi-common.c')
backend_po_sources += backend_ssl_sources + backend_gss_sources
if ssl.found()
  backend_sources += backend_ssl_sources
endif

if gssapi.found()
  backend_sources += backend_gss_sources
endif

install_data(
  'pg_hba.conf.sample',
  'pg_ident.conf.sample',
  'pg_hosts.conf.sample',
  install_dir: dir_data,
)

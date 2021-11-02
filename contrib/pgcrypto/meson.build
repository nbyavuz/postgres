pgcrypto_sources = files(
  'crypt-blowfish.c',
  'crypt-des.c',
  'crypt-gensalt.c',
  'crypt-md5.c',
  'mbuf.c',
  'pgcrypto.c',
  'pgp-armor.c',
  'pgp-cfb.c',
  'pgp-compress.c',
  'pgp-decrypt.c',
  'pgp-encrypt.c',
  'pgp-info.c',
  'pgp-mpi.c',
  'pgp-pgsql.c',
  'pgp-pubdec.c',
  'pgp-pubenc.c',
  'pgp-pubkey.c',
  'pgp-s2k.c',
  'pgp.c',
  'px-crypt.c',
  'px-hmac.c',
  'px.c',
)

pgcrypto_regress = [
  'init',
  'md5',
  'sha1',
  'hmac-md5',
  'hmac-sha1',
  'blowfish',
  'rijndael',
  'crypt-des',
  'crypt-md5',
  'crypt-blowfish',
  'crypt-xdes',
  'pgp-armor',
  'pgp-decrypt',
  'pgp-encrypt',
  'pgp-pubkey-decrypt',
  'pgp-pubkey-encrypt',
  'pgp-info',
]


pgcrypto_internal_sources = files(
  'internal.c',
  'internal-sha2.c',
  'blf.c',
  'rijndael.c',
  'pgp-mpi-internal.c',
)

pgcrypto_internal_regress = [
  'sha2',
]


pgcrypto_openssl_sources = files(
  'openssl.c',
  'pgp-mpi-openssl.c',
)
pgcrypto_openssl_regress = [
  'sha2',
  'des',
  '3des',
  'cast5',
]

imath_cflags = []
if using_declaration_after_statement_warning
  imath_cflags += ['-Wno-declaration-after-statement']
endif

imath_internal = static_library('internal_imath',
  sources: 'imath.c',
  kwargs: contrib_mod_args + {
    'c_args': c_args + contrib_mod_args.get('c_args', []),
    'install': false,
    'build_by_default': false,
  },
)

# TODO: implement rijndael.c generation

pgcrypto_deps = []
pgcrypto_link_with = []
if ssl.found()
  pgcrypto_deps += ssl
  pgcrypto_sources += pgcrypto_openssl_sources
  pgcrypto_regress += pgcrypto_openssl_regress
else
  pgcrypto_link_with += imath_internal
  pgcrypto_sources += pgcrypto_internal_sources
  pgcrypto_regress += pgcrypto_internal_regress
endif

if zlib.found()
  pgcrypto_deps += zlib
  pgcrypto_regress += 'pgp-compression'
else
  pgcrypto_regress += 'pgp-zlib-DISABLED'
endif

pgcrypto = shared_module('pgcrypto',
  pgcrypto_sources,
  c_pch: '../../src/include/pch/postgres_pch.h',
  kwargs: contrib_mod_args + {
    'link_with': [pgcrypto_link_with, contrib_mod_args.get('link_with', [])],
    'dependencies': [pgcrypto_deps, contrib_mod_args['dependencies']]
  },
)

install_data(
  'pgcrypto--1.0--1.1.sql',
  'pgcrypto--1.1--1.2.sql',
  'pgcrypto--1.2--1.3.sql',
  'pgcrypto--1.3.sql',
  'pgcrypto.control',
  kwargs: contrib_data_args,
)


regress_tests += {
  'name': 'pgcrypto',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'sql': [
    pgcrypto_regress
  ],
}

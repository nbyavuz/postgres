# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# Shared by compilation and per-program message catalog extraction.
common_compression_source = files('compression.c')
common_controldata_utils_source = files('controldata_utils.c')
common_cryptohash_source = files('cryptohash.c')
common_cryptohash_openssl_source = files('cryptohash_openssl.c')
common_exec_source = files('exec.c')
common_fe_memutils_source = files('fe_memutils.c')
common_file_utils_source = files('file_utils.c')
common_jsonapi_source = files('jsonapi.c')
common_logging_source = files('logging.c')
common_parse_manifest_source = files('parse_manifest.c')
common_percentrepl_source = files('percentrepl.c')
common_pgfnames_source = files('pgfnames.c')
common_restricted_token_source = files('restricted_token.c')
common_rmtree_source = files('rmtree.c')
common_username_source = files('username.c')
common_wait_error_source = files('wait_error.c')

common_sources = files(
  'archive.c',
  'base64.c',
  'binaryheap.c',
  'blkreftable.c',
  'checksum_helper.c',
  'encnames.c',
  'file_perm.c',
  'hashfn.c',
  'instr_time.c',
  'ip.c',
  'keywords.c',
  'kwlookup.c',
  'link-canary.c',
  'md5_common.c',
  'pg_get_line.c',
  'pg_lzcompress.c',
  'pg_parse_lsn.c',
  'pg_prng.c',
  'psprintf.c',
  'relpath.c',
  'saslprep.c',
  'scram-common.c',
  'string.c',
  'stringinfo.c',
  'unicode_case.c',
  'unicode_category.c',
  'unicode_norm.c',
  'wchar.c',
)
common_sources += (
  common_compression_source + common_controldata_utils_source +
  common_exec_source + common_file_utils_source + common_jsonapi_source +
  common_parse_manifest_source + common_percentrepl_source + common_pgfnames_source +
  common_rmtree_source + common_username_source + common_wait_error_source
)

common_ssl_sources = files(
  'hmac_openssl.c',
) + common_cryptohash_openssl_source
common_builtin_crypto_sources = files(
  'hmac.c',
  'md5.c',
  'sha1.c',
  'sha2.c',
) + common_cryptohash_source
common_po_sources = common_sources + common_ssl_sources + common_builtin_crypto_sources
if ssl.found()
  common_sources += common_ssl_sources
else
  common_sources += common_builtin_crypto_sources
endif

common_kwlist = custom_target('kwlist',
  input: files('../include/parser/kwlist.h'),
  output: 'kwlist_d.h',
  depend_files: gen_kwlist_deps,
  command: [gen_kwlist_cmd, '--extern'])
generated_sources += common_kwlist
common_sources += common_kwlist

# The code imported from Ryu gets a pass on declaration-after-statement,
# in order to keep it more closely aligned with its upstream.
ryu_sources = files(
  'd2s.c',
  'f2s.c',
)
ryu_cflags = []

ryu_cflags += cflags_no_decl_after_statement

config_info_sources = files('config_info.c',)
config_info_cflags = [
  '-DVAL_CC="@0@"'.format(var_cc),
  '-DVAL_CPPFLAGS="@0@"'.format(var_cppflags),
  '-DVAL_CFLAGS="@0@"'.format(var_cflags),
  '-DVAL_CFLAGS_SL="@0@"'.format(var_cflags_sl),
  '-DVAL_LDFLAGS="@0@"'.format(var_ldflags),
  '-DVAL_LDFLAGS_EX="@0@"'.format(var_ldflags_ex),
  '-DVAL_LDFLAGS_SL="@0@"'.format(var_ldflags_sl),
  '-DVAL_LIBS="@0@"'.format(var_libs),
]

# Some files need to be built with different cflags. The different sets are
# defined here.
common_cflags = {
  'ryu': ryu_cflags,
  'config_info': config_info_cflags,
}
common_sources_cflags = {
  'ryu': ryu_sources,
  'config_info': config_info_sources
}


# A few files are currently only built for frontend, not server.
# logging.c is excluded from OBJS_FRONTEND_SHLIB (shared library) as
# a matter of policy, because it is not appropriate for general purpose
# libraries such as libpq to report errors directly.  fe_memutils.c is
# excluded because libpq must not exit() on allocation failure.
#
# The excluded files for _shlib builds are pulled into their own static
# library, for the benefit of test programs that need not follow the
# shlib rules.

common_sources_frontend_shlib = common_sources
common_frontend_only_sources = files(
  'sprompt.c',
) + common_restricted_token_source
common_sources_frontend_shlib += common_frontend_only_sources

common_sources_excluded_shlib = common_fe_memutils_source + common_logging_source

common_sources_frontend_static = [
  common_sources_frontend_shlib,
  common_sources_excluded_shlib,
]
common_po_sources += common_frontend_only_sources + common_sources_excluded_shlib + config_info_sources

# Build pgcommon once for backend, once for use in frontend binaries, and
# once for use in shared libraries
#
# XXX: in most environments we could probably link_whole pgcommon_shlib
# against pgcommon_static, instead of compiling twice.
#
# For the server build of pgcommon, depend on lwlocknames_h and because at
# least cryptohash_openssl.c, hmac_openssl.c depend on it.
# controldata_utils.c depends on wait_event_types_h. That's arguably a
# layering violation, but ...
pgcommon = {}
pgcommon_variants = {
  '_srv': internal_lib_args + {
    'sources': common_sources + [lwlocknames_h] + [wait_event_types_h],
    'dependencies': [backend_common_code],
  },
  '': default_lib_args + {
    'sources': common_sources_frontend_static,
    'dependencies': [frontend_common_code],
    # Files in libpgcommon.a should use/export the "xxx_private" versions
    # of pg_char_to_encoding() and friends.
    'c_args': ['-DUSE_PRIVATE_ENCODING_FUNCS'],
  },
  '_shlib': default_lib_args + {
    'pic': true,
    'sources': common_sources_frontend_shlib,
    'dependencies': [frontend_common_code],
    # The JSON API normally exits on out-of-memory; disable that behavior for
    # shared library builds. This requires libpq's pqexpbuffer.h.
    'c_args': ['-DJSONAPI_USE_PQEXPBUFFER'],
    'include_directories': include_directories('../interfaces/libpq'),
  },
}

foreach name, opts : pgcommon_variants

  # Build internal static libraries for sets of files that need to be built
  # with different cflags
  cflag_libs = []
  foreach cflagname, sources : common_sources_cflags
    if sources.length() == 0
      continue
    endif
    c_args = opts.get('c_args', []) + common_cflags[cflagname]
    cflag_libs += static_library('libpgcommon@0@_@1@'.format(name, cflagname),
      c_pch: pch_c_h,
      kwargs: opts + {
        'include_directories': [
          include_directories('.'),
          opts.get('include_directories', []),
        ],
        'sources': sources,
        'c_args': c_args,
        'build_by_default': false,
        'install': false,
      },
    )
  endforeach

  lib = static_library('libpgcommon@0@'.format(name),
      link_with: cflag_libs,
      link_whole: cflag_libs,
      c_pch: pch_c_h,
      kwargs: opts + {
        'include_directories': [
          include_directories('.'),
          opts.get('include_directories', []),
        ],
        'dependencies': opts['dependencies'] + [ssl],
        'install': install_internal_static_lib and name != '_srv',
      }
    )
  pgcommon += {name: lib}
endforeach

common_srv = pgcommon['_srv']
common_shlib = pgcommon['_shlib']
common_static = pgcommon['']

common_excluded_shlib = static_library('libpgcommon_excluded_shlib',
  sources: common_sources_excluded_shlib,
  dependencies: [frontend_common_code],
  build_by_default: false,
  kwargs: default_lib_args + {
    'install': false,
  },
)

subdir('unicode')

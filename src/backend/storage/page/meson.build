# Copyright (c) 2022-2026, PostgreSQL Global Development Group

checksum_sources = files('checksum.c')
checksum_backend_lib = static_library('checksum_backend_lib',
  checksum_sources,
  dependencies: backend_build_deps,
  kwargs: internal_lib_args,
  c_args: vectorize_cflags + unroll_loops_cflags,
)

backend_link_with += checksum_backend_lib
backend_nls_sources += checksum_sources

backend_sources += files(
  'bufpage.c',
  'itemptr.c',
)

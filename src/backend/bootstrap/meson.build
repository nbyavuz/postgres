# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
 'bootstrap.c')

# see ../parser/meson.build
boot_parser_sources = []
bootscanner_input = files('bootscanner.l')
bootparse_input = files('bootparse.y')
backend_po_sources += bootparse_input

bootscanner = custom_target('bootscanner',
  input: bootscanner_input,
  output: 'bootscanner.c',
  command: flex_cmd,
)
generated_sources += bootscanner
backend_po_generated += bootscanner
boot_parser_sources += bootscanner

bootparse = custom_target('bootparse',
  input: bootparse_input,
  kwargs: bison_kw,
)
generated_sources += bootparse.to_list()
boot_parser_sources += bootparse

boot_parser = static_library('boot_parser',
  boot_parser_sources,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)
backend_link_with += boot_parser

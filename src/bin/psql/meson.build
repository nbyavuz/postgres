# Copyright (c) 2022-2026, PostgreSQL Global Development Group

psql_sources = files(
  'command.c',
  'common.c',
  'copy.c',
  'crosstabview.c',
  'describe.c',
  'help.c',
  'input.c',
  'large_obj.c',
  'mainloop.c',
  'prompt.c',
  'startup.c',
  'stringutils.c',
  'variables.c',
)

psqlscanslash_input = files('psqlscanslash.l')
psql_po_sources = psql_sources
psqlscanslash = custom_target('psqlscanslash',
  input: psqlscanslash_input,
  output: 'psqlscanslash.c',
  command: [flex_cmd, '--no-backup', '--', '-Cfe', '-p', '-p'])
generated_sources += psqlscanslash
psql_sources += psqlscanslash

tabcomplete = custom_target('tabcomplete',
  input: 'tab-complete.in.c',
  output: 'tab-complete.c',
  command: [
    perl, files('gen_tabcomplete.pl'), '@INPUT@',
    '--outfile', '@OUTPUT@',
  ],
)
generated_sources += tabcomplete
psql_sources += tabcomplete

sql_help = custom_target('psql_help',
  output: ['sql_help.c', 'sql_help.h'],
  depfile: 'sql_help.dep',
  command: [
    perl, files('create_help.pl'),
    '--docdir', '@SOURCE_ROOT@/doc/src/sgml/ref',
    '--depfile', '@DEPFILE@',
    '--outdir', '@OUTDIR@',
    '--basename', 'sql_help',
  ],
)
generated_sources += sql_help.to_list()
psql_sources += sql_help

if host_system == 'windows'
  psql_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'psql',
    '--FILEDESC', 'psql - the PostgreSQL interactive terminal',])
endif

psql = executable('psql',
  psql_sources,
  c_pch: pch_postgres_fe_h,
  include_directories: include_directories('.'),
  dependencies: [frontend_code, libpq, readline],
  kwargs: default_bin_args,
)
bin_targets += psql

install_data('psqlrc.sample',
  install_dir: dir_data,
)

tests += {
  'name': 'psql',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'env': {'with_readline': readline.found() ? 'yes' : 'no'},
    'tests': [
      't/001_basic.pl',
      't/010_tab_completion.pl',
      't/020_cancel.pl',
      't/030_pager.pl',
    ],
  },
}

po_catalogs += {'src/bin/psql': {
  'name': 'psql', 'frontend': true,
  'generated': [psqlscanslash, psqlscan, tabcomplete] + sql_help.to_list(),
  'sources': psql_po_sources +
    common_logging_source + common_exec_source + common_fe_memutils_source +
    common_username_source + common_wait_error_source + fe_utils_cancel_source +
    fe_utils_print_source + fe_utils_string_utils_source,
  'keywords': ['HELP0', 'HELPN', 'N_', 'simple_prompt', 'simple_prompt_extended'],
  'flags': ['HELPN:1:c-format'],
}}

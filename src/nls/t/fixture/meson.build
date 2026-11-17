# Copyright (c) 2026, PostgreSQL Global Development Group

project('nls-test', meson_version: '>=0.57.2')

python = find_program(get_option('PYTHON'))
flex = find_program(get_option('FLEX'))
bison = find_program(get_option('BISON'))
libintl = declare_dependency()
pg_version_major = 99

generated = custom_target('generated',
  input: 'message.txt',
  output: 'generated.h',
  command: [python, files('generate.py'), '@INPUT@', '@OUTPUT@'],
  build_by_default: false,
)

scanner = custom_target('scanner',
  input: 'scanner.l',
  output: 'scanner.c',
  command: [flex, '-o', '@OUTPUT@', '@INPUT@'],
  build_by_default: false,
)

parser = custom_target('parser',
  input: 'parser.y',
  output: ['parser.c', 'parser.h'],
  command: [bison, '-d', '-o', '@OUTPUT0@', '@INPUT@'],
  build_by_default: false,
)

alpha_sources = files('alpha.c', 'shared.c')
alpha_sources += [generated, scanner, parser]
alpha_nls_sources = alpha_sources

# This compilation input does not contribute messages to the catalog.
error_code_header = custom_target('errcodes',
  input: 'errcodes.txt',
  output: 'errcodes.h',
  command: [
    python, '-c',
    'import shutil, sys; shutil.copyfile(sys.argv[1], sys.argv[2])',
    '@INPUT@', '@OUTPUT@',
  ],
  build_by_default: false,
)
alpha_sources += error_code_header

nls_catalogs = {
  'alpha': {
    'sources': alpha_nls_sources,
    'scanners': [scanner],
    'keywords': ['report:2', 'report_plural:1,2', 'YY_FATAL_ERROR'],
    'flags': ['report:2:c-format', 'report_plural:1:c-format', 'report_plural:2:c-format'],
    'po_dir': meson.current_source_dir() / 'alpha',
  },
  'beta': {
    'sources': files('beta.c', 'shared.c'),
    'po_dir': meson.current_source_dir() / 'beta',
  },
  'empty': {
    'sources': files('empty.c'),
    'po_dir': meson.current_source_dir() / 'empty',
  },
}

subdir('nls')

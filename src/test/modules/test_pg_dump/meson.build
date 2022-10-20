test_install_files += files(
  'test_pg_dump.control',
  'test_pg_dump--1.0.sql',
)

tests += {
  'name': 'test_pg_dump',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_pg_dump',
    ],
    # doesn't delete its user
    'runningcheck': false,
  },
  'tap': {
    'tests': [
      't/001_base.pl',
    ],
  },
}

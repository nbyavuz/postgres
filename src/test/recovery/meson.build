# Copyright (c) 2022, PostgreSQL Global Development Group

tests += {
  'name': 'recovery',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'test_kwargs': {'priority': 40}, # recovery tests are slow, start early
    'tests': [
      't/031_recovery_conflict.pl',
    ],
  },
}

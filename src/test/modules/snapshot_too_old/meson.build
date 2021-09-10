isolation_tests += {
  'name': 'snapshot_too_old',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'specs': [
    'sto_using_cursor',
    'sto_using_select',
    'sto_using_hash_index',
  ],
  'regress_args': ['--temp-config', files('sto.conf')],
}

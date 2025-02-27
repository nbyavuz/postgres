# Copyright (c) 2022-2026, PostgreSQL Global Development Group

auth_delay_sources = files(
  'auth_delay.c',
)

if host_system == 'windows'
  auth_delay_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'auth_delay',
    '--FILEDESC', 'auth_delay - delay authentication failure reports',])
endif

auth_delay = shared_module('auth_delay',
  auth_delay_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += auth_delay

meson_extension_tests += {
  'name': 'auth_delay',
  'kind': 'pkg_config',
  'sd': meson.current_source_dir(),
  'input': files('meson-test.build'),
}
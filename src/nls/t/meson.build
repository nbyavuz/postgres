# Copyright (c) 2026, PostgreSQL Global Development Group

if libintl.found() and xgettext.found() and meson_impl == 'meson'
  ninja = find_program('ninja', 'ninja-build', required: false, native: true)
  if ninja.found()
    test('nls/catalogs', python,
      args: [files('test_nls.py'), flex.full_path(), bison.full_path(),
             meson_bin.full_path(), meson_args],
      suite: 'nls',
      timeout: 120,
    )
  endif
endif

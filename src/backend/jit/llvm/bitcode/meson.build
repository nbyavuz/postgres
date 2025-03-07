# Copyright (c) 2022-2024, PostgreSQL Global Development Group
#
# emit LLVM bitcode for JIT inlining

assert(llvm.found())

foreach bitcode_module : bitcode_modules
  bitcode_targets = []
  bitcode_obj = bitcode_module['target']
  bitcode_cflags_local = bitcode_cflags + bitcode_module.get('additional_flags', [])
  bitcode_name = bitcode_module.get('name', bitcode_obj.name())

  foreach srcfile : bitcode_module['srcfiles']
    if meson.version().version_compare('>=0.59')
      srcfilename = fs.parent(srcfile) / fs.name(srcfile)
    else
      srcfilename = '@0@'.format(srcfile)
    endif

    targetname = '@0@_@1@.bc'.format(
      bitcode_name,
      srcfilename.underscorify(),
    )
    bitcode_targets += custom_target(
      targetname,
      depends: [bitcode_obj],
      input: [srcfile],
      output: targetname,
      command: [llvm_irgen_command, bitcode_cflags_local],
      install: true,
      install_dir: dir_bitcode,
    )
  endforeach

  # Process generated sources, which may include custom compilation flags.
  foreach gen_sources: bitcode_module.get('gen_sources', [])
    bitcode_cflags_gen_local = bitcode_cflags_local + gen_sources.get('additional_flags', [])

    foreach srcfile: gen_sources['srcfiles']
      # Generated sources are stored in some folder under meson.build_root()/**,
      # remove the build prefix from the string.
      srcfilename = srcfile.full_path().split(meson.project_build_root() + '/')[1]

      targetname = '@0@_@1@.bc'.format(
        bitcode_name,
        srcfilename.underscorify(),
      )
      bitcode_targets += custom_target(
        targetname,
        depends: [bitcode_obj],
        input: [srcfile],
        output: targetname,
        command: [llvm_irgen_command, bitcode_cflags_gen_local],
        install: true,
        install_dir: dir_bitcode,
      )
    endforeach
  endforeach

  index_name = '@0@.index.bc'.format(bitcode_name)
  bitcode_index = custom_target('@0@'.format(bitcode_name),
    output: index_name,
    input: bitcode_targets,
    command: [irlink, '--lto', llvm_lto, '--outdir', '@OUTDIR@', '--index', index_name, '@INPUT@'],
    install: true,
    install_dir: dir_bitcode,
  )
  backend_targets += bitcode_index
endforeach

# Copyright (c) 2026, PostgreSQL Global Development Group

xgettext = find_program('xgettext', native: true, required: false)

nls_command = [
  python, files('../tools/maintain-po'),
  '--source-root', meson.project_source_root(),
  '--build-root', meson.project_build_root(),
  '--major-version', pg_version_major.to_string(),
  '--xgettext', xgettext.found() ? xgettext.full_path() : 'xgettext',
  '--extra-file', 'plperl', 'SPI.c',
  meson.project_source_root() / 'src/pl/plperl/SPI.xs',
]

# Some files are generated in different directories by Meson and Make.  Give
# them the names used in the catalogs, so that both builds produce the same
# source references.
foreach output : node_support_output
  if output.endswith('.c')
    path = meson.project_build_root() / 'src/include/nodes' / output
    nls_command += [
      '--extra-file', 'postgres', 'nodes/' + output, path,
      '--skip-file', 'postgres', path,
    ]
  endif
endforeach

foreach output : wait_event_output
  if output.endswith('.c')
    nls_command += [
      '--extra-file', 'postgres', 'utils/' + output,
      meson.project_build_root() / 'src/include/utils' / output,
    ]
  endif
endforeach

nls_command += [
  '--extra-file', 'postgres', 'utils/guc_tables.inc.c',
  meson.project_build_root() / 'src/include/utils/guc_tables.inc.c',

  '--extra-file', 'postgres', 'utils/fmgrtab.c',
  meson.project_build_root() / 'src/include/utils/fmgrtab.c',
  '--skip-file', 'postgres',
  meson.project_build_root() / 'src/include/utils/fmgrtab.c',
]

# A Make build links the selected implementations under these names.
if (cdata.has('USE_UNNAMED_POSIX_SEMAPHORES') or
    cdata.has('USE_NAMED_POSIX_SEMAPHORES'))
  nls_command += [
    '--extra-file', 'postgres', 'port/pg_sema.c',
    meson.project_source_root() / 'src/backend/port/posix_sema.c',
  ]
elif cdata.has('USE_SYSV_SEMAPHORES')
  nls_command += [
    '--extra-file', 'postgres', 'port/pg_sema.c',
    meson.project_source_root() / 'src/backend/port/sysv_sema.c',
  ]
elif cdata.has('USE_WIN32_SEMAPHORES')
  nls_command += [
    '--extra-file', 'postgres', 'port/pg_sema.c',
    meson.project_source_root() / 'src/backend/port/win32_sema.c',
  ]
endif

if cdata.has('USE_SYSV_SHARED_MEMORY')
  nls_command += [
    '--extra-file', 'postgres', 'port/pg_shmem.c',
    meson.project_source_root() / 'src/backend/port/sysv_shmem.c',
  ]
elif cdata.has('USE_WIN32_SHARED_MEMORY')
  nls_command += [
    '--extra-file', 'postgres', 'port/pg_shmem.c',
    meson.project_source_root() / 'src/backend/port/win32_shmem.c',
  ]
endif

xlogreader_source = meson.project_source_root() / 'src/backend/access/transam/xlogreader.c'
nls_command += [
  '--extra-file', 'pg_rewind', 'xlogreader.c', xlogreader_source,
  '--extra-file', 'pg_waldump', 'xlogreader.c', xlogreader_source,
  '--extra-file', 'pg_waldump', 'xlogstats.c',
  meson.project_source_root() / 'src/backend/access/transam/xlogstats.c',
]

# Use whole custom targets as dependencies for compatibility with the minimum
# supported Meson version, which does not accept individual custom-target
# outputs in run_target() dependencies.
nls_deps = [
  generated_nodes,
  wait_event_target,
  guc_tables,
  fmgrtab_target,
  backend_scanner,
  backend_parser,
  bootscanner,
  bootparse,
  repl_scanner,
  repl_gram,
  syncrep_scanner,
  syncrep_gram,
  jsonpath_scan,
  jsonpath_gram,
  guc_scan,
  psqlscanslash,
  psqlscan,
  tabcomplete,
  sql_help,
  pgc,
  preproc,
  pl_gram,
]

run_target('init-po',
  command: nls_command + ['init'],
  depends: nls_deps,
)

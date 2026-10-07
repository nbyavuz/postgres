# Copyright (c) 2026, PostgreSQL Global Development Group

maintain_po = files('maintain_po.py')
po_tools = []
po_test_tools = []
po_tool_files = {}
foreach tool : ['xgettext', 'msgmerge']
  program = find_program(tool, required: false, native: true)
  po_tools += ['--' + tool, program.found() ? program.full_path() : tool]
  if program.found()
    po_test_tools += ['--' + tool, program.full_path()]
    po_tool_files += {tool: files(program.full_path())}
  endif
endforeach

if po_test_tools.length() == 4
  test('po/maintain', python,
    args: [files('test_maintain_po.py'), po_test_tools],
    suite: 'po',
  )
endif

po_frontend_keywords = [
  'pg_fatal', 'pg_log_error', 'pg_log_error_detail', 'pg_log_error_hint',
  'pg_log_warning', 'pg_log_warning_detail', 'pg_log_warning_hint',
  'pg_log_info', 'pg_log_info_detail', 'pg_log_info_hint',
  'pg_log_generic:3', 'pg_log_generic_v:3',
]
po_frontend_flags = [
  'pg_fatal:1:c-format',
  'pg_log_error:1:c-format', 'pg_log_error_detail:1:c-format', 'pg_log_error_hint:1:c-format',
  'pg_log_warning:1:c-format', 'pg_log_warning_detail:1:c-format', 'pg_log_warning_hint:1:c-format',
  'pg_log_info:1:c-format', 'pg_log_info_detail:1:c-format', 'pg_log_info_hint:1:c-format',
  'pg_log_generic:3:c-format', 'pg_log_generic_v:3:c-format',
]
po_backend_keywords = po_frontend_keywords + [
  'errmsg', 'errmsg_plural:1,2', 'errdetail', 'errdetail_log',
  'errdetail_plural:1,2', 'errhint', 'errhint_plural:1,2', 'errcontext',
  'XactLockTableWait:4', 'MultiXactIdWait:6', 'ConditionalMultiXactIdWait:6',
]
po_backend_flags = po_frontend_flags + [
  'errmsg:1:c-format', 'errmsg_plural:1:c-format', 'errmsg_plural:2:c-format',
  'errdetail:1:c-format', 'errdetail_log:1:c-format',
  'errdetail_plural:1:c-format', 'errdetail_plural:2:c-format',
  'errhint:1:c-format', 'errhint_plural:1:c-format', 'errhint_plural:2:c-format',
  'errcontext:1:c-format',
]

# Include translations from optional components even when they are disabled.
# Reading LINGUAS registers reconfiguration dependencies for language changes.
po_directories = [
  'src/backend',
  'src/bin/initdb', 'src/bin/pg_amcheck', 'src/bin/pg_archivecleanup',
  'src/bin/pg_basebackup', 'src/bin/pg_checksums', 'src/bin/pg_combinebackup',
  'src/bin/pg_config', 'src/bin/pg_controldata', 'src/bin/pg_ctl',
  'src/bin/pg_dump', 'src/bin/pg_resetwal', 'src/bin/pg_rewind',
  'src/bin/pg_test_fsync', 'src/bin/pg_test_timing', 'src/bin/pg_upgrade',
  'src/bin/pg_verifybackup', 'src/bin/pg_waldump', 'src/bin/pg_walsummary',
  'src/bin/psql', 'src/bin/scripts',
  'src/interfaces/ecpg/ecpglib', 'src/interfaces/ecpg/preproc',
  'src/interfaces/libpq', 'src/pl/plperl', 'src/pl/plpgsql/src',
  'src/pl/plpython', 'src/pl/tcl', 'src/test/regress',
]
po_compendia = {}
foreach directory : po_directories
  foreach language : fs.read(meson.project_source_root() / directory / 'po/LINGUAS').split()
    po_compendia += {language: po_compendia.get(language, []) +
      files(meson.project_source_root() / directory / 'po' / (language + '.po'))}
  endforeach
endforeach

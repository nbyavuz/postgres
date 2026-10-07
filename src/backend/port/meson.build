# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'atomics.c',
)


foreach filename, enabled : {
  'posix_sema.c': cdata.has('USE_UNNAMED_POSIX_SEMAPHORES') or cdata.has('USE_NAMED_POSIX_SEMAPHORES'),
  'sysv_sema.c': cdata.has('USE_SYSV_SEMAPHORES'),
  'win32_sema.c': cdata.has('USE_WIN32_SEMAPHORES'),
  'sysv_shmem.c': cdata.has('USE_SYSV_SHARED_MEMORY'),
  'win32_shmem.c': cdata.has('USE_WIN32_SHARED_MEMORY'),
}
  sources = files(filename)
  backend_po_sources += sources
  if enabled
    backend_sources += sources
  endif
endforeach

subdir('win32')

# autoconf generates the file there, ensure we get a conflict
generated_sources_ac += {'src/backend/port': ['pg_sema.c', 'pg_shmem.c']}

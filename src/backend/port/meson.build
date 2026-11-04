# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'atomics.c',
)


posix_sema_sources = files('posix_sema.c')
sysv_sema_sources = files('sysv_sema.c')
win32_sema_sources = files('win32_sema.c')
sysv_shmem_sources = files('sysv_shmem.c')
win32_shmem_sources = files('win32_shmem.c')
backend_nls_sources += posix_sema_sources + sysv_sema_sources + win32_sema_sources + \
  sysv_shmem_sources + win32_shmem_sources

if cdata.has('USE_UNNAMED_POSIX_SEMAPHORES') or cdata.has('USE_NAMED_POSIX_SEMAPHORES')
  backend_sources += posix_sema_sources
endif

if cdata.has('USE_SYSV_SEMAPHORES')
  backend_sources += sysv_sema_sources
endif

if cdata.has('USE_WIN32_SEMAPHORES')
  backend_sources += win32_sema_sources
endif

if cdata.has('USE_SYSV_SHARED_MEMORY')
  backend_sources += sysv_shmem_sources
endif

if cdata.has('USE_WIN32_SHARED_MEMORY')
  backend_sources += win32_shmem_sources
endif

subdir('win32')

# autoconf generates the file there, ensure we get a conflict
generated_sources_ac += {'src/backend/port': ['pg_sema.c', 'pg_shmem.c']}

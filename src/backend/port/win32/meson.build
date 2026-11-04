# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_win32_sources = files(
  'crashdump.c',
  'signal.c',
  'socket.c',
  'timer.c',
)

backend_nls_sources += backend_win32_sources
if host_system == 'windows'
  backend_sources += backend_win32_sources
endif

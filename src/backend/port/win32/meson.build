# Copyright (c) 2022-2026, PostgreSQL Global Development Group

win32_port_sources = files(
  'crashdump.c',
  'signal.c',
  'socket.c',
  'timer.c',
)

backend_nls_sources += win32_port_sources
if host_system == 'windows'
  backend_sources += win32_port_sources
endif

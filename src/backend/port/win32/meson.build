# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_windows_sources = files(
  'crashdump.c',
  'signal.c',
  'socket.c',
  'timer.c',
)
backend_po_sources += backend_windows_sources
if host_system == 'windows'
  backend_sources += backend_windows_sources
endif

# src/bin/pg_waldump/nls.mk
CATALOG_NAME     = pg_waldump
# Use the original source paths, not the symlinks created only by Make.
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) \
                   archive_waldump.c \
                   pg_waldump.c \
                   ../../backend/access/transam/xlogreader.c \
                   ../../backend/access/transam/xlogstats.c \
                   ../../common/fe_memutils.c \
                   ../../common/file_utils.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS) \
                   report_invalid_record:2
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS) \
                   report_invalid_record:2:c-format

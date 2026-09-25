# src/nls-common.mk
#
# Shared extraction metadata for nls-global.mk and src/tools/maintain-po.py.
# Keep this file declarative: assignments, backslash continuations, and simple
# $(VARIABLE) references are understood by both consumers.  In particular, do
# not put Make functions or recipes here; Meson does not run Make to read it.

# Common settings that apply to backend and all backend modules.
BACKEND_COMMON_GETTEXT_TRIGGERS = \
    $(FRONTEND_COMMON_GETTEXT_TRIGGERS) \
    errmsg errmsg_plural:1,2 \
    errdetail errdetail_log errdetail_plural:1,2 \
    errhint errhint_plural:1,2 \
    errcontext \
    XactLockTableWait:4 \
    MultiXactIdWait:6 \
    ConditionalMultiXactIdWait:6
BACKEND_COMMON_GETTEXT_FLAGS = \
    $(FRONTEND_COMMON_GETTEXT_FLAGS) \
    errmsg:1:c-format errmsg_plural:1:c-format errmsg_plural:2:c-format \
    errdetail:1:c-format errdetail_log:1:c-format errdetail_plural:1:c-format errdetail_plural:2:c-format \
    errhint:1:c-format errhint_plural:1:c-format errhint_plural:2:c-format \
    errcontext:1:c-format

FRONTEND_COMMON_GETTEXT_FILES = $(top_srcdir)/src/common/logging.c

FRONTEND_COMMON_GETTEXT_TRIGGERS = \
    pg_log_error pg_log_error_detail pg_log_error_hint \
    pg_log_warning pg_log_warning_detail pg_log_warning_hint \
    pg_log_info pg_log_info_detail pg_log_info_hint \
    pg_fatal pg_log_generic:3 pg_log_generic_v:3

FRONTEND_COMMON_GETTEXT_FLAGS = \
    pg_log_error:1:c-format pg_log_error_detail:1:c-format pg_log_error_hint:1:c-format \
    pg_log_warning:1:c-format pg_log_warning_detail:1:c-format pg_log_warning_hint:1:c-format \
    pg_log_info:1:c-format pg_log_info_detail:1:c-format pg_log_info_hint:1:c-format \
    pg_fatal:1:c-format pg_log_generic:3:c-format pg_log_generic_v:3:c-format

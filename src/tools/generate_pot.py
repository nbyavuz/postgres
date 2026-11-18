#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Extract a PostgreSQL message catalog template from source files."""

import argparse
import os
import subprocess
from datetime import date

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--xgettext', required=True)
parser.add_argument('--srcdir', required=True, help="the program's source directory")
parser.add_argument('--catalog', required=True)
parser.add_argument('--version', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('inputs', nargs='+')
args = parser.parse_args()

xgettext_general_flags = [
    '-ctranslator',
    '--no-wrap',
    '--sort-by-file',
    '-k_',
    '--flag=_:1:pass-c-format',
    '--copyright-holder=PostgreSQL Global Development Group',
    '--msgid-bugs-address=pgsql-bugs@lists.postgresql.org',
    f'--package-name={args.catalog} (PostgreSQL)',
    f'--package-version={args.version}',
]

# xgettext records its input names in the catalog's #: comments.  Use the
# same program-relative names regardless of where the build tree lives.
sources = [os.path.relpath(path, args.srcdir).replace(os.sep, '/')
           for path in args.inputs]
pot = subprocess.check_output([
    args.xgettext,
    *xgettext_general_flags,
    '-o', '-', *sources,
], cwd=args.srcdir, encoding='utf-8')

# Restrict the substitutions to the header, since the messages themselves can
# say YEAR, etc.
header, separator, messages = pot.partition('\n\n')
header = header.replace('SOME DESCRIPTIVE TITLE.', f'LANGUAGE message translation file for {args.catalog}')
header = header.replace('PACKAGE', 'PostgreSQL').replace('VERSION', args.version)
header = header.replace('YEAR', str(date.today().year))
with open(args.output, 'w', encoding='utf-8', newline='\n') as out:
    out.write(header + separator + messages)

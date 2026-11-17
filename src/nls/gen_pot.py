#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Extract a catalog with source-tree-relative references, including #line."""

import argparse
import datetime
import os
from pathlib import Path
import re
import subprocess
import tempfile


def relative_path(path, root):
    """Return a relative path if path is within root, otherwise None."""
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return None


def normalize_line_directives(text, source_root, build_root, source_dir):
    """Generators run in the build root, but some emit bare source names."""
    def replace(match):
        filename = match.group(2)
        for directory in (build_root, source_root, source_dir):
            path = Path(os.path.abspath(directory / filename))
            if not path.is_file():
                continue
            for root in sorted((source_root, build_root),
                               key=lambda root: len(root.parts), reverse=True):
                relative = relative_path(path, root)
                if relative is not None:
                    return match.group(1) + '"' + relative + '"'
        return match.group(0)

    return re.sub(r'(?m)^(\s*#\s*(?:line\s+)?\d+\s+)"([^"]+)"',
                  replace, text)


def scanner_source(text):
    """Keep user code, with its #line directives, but omit Flex's skeleton.

    xgettext's C parser cannot read Flex's regular expressions reliably.
    Flex's output is C, and its #line directives delimit the user code.
    """
    result = []
    in_source = False
    for line in text.splitlines(keepends=True):
        directive = re.match(r'^#line\s+\d+\s+"([^"]+)"', line)
        if directive:
            in_source = directive.group(1).endswith('.l')
        result.append(line if in_source else '\n')
    return ''.join(result)


def generate(args):
    source_root = Path(os.path.abspath(args.source_root))
    build_root = Path(os.path.abspath(args.build_root))
    output = Path(os.path.abspath(args.output))
    scanners = {Path(os.path.abspath(name)) for name in args.scanner}
    roots = sorted((source_root, build_root),
                   key=lambda root: len(root.parts), reverse=True)

    with tempfile.TemporaryDirectory(dir=str(output.parent)) as temporary:
        temporary = Path(temporary)
        sources = set()
        for filename in args.inputs:
            path = Path(os.path.abspath(filename))
            for root in roots:
                relative = relative_path(path, root)
                if relative is not None:
                    break
            else:
                raise ValueError(
                    'input is outside the source and build trees: ' + filename)

            if root == build_root:
                # Stage generated sources under their logical names.  This also
                # gives us a place to normalize #line without changing a file
                # that another build target might be reading.
                staged = temporary / relative
                staged.parent.mkdir(parents=True, exist_ok=True)
                text = path.read_text(encoding='utf-8')
                if path in scanners:
                    text = scanner_source(text)
                text = normalize_line_directives(
                    text, source_root, build_root,
                    source_root / Path(relative).parent)
                staged.write_text(text, encoding='utf-8')
            sources.add(relative)

        # A file list also avoids xgettext's command-line length limit.
        file_list = temporary / 'POTFILES'
        file_list.write_text(''.join(name + '\n' for name in sorted(sources)),
                            encoding='utf-8')
        pot = temporary / 'messages.pot'
        command = [
            args.xgettext,
            '--language=C', '--from-code=UTF-8', '--add-comments=translator',
            '--copyright-holder=PostgreSQL Global Development Group',
            '--msgid-bugs-address=pgsql-bugs@lists.postgresql.org',
            '--no-wrap', '--sort-by-file', '--force-po',
            '--package-name=' + args.catalog + ' (PostgreSQL)',
            '--package-version=' + args.version,
            '--directory=' + str(source_root),
            '--directory=' + str(temporary),
            '--files-from=' + str(file_list),
            '--output=' + str(pot),
        ]
        command += ['--keyword=' + keyword for keyword in args.keyword]
        command += ['--flag=' + flag for flag in args.flag]
        subprocess.run(command, check=True)

        lines = pot.read_text(encoding='utf-8').splitlines(keepends=True)
        for index in range(min(18, len(lines))):
            lines[index] = (lines[index]
                            .replace('SOME DESCRIPTIVE TITLE.',
                                     'LANGUAGE message translation file for ' + args.catalog)
                            .replace('PACKAGE', 'PostgreSQL')
                            .replace('VERSION', args.version)
                            .replace('YEAR', str(datetime.date.today().year)))
        pot.write_text(''.join(lines), encoding='utf-8')
        os.replace(str(pot), str(output))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--xgettext', required=True)
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--source-root', required=True)
    parser.add_argument('--build-root', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--keyword', action='append', default=[])
    parser.add_argument('--flag', action='append', default=[])
    parser.add_argument('--scanner', action='append', default=[])
    parser.add_argument('inputs', nargs='+')
    generate(parser.parse_args())

#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Generate message catalogs without exposing source or build directory names."""

import argparse
import datetime
import os
from pathlib import Path
import re
import subprocess
import tempfile


def extract(args):
    source_root = Path(args.source_root).resolve()
    build_root = Path(args.build_root).resolve()
    catalog_dir = Path(args.catalog_dir)

    def relative_name(path):
        path = Path(os.path.abspath(path))
        # A build directory may be inside the source tree.
        for root in (build_root, source_root):
            try:
                return path.relative_to(root)
            except ValueError:
                pass
        raise ValueError(f'extraction input is outside the source and build trees: {path}')

    inputs = {}
    for filename in args.inputs:
        path = Path(filename).absolute()
        if path.suffix not in ('.c', '.h', '.cpp', '.y', '.xs'):
            continue
        name = relative_name(path)
        inputs[name] = path

    # Grammar inputs are registered explicitly, so changes to them (including
    # generated grammars) participate in the build graph.  Do not extract the
    # gettext calls in Bison's skeleton.  Flex output is needed to avoid parsing
    # scanner regular expressions as C strings.
    for name in list(inputs):
        if name.suffix == '.c' and name.with_suffix('.y') in inputs:
            del inputs[name]

    # Stage inputs with stable names.  This also handles #line directives from
    # generators, without modifying their outputs or rewriting msgid text.
    with tempfile.TemporaryDirectory(prefix='postgres-nls-') as tmp:
        stage = Path(tmp)
        for name, path in sorted(inputs.items()):
            text = path.read_text(encoding='utf-8')

            def line_directive(match):
                filename = match[2]
                if os.path.isabs(filename):
                    filename = relative_name(filename).as_posix()
                else:
                    # Generators run in either their own directory or the
                    # build root.  Prefer paths relative to their output.
                    candidate = path.parent / filename
                    if not candidate.is_file():
                        candidate = build_root / filename
                    if candidate.is_file():
                        filename = relative_name(candidate).as_posix()
                    else:
                        filename = (name.parent / filename).as_posix()
                return match[1] + filename + match[3]

            text = re.sub(r'^(\s*#\s*(?:line\s+)?\d+\s+")([^"]+)(".*)$',
                          line_directive, text, flags=re.MULTILINE)
            output = stage / name
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(text, encoding='utf-8')

        filelist = stage / 'files'
        filelist.write_text(''.join(f'{name.as_posix()}\n' for name in sorted(inputs)),
                            encoding='utf-8')
        result = stage / 'messages.pot'
        command = [args.xgettext, '--language=C', '--from-code=UTF-8',
                   '--add-comments=translator', '--no-wrap', '--sort-by-file',
                   '--copyright-holder=PostgreSQL Global Development Group',
                   '--msgid-bugs-address=pgsql-bugs@lists.postgresql.org',
                   f'--package-name={args.catalog} (PostgreSQL)',
                   f'--package-version={args.version}',
                   '--keyword=_', '--flag=_:1:pass-c-format',
                   '--force-po', '--files-from=files', '--output=messages.pot']
        command += ['--keyword=' + keyword for keyword in args.keyword]
        command += ['--flag=' + flag for flag in args.flag]
        subprocess.run(command, cwd=stage, check=True)
        text = result.read_text(encoding='utf-8')
        lines = text.splitlines(keepends=True)
        for i, line in enumerate(lines):
            if line.startswith('#: '):
                references = []
                for reference in line[3:].split():
                    filename, separator, number = reference.rpartition(':')
                    if not separator:
                        filename, number = reference, ''
                    filename = os.path.relpath(filename, catalog_dir).replace(os.sep, '/')
                    references.append(filename + (':' + number if number else ''))
                lines[i] = '#: ' + ' '.join(references) + '\n'
            elif i < 18:
                lines[i] = (line.replace('SOME DESCRIPTIVE TITLE.',
                                        f'LANGUAGE message translation file for {args.catalog}')
                            .replace('PACKAGE', 'PostgreSQL')
                            .replace('VERSION', args.version)
                            .replace('YEAR', str(datetime.date.today().year)))
        write_output(args.output, ''.join(lines))


def write_output(filename, text):
    """Publish a complete output, leaving the old file intact on failure."""
    output = Path(filename).absolute()
    with tempfile.TemporaryDirectory(dir=output.parent, prefix='nls-') as tmp:
        result = Path(tmp) / output.name
        result.write_text(text, encoding='utf-8')
        os.replace(result, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    pot = commands.add_parser('extract')
    for name in ('source-root', 'build-root', 'catalog-dir', 'catalog', 'version',
                 'output', 'xgettext'):
        pot.add_argument('--' + name, required=True)
    pot.add_argument('--keyword', action='append', default=[])
    pot.add_argument('--flag', action='append', default=[])
    pot.add_argument('inputs', nargs='+')
    args = parser.parse_args()
    extract(args)


if __name__ == '__main__':
    main()

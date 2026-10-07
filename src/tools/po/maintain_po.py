#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Extract and merge message catalogs for Meson custom targets."""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def logical_path(path, source_root, build_root, subdir):
    """Map a physical source or generated file to a catalog-relative name."""
    path = Path(os.path.abspath(path))
    for root in (build_root, source_root):
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        return Path(os.path.relpath(source_root / relative,
                                   source_root / subdir)).as_posix()
    raise ValueError('extraction input is outside the source and build trees: ' + str(path))


def normalize_lines(text, path, args):
    # Meson commands run from the build root.  xsubpp can also emit a bare
    # filename relative to its input directory.  Resolve those spellings
    # against existing files, rather than blindly stripping ../ prefixes.
    pattern = re.compile(r'^(\s*#\s*(?:line\s+)?\d+\s+)("(?:[^"\\]|\\.)*")', re.M)

    def replace(match):
        name = json.loads(match[2])
        candidates = [Path(name)] if Path(name).is_absolute() else [
            args.build_root / name, path.parent / name,
            args.source_root / args.subdir / name,
        ]
        for candidate in candidates:
            if candidate.is_file():
                logical = logical_path(candidate, args.source_root, args.build_root, args.subdir)
                return match[1] + json.dumps(logical)
        raise ValueError('cannot resolve line directive in ' + str(path) + ': ' + name)

    return pattern.sub(replace, text)


def scanner_source(text):
    """Keep scanner actions and prologues, excluding Flex's implementation."""
    directive = re.compile(r'^\s*#\s*line\s+\d+\s+"([^"]+)"')
    if not re.search(r'^#line \d+ "[^"\n]+\.l"', text, re.M):
        return text
    owned = False
    lines = []
    for line in text.splitlines(keepends=True):
        match = directive.match(line)
        if match:
            owned = match[1].endswith('.l')
        lines.append(line if owned else '\n')
    return ''.join(lines)


def extract(args, temporary):
    # Stage a source-shaped tree so xgettext sees logical names, including
    # references to sources outside the catalog directory.  Copies isolate
    # #line normalization from the files used by the compiler.
    stage = temporary / 'sources'
    catalog_dir = stage / args.subdir
    catalog_dir.mkdir(parents=True)
    inputs = {}
    filenames = args.inputs or args.files_from.read_text(encoding='utf-8').splitlines()
    for filename in filenames:
        path = Path(filename).absolute()
        logical = logical_path(path, args.source_root, args.build_root, args.subdir)
        if logical in inputs and inputs[logical] != path:
            raise ValueError('ambiguous extraction input: ' + logical)
        inputs[logical] = path
    for logical, path in inputs.items():
        destination = catalog_dir / logical
        destination.parent.mkdir(parents=True, exist_ok=True)
        text = path.read_text(encoding='utf-8')
        destination.write_text(normalize_lines(scanner_source(text), path, args), encoding='utf-8')

    file_list = temporary / 'files'
    file_list.write_text('\n'.join(sorted(inputs)) + '\n', encoding='utf-8')
    result = temporary / 'messages.pot'
    command = [args.xgettext, '--language=C', '-ctranslator',
               '--copyright-holder=PostgreSQL Global Development Group',
               '--msgid-bugs-address=pgsql-bugs@lists.postgresql.org',
               '--no-wrap', '--sort-by-file', '--force-po',
               '--package-name=' + args.name + ' (PostgreSQL)',
               '--package-version=' + args.version,
               '-f', str(file_list), '-n', '-o', str(result)]
    command += ['--keyword=' + keyword for keyword in args.keyword]
    command += ['--flag=' + flag for flag in args.flag]
    subprocess.run(command, cwd=catalog_dir, check=True)
    lines = result.read_text(encoding='utf-8').splitlines(keepends=True)
    for index in range(min(18, len(lines))):
        lines[index] = (lines[index]
                        .replace('SOME DESCRIPTIVE TITLE.',
                                 'LANGUAGE message translation file for ' + args.name, 1)
                        .replace('PACKAGE', 'PostgreSQL')
                        .replace('VERSION', args.version)
                        .replace('YEAR', str(datetime.date.today().year)))
    result.write_text(''.join(lines), encoding='utf-8')
    return result


def merge(args, temporary):
    definition = args.original or args.template
    result = temporary / 'merged.po.new'
    command = [args.msgmerge, '--no-wrap', '--previous', '--sort-by-file',
               '--lang=' + args.language, str(definition), str(args.template),
               '-o', str(result)]
    command += ['--compendium=' + str(path) for path in sorted(set(args.compendia))
                if path != definition]
    subprocess.run(command, check=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['extract', 'merge'])
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--build-root', type=Path, required=True)
    parser.add_argument('--subdir', required=True)
    parser.add_argument('--name', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--xgettext', default='xgettext')
    parser.add_argument('--msgmerge', default='msgmerge')
    parser.add_argument('--keyword', action='append', default=[])
    parser.add_argument('--flag', action='append', default=[])
    parser.add_argument('--inputs', nargs='+')
    parser.add_argument('--files-from', type=Path)
    parser.add_argument('--template', type=Path)
    parser.add_argument('--original', type=Path)
    parser.add_argument('--language')
    parser.add_argument('--compendia', type=Path, nargs='*', default=[])
    args = parser.parse_args()
    args.source_root = args.source_root.absolute()
    args.build_root = args.build_root.absolute()
    if args.operation == 'extract' and not (args.inputs or args.files_from):
        parser.error('extract requires --inputs or --files-from')
    if args.operation == 'merge' and (not args.template or not args.language):
        parser.error('merge requires --template and --language')
    with tempfile.TemporaryDirectory(dir=args.output.parent) as temporary:
        temporary = Path(temporary).absolute()
        result = extract(args, temporary) if args.operation == 'extract' else merge(args, temporary)
        os.replace(result, args.output)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(os.path.basename(__file__) + ': ' + str(error))

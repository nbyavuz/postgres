#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Extract and merge PostgreSQL message catalogs without invoking Make."""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def expand(values, common, source_root, parents=()):
    result = []
    for value in values:
        if value.startswith('@') and value.endswith('@'):
            key = value[1:-1]
            if key in parents:
                raise ValueError('recursive common list: ' + key)
            if key not in common:
                raise ValueError('unknown common list: ' + key)
            result.extend(expand(common[key], common, source_root, parents + (key,)))
        else:
            result.append(value.replace('@source@', str(source_root)))
    return result


def read_metadata(path):
    with open(path, encoding='utf-8') as file:
        return json.load(file)


def extract(args, catalog, common):
    source_dir = args.source_root / args.subdir
    build_dir = args.build_root / args.subdir
    output_dir = build_dir / 'po'
    output_dir.mkdir(parents=True, exist_ok=True)
    pot = output_dir / (catalog['name'] + '.pot')
    inputs = []

    if 'scan' in catalog:
        # Match the backend's gettext-files rule, including its path spellings
        # and sorting order.  Do not scan arbitrary build-tree C files: those
        # include platform-dependent and duplicate generated sources.
        inputs = []
        for directory in (source_dir / name for name in catalog['scan']):
            for parent, dirs, files in os.walk(str(directory)):
                for name in files:
                    if name.endswith('.c') or name == 'proctypelist.h':
                        inputs.append(os.path.join(parent, name))
        inputs.sort()
    else:
        inputs = expand(catalog['files'], common, args.source_root)

    # Make supplies symlinks for these shared backend files.  Meson compiles
    # them directly, so resolve them here while retaining the catalog references.
    aliases = {'xlogreader.c': 'src/backend/access/transam/xlogreader.c',
               'xlogstats.c': 'src/backend/access/transam/xlogstats.c'}
    with tempfile.TemporaryDirectory(dir=str(build_dir)) as temp:
        temp = Path(temp)
        for name, path in aliases.items():
            if name in inputs and not (source_dir / name).exists():
                (temp / name).write_bytes((args.source_root / path).read_bytes())

        # Generators are run from the build root by Ninja and from the program
        # directory by Make.  Normalize their #line directives before gettext
        # reads them, so references, line wrapping, and message order match.
        for name in inputs:
            generated = build_dir / name
            if (Path(name).is_absolute() or (source_dir / name).exists() or
                    not generated.exists()):
                continue

            def line_directive(match):
                path = match[2]
                if path.startswith('../'):
                    path = os.path.abspath(str(args.build_root / path))
                elif path.startswith(args.subdir + '/'):
                    path = path[len(args.subdir) + 1:]
                # pgflex uses absolute output paths.  Make runs flex in the
                # scanner's directory, so its self-references use the basename.
                # Leave source and unrelated directives alone.
                if path == os.path.abspath(str(generated)):
                    path = generated.name
                return match[1] + path + match[3]

            text = generated.read_text(encoding='utf-8')
            text = re.sub(r'(^#(?:line)?\s+\d+\s+")([^"]+)(")',
                          line_directive, text, flags=re.MULTILINE)
            staged = temp / 'generated' / args.subdir / name
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_text(text, encoding='utf-8')

        command = [args.xgettext, '-ctranslator',
                   '--copyright-holder=PostgreSQL Global Development Group',
                   '--msgid-bugs-address=pgsql-bugs@lists.postgresql.org',
                   '--no-wrap', '--sort-by-file', '--force-po',
                   '--package-name=' + catalog['name'] + ' (PostgreSQL)',
                   '--package-version=' + args.version,
                   '-D', str(source_dir), '-D', str(temp / 'generated' / args.subdir),
                   '-D', str(build_dir), '-D', str(temp),
                   '-n', '-o', str(temp / 'messages.pot')]
        command.extend('-k' + value for value in
                       expand(catalog.get('keywords', []), common, args.source_root) + ['_'])
        command.extend('--flag=' + value for value in
                       expand(catalog.get('flags', []), common, args.source_root) +
                       ['_:1:pass-c-format'])
        # A file list avoids command-line length limits, particularly for the
        # backend, and keeps source paths with spaces as single arguments.
        with open(temp / 'files', 'w', encoding='utf-8') as file:
            file.write('\n'.join(inputs) + '\n')
        command.extend(['-f', str(temp / 'files')])
        subprocess.run(command, cwd=str(build_dir), check=True)

        with open(temp / 'messages.pot', encoding='utf-8') as file:
            lines = file.readlines()
        title = 'LANGUAGE message translation file for ' + catalog['name']
        for index in range(min(18, len(lines))):
            lines[index] = (lines[index]
                            .replace('SOME DESCRIPTIVE TITLE.',
                                     title, 1)
                            .replace('PACKAGE', 'PostgreSQL')
                            .replace('VERSION', args.version)
                            .replace('YEAR', str(datetime.date.today().year)))
        with open(temp / 'catalog.pot', 'w', encoding='utf-8') as file:
            file.writelines(lines)
        os.replace(str(temp / 'catalog.pot'), str(pot))
    return pot


def update(args, pot):
    compendia = {}
    # Match Make's recursive search, but do not follow directory symlinks.
    for parent, dirs, files in os.walk(str(args.source_root)):
        for name in files:
            if name.endswith('.po'):
                compendia.setdefault(name[:-3], []).append(Path(parent) / name)

    wanted = os.environ.get('WANTED_LANGUAGES', '').split()
    available = (args.source_root / args.subdir / 'po/LINGUAS').read_text().split()
    for language in sorted(compendia):
        if wanted and language not in wanted:
            continue
        original = args.source_root / args.subdir / 'po' / (language + '.po')
        definition = original if language in available else pot
        output = pot.parent / (language + '.po.new')
        command = [args.msgmerge, '--no-wrap', '--previous', '--sort-by-file',
                   '--lang=' + language, str(definition), str(pot)]
        command.extend('--compendium=' + str(path)
                       for path in sorted(compendia[language])
                       if path != definition)
        # Never leave a partially written catalog on a failed merge.
        with tempfile.TemporaryDirectory(dir=str(pot.parent)) as temp:
            merged = Path(temp) / 'merged.po.new'
            subprocess.run(command + ['-o', str(merged)], check=True)
            os.replace(str(merged), str(output))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['name', 'init', 'update'])
    parser.add_argument('--metadata', type=Path)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--build-root', type=Path)
    parser.add_argument('--subdir', nargs='+')
    parser.add_argument('--version')
    parser.add_argument('--xgettext', default='xgettext')
    parser.add_argument('--msgmerge', default='msgmerge')
    args = parser.parse_args()
    if args.operation == 'name':
        if args.metadata is None:
            parser.error('--metadata is required')
        print(read_metadata(args.metadata)['name'])
        return
    for name in ('source_root', 'build_root', 'subdir', 'version'):
        if getattr(args, name) is None:
            parser.error('--' + name.replace('_', '-') + ' is required')
    args.source_root = args.source_root.absolute()
    args.build_root = args.build_root.absolute()
    common = read_metadata(args.source_root / 'src/nls-common.json')
    subdirs = args.subdir
    for subdir in subdirs:
        args.subdir = subdir
        catalog = read_metadata(args.source_root / subdir / 'nls.json')
        pot = extract(args, catalog, common)
        if args.operation == 'update':
            update(args, pot)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit('message catalog maintenance failed: ' + str(error))

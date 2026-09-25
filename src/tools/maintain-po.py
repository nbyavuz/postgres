#!/usr/bin/env python3
#
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Maintain PostgreSQL message catalogs without invoking Make."""

import argparse
import datetime
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def read_assignments(path, variables):
    """Read the declarative subset shared by PostgreSQL's nls.mk files."""
    # This is intentionally not a Make interpreter.  In particular, accepting
    # and ignoring an unfamiliar recipe or expression could silently lose
    # messages.  Reject such input instead, with its location for the author.
    text = path.read_text(encoding='utf-8')
    text = re.sub(r'\\\n', ' ', text)
    for number, line in enumerate(text.splitlines(), 1):
        line = line.split('#', 1)[0].strip()
        if not line:
            continue
        match = re.fullmatch(r'([A-Za-z_][A-Za-z_0-9]*)\s*=\s*(.*)', line)
        if not match:
            raise ValueError(f'{path}:{number}: expected a variable assignment')
        variables[match[1]] = match[2]


def expand(name, variables, expanding=()):
    # Make's "=" assignments are recursive: a common definition may refer to
    # another definition appearing later in the file.  Expand only after both
    # metadata files have been read, and diagnose misspellings and cycles.
    if name not in variables:
        raise ValueError(f'undefined metadata variable "{name}"')
    if name in expanding:
        raise ValueError(f'recursive metadata variable "{name}"')
    value = variables[name]
    reference = re.compile(r'\$\(([A-Za-z_][A-Za-z_0-9]*)\)')
    if '$' in reference.sub('', value):
        raise ValueError(f'unsupported expression in metadata variable "{name}"')
    return reference.sub(
        lambda match: expand(match[1], variables, expanding + (name,)), value)


def source_files(args, variables, directory):
    names = expand('GETTEXT_FILES', variables).split()
    if names == ['+', 'gettext-files'] and directory == Path('src/backend'):
        # The backend deliberately includes sources for every platform and
        # optional feature, not just those compiled in this configuration.
        # Generated inputs come from the build system explicitly, never from
        # a scan of a possibly stale build.  In an in-source Make build the
        # scan also sees generated files and header-directory symlinks; exclude
        # those identities and add each declared generated input exactly once.
        names = []
        generated = {Path(path).resolve() for path in args.generated}
        for subdir in ('backend', 'common', 'port', 'include'):
            for path in (args.source_root / 'src' / subdir).rglob('*'):
                if (path.is_file() and path.resolve() not in generated and
                        (path.suffix == '.c' or path.name == 'proctypelist.h')):
                    names.append(os.path.relpath(path, args.catalog_dir))
        names += [os.path.relpath(path, args.build_root / directory)
                  for path in args.generated if Path(path).suffix == '.c']
    elif names and names[0] == '+':
        if len(names) != 2:
            raise ValueError('GETTEXT_FILES requires one filename after +')
        path = args.build_root / directory / names[1]
        if not path.is_file():
            path = args.catalog_dir / names[1]
        names = path.read_text(encoding='utf-8').splitlines()

    # Resolve each logical filename in the two trees.  Run xgettext in the
    # component's build directory with the source directory as its search path;
    # this retains component-relative references without embedding build roots
    # in catalogs.  Generated files have the same logical paths in both trees.
    for name in names:
        if not ((args.catalog_dir / name).is_file() or
                (args.build_root / directory / name).is_file()):
            raise ValueError(f'translation source "{directory / name}" not found')
    return sorted(set(os.path.normpath(name).replace(os.sep, '/')
                      for name in names))


def make_template(args, variables, directory, catalog, output, work):
    names = source_files(args, variables, directory)
    # Stage only generated inputs, in a mirror of the build tree.  Normalize
    # their #line directives before xgettext sorts and wraps references, rather
    # than rewriting the finished PO file.  Ordinary component-relative input
    # paths (such as ../common/exec.c) must never undergo this transformation.
    staged = work / 'inputs'
    (staged / directory).mkdir(parents=True)
    for generated in args.generated:
        path = Path(generated)
        relative = path.relative_to(args.build_root)
        destination = staged / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        text = path.read_text(encoding='utf-8')

        def rewrite(match):
            filename = re.sub(r'\\([\\"])', r'\1', match[2])
            filename = line_filename(filename, str(args.source_root),
                                     str(args.build_root), str(directory),
                                     cwd=args.generator_dirs.get(str(path)))
            return match[1] + filename.replace('\\', '\\\\').replace('"', '\\"') + '"'

        text = re.sub(r'^(\s*#\s*(?:line\s+)?\d+\s+")((?:[^"\\]|\\.)*)"',
                      rewrite, text, flags=re.MULTILINE)
        destination.write_text(text, encoding='utf-8')

    file_list = work / 'gettext-files'
    file_list.write_text(''.join(name + '\n' for name in names), encoding='utf-8')
    raw = work / 'messages.po'
    command = [
        args.xgettext, '-D', str(staged / directory),
        '-D', str(args.catalog_dir), '-D', '.', '-n',
        '-ctranslator', '--copyright-holder=PostgreSQL Global Development Group',
        '--msgid-bugs-address=pgsql-bugs@lists.postgresql.org',
        '--no-wrap', '--sort-by-file',
        f'--package-name={catalog} (PostgreSQL)',
        f'--package-version={args.version}',
        '-k_', '--flag=_:1:pass-c-format',
    ]
    command += ['-k' + word for word in
                expand('GETTEXT_TRIGGERS', variables).split()]
    command += ['--flag=' + word for word in
                expand('GETTEXT_FLAGS', variables).split()]
    command += ['-f', str(file_list), '-o', str(raw)]
    subprocess.run(command, cwd=args.build_root / directory, check=True)

    # Apply the same header substitutions as nls-global.mk.  Restrict them to
    # the header so that messages containing PACKAGE, VERSION, or YEAR survive.
    lines = raw.read_text(encoding='utf-8').splitlines(keepends=True)
    for index in range(min(18, len(lines))):
        lines[index] = (lines[index]
                        .replace('SOME DESCRIPTIVE TITLE.',
                                 f'LANGUAGE message translation file for {catalog}')
                        .replace('PACKAGE', 'PostgreSQL')
                        .replace('VERSION', args.version)
                        .replace('YEAR', str(datetime.date.today().year)))
    raw.write_text(''.join(lines), encoding='utf-8')
    # A failed gettext invocation must not leave a partially written catalog.
    # The temporary directory is under the output directory for atomic rename.
    os.replace(raw, output)


def line_filename(filename, source_root, build_root, directory, path=os.path,
                  cwd=None):
    """Convert a generator's #line filename to a component-relative path."""
    if cwd is None and not path.dirname(filename):
        return filename
    absolute = path.normpath(path.join(cwd or build_root, filename))
    # Test the build root first because it may be inside the source tree.
    # Only compute a relative path after identifying its own tree: Windows
    # cannot compute relpath(source_root, build_root) across different drives.
    for root in (build_root, source_root):
        try:
            if path.normcase(path.commonpath([absolute, root])) != path.normcase(root):
                continue
        except ValueError:
            continue
        return path.relpath(absolute, path.join(root, directory)).replace('\\', '/')
    # Some generators use a bare component-local filename, or a pseudo-file.
    return filename


def clean_catalogs(output_dir, catalog):
    # Language selection changes at execution time, so Ninja cannot know all
    # .po.new outputs at configuration time.  Clean only maintenance products,
    # preserving source translations, compiled catalogs, and unrelated files.
    products = [output_dir / (catalog + '.pot')]
    products += list(output_dir.glob('*.po.new'))
    for product in products:
        if product.is_file():
            product.unlink()


def update_catalogs(args, template, output_dir, work):
    # Like Make's update-po, reuse translations throughout the source tree,
    # including languages not yet listed in this component's LINGUAS.  A build
    # inside the source tree must not contribute a private install or test copy.
    compendia = {}
    for root, dirs, files in os.walk(args.source_root):
        dirs[:] = sorted(d for d in dirs if d != '.git' and
                         (Path(root) / d).resolve() != args.build_root)
        for name in sorted(files):
            if name.endswith('.po'):
                compendia.setdefault(name[:-3], []).append(Path(root) / name)

    linguas = (args.catalog_dir / 'po/LINGUAS').read_text(encoding='utf-8')
    available = set(' '.join(line.split('#', 1)[0]
                             for line in linguas.splitlines()).split())
    # WANTED_LANGUAGES has the same role as in nls-global.mk.  Read it at target
    # execution time, allowing a translator to select languages without a
    # reconfiguration or changing which installed translations are built.
    wanted = args.language or os.environ.get('WANTED_LANGUAGES', '').split()
    # An explicit language also supports Make's direct po/LANG.po.new target
    # when no source-tree catalog for that language exists yet.
    for language in sorted(set(compendia) | set(args.language)):
        if wanted and language not in wanted:
            continue
        original = args.catalog_dir / 'po' / (language + '.po')
        # Merging a new language against the template itself preserves this
        # catalog's header rather than adopting a compendium's header.
        if language not in available:
            original = template
        merged = work / (language + '.po.new')
        command = [args.msgmerge, '--no-wrap', '--previous', '--sort-by-file',
                   '--lang=' + language, str(original), str(template),
                   '-o', str(merged)]
        command += ['--compendium=' + str(path)
                    for path in sorted(compendia.get(language, []))]
        subprocess.run(command, check=True)
        os.replace(merged, output_dir / merged.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['init-po', 'update-po', 'clean-po'])
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--build-root', type=Path, required=True)
    parser.add_argument('--catalog-dir', type=Path, required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--xgettext', default='xgettext')
    parser.add_argument('--msgmerge', default='msgmerge')
    parser.add_argument('--generated', nargs='*', default=[])
    parser.add_argument('--generated-cwd', nargs=2, action='append', default=[],
                        metavar=('FILE', 'DIRECTORY'),
                        help='generated input and the working directory of its generator')
    parser.add_argument('--language', action='append', default=[],
                        help='merge only this language, even if it has no compendium')
    parser.add_argument('--stamp', type=Path)
    parser.add_argument('--skip-extraction', action='store_true',
                        help='use a template already generated by init-po')
    args = parser.parse_args()
    args.source_root = args.source_root.resolve()
    args.build_root = args.build_root.resolve()
    args.catalog_dir = args.catalog_dir.resolve()
    args.generator_dirs = dict(args.generated_cwd)
    args.generated += list(args.generator_dirs)

    try:
        # Check tools only when requested.  Ordinary builds need msgfmt, but
        # should not acquire new dependencies on these maintainer programs.
        tools = []
        if args.action != 'clean-po' and not args.skip_extraction:
            tools.append(args.xgettext)
        if args.action == 'update-po':
            tools.append(args.msgmerge)
        for tool in tools:
            if not shutil.which(tool):
                raise ValueError(f'required program not found: "{tool}"')

        directory = args.catalog_dir.relative_to(args.source_root)
        variables = {'GETTEXT_TRIGGERS': '', 'GETTEXT_FLAGS': '',
                     'top_srcdir': os.path.relpath(args.source_root,
                                                args.catalog_dir)}
        read_assignments(args.source_root / 'src/nls-common.mk', variables)
        read_assignments(args.catalog_dir / 'nls.mk', variables)
        catalog = expand('CATALOG_NAME', variables).strip()
        output_dir = args.build_root / directory / 'po'
        if args.action == 'clean-po':
            clean_catalogs(output_dir, catalog)
            if args.stamp:
                args.stamp.touch()
            return
        output_dir.mkdir(parents=True, exist_ok=True)
        template = output_dir / (catalog + '.pot')
        with tempfile.TemporaryDirectory(dir=output_dir) as tmp:
            work = Path(tmp)
            if not args.skip_extraction:
                make_template(args, variables, directory, catalog, template, work)
            if args.action == 'update-po':
                update_catalogs(args, template, output_dir, work)
        if args.stamp:
            args.stamp.touch()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'{parser.prog}: {error}\n')


if __name__ == '__main__':
    main()

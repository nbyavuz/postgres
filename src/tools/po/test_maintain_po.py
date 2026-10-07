#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Exercise catalog extraction and merging without a PostgreSQL installation."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

helper = Path(__file__).resolve().with_name('maintain_po.py')


class CatalogTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / 'source with spaces'
        self.build = self.root / 'build with spaces'
        self.subdir = 'src/example'
        (self.source / self.subdir).mkdir(parents=True)
        (self.build / self.subdir / 'po').mkdir(parents=True)
        self.output = self.build / self.subdir / 'po/example.pot'

    def command(self, operation, *args, success=True):
        command = [sys.executable, str(helper), operation,
                   '--source-root', str(self.source), '--build-root', str(self.build),
                   '--subdir', self.subdir, '--name', 'example', '--version', '20',
                   '--output', str(self.output), '--xgettext', tools.xgettext,
                   '--msgmerge', tools.msgmerge, *map(str, args)]
        result = subprocess.run(command, cwd=self.build, capture_output=True, text=True)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, '')
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def test_generated_scanner_and_relocation(self):
        original = self.source / self.subdir / 'scanner.l'
        original.write_text('/* scanner source */\n')
        for build_name in ['build with spaces', 'another build']:
            self.build = self.root / build_name
            generated = self.build / self.subdir / 'scanner.c'
            generated.parent.mkdir(parents=True, exist_ok=True)
            self.output = generated.parent / 'example.pot'
            generated.write_text(
                '#line 1 ' + json.dumps(str(generated)) + '\n'
                'gettext("generator implementation");\n'
                '#line 7 ' + json.dumps(str(original)) + '\n'
                '/* translator: scanner action */\n'
                'errmsg("user message %s");\n'
                '#line 30 ' + json.dumps(str(generated)) + '\n'
                'gettext("more scaffolding");\n')
            before = generated.read_bytes()
            self.command('extract', '--keyword=errmsg', '--flag=errmsg:1:c-format',
                         '--inputs', generated)
            text = self.output.read_text()
            self.assertIn('#: scanner.l:8', text)
            self.assertIn('#. translator: scanner action', text)
            self.assertIn('#, c-format', text)
            self.assertNotIn('scaffolding', text)
            self.assertNotIn('generator implementation', text)
            self.assertNotIn(str(self.root), text)
            self.assertEqual(before, generated.read_bytes())
            text = re.sub(r'"POT-Creation-Date:.*\n', '', text)
            if build_name == 'build with spaces':
                baseline = text
            else:
                self.assertEqual(baseline, text)

    def test_plural_shared_and_failed_extraction(self):
        shared = self.source / 'src/shared.c'
        shared.write_text('/* translator: number of objects */\n'
                          'ngettext("one object", "%d objects", count);\n')
        self.command('extract', '--inputs', shared)
        text = self.output.read_text()
        self.assertIn('#: ../shared.c:2', text)
        self.assertIn('msgid_plural "%d objects"', text)
        before = self.output.read_bytes()
        self.command('extract', '--inputs', self.source / 'missing.c', success=False)
        self.assertEqual(before, self.output.read_bytes())

    def test_compendium_and_failed_merge(self):
        source = self.source / self.subdir / 'example.c'
        source.write_text('gettext("hello");\n')
        self.command('extract', '--inputs', source)
        template = self.output
        compendium = self.source / 'de.po'
        compendium.write_text(
            'msgid ""\nmsgstr ""\n'
            '"Content-Type: text/plain; charset=UTF-8\\n"\n'
            '"Language: de\\n"\n'
            '"Plural-Forms: nplurals=2; plural=(n != 1);\\n"\n\n'
            'msgid "hello"\nmsgstr "hallo"\n')
        self.output = template.with_name('de.po.new')
        # msgmerge's progress indicator is written to stderr.
        result = subprocess.run([
            sys.executable, str(helper), 'merge', '--source-root', str(self.source),
            '--build-root', str(self.build), '--subdir', self.subdir,
            '--name', 'example', '--version', '20', '--output', str(self.output),
            '--msgmerge', tools.msgmerge, '--template', str(template),
            '--language', 'de', '--compendia', str(compendium)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('msgstr "hallo"', self.output.read_text())
        before = self.output.read_bytes()
        self.command('merge', '--template', template, '--language', 'de',
                     '--original', self.source / 'missing.po', success=False)
        self.assertEqual(before, self.output.read_bytes())

    def test_missing_tool_and_manifest(self):
        source = self.source / self.subdir / 'example.c'
        source.write_text('gettext("manifest message");\n')
        manifest = self.build / 'inputs'
        manifest.write_text(str(source) + '\n')
        self.command('extract', '--files-from', manifest)
        before = self.output.read_bytes()
        self.command('extract', '--files-from', manifest,
                     '--xgettext', self.root / 'missing-xgettext', success=False)
        self.assertEqual(before, self.output.read_bytes())

    def test_relative_line_directive_and_grammar(self):
        original = self.source / self.subdir / 'template.in.c'
        original.write_text('/* generator input */\n')
        generated = self.build / self.subdir / 'generated.c'
        generated.write_text(
            '#line 42 "../source with spaces/src/example/template.in.c"\n'
            'gettext("generated message");\n')
        grammar = self.source / self.subdir / 'parser.y'
        grammar.write_text('%{\n#define marker gettext_noop("syntax error")\n%}\n'
                           '%%\nrule: TOKEN { errmsg("grammar action"); };\n%%\n')
        self.command('extract', '--keyword=errmsg', '--inputs', generated, grammar)
        text = self.output.read_text()
        self.assertIn('#: template.in.c:42', text)
        self.assertIn('msgid "syntax error"', text)
        self.assertIn('#: parser.y:5', text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--xgettext', required=True)
    parser.add_argument('--msgmerge', required=True)
    tools, remaining = parser.parse_known_args()
    unittest.main(argv=[sys.argv[0]] + remaining)

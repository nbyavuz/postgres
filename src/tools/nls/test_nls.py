#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Exercise catalog extraction and merging with the actual gettext tools."""

import argparse
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='postgres nls ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source tree'
        self.build = self.root / 'build tree'
        (self.source / 'src/example').mkdir(parents=True)
        (self.build / 'src/example').mkdir(parents=True)

    def extract(self, inputs, output=None):
        output = output or self.build / 'example.pot'
        subprocess.run([
            sys.executable, args.tool, 'extract',
            '--xgettext', args.xgettext,
            '--source-root', str(self.source), '--build-root', str(self.build),
            '--catalog-dir', 'src/example', '--catalog', 'example', '--version', '20',
            '--keyword=report:1,2', '--flag=report:1:c-format', '--flag=report:2:c-format',
            '--output', str(output), *map(str, inputs)], check=True)
        return output

    def test_generated_references_and_plural_flags(self):
        original = self.source / 'src/example/input.y'
        original.write_text('report("%d item", "%d items", n);\n', encoding='utf-8')
        generated = self.build / 'src/example/generated.c'
        generated.write_text(
            f'#line 12 "{original}"\n'
            '/* translator: retained comment */\n'
            '_("generated message");\n', encoding='utf-8')
        pot = self.extract([original, generated]).read_text(encoding='utf-8')
        self.assertIn('msgid_plural "%d items"', pot)
        self.assertIn('#, c-format', pot)
        self.assertIn('#. translator: retained comment', pot)
        self.assertIn('#: input.y:13', pot)
        self.assertNotIn(str(self.root), pot)

    def test_parser_skeleton_excluded(self):
        grammar = self.source / 'src/example/parser.y'
        grammar.write_text('_("parser action");\n', encoding='utf-8')
        generated = self.build / 'src/example/parser.c'
        generated.write_text('gettext("skeleton diagnostic");\n', encoding='utf-8')
        pot = self.extract([grammar, generated]).read_text(encoding='utf-8')
        self.assertIn('msgid "parser action"', pot)
        self.assertNotIn('skeleton diagnostic', pot)

    def test_build_inside_source_tree(self):
        self.build = self.source / 'build'
        (self.build / 'src/example').mkdir(parents=True)
        generated = self.build / 'src/example/generated.c'
        generated.write_text('_("generated message");\n', encoding='utf-8')
        pot = self.extract([generated]).read_text(encoding='utf-8')
        self.assertIn('#: generated.c:1', pot)
        self.assertNotIn('build/src', pot)

    def test_compendium_seeds_new_language(self):
        source = self.source / 'src/example/example.c'
        source.write_text('_("hello");\n', encoding='utf-8')
        pot = self.extract([source])
        compendium = self.root / 'de.po'
        compendium.write_text(
            'msgid ""\nmsgstr ""\n'
            '"Content-Type: text/plain; charset=UTF-8\\n"\n'
            '"Language: de\\n"\n\n'
            'msgid "hello"\nmsgstr "hallo"\n', encoding='utf-8')
        before = compendium.read_bytes()
        output = self.build / 'de.po.new'
        subprocess.run([
            sys.executable, args.tool, 'merge', '--msgmerge', args.msgmerge,
            '--language', 'de', '--pot', str(pot), '--compendium', str(compendium),
            '--output', str(output)], check=True)
        self.assertIn('msgstr "hallo"', output.read_text(encoding='utf-8'))
        self.assertEqual(compendium.read_bytes(), before)

    def test_existing_translation_and_fuzzy_merge(self):
        source = self.source / 'src/example/example.c'
        source.write_text('_("hello");\n_("the connection was closed unexpectedly");\n',
                          encoding='utf-8')
        pot = self.extract([source])
        po = self.root / 'de.po'
        po.write_text(
            'msgid ""\nmsgstr ""\n'
            '"Content-Type: text/plain; charset=UTF-8\\n"\n'
            '"Language: de\\n"\n\n'
            'msgid "hello"\nmsgstr "hallo"\n\n'
            'msgid "the connection was closed"\nmsgstr "die Verbindung wurde geschlossen"\n',
            encoding='utf-8')
        before = po.read_bytes()
        output = self.build / 'de.po.new'
        subprocess.run([
            sys.executable, args.tool, 'merge', '--msgmerge', args.msgmerge,
            '--language', 'de', '--pot', str(pot), '--po', str(po),
            '--output', str(output)], check=True)
        text = output.read_text(encoding='utf-8')
        self.assertIn('msgstr "hallo"', text)
        self.assertIn('#, fuzzy', text)
        self.assertIn('#| msgid "the connection was closed"', text)
        self.assertEqual(po.read_bytes(), before)

    def test_failed_merge_preserves_output(self):
        output = self.build / 'de.po.new'
        output.write_text('previous output', encoding='utf-8')
        result = subprocess.run([
            sys.executable, args.tool, 'merge', '--msgmerge', args.msgmerge,
            '--language', 'de', '--pot', str(self.root / 'missing.pot'),
            '--output', str(output)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output.read_text(encoding='utf-8'), 'previous output')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tool', required=True)
    parser.add_argument('--xgettext', required=True)
    parser.add_argument('--msgmerge', required=True)
    args, remaining = parser.parse_known_args()
    unittest.main(argv=[sys.argv[0]] + remaining)

#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group

"""Exercise the real catalog rules in a small, out-of-tree Meson project."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


FLEX = sys.argv[1]
BISON = sys.argv[2]
MESON = sys.argv[3:]
TEST_DIR = Path(__file__).resolve().parent


class CatalogTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='nls-', dir=os.getcwd())
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.source = self.directory / 'source with spaces'
        self.build = self.directory / 'build one'
        shutil.copytree(str(TEST_DIR / 'fixture'), str(self.source))
        (self.source / 'nls').mkdir()
        for filename in ('meson.build', 'gen_pot.py'):
            shutil.copyfile(str(TEST_DIR.parent / filename),
                            str(self.source / 'nls' / filename))

        self.write('alpha.c', '''
/* translator: number of items */
report_plural("one item: %d", "many items: %d", count);
report(level, "a format: %s", value);
_("alpha message");
_("non-ASCII: café");
''')
        self.write('scanner.l', '''
quote [']
%%
{quote} { _("scanner message"); }
%%
''')
        self.write('parser.y', '''
%token WORD
%%
input:
    WORD {
        /* translator: word recognized by the parser */
        report(0, "parser message: %s", "word");
    }
;
''')
        self.write('beta.c', '_("beta message");\n')
        self.write('shared.c', '_("shared message");\n')
        self.write('stray.c', '_("stray message");\n')
        self.write('empty.c', '/* no messages */\n')
        self.write('message.txt', 'generated message\n')
        self.write('errcodes.txt', '#define ERRCODE_TEST 1\n')
        self.configure()

    def configure(self):
        self.run_meson('setup', '--backend=ninja', '-DFLEX=' + FLEX,
                       '-DBISON=' + BISON, '-DPYTHON=' + sys.executable,
                       str(self.build), str(self.source))

    def write(self, filename, text):
        path = self.source / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    def run_meson(self, *args, success=True):
        result = subprocess.run(MESON + list(args), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, universal_newlines=True)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def compile(self, *targets, success=True):
        return self.run_meson('compile', '-C', str(self.build), *targets,
                              success=success)

    def pot(self, name):
        return self.build / 'nls' / (name + '.pot')

    def contents(self, name):
        return self.pot(name).read_text(encoding='utf-8')

    def change_input(self, filename, text):
        self.write(filename, text)
        path = self.source / filename
        previous_time = path.stat().st_mtime_ns
        newest_output = max(p.stat().st_mtime_ns for p in
                            (self.build / 'nls').glob('*.pot'))
        # Avoid sleeping to accommodate filesystems with coarse timestamps.
        future = newest_output + 2000000000
        os.utime(str(path), ns=(future, future))
        try:
            self.compile('init-po')
        finally:
            os.utime(str(path), ns=(previous_time, previous_time))

    def test_extraction(self):
        self.compile()
        self.assertFalse(self.pot('alpha').exists())
        self.compile('alpha-init-po')
        self.assertFalse(self.pot('beta').exists())
        self.assertFalse((self.build / 'errcodes.h').exists())
        text = self.contents('alpha')
        for message in ('alpha message', 'shared message', 'scanner message',
                        'parser message: %s', 'generated message',
                        'non-ASCII: café'):
            self.assertIn('msgid "' + message + '"', text)
        self.assertNotIn('stray message', text)
        self.assertNotIn('fatal flex scanner', text)
        self.assertNotIn('msgid "syntax error"', text)
        self.assertNotIn('msgid "memory exhausted"', text)
        self.assertIn('#. translator: number of items', text)
        self.assertIn('#. translator: word recognized by the parser', text)
        self.assertIn('msgid_plural "many items: %d"', text)
        self.assertIn('#, c-format\nmsgid "a format: %s"', text)
        self.assertIn('#, c-format\nmsgid "parser message: %s"', text)
        self.assertRegex(text, r'(?m)^#: parser\.y:\d+$')
        self.assertIn('#: message.txt:42', text)
        self.assertNotIn(str(self.directory), text)
        self.assertIn('LANGUAGE message translation file for alpha', text)
        self.assertIn('Project-Id-Version: alpha (PostgreSQL) 99', text)
        self.compile('init-po')
        self.assertIn('msgid "shared message"', self.contents('beta'))
        self.assertIn('msgid ""', self.contents('empty'))

    def test_incremental_extraction(self):
        self.compile('init-po')
        times = {name: self.pot(name).stat().st_mtime_ns
                 for name in ('alpha', 'beta', 'empty')}
        self.compile('init-po')
        for name, timestamp in times.items():
            self.assertEqual(self.pot(name).stat().st_mtime_ns, timestamp)

        self.change_input('errcodes.txt', '#define ERRCODE_TEST 2\n')
        self.assertFalse((self.build / 'errcodes.h').exists())
        for name, timestamp in times.items():
            self.assertEqual(self.pot(name).stat().st_mtime_ns, timestamp)

        self.change_input('alpha.c', '_("changed alpha");\n')
        self.assertIn('msgid "changed alpha"', self.contents('alpha'))
        self.assertEqual(self.pot('beta').stat().st_mtime_ns, times['beta'])
        self.change_input('message.txt', 'changed generated message\n')
        self.assertIn('msgid "changed generated message"', self.contents('alpha'))
        grammar = (self.source / 'parser.y').read_text(encoding='utf-8')
        self.change_input('parser.y',
                          grammar.replace('parser message', 'changed parser message'))
        self.assertIn('msgid "changed parser message: %s"', self.contents('alpha'))
        self.assertEqual(self.pot('beta').stat().st_mtime_ns, times['beta'])
        self.change_input('shared.c', '_("changed shared message");\n')
        for name in ('alpha', 'beta'):
            self.assertIn('msgid "changed shared message"', self.contents(name))
        self.assertEqual(self.pot('empty').stat().st_mtime_ns, times['empty'])

    def test_source_list_changes(self):
        self.compile('init-po')
        meson_file = self.source / 'meson.build'
        original = meson_file.read_text(encoding='utf-8')
        self.write('added.c', '_("added message");\n')
        self.write('meson.build', original.replace("'alpha.c'", "'alpha.c', 'added.c'"))
        self.compile('alpha-init-po')
        self.assertIn('msgid "added message"', self.contents('alpha'))

        (self.source / 'alpha.c').rename(self.source / 'renamed.c')
        (self.source / 'added.c').unlink()
        self.write('meson.build', original.replace("'alpha.c'", "'renamed.c'"))
        self.compile('alpha-init-po')
        self.assertNotIn('added message', self.contents('alpha'))
        self.assertIn('#: renamed.c:', self.contents('alpha'))

    def test_build_directory_independence(self):
        self.compile('init-po')
        first = self.contents('alpha')
        self.build = self.source / 'build inside source'
        self.configure()
        self.compile('alpha-init-po')
        creation_date = r'(?m)^"POT-Creation-Date:.*\n'
        self.assertEqual(re.sub(creation_date, '', first),
                         re.sub(creation_date, '', self.contents('alpha')))

    def test_failed_extraction_preserves_output(self):
        self.compile('init-po')
        original = self.pot('alpha').read_bytes()
        path = self.source / 'alpha.c'
        path.write_bytes(b'_("invalid UTF-8: \xff");\n')
        future = self.pot('alpha').stat().st_mtime_ns + 2000000000
        os.utime(str(path), ns=(future, future))
        self.compile('alpha-init-po', success=False)
        self.assertEqual(self.pot('alpha').read_bytes(), original)


if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]])

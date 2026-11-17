# Copyright (c) 2026, PostgreSQL Global Development Group

from pathlib import Path
import sys

source = Path(sys.argv[1])
output = Path(sys.argv[2])
output.write_text('#line 42 "' + source.as_posix() + '"\n'
                  '_("' + source.read_text(encoding='utf-8').strip() + '");\n',
                  encoding='utf-8')

#!/usr/bin/env python3
# Copyright (c) 2026, PostgreSQL Global Development Group
#
# List sorted source .po files

import os
import sys
from pathlib import Path

catalogs = []
for root, dirs, names in os.walk(sys.argv[1]):
    # A Meson build directory can itself be inside the source tree.
    if 'meson-private' in dirs:
        dirs.clear()
        continue
    if '.git' in dirs:
        dirs.remove('.git')
    for name in names:
        if name.endswith('.po'):
            path = Path(root, name)
            if path.is_file():
                catalogs.append(path.as_posix())

for catalog in sorted(catalogs):
    print(catalog)

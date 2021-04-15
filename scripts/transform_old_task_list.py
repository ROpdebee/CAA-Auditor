"""One-off script to transform an old task list."""

import json
import os
import sys

from tqdm import tqdm

SRC = sys.argv[1]
OUT = sys.argv[2]

with open(OUT, 'w') as out_f, open(SRC, 'r') as src_f:
    for line in tqdm(src_f):
        old_data = json.loads(line)
        new_data = {'state': 'active', 'id': old_data['release_gid'], 'data': old_data}
        out_f.write(json.dumps(new_data) + os.linesep)

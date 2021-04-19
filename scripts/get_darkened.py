import csv
import sys
from collections import defaultdict
from pathlib import Path

skipped_log_path = Path(sys.argv[1])

with open('darkened_items', 'wt') as out_f, skipped_log_path.open('rt') as in_f:
    for line in in_f:
        mbid, reason = line.strip().split('\t')
        if '::darkened' in reason:
            out_f.write(mbid + '\n')

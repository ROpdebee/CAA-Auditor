"""Script to transform MB data to JSONL format for feeding into the auditor.

Currently using data dumps, could be adapted to use a DB instance.

Call with root MB dump path as first arg, and output file as second arg.
"""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

if TYPE_CHECKING:
    from collections.abc import Sequence

import pendulum

DUMP_PATH = Path(sys.argv[1])
TABLE_DUMP_PATH = DUMP_PATH / 'mbdump'
OUT_PATH = Path(sys.argv[2])

APPROVED_EDIT_STATUSES: Sequence[int] = ()
ASIN_RGX = r'https?://(?:www\.)?amazon\.\w{2,3}/gp/product/(\w+)'

_: Any

# DATA
art_id_to_types: dict[str, set[str]]
artist_id_to_gid: dict[str, str]
artist_id_to_name: dict[str, str]
artist_credit_to_artist_ids: dict[str, list[str]]
edit_to_opened: dict[str, bool]
lang_id_to_lang: dict[str, str]
url_to_asin: dict[str, str]
rel_id_to_urls: dict[str, list[str]]
rel_id_to_release: dict[str, Sequence[str]]
rel_id_to_covers: dict[str, list[list[str]]]
rel_id_to_dates: dict[str, set[str]]

def _load_rows(table_name: str) -> Sequence[Sequence[str]]:
    with (TABLE_DUMP_PATH / table_name).open() as f:
        return [line.split('\t') for line in f]

art_types = {
        type_id: type_name
        for type_id, type_name, *_ in _load_rows('cover_art_archive.art_type')}

art_mime_types_to_ext = {
        mime_type: extension
        for mime_type, extension in _load_rows('cover_art_archive.image_type')
}

art_id_to_types = defaultdict(set)
for cover_id, type_id in _load_rows('cover_art_archive.cover_art_type'):
    art_id_to_types[cover_id].add(art_types[type_id])

del art_types

artist_id_to_gid = {}
artist_id_to_name = {}
for a_rowid, a_gid, a_name, *_ in _load_rows('artist'):
    artist_id_to_gid[a_rowid] = a_gid
    artist_id_to_name[a_rowid] = a_name

artist_credit_to_artist_ids = defaultdict(list)
for ac_id, _, artist_id, *_ in _load_rows('artist_credit_name'):
    artist_credit_to_artist_ids[ac_id].append(artist_id)

assert False, 'APPROVED_EDIT_STATUSES not implemented yet'
edit_to_opened = {
    edit_id: edit_status in APPROVED_EDIT_STATUSES
    for edit_id, _, _, edit_status, *_ in _load_rows('edit')
}

lang_id_to_lang = {
    row[0]: row[-1] for row in _load_rows('language')
}

url_to_asin = {
    url_id: match.groups()[0] for url_id, _, url, *_ in _load_rows('url')
    if (match := re.match(ASIN_RGX, url)) is not None
}

rel_id_to_urls = defaultdict(list)
for _, _, rel_id, url_id in _load_rows('l_release_url'):
    rel_id_to_urls[rel_id].append(url_id)

rel_id_to_release = {
    row[0]: row for row in _load_rows('release')
}

def row_to_date(row: Sequence[str]) -> str:
    ymd = [('????' if s == '\\N' else s) for s in row[2:]]
    date = '-'.join(ymd)
    date = re.sub(r'(?:-\?\?){1,2}$', '', date)
    if date == '????':
        date = ''

    return date

rel_id_to_dates = defaultdict(set)
for row in _load_rows('release_country'):
    date = row_to_date(row)
    if date:
        rel_id_to_dates[row[0]].add(date)

rel_id_to_covers = defaultdict(list)
for cover_row in _load_rows('cover_art_archive.cover_art'):
    cover_id, rel_id, comment, edit_id, order, _, _, mime_type, *_ = row
    rel_id_to_covers[rel_id].append([cover_id, comment, edit_id, order, mime_type])

for cover_list in rel_id_to_covers.values():
    cover_list.sort(key=lambda row: int(row[3]))

db_timestamp = (TABLE_DUMP_PATH / 'TIMESTAMP').read_text().strip()

with OUT_PATH.open('w') as out_f:
    for rel_id, covers in rel_id_to_covers.items():
        rel_row = rel_id_to_release[rel_id]
        _, rel_gid, rel_name, ac_id, *_ = rel_row
        lid = rel_row[7]
        barcode = rel_row[9]

        info = {
            'release_gid': rel_gid,
            'release_name': rel_name,
            'artists': [
                {
                    'artist_gid': artist_id_to_gid[artist_id],
                    # IA seems to use normal name, not as credited
                    'artist_name': artist_id_to_name[artist_id],
                } for artist_id in artist_credit_to_artist_ids[ac_id]
            ],
            'language_code': lang_id_to_lang[lid],
            'barcode': barcode,
            'asins': [
                url_to_asin[url_id]
                for url_id in rel_id_to_urls[rel_id]
                if url_id in url_to_asin
            ],
            'release_dates': list(rel_id_to_dates[rel_id]),
            'covers': [
                {
                    'id': int(cover_id),
                    'edit_id': int(edit_id),
                    'edit_approved': not edit_to_opened[edit_id],
                    'comment': comment,
                    'types': art_id_to_types[cover_id],
                    'extension': art_mime_types_to_ext[mime_type],
                }
                for cover_id, comment, edit_id, _, mime_type in rel_id_to_covers[rel_id]
            ],
            'max_last_modified': db_timestamp,
        }

        # One JSON-serialized dict per line, so we get JSONL
        out_f.write(json.dumps(info))

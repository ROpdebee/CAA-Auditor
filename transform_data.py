"""Script to transform MB data to JSONL format for feeding into the auditor.

Currently using data dumps, could be adapted to use a DB instance.

Call with output file as first arg and the following environment values set:
MB_USER = The postgresql user that has access to the MB database.
MB_PASS = Its password
MB_HOST = MB DB host
MB_DB = Database name of the DB
"""

from __future__ import annotations

from typing import Any

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import pendulum
from mbdata.models import (
        Artist, ArtistCredit, ArtistCreditName, ArtType, CoverArt, CoverArtType,
        ImageType, Language, Link, LinkReleaseURL, LinkType, Release, ReleaseCountry,
        ReleaseUnknownCountry, URL)
from mbdata.types import PartialDate
from sqlalchemy import create_engine
from sqlalchemy.orm import backref, joinedload, load_only, raiseload, relationship, sessionmaker, Load
from tqdm import tqdm

if not {'MB_USER', 'MB_PASS', 'MB_HOST', 'MB_DB'}.issubset(os.environ.keys()):
    raise ValueError('Missing required environment arguments')

MB_USER = os.environ['MB_USER']
MB_PASS = os.environ['MB_PASS']
MB_HOST = os.environ['MB_HOST']
MB_DB = os.environ['MB_DB']
DB_CONN_ADDR = f'postgresql://{MB_USER}:{MB_PASS}@{MB_HOST}/{MB_DB}'
OUT_PATH = Path(sys.argv[1])

APPROVED_EDIT_STATUSES = (2,)

engine = create_engine(DB_CONN_ADDR)
Session = sessionmaker(bind=engine)
session = Session()

all_caa_ids = session.query(CoverArt.release_id).order_by(CoverArt.release_id).distinct().all()

CoverArt.release = relationship('Release', foreign_keys=[CoverArt.release_id], innerjoin=True, backref=backref('cover_arts', order_by='CoverArt.ordering', viewonly=True))
CoverArtType.cover_art = relationship('CoverArt', foreign_keys=[CoverArtType.id], innerjoin=True, backref=backref('types', viewonly=True))

def stringify_date(date: PartialDate) -> str:
    year = f'{date.year:04d}' if date.year is not None else '????'
    month = f'{date.month:02d}' if date.month is not None else '??'
    day = f'{date.day:02d}' if date.day is not None else '??'

    date = '-'.join((year, month, day))
    date = re.sub(r'(?:-\?\?){1,2}$', '', date)
    if date == '????':
        date = ''

    return date

mime_type_to_ext = {
    mime: ext
    for (mime, ext) in session.query(ImageType.mime_type, ImageType.suffix).all()
}

ca_type_id_to_name = {
    ca_type.id: ca_type.name
    for ca_type in session.query(ArtType).all()
}

# Not very scalable, but quicker than a separate query
amzn_link_id = session.query(LinkType.id).filter_by(name='amazon asin').one()[0]
rels_and_amzn_urls = (session.query(LinkReleaseURL.release_id, URL.url)
        .join(URL, Link)
        .filter_by(link_type_id=amzn_link_id)
        .all())
rel_ids_to_asins: dict[str, set[str]] = defaultdict(set)
for (rel_id, amzn_url) in rels_and_amzn_urls:
    asin = amzn_url.split('/')[-1]
    assert len(asin) == 10 and asin.isalnum()
    rel_ids_to_asins[rel_id].add(asin)
del rels_and_amzn_urls

# Again, caching this here to save on a merge
lang_id_to_iso = {
    lang_id: iso_code
    for lang_id, iso_code in session.query(Language.id, Language.iso_code_3)
}


def extract_cover(cover: CoverArt) -> dict[str, Any]:
    return {
        'id': cover.id,
        'edit_id': cover.edit_id,
        'edit_approved': cover.edit.status in APPROVED_EDIT_STATUSES,
        'comment': cover.comment,
        'types': [ca_type_id_to_name[cover_type.type_id] for cover_type in cover.types],
        'extension': mime_type_to_ext[cover.mime_type],
    }

def extract_data(id: int) -> dict[str, Any]:
    release = (session.query(Release)
            .options(
                load_only('id', 'gid', 'barcode', 'language_id'),
                (joinedload(Release.artist_credit, innerjoin=True)
                    .joinedload(ArtistCredit.artists, innerjoin=True)
                    .joinedload(ArtistCreditName.artist, innerjoin=True)
                    .load_only('gid', 'name')),
                joinedload(Release.country_dates).load_only('date_year', 'date_month', 'date_day'),
                joinedload(Release.unknown_country_dates).load_only('date_year', 'date_month', 'date_day'),
                (joinedload(Release.cover_arts)
                    .options(
                        joinedload(CoverArt.types),
                        joinedload(CoverArt.edit).load_only('status')))
            )
            .filter_by(id=id)
            .one())
    dates = [
        stringify_date(rel_date.date)
        for rel_date in (*release.country_dates, *release.unknown_country_dates)]

    return {
        'release_gid': release.gid,
        'release_name': release.name,
        'artists': [
            {
                'artist_gid': ac_name.artist.gid,
                # IA seems to use normal name, not as credited
                'artist_name': ac_name.artist.name,
            } for ac_name in release.artist_credit.artists
        ],
        'language_code': release.language_id is not None and lang_id_to_iso[release.language_id],
        'barcode': release.barcode,
        'asins': list(rel_ids_to_asins[release.id]),
        'release_dates': dates,
        'covers': [extract_cover(cover) for cover in release.cover_arts],
    }

with OUT_PATH.open('w') as out_f:
    for (caa_id,) in tqdm(all_caa_ids, desc='Extract data'):
        mb_data = extract_data(caa_id)
        mb_data_json = json.dumps(mb_data)
        out_f.write(mb_data_json + os.linesep)


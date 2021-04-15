"""Script to transform MB data to JSONL format for feeding into the auditor.

Call with output file as first arg and the following environment values set:
MB_USER = The postgresql user that has access to the MB database.
MB_PASS = Its password
MB_HOST = MB DB host
MB_DB = Database name of the DB


Output description:
Output will be a file containing JSONL, i.e., one JSON object per line.
Each JSON object has a key 'state', along with other data depending on the state.
The five states are the following:
    - meta: Metadata information, currently containing a count of the remaining rows.
            The first line will always be the one and only meta line, for progress bar purposes.
    - active: Releases that are in the MB database we're using. These objects
              contain the 'id' key (GID of the release) and the 'data' key which
              contains an object with MB data for use in the audit.
    - merged: Releases that have been merged into another, supplemented by an
              'id' key with the old GID
    - possibly_deleted: Releases that were not found in the MB database. These
                        may have been removed, or the MBID may belong to another
                        instance (e.g. test.musicbrainz.org). Contains 'id'.
    - empty: Releases that were found in the DB, but have no cover art associated
             with them. Also contains 'id' and 'data', although 'data' will have
             empty images.

'active' records are always primarily sourced from the MB DB itself.
'merged', 'empty', and 'possibly_deleted' can only be sourced from a caa_items
file.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import click
import pendulum
import mbdata.config
from mbdata.models import (
        Artist, ArtistCredit, ArtistCreditName, ArtType, Base, CoverArt, CoverArtType,
        ImageType, Language, Link, LinkReleaseURL, LinkType, Release, ReleaseCountry,
        ReleaseGIDRedirect, ReleaseUnknownCountry, URL, apply_schema)
from mbdata.types import PartialDate, SMALLINT
from sqlalchemy import create_engine, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import backref, composite, joinedload, load_only, raiseload, relationship, sessionmaker, Load
from tqdm import tqdm

IA_ITEM_MBID_RGX = re.compile(r'mbid-([0-9a-f-]{36})$')

def create_session() -> Any:
    if not {'MB_USER', 'MB_PASS', 'MB_HOST', 'MB_DB'}.issubset(os.environ.keys()):
        raise ValueError('Missing required environment arguments')

    MB_USER = os.environ['MB_USER']
    MB_PASS = os.environ['MB_PASS']
    MB_HOST = os.environ['MB_HOST']
    MB_DB = os.environ['MB_DB']
    DB_CONN_ADDR = f'postgresql://{MB_USER}:{MB_PASS}@{MB_HOST}/{MB_DB}'

    engine = create_engine(DB_CONN_ADDR)
    Session = sessionmaker(bind=engine)
    return Session()

class IndexListing(CoverArt):
    """Mapping for cover_art_archive.index_listing view."""
    __tablename__ = 'index_listing'
    __table_args__ = (
        {'schema': mbdata.config.schemas.get('cover_art_archive', 'cover_art_archive')}
    )

    id = Column(None, ForeignKey(apply_schema('cover_art.id', 'cover_art_archive')))
    approved = Column(Boolean, nullable=False)
    is_front = Column(Boolean, nullable=False)
    is_back = Column(Boolean, nullable=False)
    types = Column(postgresql.ARRAY(String), nullable=False)

    mime_type2 = relationship('ImageType', foreign_keys=[CoverArt.mime_type], innerjoin=True, lazy='joined')

class ReleaseEvent(Base):
    """Mapping for musicbrainz.release_event view."""
    __tablename__ = 'release_event'
    __table_args__ = (
        {'schema': mbdata.config.schemas.get('musicbrainz', 'musicbrainz')}
    )

    release_id = Column('release', Integer, ForeignKey(apply_schema('release.id', 'musicbrainz'), name='release_country_fk_release'), nullable=False, primary_key=True)
    country_id = Column('country', Integer, ForeignKey(apply_schema('country_area.area', 'musicbrainz'), name='release_country_fk_country'), nullable=True, primary_key=True)
    date_year = Column(SMALLINT)
    date_month = Column(SMALLINT)
    date_day = Column(SMALLINT)

    release = relationship('Release', foreign_keys=[release_id], innerjoin=True, backref=backref('release_events'))
    country = relationship('CountryArea', foreign_keys=[country_id], innerjoin=False)

    date = composite(PartialDate, date_year, date_month, date_day)


def stringify_date(date: PartialDate) -> str:
    year = f'{date.year:04d}' if date.year is not None else '????'
    month = f'{date.month:02d}' if date.month is not None else '??'
    day = f'{date.day:02d}' if date.day is not None else '??'

    date = '-'.join((year, month, day))
    date = re.sub(r'(?:-\?\?){1,2}$', '', date)
    if date == '????':
        date = ''

    return date

def _build_get_asins() -> Callable[[int, Any], set[str]]:
    rel_ids_to_asins: Optional[dict[int, set[str]]] = None

    def inner(want_rel_id: int, session: Any) -> set[str]:
        nonlocal rel_ids_to_asins
        if rel_ids_to_asins is None:
            # Not very scalable, but quicker than a separate query or many merges
            amzn_link_id = session.query(LinkType.id).filter_by(name='amazon asin').one()[0]
            rels_and_amzn_urls = (session.query(LinkReleaseURL.release_id, URL.url)
                    .join(URL, Link)
                    .filter_by(link_type_id=amzn_link_id)
                    .all())
            rel_ids_to_asins = defaultdict(set)
            for (rel_id, amzn_url) in rels_and_amzn_urls:
                asin = amzn_url.split('/')[-1]
                assert len(asin) == 10 and asin.isalnum()
                rel_ids_to_asins[rel_id].add(asin)

        return rel_ids_to_asins[want_rel_id]

    return inner

get_asins = _build_get_asins()

def _build_get_language_code() -> Callable[[int, Any], str]:
    # Again, caching this here to save on a merge
    lang_id_to_iso: Optional[dict[int, str]] = None

    def inner(want_lang_id: int, session: Any) -> str:
        nonlocal lang_id_to_iso
        if lang_id_to_iso is None:
            lang_id_to_iso = {
                lang_id: iso_code
                for lang_id, iso_code in session.query(Language.id, Language.iso_code_3)
            }

        return lang_id_to_iso[want_lang_id]

    return inner

get_language_code = _build_get_language_code()

def image_url(cover: IndexListing, size_suffix: str, extension: str) -> str:
    return f'http://coverartarchive.org/release/{cover.release_id}/{cover.id}{size_suffix}.{extension}'

def extract_cover(cover: IndexListing) -> dict[str, Any]:
    return {
        'types': cover.types,
        'front': cover.is_front,
        'back': cover.is_back,
        'comment': cover.comment,
        'suffix': cover.mime_type2.suffix,
        'approved': cover.approved,
        'edit': cover.edit_id,
        'id': cover.id,
    }

def extract_data(mbid: str, session: Any) -> dict[str, Any]:
    release = (session.query(Release)
        .options(
            load_only('id', 'gid', 'barcode', 'language_id'),
            (joinedload(Release.artist_credit, innerjoin=True)
                .joinedload(ArtistCredit.artists, innerjoin=True)
                .joinedload(ArtistCreditName.artist, innerjoin=True)
                .load_only('gid', 'name')))
        .filter_by(gid=mbid)
        .one_or_none())
    if release is None:
        return extract_data_for_missing_release(mbid, session)
    else:
        return extract_data_from_release(release, session)

def extract_data_from_release(release: Release, session: Any) -> dict[str, Any]:
    dates = [
        stringify_date(rel_date)
        for (rel_date,) in session.query(ReleaseEvent.date).filter_by(release_id=release.id).distinct().all()]

    data = {
        'release_gid': release.gid,
        'release_name': release.name,
        'artists': [
            {
                'artist_gid': ac_name.artist.gid,
                # IA seems to use normal name, not as credited
                'artist_name': ac_name.artist.name,
            } for ac_name in release.artist_credit.artists
        ],
        'language_code': release.language_id is not None and get_language_code(release.language_id, session),
        'barcode': release.barcode,
        'asins': list(get_asins(release.id, session)),
        'release_dates': dates,
        'images': [extract_cover(cover) for cover in session.query(IndexListing).filter_by(release_id=release.id).order_by('ordering').all()],
    }

    state = 'active' if data['images'] else 'empty'
    return {'state': state, 'id': release.gid, 'data': data}


def extract_data_for_missing_release(mbid: str, session: Any) -> dict[str, Any]:
    gid_redirect = (session.query(ReleaseGIDRedirect)
            .filter_by(gid=mbid)
            .one_or_none())
    if gid_redirect is not None:
        return {'state': 'merged', 'id': mbid}
    else:
        return {'state': 'possibly_deleted', 'id': mbid}


def parse_ia_mbid(ia_item_id: str) -> str:
    mtch = IA_ITEM_MBID_RGX.match(ia_item_id)
    if mtch is not None:
        return mtch.groups()[0]
    raise ValueError(f'{ia_item_id} does not look like a CAA item')


def read_timestamp_from_record_file(record_file: str) -> Optional[pendulum.datetime.DateTime]:
    with open(record_file, 'r') as f:
        first_line = next(f, None)
        if first_line is None:
            return None
        first_data = json.loads(first_line)
        if first_data['state'] != 'meta':
            return None
        return pendulum.from_timestamp(first_data['max_last_modified'])

@click.command()
@click.argument('out_path', type=click.Path(writable=True, dir_okay=False))
@click.option('--caa-items', type=click.Path(readable=True, dir_okay=False), help='File containing additional items in IA to audit')
@click.option('--continue-from', type=click.Path(readable=True, dir_okay=False), help='Continue from a previous file')
@click.option('--timestamp', type=click.Path(readable=True, dir_okay=False), help='DB timestamp file')
def run(out_path: str, caa_items: Optional[str], continue_from: Optional[str], timestamp: Optional[str]) -> None:
    """Run the data extraction.

    Will query the MB database to find release MBIDs for which the CAA item
    needs to be audited.
    If --caa-items is provided, it is expected to point to a file containing
    IA item identifiers corresponding to CAA items. These items will be
    included in the audit results. If the corresponding release does not exist
    in the MB database, this script will attempt to identify it as a previously
    merged release or potentially removed release.
    If --continue-from is provided, the script will load all items from this file
    and only process MBIDs that had not been processed previously. This is useful
    to recover from errors or append new tasks to the list. Note: For progress
    purposes, it is assumed that all MBIDs in the continued-from file are present
    in either the DB or in --caa-items. If not, the total row count in the
    meta record may be wrong!
    If --timestamp is provided, the pointed-to file will be read and assumed to
    contain the timestamp of a DB dump. This timestamp is then used to prevent
    auditing items that have been modified since that timestamp. If not provided,
    it will be set to the timestamp found in the --continue-from file, or if
    no such file is provided, the time at which extraction first starts.
    """
    if continue_from is not None and continue_from == out_path:
        raise ValueError('Refusing to output to the same file as needs to be continued from')

    additional_ids: set[str] = set()
    if caa_items is not None:
        with open(caa_items, 'r') as caa_items_f:
            additional_ids |= {parse_ia_mbid(line.strip()) for line in caa_items_f}

    session = create_session()
    all_caa_ids = [gid for (gid,) in session.query(Release.gid).join(CoverArt).order_by(Release.id).distinct().all()]

    additional_ids -= set(all_caa_ids)
    total_num_rows = len(additional_ids | set(all_caa_ids))

    max_time = pendulum.now()
    if timestamp is not None:
        max_time = pendulum.parse(Path(timestamp).read_text().strip())
    elif continue_from is not None:
        old_max_time = read_timestamp_from_record_file(continue_from)
        if old_max_time is not None:
            max_time = old_max_time

    already_processed: set[str] = set()

    with open(out_path, 'w') as out_f:
        # Write a meta header as first row for progress
        out_f.write(json.dumps(
            {'state': 'meta', 'count': total_num_rows, 'max_last_modified': max_time.timestamp()}))
        out_f.write(os.linesep)
        if continue_from is not None:
            for line in tqdm(open(continue_from, 'r'), desc='Transfer old data'):
                record = json.loads(line)
                if record['state'] != 'meta':
                    out_f.write(line)
                    already_processed.add(record['id'])

        todo_ids = [
            mbid for mbid in (*all_caa_ids, *additional_ids)
            if mbid not in already_processed]

        num_skipped = total_num_rows - len(todo_ids)
        if num_skipped:
            print(f'Skipped processing of {num_skipped} IDs, already in {continue_from}')
        print(f'Querying {len(todo_ids)} IDs')

        for mbid in tqdm(todo_ids, desc='Extract data'):
            record = extract_data(mbid, session)
            out_f.write(json.dumps(record) + os.linesep)


if __name__ == '__main__':
    run()

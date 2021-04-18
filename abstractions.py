from __future__ import annotations

from typing import Any, Callable, NamedTuple, Optional, TYPE_CHECKING

import json
import re
from collections import defaultdict
from enum import Enum
from functools import cached_property

import pendulum

if TYPE_CHECKING:
    from pendulum.datetime import DateTime

class CheckStage(Enum):
    preprocess = 0
    fetch = 1
    meta = 2
    files = 3
    index = 4
    report = 5
    postprocess = 6

class IAState:

    files: IAFiles
    meta: IAMeta
    is_dark: bool
    last_modified: DateTime

    def __init__(self, d: dict[str, Any]) -> None:
        self.files = IAFiles(d.get('files', []))
        self.meta = IAMeta(d.get('metadata', {}))
        self.is_dark = d.get('is_dark', False)
        self.last_modified = pendulum.from_timestamp(d.get('item_last_updated', 0))

class IAFile:

    original_name: str
    name: str
    original: Optional[str]
    is_derived: bool
    is_historical: bool
    revno: Optional[int]

    def __init__(self, filedata: dict[str, str]) -> None:
        self._d = filedata
        self.original_name: str = filedata.get('name', '')
        self.is_derived = filedata.get('source', 'original') == 'derivative'
        self.original: Optional[str] = filedata.get('original')
        self.is_historical = self.original_name.startswith('history/files/')
        self.name = self.original_name
        self.revno = None
        if self.is_historical:
            self.name = self.name.removeprefix('history/files/')
            self.revno = int(re.search(r'~(\d+)~$', self.name).groups()[0])  # type: ignore[union-attr]
            self.name = re.sub(r'~\d+~$', '', self.name)

    @cached_property
    def last_modified(self) -> DateTime:
        return pendulum.from_timestamp(int(self._d.get('mtime', 0)))

class IAFiles:

    def __init__(self, filelist: list[dict[str, Any]]) -> None:
        self._original_files = {}
        self._derived_files = {}
        self._history_files: dict[str, list[IAFile]] = defaultdict(list)

        for f in filelist:
            iaf = IAFile(f)
            if iaf.is_historical:
                self._history_files[iaf.name].append(iaf)
            elif iaf.is_derived:
                self._derived_files[iaf.name] = iaf
            else:
                self._original_files[iaf.name] = iaf

    def has_original(self, name: str) -> bool:
        return name in self._original_files

    def find_originals(self, predicate: Callable[[IAFile], bool]) -> list[IAFile]:
        return [iaf for iaf in self._original_files.values() if predicate(iaf)]

    def has_any_original(self, predicate: Callable[[IAFile], bool]) -> bool:
        return any(map(predicate, self._original_files.values()))

    def has_derivative(self, name: str) -> bool:
        return name in self._derived_files

    def has_any_derivative(self, predicate: Callable[[str], bool]) -> bool:
        return any(map(predicate, self._derived_files.keys()))

    def has_historical(self, name: str) -> bool:
        return name in self._history_files

    def get_original(self, name: str) -> Optional[IAFile]:
        return self._original_files.get(name)

    def get_derived(self, name: str) -> Optional[IAFile]:
        return self._derived_files.get(name)

    def get_historical(self, name: str) -> Optional[list[IAFile]]:
        return self._history_files.get(name)


class IAMeta:

    external_ids: set[str]
    collections: list[str]
    is_noindex: bool
    mediatype: str
    title: str
    creators: list[str]
    date: Optional[str]
    language: Optional[str]

    def __init__(self, metadict: dict[str, Any]) -> None:
        self._d = metadict
        self.external_ids = set(self._get_list('external-identifier'))
        self.collections = self._get_list('collection')
        self.is_noindex = self._d.get('noindex', False)
        self.mediatype = self._d['mediatype']
        self.title = self._d.get('title', '')
        self.creators = self._get_list('creator')
        self.date = self._d.get('date')
        self.language = self._d.get('language')

    def _get_list(self, key: str) -> list[Any]:
        raw = self._d.get(key, [])
        if isinstance(raw, list):
            return raw
        return [raw]

class MBState:

    gid: str
    title: str
    artists: list[MBArtist]
    release_dates: list[str]
    language: Optional[str]
    barcode: Optional[str]
    asins: list[str]
    images: list[MBImage]

    def __init__(self, d: dict[str, Any]) -> None:
        self.gid = d['release_gid']
        self.title = d['release_name']
        self.artists = [MBArtist(a['artist_name'], a['artist_gid']) for a in d['artists']]
        self.release_dates = d['release_dates']
        self.language = d.get('language_code')
        self.barcode = d.get('barcode')
        self.asins = d['asins']
        self.images = [MBImage(img, self.gid) for img in d['images']]


class MBArtist(NamedTuple):
    name: str
    gid: str


class MBImage:

    def __init__(self, d: dict[str, Any], mbid: str) -> None:
        self._d = d
        self._mbid = mbid
        self.id = d['id']
        self.filename = f'mbid-{mbid}-{self.id}.{d["suffix"]}'
        self.thumbnails = {
            250: f'mbid-{mbid}-{self.id}_thumb250.jpg',
            500: f'mbid-{mbid}-{self.id}_thumb500.jpg',
            1200: f'mbid-{mbid}-{self.id}_thumb1200.jpg',
        }

    def as_dict(self) -> dict[str, Any]:
        """Imitate the dict entry as it would appear in the index.json."""
        d = dict(self._d)
        suffix = d['suffix']
        del d['suffix']
        d['image'] = f'http://coverartarchive.org/release/{self._mbid}/{self.id}.{suffix}'
        d['thumbnails'] = {
            k: f'http://coverartarchive.org/release/{self._mbid}/{self.id}-{size}.jpg'
            for k, size in (('250', 250), ('500', 500), ('1200', 1200), ('small', 250), ('large', 500))}
        return d


class CAAIndex:

    def __init__(self, json_raw: str) -> None:
        self._d = json.loads(json_raw)

    @property
    def is_dict(self) -> bool:
        return isinstance(self._d, dict)

    @property
    def release_url(self) -> str:
        return self._d.get('release')

    @property
    def images(self) -> Any:
        return self._d.get('images')

    def keys(self) -> set[str]:
        return set(self._d.keys())

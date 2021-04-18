from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

import asyncio
import sys
from pathlib import Path

import aiohttp
import backoff
from aiopath import AsyncPath
from async_property import async_property

if TYPE_CHECKING:
    from loguru import Logger

from json_parser import JSONObject, parse as json_parse

class IAException(Exception):
    """Exceptions in IA responses."""


def handle_backoff(details):
    that = details['args'][0]
    exc = sys.exc_info()[1]
    if exc is None:
        # Shouldn't actually happen
        that._logger.error('Backing off without exception')
        return

    if isinstance(exc, aiohttp.ClientResponseError):
        that._logger.info(
                '{tries}={status} {method} {url} {message}. Retry after {wait:0.1f}s…',
                tries=details['tries'], wait=details['wait'],
                url=exc.request_info.url, method=exc.request_info.method,
                status=exc.status, message=exc.message)
    else:
        that._logger.info(
                '{tries} {message}. Retry after {wait:0.1f}s…',
                tries=details['tries'], wait=details['wait'], message=str(exc))


def handle_giveup(details):
    that = details['args'][0]
    exc = sys.exc_info()[1]
    if exc is None:
        # Shouldn't actually happen
        that._logger.error('Backoff error without exception')
        return

    if isinstance(exc, aiohttp.ClientResponseError):
        that._logger.info(
                '{tries}={status} {method} {url} {message}. Giving up after {elapsed:0.1f}s.',
                tries=details['tries'], elapsed=details['elapsed'],
                url=exc.request_info.url, method=exc.request_info.method,
                status=exc.status, message=exc.message)
    else:
        that._logger.info(
                '{tries} {message}. Giving up after {elapsed:0.1f}s.',
                tries=details['tries'], elapsed=details['elapsed'], message=str(exc))


def handle_success(details):
    that = details['args'][0]
    that._logger.info(
            '{tries} Succeeded after {elapsed:0.4f}s.',
            elapsed=details['elapsed'], tries=details['tries'])


class IAItem:

    def __init__(
            self, identifier: str, cache_dir_path: Path,
            session: aiohttp.ClientSession, logger: Logger
    ) -> None:
        self._cache_dir_path = AsyncPath(cache_dir_path)
        self._identifier = identifier
        self._session = session
        self._logger = logger

    @async_property
    async def metadata(self) -> JSONObject:
        cache_file_path = self._cache_dir_path / 'ia_metadata.json'
        # Try loading cached copy
        cached_json = await self._load_json_from_cache(cache_file_path)

        if cached_json is not None:
            return cached_json

        # Fetch and save
        metadata_json, raw = await self._fetch_metadata()
        await cache_file_path.write_bytes(raw)
        return metadata_json

    @async_property
    async def caa_index(self) -> Optional[bytes]:
        cache_file_path = self._cache_dir_path / 'index.json'
        # Try loading cached copy. Don't attempt to parse JSON, it might be invalid
        cached = await self._load_from_cache(cache_file_path)

        if cached is not None:
            return cached

        # Fetch and save
        index_content = await self._fetch_index()
        if index_content is not None:
            await cache_file_path.write_bytes(index_content)
        return index_content

    @backoff.on_exception(
            backoff.expo, (aiohttp.ClientError, IAException, asyncio.TimeoutError), max_tries=15,
            on_backoff=handle_backoff, on_success=handle_success, on_giveup=handle_giveup)
    async def has_pending_tasks(self) -> bool:
        async with self._session.get('https://archive.org/services/tasks.php', params={
                'summary': 1,
                'identifier': self._identifier}) as resp:
            summary_raw = await resp.read()
            try:
                summary = json_parse(summary_raw)
                if not summary['success']:
                    raise IAException(summary['error'])

                return any(v != 0 for _, v in summary.at_pointer('/value/summary').items())
            except ValueError as exc:
                raise IAException('Unable to parse catalog summary') from exc

    async def _load_from_cache(self, path: AsyncPath) -> Optional[bytes]:
        try:
            content = await path.read_bytes()
            self._logger.info(f'Loaded cached {path.name}')
            return content
        except OSError as exc:
            if not isinstance(exc, FileNotFoundError):
                self._logger.error(f'Failed to load {path.name} from cache: {exc}')
            return None

    async def _load_json_from_cache(self, path: AsyncPath) -> Optional[JSONObject]:
        raw = await self._load_from_cache(path)
        if raw is None:
            return None

        try:
            return json_parse(raw)
        except ValueError as exc:
            self._logger.error(f'Failed to load {path.name} from cache: {exc}')
            return None

    @backoff.on_exception(
            backoff.expo, (aiohttp.ClientError, IAException, asyncio.TimeoutError), max_tries=15,
            on_backoff=handle_backoff, on_success=handle_success, on_giveup=handle_giveup)
    async def _fetch_metadata(self) -> tuple[JSONObject, bytes]:
        """Fetch the metadata of the item.

        :returns:   The metadata. May be empty dict if item doesn't exist.
        """
        url = f'https://archive.org/metadata/{self._identifier}'
        self._logger.info(f'Loading {url}')
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            metadata_raw = await resp.read()

            try:
                metadata = json_parse(metadata_raw)
            except ValueError as exc:
                raise IAException('Malformed metadata') from exc

            if metadata and 'error' in metadata:
                raise IAException(metadata['error'])

            # Empty metadata, should be 404
            if not metadata:
                self._logger.info('Got empty metadata, item should be 404')
                if not await self._is_404():
                    raise IAException('Empty response on non-404 item')

            return metadata, metadata_raw

    @backoff.on_exception(
            backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=15,
            on_backoff=handle_backoff, on_success=handle_success, on_giveup=handle_giveup)
    async def _fetch_index(self) -> Optional[bytes]:
        """Fetch the index.json of the item.

        :returns:   The index.json content, or None is it doesn't exist.
        """
        url = f'https://archive.org/download/{self._identifier}/index.json'
        self._logger.info(f'Loading {url}')
        async with self._session.get(url) as resp:
            # Allow 404, it's a possible problem
            if resp.status == 404:
                return None

            # Any other error should be retried or eventually skip the item
            resp.raise_for_status()

            return await resp.read()

    @backoff.on_exception(
            backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=15,
            on_backoff=handle_backoff, on_success=handle_success, on_giveup=handle_giveup)
    async def _is_404(self) -> bool:
        self._logger.info(f'Checking whether {self._identifier} is 404')
        async with self._session.get(f'https://archive.org/details/{self._identifier}') as resp:
            return resp.status == 404

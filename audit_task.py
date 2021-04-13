from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

import json
import textwrap
from pathlib import Path

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

import pendulum
from aiohttp import ClientSession
from aiopath import AsyncPath

if TYPE_CHECKING:
    from loguru import Logger

from audit_result import CheckFailed, CheckPassed, CheckResult, CheckSkipped, ItemSkipped
from ia_item import IAItem
from index_schema import INDEX_SCHEMA
from result_aggregator import ResultAggregator

class AuditTask:
    def __init__(
            self, mbstate: dict[str, Any], audit_path: Path,
            session: ClientSession, logger: Logger
    ) -> None:
        self._mbstate = mbstate
        self._mbid = mbstate['release_gid']
        self._audit_path = AsyncPath(audit_path)
        self._logger = logger
        self._ia_item = IAItem(f'mbid-{self._mbid}', audit_path, session, logger)

    async def run(self, aggregator: ResultAggregator) -> None:
        start_time = pendulum.now()
        self._logger.info(
                f'STARTING AUDIT TASK FOR {self._mbid} AT {start_time.to_rfc1123_string()}')
        results: list[CheckResult]
        had_exception = False
        try:
            results = [res async for res in self._run()]
        except Exception as exc:
            had_exception = True
            self._logger.exception(exc)
            results = [ItemSkipped(self._mbid, f'InternalError::{exc.__class__.__name__}', exc)]

        await self._report_results(results, aggregator)

        if had_exception:
            status_text = 'FAILED'
        else:
            status_text = 'FINISHED'
        end_time = pendulum.now()
        self._logger.error(
                'AUDIT TASK FOR {mbid} {status_text} AT {end_time_str} (took {elapsed:0.1f}s)',
                mbid=self._mbid, status_text=status_text,
                end_time_str=end_time.to_rfc1123_string(),
                elapsed=(end_time - start_time).total_seconds)

    async def _report_results(
            self, results: list[CheckResult], aggregator: ResultAggregator
    ) -> None:
        await (self._audit_path / 'failures.log').write_text('\n'.join(
                str(res) for res in results if isinstance(res, CheckFailed)))
        async with aggregator.lock:
            await aggregator.put(results)

        num_failed = len([res for res in results if isinstance(res, CheckFailed)])
        num_skipped = len([res for res in results if isinstance(res, ItemSkipped)])
        num_success = len([res for res in results if isinstance(res, CheckPassed)])

        if num_success == len(results):
            self._logger.info(f'All {num_success} checks passed.')
        else:
            self._logger.info(f'{num_success} successful checks, {num_failed} failed checks, {num_skipped} skipped checks.')
            self._logger.info('Summary:')
            max_desc_length = max(len(res.check_description) for res in results)
            for check_result in results:
                self._logger.info(check_result.check_description.ljust(max_desc_length) + ' …' + check_result.check_state)
                if isinstance(check_result, CheckFailed) and check_result.additional_data is not None:
                    self._logger.info('    Additional failure data:')
                    self._logger.info(textwrap.indent(str(check_result.additional_data), ' ' * 4))

    async def _run(self) -> AsyncIterable[CheckResult]:
        self._logger.info('Retrieving IA item metadata…')
        ia_meta = self._ia_item.metadata

        if not ia_meta:
            # 404, BAD!
            self._logger.error('Received empty metadata, item does not exist! Aborting…')
            yield CheckFailed(self._mbid, 'Item::exists')
            return

        self._logger.info('Metadata fetched')
        yield CheckPassed(self._mbid, 'Item::exists')

        self._logger.info('Checking whether there are any pending catalog tasks…')
        if await self._ia_item.has_pending_tasks():
            self._logger.info('Item has pending tasks and may get modified later. Aborting…')
            yield ItemSkipped(self._mbid, 'Item::has pending tasks')
            return

        self._logger.info('No pending tasks, continuing with audit')

        self._logger.info('Checking whether item is dark…')
        if ia_meta.get('is_dark', False):
            self._logger.info('Cannot audit this item since it is darkened. Aborting…')
            yield ItemSkipped(self._mbid, 'Item::darkened')
            return

        self._logger.info('Item is accessible!')

        ia_last_modified = pendulum.from_timestamp(ia_meta['item_last_modified'])
        mb_max_last_modified = pendulum.parse(self._mbstate['max_last_modified'])

        if ia_last_modified >= mb_max_last_modified:
            self._logger.info(''.join([
                    'Cannot audit this item since it was modified on ',
                    ia_last_modified.to_rfc1123_string(),
                    ', which is after the DB state as of ',
                    mb_max_last_modified.to_rfc1123_string(),
                    '. Aborting…']))
            yield ItemSkipped(self._mbid, 'Item::outdated mb state')
            return

        self._logger.info('IA item has not recently been modified.')

        if 'metadata' not in ia_meta:
            self._logger.error('Item missing IA metadata. Aborting…')
            yield CheckFailed(self._mbid, 'Metadata::missing metadata key')
            return

        async for metadata_check_result in self._run_metadata_checks(ia_meta):
            yield metadata_check_result

        self._logger.log('')

        async for files_check_result in self._run_files_checks(ia_meta):
            yield files_check_result

        self._logger.log('')

        async for index_check_result in self._run_index_checks():
            yield index_check_result

    def _simple_check(
        self, category: str, check_success: bool,
        pre_log_msg: str, failure_msg: str, additional_data: Optional[Any] = None,
    ) -> CheckResult:
        if check_success:
            self._logger.info(f'{pre_log_msg}… Yes')
            return CheckPassed(self._mbid, category, additional_data)
        else:
            self._logger.info(f'{pre_log_msg}… No')
            self._logger.error(failure_msg)
            return CheckFailed(self._mbid, category, additional_data)

    async def _run_metadata_checks(
            self, ia_meta: dict[str, Any]
    ) -> AsyncIterable[CheckResult]:
        start_time = pendulum.now()
        self._logger.info('*** Starting IA metadata checks')

        metadata = ia_meta['metadata']

        def get_as_list(key: str) -> list[Any]:
            data = metadata.get(key)
            if data is None:
                return []
            if isinstance(data, list):
                return data
            return [data]

        def get_single(key: str, default: Any = None) -> tuple[Optional[Any], CheckResult]:
            data = metadata.get(key, default)
            if data is None:
                return (None, CheckFailed(self._mbid, f'Metadata::Precheck::{key} is singular', data))
            if not isinstance(data, (str, int, bool, float)):
                self._logger.error(f'Expected {key} to be a single value, got {data}')
                return (None, CheckFailed(self._mbid, f'Metadata::Precheck::{key} is singular', data))
            return (data, CheckPassed(self._mbid, f'Metadata::Precheck::{key} is singular', data))

        # Metadata::in caa collection
        collections = get_as_list('collection')
        yield self._simple_check(
                'Metadata::in caa collection',
                'coverartarchive' in collections,
                '`coverartarchive` present in `metadata.collection`',
                f'Expected coverartarchive to be in {collections}',
                collections)

        # Metadata::item is noindex
        actual_noindex, noindex_precheck = get_single('noindex', False)
        yield noindex_precheck
        yield self._simple_check(
                'Metadata::item is noindex',
                bool(actual_noindex),
                'Item is set to `noindex`',
                'Expected item to be set to `noindex`',
                actual_noindex)

        # Metadata::mediatype is image
        actual_mediatype, mediatype_precheck = get_single('mediatype')
        yield mediatype_precheck
        yield self._simple_check(
                'Metadata::mediatype is image',
                actual_mediatype == 'image',
                'Item is of `image` media type',
                'Expected item to be of `image` media type',
                actual_mediatype)

        # Metadata::title correct
        expected_title = self._mbstate["release_name"]
        actual_title, title_precheck = get_single('title')
        yield title_precheck
        yield self._simple_check(
                'Metadata::title correct',
                actual_title == expected_title,
                f'Title is {expected_title}',
                f'Expected title to be {expected_title}, got {actual_title}',
                actual_title)

        # Metadata::creators correct
        expected_creators = set(artist['artist_name'] for artist in self._mbstate['artists'])
        actual_creators = set(get_as_list('creator'))
        yield self._simple_check(
                'Metadata::creators correct',
                actual_creators == expected_creators,
                f'Creators are {"; ".join(expected_creators)}',
                f'Expected creators to be {"; ".join(expected_creators)}, got {"; ".join(actual_creators)}',
                actual_creators)

        # Metadata::date correct
        expected_date = set(self._mbstate['release_dates'])
        exp_date_str = str(expected_date) if expected_date else '{}'
        actual_date, date_precheck = get_single('date')
        yield date_precheck
        yield self._simple_check(
                'Metadata::date correct',
                bool(expected_date) == bool(actual_date) and actual_date in expected_date,
                f'Date is in {exp_date_str}',
                f'Expected date to be one of {exp_date_str}, got {actual_date}',
                actual_date)

        # Metadata::language correct
        expected_language = self._mbstate['language_code']
        actual_language, language_precheck = get_single('language')
        yield self._simple_check(
                'Metadata::language correct',
                expected_language == actual_language,
                f'Language is {expected_language}',
                f'Expected language to be {expected_language}, got {actual_language}',
                actual_language)

        external_ids = set(get_as_list('external-identifier'))
        expected_ext_ids = {
            f'urn:mb_release_id:{self._mbstate["release_gid"]}',
            *(f'urn:mb_artist_id:{artist["artist_gid"]}' for artist in self._mbstate['artists']),
            *(f'urn:asin:{asin}' for asin in self._mbstate['asins']),
            f'urn:barcode:{self._mbstate["barcode"]}',
        }
        # Metadata::unexpected external id
        for ext_id in external_ids:
            yield self._simple_check(
                    'Metadata::unexpected external id',
                    ext_id in expected_ext_ids,
                    f'{ext_id} is an expected identifier',
                    f'{ext_id} should not be attached to this item',
                    ext_id)

        # Metadata::missing external id
        for ext_id in expected_ext_ids:
            yield self._simple_check(
                    'Metadata::missing external id',
                    ext_id in external_ids,
                    f'{ext_id} is attached to this item',
                    f'{ext_id} is not attached to this item, but should be',
                    ext_id)

        self._logger.info(
                '*** Finished IA metadata checks (took {elapsed:0.1f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

    async def _run_files_checks(
            self, ia_meta: dict[str, Any]
    ) -> AsyncIterable[CheckResult]:
        start_time = pendulum.now()
        self._logger.info('*** Starting IA files checks')

        files = ia_meta.get('files', [])
        def get_file(name: str) -> Optional[dict[str, str]]:
            return next((f for f in files if f.get('name') == name), None)

        def has_file(name: str) -> bool:
            return get_file(name) is None

        # Files::index.json exists
        # Files::mb_metadata.xml exists
        for exp_file_name in ('index.json', 'mb_metadata.xml'):
            yield self._simple_check(
                    f'Files::{exp_file_name} exists',
                    has_file(exp_file_name),
                    f'Checking whether {exp_file_name} exists',
                    f'{exp_file_name} is not in item file list')

        for image in self._mbstate['covers']:
            cover_id = image['id']
            cover_ext = image['extension']

            # Files::original image exists
            yield self._simple_check(
                    'Files::original image exists',
                    has_file(f'mbid-{self._mbid}-{cover_id}.{cover_ext}'),
                    f'Checking whether {cover_id} exists',
                    f'{cover_id} is not in IA file list, possibly disastrous!')

            # Files::250px thumbnail exists
            # Files::500px thumbnail exists
            # Files::1200px thumbnail exists
            for thumbsize in ('250', '500', '1200'):
                yield self._simple_check(
                    f'Files::{thumbsize}px thumbnail exists',
                    has_file(f'mbid-{self._mbid}-{cover_id}_thumb{thumbsize}.jpg'),
                    f'Checking whether {thumbsize}px thumbnail for {cover_id} exists',
                    f'{thumbsize}px thumbnail for {cover_id} is not in IA file list, should be re-derived')

            # Files::image id is unique
            images_with_id = [
                    f for f in files
                    if (f.get('name', '').startswith(
                            f'mbid-{self._mbid}-{cover_id}.')
                        and f.get('source') == 'original')]
            yield self._simple_check(
                    'Files::image id is unique',
                    len(images_with_id) == 1,
                    f'Checking whether {cover_id} has only one source file',
                    f'Multiple source files for {cover_id} exist, this may lead to issues with derivation',
                    images_with_id)

            self._logger.info(
                '*** Finished IA files checks (took {elapsed:0.1f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

    async def _run_index_checks(self) -> AsyncIterable[CheckResult]:
        start_time = pendulum.now()
        self._logger.info('*** Starting CAA index.json checks')

        self._logger.info('Loading CAA index.json')
        index_raw = self._ia_item.caa_index

        # CAAIndex::is present
        yield self._simple_check(
                'CAAIndex::is present',
                index_raw is not None,
                'Checking whether index.json is present',
                'index.json not present. Aborting rest of checks…')
        if index_raw is None:
            return

        # CAAIndex::is well-formed
        self._logger.info('Attempting to parse index.json as JSON')
        try:
            index = json.loads(index_raw)
            yield CheckPassed(self._mbid, 'CAAIndex::is well-formed')
        except json.JSONDecodeError as exc:
            yield CheckFailed(self._mbid, 'CAAIndex::is well-formed', exc)
            self._logger.error('index.json not well-formed!')
            self._logger.exception(exc)
            self._logger.error('Aborting rest of checks…')
            return

        self._logger.info('index.json parsed successfully')

        # CAAIndex::Schema::
        outdated_schema = False
        self._logger.info('Verifying index.json schema')
        async for schema_check_result in self._verify_schema_rec(index, INDEX_SCHEMA, 'root', 'root'):
            if isinstance(schema_check_result, CheckFailed):
                outdated_schema = True
            yield schema_check_result

        # Don't check the index if its schema is outdated, otherwise we may run
        # into non-existent keys
        if outdated_schema:
            self._logger.info('Schema verification encountered failures, aborting rest of index check…')
            return

        self._logger.info('Schema verification passed successfully')

        # CAAIndex::release url correct
        yield self._simple_check(
                'CAAIndex::release url correct',
                index['release'] == f'https://musicbrainz.org/release/{self._mbid}',
                'Checking whether release URL is correct',
                'Encountered incorrect release URL!',
                index['release'])

        cover_id_to_cover = {
                cover['id']: cover for cover in self._mbstate['covers']}
        ia_images = index['images']

        for cover_id in cover_id_to_cover:
            matching_covers = [c for c in ia_images if c['id'] == cover_id]
            # CAAIndex::Image::missing image
            yield self._simple_check(
                    'CAAIndex::Image::missing image',
                    len(matching_covers) > 1,
                    f'Checking whether {cover_id} exists in index.json',
                    f'{cover_id} not present in index.json')

            # CAAIndex::Image::image id is unique
            yield self._simple_check(
                    'CAAIndex::Image::image id is unique',
                    len(matching_covers) <= 1,
                    f'Checking whether at most one image with {cover_id} exists in index.json',
                    f'{cover_id} has multiple images',
                    matching_covers)

        for image in ia_images:
            # CAAIndex::Image::unexpected image
            yield self._simple_check(
                    'CAAIndex::Image::unexpected image',
                    image['id'] in cover_id_to_cover,
                    f'Checking whether {image["id"]} is an expected image',
                    f'Unexpected image {image["id"]} in index.json')
            if image['id'] not in cover_id_to_cover:
                continue

            caa_image = cover_id_to_cover[image['id']]

            # CAAIndex::Image::edit
            edit_cr = self._simple_check(
                    'CAAIndex::Image::edit',
                    image['edit'] == caa_image['edit_id'],
                    'Checking whether edit is correct',
                    f'Wrong edit ID for image {image["id"]}',
                    image)
            yield edit_cr

            # CAAIndex::Image::edit approval status
            if isinstance(edit_cr, CheckFailed):
                self._logger.info('Skipping edit approval status check, edit is incorrect')
                yield CheckSkipped(self._mbid, 'CAAIndex::Image::edit approval status')
            else:
                yield self._simple_check(
                        'CAAIndex::Image::edit approval status',
                        image['approved'] == caa_image['edit_approved'],
                        'Checking whether edit approval status is correct',
                        f'Wrong edit approval status for image {image["id"]}',
                        image)

            # CAAIndex::Image::comment
            yield self._simple_check(
                    'CAAIndex::Image::comment',
                    image['comment'] == caa_image['comment'],
                    'Checking whether comment is correct',
                    f'Wrong comment for image {image["id"]}',
                    image)

            # CAAIndex::Image::types
            types_cr = self._simple_check(
                    'CAAIndex::Image::types',
                    set(image['types']) == set(caa_image['types']),
                    'Checking whether types are correct',
                    f'Wrong types for image {image["id"]}',
                    image)
            yield types_cr

            # CAAIndex::Image::front
            # CAAIndex::Image::back
            main_front_id = next((c['id'] for c in caa_image if 'Front' in caa_image['types']), None)
            main_back_id = next((c['id'] for c in caa_image if 'Back' in caa_image['types']), None)
            if isinstance(edit_cr, CheckFailed):
                self._logger.info('Skipping front and back checks, types are incorrect')
                yield CheckSkipped(self._mbid, 'CAAIndex::Image::front')
                yield CheckSkipped(self._mbid, 'CAAIndex::Image::back')
            else:
                yield self._simple_check(
                        'CAAIndex::Image::front',
                        image['front'] == (image['id'] == main_front_id),
                        'Checking whether main front status is correct',
                        f'Wrong main front status for image {image["id"]}',
                        image)

                yield self._simple_check(
                        'CAAIndex::Image::front',
                        image['back'] == (image['id'] == main_back_id),
                        'Checking whether main back status is correct',
                        f'Wrong main back status for image {image["id"]}',
                        image)

            # CAAIndex::Image::image url
            yield self._simple_check(
                    'CAAIndex::Image::image url',
                    image['image'] == f'http://coverartarchive.org/release/{self._mbid}/{image["id"]}.{caa_image["extension"]}',
                    'Checking whether image url is correct',
                    f'Wrong image url for image {image["id"]}',
                    image)

            # CAAIndex::Image::thumbnails
            exp_thumbnails = {
                name: f'http://coverartarchive.org/release/{self._mbid}/{image["id"]}-{size}.jpg'
                for name, size in (
                    ('small', '250'), ('large', '500'),
                    ('250', '250'), ('500', '500'), ('1200', '1200'))
            }
            yield self._simple_check(
                    'CAAIndex::Image::thumbnails',
                    image['thumbnails'] == exp_thumbnails,
                    'Checking whether image thumbnails are correct',
                    f'Wrong thumbnails for image {image["id"]}',
                    image)

        # CAAIndex::Image::order
        caa_order = [image['id'] for image in self._mbstate['covers']]
        index_order = [image['id'] for image in ia_images]
        yield self._simple_check(
                'CAAIndex::Image::order',
                caa_order == index_order,
                'Checking whether images are ordered correctly',
                'Wrong order for images',
                index_order)

        self._logger.info(
                '*** Finished CAA index.json checks (took {elapsed:0.1f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

    async def _verify_schema_rec(
            self, index: Any, schema: Any, path: str, full_path: str
    ) -> AsyncIterable[CheckResult]:
        if isinstance(schema, type):
            exp_type = schema
        else:
            exp_type = type(schema)

        cr = self._simple_check(
                f'CAAIndex::Schema::{path} is {exp_type.__name__}',
                isinstance(schema, exp_type),
                f'Checking whether {full_path} is a {exp_type.__name__}',
                f'{full_path} is not a {exp_type.__name__}, aborting further checks along this path…',
                index)
        if isinstance(cr, CheckFailed):
            return

        if isinstance(schema, dict):
            # Check keys
            # If we're in thumbnails, first check them all together, as we don't
            # want to spam 3 audit failures on old-style thumbnails
            if path == 'root.images[].thumbnails':
                yield self._simple_check(
                        f'CAAIndex::Schema::{path} is new-style',
                        index.keys() == {'small', 'large', '250', '500', '1200'},
                        f'Checking whether {full_path} is new-style',
                        f'{full_path} is not new-style and may lead to incompatibilities',
                        index)
            else:
                for k in schema.keys():
                    yield self._simple_check(
                            f'CAAIndex::Schema::{k} in {path}',
                            k in index,
                            f'Checking whether {k} is in {full_path}',
                            f'Expected key {k} in {full_path}, but absent',
                            index)

                for k in index.keys() - schema.keys():
                    yield self._simple_check(
                            f'CAAIndex::Schema::unexpected {k} in {path}',
                            True,
                            f'Checking whether {k} is expected in {full_path}',
                            f'Unexpected key {k} in {full_path}',
                            index)

            # Check values
            for k in schema.keys():
                if k not in index:
                    # Already warned previously
                    self._logger.info(f'Skipping check for value of {k}, not present in index')
                    continue

                async for rec_cr in self._verify_schema_rec(
                        index[k], schema[k], f'{path}.{k}', f'{full_path}.{k}'):
                    yield rec_cr

        elif isinstance(schema, list):
            # Check each entry
            subschema = schema[0]
            ext_simple_path = f'{path}[]'
            for el_idx, subindex in enumerate(index):
                ext_full_path = f'{full_path}[{el_idx}]'
                async for rec_cr in self._verify_schema_rec(
                        subindex, subschema, ext_simple_path, ext_full_path):
                    yield rec_cr

from __future__ import annotations

from typing import Any, Callable, Optional, TYPE_CHECKING

import re
import textwrap
from collections import Counter, defaultdict
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path

if TYPE_CHECKING:
    from collections.abc import Generator, AsyncIterable

import pendulum
from aiohttp import ClientSession
from aiopath import AsyncPath

if TYPE_CHECKING:
    from loguru import Logger
    from pendulum.datetime import DateTime

from abstractions import CAAIndex, CheckStage, IAFile, IAFiles, IAMeta, IAState, MBImage, MBState
from audit_result import CheckFailed, CheckPassed, CheckResult, ItemSkipped
from ia_item import IAItem
from result_aggregator import ResultCollector


class AuditTask:

    _start_stage_cbs: list[Callable[[CheckStage], None]]
    _finish_stage_cbs: list[Callable[[CheckStage], None]]

    def __init__(
            self, task_record: dict[str, Any], max_last_modified: DateTime,
            audit_path: Path, session: ClientSession, logger: Logger,
    ) -> None:
        self._record = task_record
        self._mbid = task_record['id']
        self._max_last_modified = max_last_modified
        self.audit_path = AsyncPath(audit_path)
        self._logger = logger
        self._ia_item = IAItem(f'mbid-{self._mbid}', audit_path, session, logger)

        self._results: list[CheckResult] = []
        self._start_stage_cbs = []
        self._finish_stage_cbs = []

    def set_start_stage_cb(
            self, cb: Callable[[CheckStage], None],
    ) -> None:
        self._start_stage_cbs.append(cb)

    def set_finish_stage_cb(
            self, cb: Callable[[CheckStage], None],
    ) -> None:
        self._finish_stage_cbs.append(cb)

    @contextmanager
    def _stage(self, stage: CheckStage) -> Generator[None, None, None]:
        for cb in self._start_stage_cbs:
            cb(stage)
        try:
            yield
        finally:
            for cb in self._finish_stage_cbs:
                cb(stage)

    async def run(self, aggregator: ResultCollector) -> None:
        start_time = pendulum.now()
        self._logger.info(
                f'STARTING AUDIT TASK FOR {self._mbid} AT {start_time.to_rfc1123_string()}')
        had_exception = False
        try:
            await self._run()
        except Exception as exc:
            had_exception = True
            self._logger.opt(exception=True).critical(f'Internal error')
            self._results.append(ItemSkipped(self._mbid, f'InternalError::{exc.__class__.__name__}', exc))

        with self._stage(CheckStage.report):
            await self._report_results(aggregator)

        if had_exception:
            status_text = 'FAILED'
        else:
            status_text = 'FINISHED'
        end_time = pendulum.now()
        self._logger.info(
                'AUDIT TASK FOR {mbid} {status_text} AT {end_time_str} (took {elapsed:0.4f}s)',
                mbid=self._mbid, status_text=status_text,
                end_time_str=end_time.to_rfc1123_string(),
                elapsed=(end_time - start_time).total_seconds())

    async def _report_results(
            self, aggregator: ResultCollector
    ) -> None:
        failed: list[CheckFailed] = []
        num_failed = num_skipped = num_passed = 0

        for res in self._results:
            if isinstance(res, CheckFailed):
                num_failed += 1
                failed.append(res)
            elif isinstance(res, CheckPassed):
                num_passed += 1
            elif isinstance(res, ItemSkipped):
                num_skipped += 1
            else:
                assert False

        await (self.audit_path / 'failures.log').write_text('\n'.join(map(str, failed)))

        aggregator.put(self._results)

        if num_passed == len(self._results):
            self._logger.info(f'All {num_passed} checks passed.')
        else:
            self._logger.info(f'{num_passed} successful checks, {num_failed} failed checks, {num_skipped} skipped checks.')
            self._logger.info('Summary:')
            max_desc_length = max(len(res.check_description) for res in self._results)
            for check_result in self._results:
                self._logger.info(check_result.check_description.ljust(max_desc_length) + ' … ' + check_result.check_state)
                if isinstance(check_result, CheckFailed) and check_result.additional_data is not None:
                    self._logger.info('    Additional failure data:')
                    self._logger.info(textwrap.indent(str(check_result.additional_data), ' ' * 4))

    @cached_property
    def base_category(self) -> str:
        return {
            'possibly_deleted': 'DeletedItem',
            'merged': 'MergedItem',
            'empty': 'EmptyItem',
            'active': 'Item'
        }[self._record['state']]

    def _pass(self, category: str) -> None:
        self._results.append(CheckPassed(self._mbid, f'{self.base_category}::{category}'))

    def _fail(self, category: str) -> None:
        self._results.append(CheckFailed(self._mbid, f'{self.base_category}::{category}'))

    def _item_skip(self, category: str) -> None:
        self._results.append(ItemSkipped(self._mbid, f'{self.base_category}::{category}'))

    def _check(
        self, category: str, check_success: bool,
        failure_msg: str, fail_reporter: Optional[Callable[[str], None]] = None
    ) -> bool:
        reporter: Callable[[str], None]
        if check_success:
            reporter = self._pass
        else:
            if fail_reporter is None:
                reporter = self._fail
            else:
                reporter = fail_reporter
            self._logger.error(failure_msg)

        reporter(category)

        return check_success

    def _original_recently_modified(self, iaf: IAFile) -> bool:
        return (iaf.name not in ('__ia_thumb.jpg', f'mbid-{self._mbid}_files.xml')
                and iaf.last_modified > self._max_last_modified)

    async def _run(self) -> None:
        self._logger.info('Retrieving IA item metadata…')
        with self._stage(CheckStage.fetch):
            ia_state_raw = await self._ia_item.metadata

        if not self._check(
                'exists', bool(ia_state_raw),
                'Received empty metadata, item does not exist! Aborting…'):
            return

        ia_state = IAState(ia_state_raw)

        self._logger.info('Checking whether there are any pending catalog tasks…')
        if not self._check(
                'has pending tasks', not await self._ia_item.has_pending_tasks(),
                'Item has pending tasks and may get modified later. Aborting…',
                self._item_skip):
            return

        if not self._check(
                'darkened', not ia_state.is_dark,
                'Cannot audit this item since it is darkened. Aborting…',
                self._item_skip):
            return

        if not self._check(
                'ia modified',
                ia_state.last_modified < self._max_last_modified or not ia_state.files.has_any_original(self._original_recently_modified),
                ''.join((
                    'Cannot audit this item since it was modified on ',
                    ia_state.last_modified.to_rfc1123_string(),
                    ', which is after the DB state as of ',
                    self._max_last_modified.to_rfc1123_string(),
                    '. Aborting…')),
                self._item_skip):
            return

        if self._record['state'] in ('active', 'empty'):
            await self._run_active_checks(ia_state)
        elif self._record['state'] in ('possibly_deleted', 'merged'):
            with self._stage(CheckStage.meta):
                self._run_deleted_checks(ia_state)

    async def _run_active_checks(self, ia_state: IAState) -> None:
        mb_state = MBState(self._record['data'])
        with self._stage(CheckStage.fetch):
            self._logger.info('Loading index.json')
            index_raw = await self._ia_item.caa_index

        with self._stage(CheckStage.meta):
            self._run_metadata_checks(ia_state.meta, mb_state)

        with self._stage(CheckStage.files):
            self._run_files_checks(ia_state.files, mb_state)

        with self._stage(CheckStage.index):
            self._run_index_checks(index_raw, mb_state)

    def _run_deleted_checks(self, ia_state: IAState) -> None:
        start_time = pendulum.now()
        self._logger.info('*** Starting IA deleted item checks')

        files = ia_state.files

        # Skip this check if the item never contained an index.json and is a
        # possibly_deleted item, it was probably uploaded from test.
        if not self._check(
                'test item', files.has_original('index.json') or files.has_historical('index.json'),
                'Possibly deleted item does not contain and never has contained index.json. Likely from test, aborting…',
                self._item_skip):
            return

        self._check(
                'index is absent', not files.has_original('index.json'),
                'Item still has an index.json file which has not been removed')

        self._check(
                'images are absent',
                not files.has_any_original(lambda iaf: iaf.name.split('.')[-1] in ('png', 'jpg', 'gif', 'pdf') and iaf.name.startswith(f'mbid-{self._mbid}-')),
                'Item still has an original image which has not been removed')

        self._check(
                'derivatives are absent',
                not files.has_any_derivative(lambda name: name.startswith(f'mbid-{self._mbid}-')),
                'Item still has a derived file which has not been removed')

        self._check(
                'mb_metadata is absent', not files.has_original(f'mbid-{self._mbid}_mb_metadata.xml'),
                'Item still has an mb_metadata.xml file which has not been removed')

        if self._record['state'] == 'possibly_deleted':
            # Only check this for deleted items, for merged ones, it still redirects
            self._check(
                    'release url is absent',
                    f'urn:mb_release_id:{self._mbid}' not in ia_state.meta.external_ids,
                    'Item still has a release URN, but the link will be dead')

        self._logger.info(
                '*** Finished IA deleted item checks (took {elapsed:0.4f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

    def _run_metadata_checks(self, ia_meta: IAMeta, mb_state: MBState) -> None:
        start_time = pendulum.now()
        self._logger.info('*** Starting IA metadata checks')

        # Metadata::in caa collection
        self._check(
                'Metadata::in caa collection',
                'coverartarchive' in ia_meta.collections,
                f'Item not in coverartarchive collection, but in {ia_meta.collections}')

        # Metadata::item is noindex
        self._check(
                'Metadata::item is noindex', ia_meta.is_noindex,
                'Item is not set to noindex')

        # Metadata::mediatype is image
        self._check(
                'Metadata::mediatype is image', ia_meta.mediatype == 'image',
                f'Expected item to be of `image` media type, is actually {ia_meta.mediatype}')

        # Metadata::title correct
        self._check(
                'Metadata::title correct', ia_meta.title == mb_state.title,
                f'Expected title to be {mb_state.title}, got {ia_meta.title}')

        # Metadata::creators correct
        expected_creators = [artist.name for artist in mb_state.artists]
        self._check(
                'Metadata::creators correct',
                ia_meta.creators == expected_creators,
                f'Expected creators to be {"; ".join(expected_creators)}, got {"; ".join(ia_meta.creators)}')

        # Metadata::date correct
        self._check(
                'Metadata::date correct',
                bool(ia_meta.date) == bool(mb_state.release_dates) and ia_meta.date in mb_state.release_dates,
                f'Expected date to be one of {mb_state.release_dates}, got {ia_meta.date}')

        # Metadata::language correct
        self._check(
                'Metadata::language correct', ia_meta.language == mb_state.language,
                f'Expected language to be {mb_state.language}, got {ia_meta.language}')

        expected_ext_ids = {
            f'urn:mb_release_id:{mb_state.gid}',
            *(f'urn:mb_artist_id:{artist.gid}' for artist in mb_state.artists),
            *(f'urn:asin:{asin}' for asin in mb_state.asins),
        }
        if mb_state.barcode:
            expected_ext_ids.add(f'urn:upc:{mb_state.barcode}')

        # Metadata::unexpected external id
        for ext_id in ia_meta.external_ids:
            id_type = ext_id.split(':')[1]
            self._check(
                    f'Metadata::unexpected external id::{id_type}',
                    ext_id in expected_ext_ids,
                    f'Unexpected external ID {ext_id}')

        # Metadata::missing external id
        for ext_id in expected_ext_ids:
            id_type = ext_id.split(':')[1]
            self._check(
                    f'Metadata::missing external id::{id_type}',
                    ext_id in ia_meta.external_ids,
                    f'Missing external ID {ext_id}')

        self._logger.info(
                '*** Finished IA metadata checks (took {elapsed:0.4f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

    def _run_files_checks(
            self, ia_files: IAFiles, mb_state: MBState,
    ) -> None:
        start_time = pendulum.now()
        self._logger.info('*** Starting IA files checks')

        # Files::index.json exists
        self._check(
                'Files::index.json exists',
                ia_files.has_original('index.json'),
                'index.json is not in item file list')

        # Files::mb_metadata.xml exists
        self._check(
                'Files::mb_metadata.xml exists',
                ia_files.has_original(f'mbid-{self._mbid}_mb_metadata.xml'),
                'mb_metadata.xml is not in item file list')

        for caa_image in mb_state.images:
            self._run_image_check(caa_image, ia_files)

        self._logger.info(
                '*** Finished IA files checks (took {elapsed:0.4f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

    def _run_image_check(
            self, caa_image: MBImage, ia_files: IAFiles
    ) -> None:
        self._logger.info(f'Checking image {caa_image.id}')

        # Files::original image exists
        self._check(
                'Files::original image exists',
                ia_files.has_original(caa_image.filename),
                f'{caa_image.id} is not in IA file list, possibly disastrous!')

        # Files::250px thumbnail exists
        # Files::500px thumbnail exists
        # Files::1200px thumbnail exists
        for thumb_size, thumb_filename in caa_image.thumbnails.items():
            self._check(
                    f'Files::{thumb_size}px thumbnail exists',
                    ia_files.has_derivative(thumb_filename),
                    f'{thumb_size}px thumbnail for {caa_image.id} not in IA file list')

        # Files::image id is unique
        self._check(
                'Files::image id is unique',
                # Include extension in match, ...jpg_meta.txt shouldn't be counted
                len(ia_files.find_originals(lambda iaf: re.match(rf'mbid-{self._mbid}-{caa_image.id}\.[a-zA-Z0-9]+$', iaf.name) is not None)) == 1,
                f'Multiple source files for {caa_image.id} exist, this may lead to issues with derivation')

    def _run_index_checks(self, index_raw: bytes, mb_state: MBState) -> None:
        start_time = pendulum.now()
        self._logger.info('*** Starting CAA index.json checks')

        # CAAIndex::is present
        if not self._check(
                'CAAIndex::is present',
                index_raw is not None,
                'index.json not present. Aborting rest of checks…'):
            return

        # CAAIndex::is well-formed
        self._logger.info('Attempting to parse index.json as JSON')
        try:
            index = CAAIndex(index_raw)
        except ValueError as exc:
            self._fail('CAAIndex::is well-formed')
            self._logger.error('index.json not well-formed!')
            self._logger.exception(exc)
            self._logger.error('Aborting rest of checks…')
            return

        if not self._check(
                'CAAIndex::is well-formed', index.is_dict,
                'index.json is not well-formed, aborting…'):
            return

        self._logger.info('index.json parsed successfully')

        if not self._check(
                'CAAIndex::has all keys', {'release', 'images'}.issubset(index.keys()),
                'CAA index missing a required key! Aborting…'):
            return

        self._check(
                'CAAIndex::unexpected key', {'release', 'images'} == index.keys(),
                f'CAA index has unexpected key: {index.keys()}')

        # CAAIndex::release url correct
        self._check(
                'CAAIndex::release url correct',
                index.release_url == f'https://musicbrainz.org/release/{self._mbid}',
                f'Encountered incorrect release URL: {index.release_url}')

        id_to_mb_image = {cover.id: cover for cover in mb_state.images}
        idx_image_id_count: Counter[int] = Counter()
        found_images: set[int] = set()
        idx_order: list[int] = []

        for idx_image in index.images:
            if not self._check(
                    'CAAIndex::Image::is well-formed', isinstance(idx_image, dict) and 'id' in idx_image,
                    f'index image is not a dict: {idx_image}'):
                continue

            if self._check(
                    'CAAIndex::Image::id is int', isinstance(idx_image['id'], int),
                    'index image id is not an int'):
                idx_id = idx_image['id']
            else:
                try:
                    idx_id = int(idx_image['id'])
                except ValueError as exc:
                    self._logger.exception('Could not convert old-style id to int, skipping…')
                    continue

            idx_order.append(idx_id)

            if not self._check(
                    'CAAIndex::Image::unexpected image', idx_id in id_to_mb_image,
                    f'index image {idx_id} not found in MB state, skipping…'):
                continue

            mb_image = id_to_mb_image[idx_id].as_dict()

            for k in mb_image.keys():
                if not self._check(
                        f'CAAIndex::Image::has {k}', k in idx_image,
                        f'Missing key {k} in image {idx_id}'):
                    # Skip next check, key not found
                    continue
                self._check(
                        f'CAAIndex::Image::{k} correct', idx_image[k] == mb_image[k],
                        f'Incorrect value for {k} in {idx_id}: {idx_image[k]}')

            for k in idx_image.keys():
                self._check(
                        'CAAIndex::Image::unexpected key', k in mb_image,
                        f'Unexpected key {k} in {idx_id}')

            idx_image_id_count[idx_id] += 1
            found_images.add(idx_id)

        for mb_id in id_to_mb_image.keys():
            self._check(
                    'CAAIndex::Image::missing image', mb_id in found_images,
                    f'Missing image {mb_id}! Possibly disastrous!')

        for img_id, img_count in idx_image_id_count.items():
            self._check(
                    'CAAIndex::Image::image id is unique', img_count == 1,
                    f'{img_id} has multiple images')

        # CAAIndex::Image::order
        mb_order = [image.id for image in mb_state.images]
        self._check(
                'CAAIndex::Image::order', mb_order == idx_order,
                f'Wrong order for images: {idx_order} vs {mb_order}')

        self._logger.info(
                '*** Finished CAA index.json checks (took {elapsed:0.4f}s)',
                elapsed=(pendulum.now() - start_time).total_seconds())

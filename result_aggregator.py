from __future__ import annotations

from typing import Any, NamedTuple, TYPE_CHECKING

import asyncio
import csv
import gzip
import os
import sys

from collections import Counter, defaultdict
from pathlib import Path

if TYPE_CHECKING:
    from collections.abc import Iterable

from tabulate import tabulate
from tqdm import tqdm

from audit_result import CheckFailed, CheckPassed, CheckResult, ItemSkipped
from progress import ProgressBar

class ResultType(NamedTuple):
    mbid: str
    check_description: str
    check_state: str

class RowType(NamedTuple):
    name: str
    num_checks: str
    num_releases: str
    num_failed: str
    perc_failed: str
    num_failed_releases: str
    perc_failed_releases: str

class TableType(NamedTuple):
    header: RowType
    checks: list[RowType]
    skips: list[RowType]
    total: RowType

class ResultCollector:

    def put(self, audit_results: list[CheckResult]) -> None:
        raise NotImplementedError()

class ReasonCounter:
    def __init__(self) -> None:
        self._num_passed = 0
        self._num_failed = 0
        self._num_skipped = 0

        self._failed_rels: set[str] = set()
        self._all_rels: set[str] = set()

    def add(self, cr: ResultType) -> None:
        self._all_rels.add(cr.mbid)
        if cr.check_state == 'FAILED':
            self._num_failed += 1
            self._failed_rels.add(cr.mbid)
        elif cr.check_state == 'ITEM SKIPPED':
            self._num_skipped += 1
        else:
            assert cr.check_state == 'PASSED'
            self._num_passed += 1

    @property
    def num_checks(self) -> int:
        return self._num_passed + self._num_failed + self._num_skipped

    @property
    def num_releases(self) -> int:
        return len(self._all_rels)

    @property
    def num_failed(self) -> int:
        return self._num_failed

    @property
    def num_failed_rels(self) -> int:
        return len(self._failed_rels)

    @property
    def num_skipped(self) -> int:
        return self._num_skipped

class ResultAggregator(ResultCollector):
    """Aggregator for results provided by the tasks."""

    def __init__(self, root_path: Path, progress: ProgressBar, open_cache: bool = True) -> None:
        super().__init__()
        self._progress = progress

        self._internal_error_counter = 0

        # Ideally should be closed at some point. Used to store intermediate
        # results since there's a chance they won't all fit in memory at the
        # same time.
        root_path.mkdir(exist_ok=True, parents=True)
        self._cache_file_path = root_path / 'results_cache.gz'
        if open_cache:
            self._cache_file = gzip.open(self._cache_file_path, mode='wt')
        self._finished = not open_cache

    def put(self, audit_results: list[CheckResult]) -> None:
        skipped = failed = False

        for res in audit_results:
            if res.category[0] == 'InternalError':
                self._flag_internal_error()

            if isinstance(res, ItemSkipped):
                skipped = True
            elif isinstance(res, CheckFailed):
                failed = True

            self._cache_file.write('\t'.join([
                    res.mbid, res.check_description, res.check_state]) + os.linesep)

        self._cache_file.flush()

        if skipped:
            self._progress.task_skipped()
        elif failed:
            self._progress.task_failed()
        else:
            self._progress.task_success()

    def _flag_internal_error(self):
        self._internal_error_counter += 1
        if self._internal_error_counter > 10:
            raise RuntimeError('Exceeded internal error counter, aborting.')

    def finish(self) -> None:
        self._cache_file.close()
        self._finished = True

    def _iter_results(self) -> Iterable[ResultType]:
        """finish must be called beforehand!"""
        assert self._finished
        with gzip.open(self._cache_file_path, mode='rt') as results_f:
            # Intern the loaded strings, they may be repeated often
            yield from (ResultType(*map(sys.intern, line.strip().split('\t'))) for line in results_f)

    def write_items_log(self, skipped_path: Path, failed_path: Path) -> None:
        with skipped_path.open('w') as skipped_out, failed_path.open('w') as failed_out:
            for cr in tqdm(self._iter_results(), desc='Load check results'):
                if cr.check_state == 'ITEM SKIPPED':
                    file = skipped_out
                elif cr.check_state == 'FAILED':
                    file = failed_out
                else:
                    continue

                file.write(''.join([cr.mbid, '\t', cr.check_description, os.linesep]))

    def write_failures_csv(self, path: Path) -> None:
        fail_reasons: set[str] = set()
        failed_mbids: set[str] = set()
        failure_counter: Counter[tuple[str, str]] = Counter()

        for cr in tqdm(self._iter_results(), desc='Load failures'):
            if cr.check_state != 'FAILED':
                continue
            fail_reasons.add(cr.check_description)
            failed_mbids.add(cr.mbid)
            failure_counter[(cr.mbid, cr.check_description)] += 1

        fail_reasons_ordered = sorted(fail_reasons)
        header = ['mbid'] + fail_reasons_ordered

        with path.open('w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for mbid in tqdm(sorted(failed_mbids), desc='Write failure rows'):
                writer.writerow([mbid] + [str(failure_counter[mbid, reason]) for reason in fail_reasons_ordered])

    # Awfulness below…
    # Consider the return value opaque and to be processed further by the table
    # writers.
    def generate_table_data(self) -> TableType:
        all_releases: set[str] = set()
        all_failed_releases: set[str] = set()
        check_counter: dict[str, ReasonCounter] = defaultdict(ReasonCounter)

        for cr in tqdm(self._iter_results(), desc='Load check results'):
            check_counter[cr.check_description].add(cr)
            all_releases.add(cr.mbid)
            if cr.check_state == 'FAILED':
                all_failed_releases.add(cr.mbid)

        header: RowType = RowType('',
                '#checks', '#checked rels',
                '#failed', '%failed', '#failed rels', '%failed rels')
        check_rows: list[RowType] = []
        item_skip_rows: list[RowType] = [
            RowType('SKIPPED ITEMS', '', '', '', '', '', '')]
        total_num_checks = 0
        total_num_failed = 0

        def p(count: int, total: int) -> str:
            return f'{count / total :.2%}'

        for reason, counter in sorted(check_counter.items(), key=lambda kv: kv[0]):
            checks = counter.num_checks
            num_releases = counter.num_releases
            failed = counter.num_failed
            failed_rels = counter.num_failed_rels

            total_num_checks += checks
            total_num_failed += failed

            check_rows.append(RowType(
                reason,
                str(checks), str(num_releases),
                str(failed), p(failed, checks),
                str(failed_rels), p(failed_rels, num_releases)))
            if counter.num_skipped:
                item_skip_rows.append(RowType(reason, '', '', str(counter.num_skipped), '', '', ''))

        total_row: RowType = RowType(
                'TOTAL', str(total_num_checks), str(len(all_releases)),
                str(total_num_failed), p(total_num_failed, total_num_checks),
                str(len(all_failed_releases)), p(len(all_failed_releases), len(all_releases)),
        )

        return TableType(header, check_rows, item_skip_rows, total_row)

    def _filter_failure_rows(self, data: TableType) -> TableType:
        header, check_rows, item_skip_rows, total_row = data
        check_rows = [row for row in check_rows if int(row.num_failed.split(' ')[0])]
        return TableType(header, check_rows, item_skip_rows, total_row)

    def _write_table(self, out: Path, data: TableType, /, only_failure_rows: bool, tablefmt: str) -> None:
        if only_failure_rows:
            data = self._filter_failure_rows(data)

        header, check_rows, item_skip_rows, total_row = data
        table = [header, *check_rows, *item_skip_rows, total_row]
        table_list = [list(row) for row in table]

        table_str = tabulate(table_list, headers='firstrow', tablefmt=tablefmt, floatfmt='.2f')

        out.write_text(table_str)

    def write_jira_table(self, out: Path, data: TableType, /, only_failure_rows: bool = True) -> None:
        self._write_table(out, data, only_failure_rows=only_failure_rows, tablefmt='jira')

    def write_plaintext_table(self, out: Path, data: TableType, /, only_failure_rows: bool = True) -> None:
        self._write_table(out, data, only_failure_rows=only_failure_rows, tablefmt='simple')

    def get_terminal_table(self, data: TableType, /, only_failure_rows: bool = True) -> str:
        if only_failure_rows:
            data = self._filter_failure_rows(data)

        header, check_rows, item_skip_rows, total_row = data
        table = [header, *check_rows, *item_skip_rows, total_row]
        table_list = [list(row) for row in table]

        return tabulate(table, headers='firstrow', tablefmt='fancy_grid', floatfmt='.2f')

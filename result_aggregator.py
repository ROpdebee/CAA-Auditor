from typing import Any, NamedTuple

import asyncio
import csv

from collections import Counter, defaultdict
from functools import singledispatchmethod
from pathlib import Path

from tabulate import tabulate

from audit_result import CheckFailed, CheckPassed, CheckResult, ItemSkipped
from progress import ProgressBar

class RowType(NamedTuple):
    name: str
    num_checks: str
    num_releases: str
    num_failed: str
    num_failed_releases: str

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

    @singledispatchmethod
    def add(self, cr: CheckResult) -> None:
        raise NotImplementedError()

    @add.register
    def _(self, cr: CheckFailed) -> None:
        self._num_failed += 1
        self._failed_rels.add(cr.mbid)
        self._all_rels.add(cr.mbid)

    @add.register  # type: ignore[no-redef]
    def _(self, cr: CheckPassed) -> None:
        self._num_passed += 1
        self._all_rels.add(cr.mbid)

    @add.register  # type: ignore[no-redef]
    def _(self, cr: ItemSkipped) -> None:
        self._num_skipped += 1
        self._all_rels.add(cr.mbid)

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

    def __init__(self, progress: ProgressBar) -> None:
        super().__init__()
        self._progress = progress

        self._internal_error_counter = 0
        self._results: list[CheckResult] = []

    def put(self, audit_results: list[CheckResult]) -> None:
        skipped = failed = False

        for res in audit_results:
            if res.category[0] == 'InternalError':
                self._flag_internal_error()

            if isinstance(res, ItemSkipped):
                skipped = True
            elif isinstance(res, CheckFailed):
                failed = True

            self._results.append(res)

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

    def write_skipped_items_log(self, path: Path) -> None:
        skipped_items = [cr for cr in self._results if isinstance(cr, ItemSkipped)]
        path.write_text('\n'.join(map(str, skipped_items)))

    def write_failed_checks_log(self, path: Path) -> None:
        failed_checks = [cr for cr in self._results if isinstance(cr, CheckFailed)]
        path.write_text('\n'.join(map(str, failed_checks)))

    def write_failures_csv(self, path: Path) -> None:
        failed_checks = [cr for cr in self._results if isinstance(cr, CheckFailed)]
        fail_reasons: set[str] = set()
        failed_mbids: set[str] = set()
        failure_counter: Counter[tuple[str, str]] = Counter()

        for failure in failed_checks:
            fail_reasons.add(failure.check_description)
            failed_mbids.add(failure.mbid)
            failure_counter[(failure.mbid, failure.check_description)] += 1

        fail_reasons_ordered = sorted(fail_reasons)
        header = ['mbid'] + fail_reasons_ordered
        rows = [header]
        for mbid in sorted(failed_mbids):
            rows.append([mbid] + [str(failure_counter[mbid, reason]) for reason in fail_reasons_ordered])

        with path.open('w') as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow(row)

    # Awfulness below…
    # Consider the return value opaque and to be processed further by the table
    # writers.
    def generate_table_data(self) -> TableType:
        all_releases: set[str] = set()
        all_failed_releases: set[str] = set()
        check_counter: dict[str, ReasonCounter] = defaultdict(ReasonCounter)

        for cr in self._results:
            check_counter[cr.check_description].add(cr)
            all_releases.add(cr.mbid)
            if cr.check_state == 'FAILED':
                all_failed_releases.add(cr.mbid)

        header: RowType = RowType('',
                '#checks', '#checked rels',
                '#failed', '#failed rels')
        check_rows: list[RowType] = []
        item_skip_rows: list[RowType] = [
            RowType('SKIPPED ITEMS', '', '', '', '')]
        total_num_checks = 0
        total_num_failed = 0

        def c(count: int, total: int) -> str:
            return f'{count} ({count / total :.2%})'

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
                c(failed, checks), c(failed_rels, num_releases)))
            if counter.num_skipped:
                item_skip_rows.append(RowType(reason, '', '', str(counter.num_skipped), ''))

        total_row: RowType = RowType(
                'TOTAL', str(total_num_checks), str(len(all_releases)),
                c(total_num_failed, total_num_checks),
                c(len(all_failed_releases), len(all_releases)),
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

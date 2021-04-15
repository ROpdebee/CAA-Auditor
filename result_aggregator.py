from typing import Any

import asyncio
import csv

from collections import Counter
from pathlib import Path

from tabulate import tabulate

from audit_result import CheckFailed, CheckPassed, CheckResult, CheckSkipped, ItemSkipped
from progress import ProgressBar

class ResultCollector:

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    async def put(self, audit_results: list[CheckResult]) -> None:
        raise NotImplementedError()

class ResultAggregator(ResultCollector):
    """Aggregator for results provided by the tasks."""

    def __init__(self, progress: ProgressBar) -> None:
        super().__init__()
        self._progress = progress

        self._failed_checks: list[CheckFailed] = []
        self._skipped_checks: list[CheckSkipped] = []
        self._skipped_items: list[ItemSkipped] = []
        self._check_counter: Counter[tuple[str, str, str]] = Counter()
        self._item_skip_counter: Counter[str] = Counter()

    async def put(self, audit_results: list[CheckResult]) -> None:
        if [res for res in audit_results if isinstance(res, ItemSkipped)]:
            await self._progress.task_skipped()
        elif [res for res in audit_results if isinstance(res, CheckFailed)]:
            await self._progress.task_failed()
        else:
            await self._progress.task_success()

        for result in audit_results:
            self._count_result(result)

    def _count_result(self, result: CheckResult) -> None:
        if isinstance(result, ItemSkipped):
            self._item_skip_counter[result.check_description] += 1
            self._skipped_items.append(result)
        else:
            self._check_counter.update(
                (result.check_state, result.mbid, '::'.join(result.category[:i]))
                for i in range(len(result.category)))
            if isinstance(result, CheckFailed):
                self._failed_checks.append(result)
            elif isinstance(result, CheckSkipped):
                self._skipped_checks.append(result)

    def write_skipped_items_log(self, path: Path) -> None:
        path.write_text('\n'.join(map(str, self._skipped_items)))

    def write_failed_checks_log(self, path: Path) -> None:
        path.write_text('\n'.join(map(str, self._failed_checks)))

    def write_skipped_checks_log(self, path: Path) -> None:
        path.write_text('\n'.join(map(str, self._skipped_checks)))

    def write_failures_csv(self, path: Path) -> None:
        fail_reasons: set[str] = set()
        failed_mbids: set[str] = set()
        failure_counter: Counter[tuple[str, str]] = Counter()

        for failure in self._failed_checks:
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
    def generate_table_data(self) -> Any:
        check_counter: Counter[tuple[str, str]] = Counter()
        check_counter_rels: Counter[tuple[str, str]] = Counter()
        total_checks: Counter[str] = Counter()
        total_releases: Counter[str] = Counter()

        all_releases: set[str] = set()
        all_failed_releases: set[str] = set()
        all_skipped_releases: set[str] = set()
        total_num_failed = 0
        total_num_checks = 0
        total_num_skipped = 0
        for ((state, mbid, check), count) in self._check_counter.items():
            check_counter[(state, check)] += count
            check_counter_rels[(state, check)] += 1
            total_checks[check] += count
            total_releases[check] += 1
            all_releases.add(mbid)
            total_num_checks += count
            if state == 'FAILED':
                total_num_failed += count
                all_failed_releases.add(mbid)
            elif state == 'SKIPPED':
                total_num_skipped += count
                all_skipped_releases.add(mbid)

        header = (
                '',
                '#checks', '#checked releases',
                '#failed', '#failed releases', '%failed', '%failed releases',
                '#skipped', '#skipped releases', '%skipped', '%skipped releases')
        reason_rows: list[tuple[str, int, int, int, int, float, float, int, int, float, float]] = []
        for reason in sorted(total_checks.keys()):
            num_checked = total_checks[reason]
            num_releases = total_releases[reason]
            num_failed = check_counter[('FAILED', reason)]
            num_failed_rels = check_counter[('FAILED', reason)]
            num_skipped = check_counter[('SKIPPED', reason)]
            num_skipped_rels = check_counter[('SKIPPED', reason)]

            reason_rows.append((
                reason, num_checked, num_releases,
                num_failed, num_failed_rels, num_failed / num_checked, num_failed_rels / num_releases,
                num_skipped, num_skipped_rels, num_skipped / num_checked, num_skipped_rels / num_releases))

        item_skip_rows: list[tuple[Any, ...]] = [
            tuple(['SKIPPED ITEMS'] + [''] * 10)]
        for skip_reason, skip_count in sorted(self._item_skip_counter.items()):
            item_skip_rows.append(tuple([skip_reason] + [''] * 6 + [str(skip_count)] + [''] * 3))

        total_row = (
                'TOTAL', total_num_checks, len(all_releases),
                total_num_failed, len(all_failed_releases),
                total_num_failed / total_num_checks,
                len(all_failed_releases) / len(all_releases),
                total_num_skipped, len(all_skipped_releases),
                total_num_skipped / total_num_checks,
                len(all_skipped_releases) / len(all_releases))

        return (header, reason_rows, item_skip_rows, total_row)

    def _filter_failure_rows(self, data: Any) -> Any:
        header, check_rows, item_skip_rows, total_row = data
        check_rows = [row for row in check_rows if row[3]]
        return (header, check_rows, item_skip_rows, total_row)

    def _write_table(self, out: Path, data: Any, /, only_failure_rows: bool, tablefmt: str) -> None:
        if only_failure_rows:
            data = self._filter_failure_rows(data)

        table_str = tabulate(data, headers='firstrow', tablefmt=tablefmt, floatfmt='.2f')

        out.write_text(table_str)

    def write_jira_table(self, out: Path, data: Any, /, only_failure_rows: bool = True) -> None:
        self._write_table(out, data, only_failure_rows=only_failure_rows, tablefmt='jira')

    def write_plaintext_table(self, out: Path, data: Any, /, only_failure_rows: bool = True) -> None:
        self._write_table(out, data, only_failure_rows=only_failure_rows, tablefmt='simple')

    def get_terminal_table(self, data: Any, /, only_failure_rows: bool = True) -> str:
        if only_failure_rows:
            data = self._filter_failure_rows(data)

        return tabulate(data, headers='firstrow', tablefmt='fancy_grid', floatfmt='.2f')

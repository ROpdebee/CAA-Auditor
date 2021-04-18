from __future__ import annotations

import asyncio
from collections import Counter

import enlighten

from abstractions import CheckStage

class ProgressBar:

    bar_format = '{desc}{desc_pad}{percentage:3.0f}%|{bar}| ' + \
            '[{elapsed}<{eta}, {rate:.2f}{unit_pad}{unit}/s]'
    _status_format_stage = ', '.join(
            f'{{stage_{stage.name}}} {stage.name}'
            for stage in sorted(CheckStage, key=lambda cs: cs.value))
    status_format = '{todo} to do, ' + \
            '{queued} queued, ' + \
            '{pending} in progress (' + _status_format_stage + '), ' + \
            '{finished} finished ' + \
            '({success} successful, {failed} failed, {skipped} skipped)'

    def __init__(self, total: int) -> None:
        self.mgr = enlighten.Manager()
        self.pending = self.mgr.counter(
                total=total, desc='Auditing', unit='tasks', color='gray',
                bar_format=self.bar_format)
        self.success = self.pending.add_subcounter('green')
        self.skipped = self.pending.add_subcounter('orange')
        self.failed = self.pending.add_subcounter('red')
        self.queued = 0

        self.status = self.mgr.status_bar(
                status_format=self.status_format,
                queued=0, success=0, pending=0, skipped=0, failed=0, finished=0,
                todo=total, **{f'stage_{stage.name}': 0 for stage in CheckStage})

        self._stage_count: Counter[CheckStage] = Counter()

    def __enter__(self) -> ProgressBar:
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        self.close()

    def close(self) -> None:
        self.mgr.stop()

    def _update_statusbar(self) -> None:
        self.status.update(
            queued=self.queued,
            success=self.success.count,
            pending=self.pending.count - self.pending.subcount,
            skipped=self.skipped.count,
            failed=self.failed.count,
            finished=self.success.count + self.skipped.count + self.failed.count,
            todo=self.pending.total - self.pending.count,
            **{f'stage_{stage.name}': self._stage_count[stage] for stage in CheckStage})

    def task_enqueued(self) -> None:
        self.queued += 1
        self._update_statusbar()

    def task_running(self) -> None:
        self.pending.update()
        self.queued -= 1
        self._update_statusbar()

    def task_success(self) -> None:
        self.success.update_from(self.pending)
        self._update_statusbar()

    def task_failed(self) -> None:
        self.failed.update_from(self.pending)
        self._update_statusbar()

    def task_skipped(self) -> None:
        self.skipped.update_from(self.pending)
        self._update_statusbar()

    def enter_stage(self, stage: CheckStage) -> None:
        self._stage_count[stage] += 1
        self._update_statusbar()

    def exit_stage(self, stage: CheckStage) -> None:
        self._stage_count[stage] -= 1
        self._update_statusbar()


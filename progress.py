from __future__ import annotations

import asyncio

import enlighten

class ProgressBar:

    bar_format = '{desc}{desc_pad}{percentage:3.0f}%|{bar}| ' + \
            '[{elapsed}<{eta}, {rate:.2f}{unit_pad}{unit}/s]'
    status_format = '{todo} to do, ' + \
            '{queued} queued, ' + \
            '{pending} in progress, ' + \
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
                todo=total)

        self._lock = asyncio.Lock()

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
            todo=self.pending.total - self.pending.count)

    async def task_enqueued(self) -> None:
        async with self._lock:
            self.queued += 1
            self._update_statusbar()

    async def task_running(self) -> None:
        async with self._lock:
            self.pending.update()
            self.queued -= 1
            self._update_statusbar()

    async def task_success(self) -> None:
        async with self._lock:
            self.success.update_from(self.pending)
            self._update_statusbar()

    async def task_failed(self) -> None:
        async with self._lock:
            self.failed.update_from(self.pending)
            self._update_statusbar()

    async def task_skipped(self) -> None:
        async with self._lock:
            self.skipped.update_from(self.pending)
            self._update_statusbar()

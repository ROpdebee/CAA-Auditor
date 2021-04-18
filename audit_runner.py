from __future__ import annotations

from typing import Any, Optional, TypeVar, TYPE_CHECKING

import asyncio
import json
import os
import random
import sys
from configparser import ConfigParser
from pathlib import Path
if TYPE_CHECKING:
    from collections.abc import AsyncIterable, AsyncIterator

import loguru
import pendulum
import aiofiles
from aiohttp import ClientSession, TCPConnector
from aiopath import AsyncPath

if TYPE_CHECKING:
    from pendulum.datetime import DateTime

from abstractions import CheckStage
from audit_task import AuditTask
from progress import ProgressBar
from result_aggregator import ResultAggregator

FANOUT_FACTOR = 3

_T = TypeVar('_T')
async def anext(ait: AsyncIterator[_T]) -> _T:
    return await ait.__anext__()

def aiter(ait: AsyncIterable[_T]) -> AsyncIterator[_T]:
    return ait.__aiter__()

def _fanout_path(root_path: Path, mbid: str) -> Path:
    return root_path.joinpath(*list(mbid[:FANOUT_FACTOR])) / mbid

class LogQueue:
    def __init__(self, path: AsyncPath) -> None:
        self._path = path
        self._buffer: list[str] = []

    def put(self, msg: str) -> None:
        self._buffer.append(msg)

    async def flush(self) -> None:
        async with self._path.open('a') as out_f:
            await out_f.write(''.join(self._buffer))
            self._buffer = []

async def create_tasks(
        data_path: AsyncPath, root_path: Path, aiosession: ClientSession,
        logger: loguru.Logger
) -> tuple[dict[str, Any], AsyncIterable[tuple[AuditTask, LogQueue]]]:
    async with data_path.open('r') as data_f:
        meta_line = await anext(aiter(data_f))
        audit_meta = json.loads(meta_line)
        if audit_meta['state'] != 'meta':
            raise ValueError('Expected first line of task list to be meta record')
        return audit_meta, _create_task_stream(
                data_path, pendulum.from_timestamp(audit_meta['max_last_modified']),
                root_path, aiosession, logger)

async def _create_task_stream(
        task_path: AsyncPath, max_last_modified: DateTime,
        root_path: Path, aiosession: ClientSession, logger: loguru.Logger
) -> AsyncIterable[tuple[AuditTask, LogQueue]]:
    async with aiofiles.open(task_path, 'r') as task_f:
        _ = await task_f.readline()  # Skip over meta line
        while (lines := await task_f.readlines(2**26)):
            for data_line in lines:
                mb_data = json.loads(data_line)
                task_path = AsyncPath(_fanout_path(root_path, mb_data['id']))
                log_queue = LogQueue(task_path / 'audit_log')
                task_logger = logger.bind(log_queue=log_queue)
                yield AuditTask(
                        mb_data, max_last_modified, task_path, aiosession,
                        task_logger, ), log_queue

async def queue_tasks(
        tasks: AsyncIterable[tuple[AuditTask, LogQueue]], task_q: asyncio.Queue[tuple[AuditTask, LogQueue]],
        progress: ProgressBar,
) -> None:
    async for task, logqueue in tasks:
        task.set_start_stage_cb(progress.enter_stage)
        task.set_finish_stage_cb(progress.exit_stage)
        await task_q.put((task, logqueue))
        progress.task_enqueued()

async def task_runner(
        task_q: asyncio.Queue[tuple[AuditTask, LogQueue]], result_aggr: ResultAggregator,
        progress: ProgressBar
) -> None:
    await asyncio.sleep(random.random())  # Random jitter at the start so that not all requests get fired immediately
    while True:
        task, logqueue = await task_q.get()
        progress.task_running()
        progress.enter_stage(CheckStage.preprocess)
        await task.audit_path.mkdir(exist_ok=True, parents=True)
        progress.exit_stage(CheckStage.preprocess)
        await task.run(result_aggr)
        progress.enter_stage(CheckStage.postprocess)
        await logqueue.flush()
        progress.exit_stage(CheckStage.postprocess)
        task_q.task_done()

def get_ia_credentials() -> Optional[tuple[str, str]]:
    config = ConfigParser()
    config.read(Path.home() / '.ia')
    if not 's3' in config:
        return None
    if not {'access', 'secret'}.issubset(config['s3'].keys()):
        return None
    return config['s3']['access'], config['s3']['secret']

async def do_audit(mb_data_file_path: Path, output_path: Path, concurrency: int, spam: bool) -> None:
    configure_logging(spam)

    ia_creds = get_ia_credentials()
    if ia_creds is None:
        loguru.logger.error('IA credentials not found in ~/.ia')
        return

    s3_access, s3_secret = ia_creds

    with mb_data_file_path.open('r') as f:
        num_items = sum(1 for l in f)

    task_q: asyncio.Queue[tuple[AuditTask, LogQueue]] = asyncio.Queue(concurrency * 2)

    session = ClientSession(
            connector=TCPConnector(limit=concurrency),
            headers={'Authorization': f'LOW {s3_access}:{s3_secret}'})
    async with session:
        audit_meta, task_stream = await create_tasks(
                AsyncPath(mb_data_file_path), output_path, session,
                loguru.logger)
        num_items = audit_meta['count']
        with ProgressBar(num_items) as progress:
            queuer = asyncio.create_task(queue_tasks(task_stream, task_q, progress))

            aggregator = ResultAggregator(progress)
            runners = [
                    asyncio.create_task(task_runner(task_q, aggregator, progress))
                    for _ in range(concurrency)]

            # Wait until all tasks are queued
            await queuer

            # Wait until all tasks are done
            await task_q.join()

            # Terminate the runners, all jobs done
            for runner in runners:
                runner.cancel()

        aggregator.write_skipped_items_log(output_path / 'skipped_items.log')
        aggregator.write_failed_checks_log(output_path / 'failed_checks.log')
        aggregator.write_failures_csv(output_path / 'bad_items.csv')
        table_data = aggregator.generate_table_data()
        aggregator.write_plaintext_table(output_path / 'results_all.txt', table_data, only_failure_rows=False)
        aggregator.write_jira_table(output_path / 'results_jira.txt', table_data)
        aggregator.write_plaintext_table(output_path / 'results_condensed.txt', table_data)
        print(aggregator.get_terminal_table(table_data))


def configure_logging(spam: bool):
    def task_logger_sink(msg: loguru.Message) -> None:
        log_queue = msg.record['extra']['log_queue']
        log_queue.put(msg)

    loguru.logger.remove()
    loguru.logger.add(task_logger_sink, filter=lambda msg: 'log_queue' in msg['extra'], format='{message}')
    if spam:
        loguru.logger.add(sys.stderr)
    else:
        loguru.logger.add(sys.stderr, filter=lambda msg: 'log_queue' not in msg['extra'])

    loguru.logger.add(sys.stderr, level='CRITICAL')

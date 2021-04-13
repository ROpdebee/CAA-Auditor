from __future__ import annotations

from typing import Optional, TYPE_CHECKING

import asyncio
import json
import sys
from configparser import ConfigParser
from pathlib import Path
if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable

import loguru
import pendulum
from aiohttp import ClientSession, TCPConnector
from aiopath import AsyncPath

from audit_task import AuditTask
from progress import ProgressBar
from result_aggregator import ResultAggregator

FANOUT_FACTOR = 3

def _fanout_path(root_path: Path, mbid: str) -> Path:
    return root_path.joinpath(*list(mbid[:FANOUT_FACTOR])) / mbid

async def create_tasks(
        data_path: AsyncPath, root_path: Path, aiosession: ClientSession,
        logger: loguru.Logger
) -> AsyncIterable[AuditTask]:
    async with data_path.open('r') as data_f:
        async for data_line in data_f:
            mb_data = json.loads(data_line)
            task_path = _fanout_path(root_path, mb_data['release_id'])
            task_logger = logger.bind(log_path=task_path / 'audit.log')
            yield AuditTask(mb_data, task_path, aiosession, task_logger)

async def queue_tasks(
        tasks: AsyncIterable[AuditTask], task_q: asyncio.Queue[AuditTask],
        progress: ProgressBar,
) -> None:
    async for task in tasks:
        await task_q.put(task)
        await progress.task_enqueued()

async def task_runner(
        task_q: asyncio.Queue[AuditTask], result_aggr: ResultAggregator,
        progress: ProgressBar
) -> None:
    while True:
        task = await task_q.get()
        await progress.task_running()
        task.run(result_aggr)
        task_q.task_done()

def get_ia_credentials() -> Optional[tuple[str, str]]:
    config = ConfigParser()
    config.read(Path.home() / '.ia')
    if not 's3' in config:
        return None
    if not {'access', 'secret'}.issubset(config['s3'].keys()):
        return None
    return config['s3']['access'], config['s3']['secret']

async def do_audit(mb_data_file_path: Path, output_path: Path, concurrency: int) -> None:
    configure_logging()

    ia_creds = get_ia_credentials()
    if ia_creds is None:
        loguru.logger.error('IA credentials not found in ~/.ia')
        return

    s3_access, s3_secret = ia_creds

    with mb_data_file_path.open('r') as f:
        num_items = sum(1 for l in f)

    task_q: asyncio.Queue[AuditTask] = asyncio.Queue()

    session = ClientSession(
            connector=TCPConnector(limit=concurrency),
            headers={'Authorization': f'LOW {s3_access}:{s3_secret}'})
    async with session:
        with ProgressBar(num_items) as progress:
            task_stream = create_tasks(
                    AsyncPath(mb_data_file_path), output_path, session,
                    loguru.logger)
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
            aggregator.write_skipped_checks_log(output_path / 'skipped_checks.log')
            aggregator.write_failures_csv(output_path / 'bad_items.csv')
            table_data = aggregator.generate_table_data()
            aggregator.write_plaintext_table(output_path / 'results_all.txt', table_data, only_failure_rows=False)
            aggregator.write_jira_table(output_path / 'results_jira.txt', table_data)
            aggregator.write_plaintext_table(output_path / 'results_condensed.txt', table_data)
            print(aggregator.get_terminal_table(table_data))


def configure_logging():
    def task_logger_sink(msg: loguru.Message) -> None:
        log_path = msg.record['extra']['log_path']
        with log_path.open('w+') as f:
            f.write(msg.record['message'])

    loguru.logger.remove()
    loguru.logger.configure(handlers=[
        {'sink': task_logger_sink, 'filter': lambda msg: 'log_path' in msg['extra']},
        {'sink': sys.stderr, 'filter': lambda msg: 'log_path' not in msg['extra']}])

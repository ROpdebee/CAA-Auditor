import asyncio
from pathlib import Path

import click

from audit_runner import configure_logging, do_audit, write_failed_items, write_logs, write_tables
from result_aggregator import ResultAggregator

@click.group()
def main() -> None:
    ...


@main.command()
@click.argument('input', type=click.Path(exists=True, dir_okay=False, readable=True))
@click.argument('output', type=click.Path(file_okay=False, dir_okay=True, writable=True))
@click.option('--concurrency', default=50, help='Number of concurrent tasks')
@click.option('--spam/--no-spam', default=False, help='Spammy output')
def audit(input: str, output: str, concurrency: int, spam: bool) -> None:
    """Run the audit.

    INPUT: Input file with JSON-serialized MB data, one per line, in JSONL
    format. Use transform_data.py to generate this file from a DB dump.
    OUTPUT: Output directory to save audit results into.
    """
    asyncio.run(do_audit(Path(input), Path(output), concurrency, spam))


@main.command()
@click.argument('output', type=click.Path(file_okay=False, dir_okay=True, writable=True))
@click.option('--logs/--no-logs', default=True, help='Whether to generate failure/skip logs')
@click.option('--bad-items/--no-bad-items', default=True, help='Whether to generate bad items CSV')
@click.option('--tables/--no-tables', default=True, help='Whether to generate failure/skip logs')
def generate_output(output: str, logs: bool, bad_items: bool, tables: bool) -> None:
    """Generate output files. Assumes that the audit has already run previously
    but generated reports have failed to generate or have been lost.

    OUTPUT: Output directory.
    """

    outp = Path(output)
    if not (outp / 'results_cache.gz').is_file():
        raise click.BadParameter('results_cache.gz does not exist!')

    configure_logging(False)
    aggr = ResultAggregator(outp, None, open_cache=False)

    if logs:
        write_logs(outp, aggr)
    if bad_items:
        write_failed_items(outp, aggr)
    if tables:
        write_tables(outp, aggr)


if __name__ == '__main__':
    main()

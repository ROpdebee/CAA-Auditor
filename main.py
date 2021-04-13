import asyncio
from pathlib import Path

import click

from audit_runner import do_audit


@click.command()
@click.argument('input', type=click.Path(exists=True, dir_okay=False, readable=True))
@click.argument('output', type=click.Path(file_okay=False, dir_okay=True, writable=True))
@click.option('--concurrency', default=50, help='Number of concurrent tasks')
def audit(input: str, output: str, concurrency: int) -> None:
    """Run the audit.

    INPUT: Input file with JSON-serialized MB data, one per line, in JSONL
    format. Use transform_data.py to generate this file from a DB dump.
    OUTPUT: Output directory to save audit results into.
    """
    asyncio.run(do_audit(Path(input), Path(output), concurrency))


if __name__ == '__main__':
    audit()

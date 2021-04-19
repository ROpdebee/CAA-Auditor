# CAA Auditor Architecture

The main auditing tool is built on top of a worker infrastructure using async I/O.
The three main parts of this infrastructure are the audit runner (`audit_runner.py`), the audit tasks (`audit_task.py`) and the result aggregator (`result_aggregator.py`).

The design is mainly influenced by a desire to complete the whole audit as quickly as possible, producing detailed results of issues. However, to be used in practice, this tool should be adapted to run in a daemonised manner at low concurrency over a long period of time, e.g., to check and automatically fix each item once a month. In that case, multiple major optimisations can be made.

## Elements

### Audit runner
The audit runner creates a task queue and three types of tasks to process it.
- Task creator: An async task that lazily reads the input file and bulk-creates new audit tasks.
- Task queuer: Consumes the created tasks and feeds them into the queue.
- Task runners: `<MAX_CONCURRENCY>` number of workers that consume tasks from the queue and run them asynchronously.

### Audit task
- Fetch resources from IA server (metadata, index).
- Run checks and create check results to feed into the result aggregator.

### Result aggregator
- Receives results from the tasks and writes them to an on-disk cache.
    + Storing the results is done on-disk and not in-memory since it may exhaust all memory rather quickly.
- Provides result serialisation and prettifying.

## Possible improvements
- If running in high-throughput mode: Some form of multiprocessing may be desirable. The current main constraint appears to be single-core performance rather than network I/O.
    + `extract_data.py` could also be converted to async I/O to speed up generating the input data.

- For running in a daemonised manner:
    + Main architecture could probably be kept, but there's the possibility of major simplifications throughout the whole project.
    + Audit task checks can be heavily simplified (e.g., `desired_index == parsed_index` instead of checking each key individually). The current implementation is deliberately detailed to provide more diagnostics.
    + Data extraction and task feeding can probably be done directly from the DB rather than through an intermediate file.
    + The result aggregator could possibly implement a notification system to automatically repair the found failures. For example, it could publish a amqp message to the indexer for many failed checks.
    + Detailed logging can likely be disabled.
    + Storing any results on disk can probably be disabled (again, mainly there for diagnostics).
    + The JSON parsing tries to use a more efficient parser, this could probably be dropped if it's running at low concurrency. I'm not sure whether it drastically improved performance, but I think it did raise the throughput from 30 items/s to roughly 45 items/s.

# CAA Auditor
Tool to audit items in the [CoverArtArchive](https://coverartarchive.org/) [collection](https://archive.org/details/coverartarchive) in the [Internet Archive](https://archive.org/) against the state of [MusicBrainz](https://musicbrainz.org/) releases.

## Requirements
- Python 3.9
- [poetry](https://python-poetry.org/)
- Requirements in the `pyproject.toml` file.

## Installing
- Clone this repo
- Run `poetry install` in the root directory. This will create a virtual environment and install all dependencies.

## Running
1. Spawn a shell session in the virtual environment: `poetry shell`
2. Make sure that a recent postgresql [MusicBrainz replica](https://musicbrainz.org/doc/MusicBrainz_Database/) is installed and running. See also [mbdata](https://github.com/lalinsky/mbdata/)
3. (Optional) Download a full listing of the coverartarchive collection via [here](https://archive.org/metamgr.php?f=exportIDs&w_collection=coverartarchive) (requires being logged in to an account with sufficient privileges)
4. Set the required environment variables:
    - `MB_HOST`: The hostname/IP address of the postgresql instance that contains the DB replica, e.g. `localhost`
    - `MB_DB`: The name of the DB replica, e.g. `musicbrainz`
    - `MB_USER`, `MB_PASS`: Username and password of a user that has access to the DB.
5. Prepare the data for feeding into the auditor: `python scripts/extract_data.py --caa-items=/path/to/caa_collection_dump --timestamp=/path/to/replica_timestamp /path/to/jsonl`
    - If you skipped step 3, remove the `--caa-items` flag. Note: This will then only extract CAA information from releases that currently have cover art, excluding items that belong to releases that were merged, removed, or which no longer have cover art.
    - The timestamp is expected to be in the same format as the `TIMESTAMP` file from the database dump. If not supplied, defaults to extraction start time. This timestamp is used to skip items which have been modified after data was extracted.
    - This will generate a large JSONL file, containing one JSON object describing the expected state of the item per line.
6. Run the audit: `python main.py audit /path/to/jsonl /path/to/audit_output_directory --concurrency=<MAX_CONCURRENCY>`
    - `/path/to/jsonl` is the same as step 5.
    - `/path/to/audit_output_directory` is a path to a directory which will be used to store the results.
    - `MAX_CONCURRENCY`: The maximum number of concurrently-running audit tasks, i.e., fetching resources, checking, writing results, etc. Recommended to set to a sensible value as to not overload IA's servers. `250` is likely fine, `1000` is pushing it. Large values may lead to more failed requests and may be detrimental to performance.
7. (Optional) Postprocess the results with the scripts in the `scripts/` directory (see below).

### Output
The tool will output to the provided output directory.
Each item will be placed into its own directory, spread out across three levels of subdirectories (e.g., for MBID `0fabd...`, the item directory will be `0/f/a/0fabd...`).
Each of these directories will contain the following files:
    - `ia_metadata.json`: The JSON file returned by IA's metadata API for the item.
    - `index.json`: The `index.json` file downloaded from the item, if it exists and is accessible.
    - `audit_log`: The logging output produced by the audit task for this item
    - `failures.log`: A semi-structured list of failed checks for this item.

In addition, the following files will be present in the root of the directory for facilitated investigations:
    - `bad_items.csv`: A CSV containing release IDs and the number of times a specific check failed for this item. Does not include releases that didn't fail any checks.
    - `failed_checks.log`: A listing of every check that failed. Each line contains the release ID and the category of the failing checks, separated by a tab.
    - `skipped_items.log`: Similar to the above, but for items that were skipped.
    - `results_all.txt`, `results_jira.txt`, `results_condensed.txt`: Table-formatted overview of the results, formatted for different usage contexts.
    - `results_cache.gz`: An intermediate file containing information on every check that was performed. Contains release ID, check category, and check outcome (`PASSED`, `FAILED`, or `ITEM SKIPPED`), GZip compressed. NOTE: Extracting this file is probably a bad idea, since it may be several gigabytes large.

### Implemented Checks
Too many to list here, see the source code.

A number of general categories exist:
- `Item::`, `EmptyItem::`, `DeletedItem::`, and `MergedItem::` indicate the type of item for which the check was performed.
- `Files::` checks check the existence of absence of specific files.
- `Metadata::` checks check the metadata attached to the Internet Archive item. These were either set during upload, or extracted by IA from a `mb_metadata.xml` file.
- `CAAIndex::` checks verify the contents of the `index.json` file uploaded by CAA against the expected state. `CAAIndex::Image::` checks verify a single image in this index.
- `InternalError::` checks hopefully shouldn't occur, this indicates a bug in the code. These only occur on skipped items.

## Advanced Usage

### Resuming an interrupted audit
There is currently no built-in support for resuming an audit that was interrupted.
However, you can significantly speed up the tasks for the already-audited items by making sure not to delete the output directory.
Previously-fetched `index.json` and `ia_metadata.json` files will automatically be reused.

It may be desirable to remove the `audit_log` files first to ensure logging output isn't appended to previously-existing logs.

### Running the audit while input is still being extracted
It's possible to start the audit while `extract_data.py` is still running.
Just make sure that the rate of item completion in the audit doesn't outpace the rate at which `extract_data.py` is producing the input.
Also, make sure that enough initial data is produced initially.
A rough estimate could be 4x as much input data as the maximum concurrency level.

### Incrementally re-checking items with an updated replica
This is currently not supported.
Please use a new data directory, otherwise it will reuse previously-fetched, stale metadata and indices.

### Generating lost reports
In case you lost the reports of a completed audit (or the tool crashed while generating them), you can re-generate them with `python main.py generate-output /path/to/data`.
Run with `--help` for possible flags indicating which output should be generated.
NOTE: This requires the `results_cache.gz` file to be present and complete.

### Appending new items to the checklist
Scenario: You forgot to download the collection listing and mistakenly generated the input JSONL file only for releases that were in the DB, thus excluding merged, deleted, and empty items.
You can amend the JSONL file by rerunning the command from step 5 with the `--continue-from=/path/to/previous_file` option.
This will also reuse the previous timestamp.

## Complementary scripts
`scripts/` contains helper scripts to extract, preprocess, and postprocess data.
An overview:

- `extract_data.py`: See step 5 in the Running section.
- `generate_actionable_results.py`: Transform the `bad_items.csv` file into a collection of files containing items categorised by the actions that need to be undertaken on them.
    + Each file contains the release ID, the release URL, the item URL on IA, and a semicolon-separated list of failed checks, separated by tabs, one per line.
    + Following files will be created:
        * `deleted_properly_delete`: Deleted releases that weren't properly purged.
        * `merged_properly_delete`: As above, but for merged releases.
        * `ia_set_mediatype`, `ia_set_noindex`: Items that should have their metadata (mediatype and noindex keys, respectively) modified by IA admins.
        * `manual_check`: Items that should be reviewed manually and/or can only be fixed manually.
        * `reindex`: Items that can be fixed by reindexing as if an edit to the cover art was made on MB.
        * `reindex_high_priority`: Subset of `reindex`, only containing items that should urgently be reindexed (e.g., because the index.json file doesn't exist at all, or some images are missing)
        * `reindex_w_metadata`: Superset of `reindex`, containing items that can be fixed by reindexing, including items where the index.json is correct but the metadata extracted from `mb_metadata.xml` is stale (i.e., `mb_metadata.xml` should be updated).
        * `properly_empty`: Existing releases without any images which should be properly emptied on IA, e.g., because the index still carries references to these images.
        * `rederive_thumbnails`: Releases whose thumbnails should be rederived. Should be performed after all other issues are fixed.
- `get_darkened.py`: Scan the skipped items log and generate a file containing all release IDs for which the corresponding item was darkened.
- `prep_for_ia.py`: Convert `ia_*` files in the actionable results to a file that can be submitted to IA admins for auto-submit of simple fixes.
- `transform_old_task_list.py`: Unused, legacy code to convert an old task list to new format.

## Data
The results of an auditing run, performed on 2021-04-19, is available at the [Internet Archive](https://archive.org/details/coverartarchive_audit_20210419).

## Architecture
See `Architecture.md`.

## License
See `LICENSE`.

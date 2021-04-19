import csv
import sys
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

bad_items_path = Path(sys.argv[1])

# To be sent to IA to set noindex
ia_set_noindex: dict[str, set[str]] = defaultdict(set)
# To be sent to IA to set mediatype
ia_set_mediatype: dict[str, set[str]] = defaultdict(set)
# To be manually processed
manual: dict[str, set[str]] = defaultdict(set)
# To be reindexed
active_reindex: dict[str, set[str]] = defaultdict(set)
# To be reindexed (high priority)
active_reindex_highprio: dict[str, set[str]] = defaultdict(set)
# To be reindexed (w/ mb_metadata.xml issues)
active_reindex_mb_metadata: dict[str, set[str]] = defaultdict(set)
# To be properly deleted (deleted releases)
deleted: dict[str, set[str]] = defaultdict(set)
# To be properly deleted (merged releases)
merged: dict[str, set[str]] = defaultdict(set)
# To be properly emptied
emptied: dict[str, set[str]] = defaultdict(set)
# To be re-derived (thumbnails)
thumbnails: dict[str, set[str]] = defaultdict(set)

# Ignore
ignore: dict[str, set[str]] = defaultdict(set)

def categorise(mbid: str, fail_reason: str) -> None:
    add_to: list[dict[str, set[str]]] = []
    if 'Metadata::item is noindex' in fail_reason:
        add_to.append(ia_set_noindex)
    elif 'Metadata::mediatype is image' in fail_reason:
        add_to.append(ia_set_mediatype)
    elif fail_reason.startswith('DeletedItem::'):
        add_to.append(deleted)
    elif fail_reason.startswith('MergedItem::'):
        add_to.append(merged)
    elif fail_reason.startswith('EmptyItem::'):
        if 'CAAIndex::is present' in fail_reason or 'Files::' in fail_reason:
            # Don't care about missing items here
            add_to.append(ignore)
        elif 'Metadata::' in fail_reason or 'CAAIndex::' in fail_reason:
            add_to.append(emptied)
    elif fail_reason.startswith('Item::'):
        if fail_reason in (
                'Item::CAAIndex::Image::missing image',
                'Item::CAAIndex::Image::unexpected image',
                'Item::CAAIndex::is present',
                'Item::CAAIndex::is well-formed',
                'Item::Files::index.json exists'):
            add_to.append(active_reindex_highprio)
            add_to.append(active_reindex)
            add_to.append(active_reindex_mb_metadata)
        if fail_reason in (
                'Item::exists',
                'Item::Metadata::in caa collection',
                'Item::CAAIndex::is well-formed',
                'Item::Files::image id is unique',
                'Item::Files::original image exists'):
            add_to.append(manual)
        elif 'thumbnail exists' in fail_reason:
            add_to.append(thumbnails)
        elif 'Metadata::' in fail_reason:
            add_to.append(active_reindex_mb_metadata)
        elif 'CAAIndex::' in fail_reason or 'Files::' in fail_reason:
            add_to.append(active_reindex)
            add_to.append(active_reindex_mb_metadata)

    assert add_to
    for d in add_to:
        d[mbid].add(fail_reason)


with bad_items_path.open('rt') as f:
    reader = csv.reader(f)
    header = next(reader)
    for mbid, *reasons in tqdm(reader, desc='Categorising check failures'):
        reason_count = map(int, reasons)
        for reason_idx, count in enumerate(reason_count):
            if not count:
                continue
            sys.intern(mbid)
            categorise(mbid, header[reason_idx + 1])

for content, filename in tqdm((
        (ia_set_noindex, 'ia_set_noindex'),
        (ia_set_mediatype, 'ia_set_mediatype'),
        (manual, 'manual_check'),
        (active_reindex, 'reindex'),
        (active_reindex_highprio, 'reindex_high_prioriy'),
        (active_reindex_mb_metadata, 'reindex_w_metadata'),
        (deleted, 'deleted_properly_delete'),
        (merged, 'merged_properly_delete'),
        (emptied, 'properly_empty'),
        (thumbnails, 'rederive_thumbnails')), desc='Writing results'):
    with open(filename, 'wt') as out_f:
        for mbid, fail_reasons in sorted(content.items(), key=lambda t: t[0]):
            line = '\t'.join([
                mbid,
                f'https://musicbrainz.org/release/{mbid}',
                f'https://archive.org/details/mbid-{mbid}',
                '; '.join(fail_reasons)]) + '\n'
            out_f.write(line)

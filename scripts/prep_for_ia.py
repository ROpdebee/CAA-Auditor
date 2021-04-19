import sys
from pathlib import Path

root_dir = Path(sys.argv[1])

def convert(content: str) -> str:
    lines = content.split('\n')
    lines = ['mbid-' + l.split('\t')[0] for l in lines]
    return '\n'.join(lines)

(root_dir / 'send_ia').mkdir(exist_ok=True)

for p in root_dir.iterdir():
    if not (p.is_file() and p.name.startswith('ia_')):
        continue

    ia_content = convert(p.read_text())

    (root_dir / 'send_ia' / p.name.removeprefix('ia_')).write_text(ia_content)

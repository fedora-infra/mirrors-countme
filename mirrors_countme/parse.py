import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

from .progress import ReadProgress


@contextmanager
def pre_process(filepath: str | Path) -> Iterator[str]:
    filepath = Path(filepath)
    with NamedTemporaryFile(
        prefix=f"mirrors-countme-{filepath.name}-",
        suffix=".preprocessed",
    ) as tmpfile:
        print(f"Preprocessing file: {filepath}", file=sys.stderr)
        cmd = ["grep", "countme", str(filepath)]
        r = subprocess.run(cmd, stdout=tmpfile)
        if r.returncode != 0:
            print(f"Preprocessing file failed, returning original: {filepath}", file=sys.stderr)
            yield str(filepath)
        yield tmpfile.name


def parse_from_iterator(
    lines,
    *,
    writer,
    matcher,
    matchmode="countme",
    header=True,
    sqlite=None,
    dupcheck=True,
    index=True,
):
    if header or sqlite:
        writer.write_header()

    for logf in lines:
        # Make an iterator object for the matching log lines
        match_iter = iter(matcher(logf))

        # TEMP WORKAROUND: filter out match items with missing values
        if matchmode == "countme":
            match_iter = (i for i in match_iter if None not in i)

        # Duplicate data check (for sqlite output)
        if dupcheck:
            for item in match_iter:
                if writer.has_item(item):  # if it's already in the db...
                    continue  # skip to next log

                writer.write_item(item)  # insert it into the db
            # There should be no items left, but to be safe...
            continue

        # Write matching items (sqlite does commit at end, or rollback on error)
        writer.write_items(match_iter)

    if index:
        writer.write_index()


def parse(
    *,
    writer,
    matcher,
    matchmode="countme",
    header=True,
    sqlite=None,
    dupcheck=True,
    index=None,
    progress=False,
    logs=None,
):
    parse_from_iterator(
        ReadProgress(logs, display=progress, pre_process=pre_process),
        writer=writer,
        matcher=matcher,
        matchmode=matchmode,
        header=header,
        sqlite=header,
        dupcheck=dupcheck,
        index=index,
    )

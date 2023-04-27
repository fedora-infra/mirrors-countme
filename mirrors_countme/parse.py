from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator, Union

from .progress import ReadProgress


@contextmanager
def pre_process(filepath: Union[str, Path]) -> Iterator[str]:
    filepath = Path(filepath)
    with NamedTemporaryFile(
        prefix=f"mirrors-countme-{filepath.name}-",
        suffix=".preprocessed",
    ) as tmpfile:
        import subprocess

        print(f"Preprocessing file: {filepath}")
        cmd = ["grep", "countme", str(filepath)]
        r = subprocess.run(cmd, stdout=tmpfile)
        if r.returncode != 0:
            print(f"Preprocessing file failed, returning original: {filepath}")
            yield str(filepath)
        yield tmpfile.name


def parse_from_iterator(args, lines):
    if args.header or args.sqlite:
        args.writer.write_header()

    for logf in lines:
        # Make an iterator object for the matching log lines
        match_iter = iter(args.matcher(logf))

        # TEMP WORKAROUND: filter out match items with missing values
        if args.matchmode == "countme":
            match_iter = filter(lambda i: None not in i, match_iter)

        # Duplicate data check (for sqlite output)
        if args.dupcheck:
            for item in match_iter:
                if args.writer.has_item(item):  # if it's already in the db...
                    continue  # skip to next log

                args.writer.write_item(item)  # insert it into the db
            # There should be no items left, but to be safe...
            continue

        # Write matching items (sqlite does commit at end, or rollback on error)
        args.writer.write_items(match_iter)

    if args.index:
        args.writer.write_index()


def parse(args=None):
    parse_from_iterator(
        args, ReadProgress(args.logs, display=args.progress, pre_process=pre_process)
    )

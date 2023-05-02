#!/usr/bin/python3

import argparse

from mirrors_countme.totals import totals
from mirrors_countme.version import __version__

# ===========================================================================
# ====== CLI parser & __main__ ==============================================
# ===========================================================================


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Aggregate 'countme' log records to weekly totals.",
    )
    p.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")

    p.add_argument("countme_totals", help="Database containing countme_totals")

    p.add_argument(
        "--update-from",
        metavar="COUNTME_RAW_DB",
        dest="countme_raw",
        help="Update totals from raw data (from ./parse-access-log.py)",
    )

    p.add_argument(
        "--csv-dump",
        type=argparse.FileType("wt", encoding="utf-8"),
        help="File to dump CSV-formatted totals data",
    )

    p.add_argument(
        "--progress",
        action="store_true",
        help="Show progress while reading and counting data.",
    )

    args = p.parse_args(argv)

    return args


if __name__ == "__main__":
    try:
        args = parse_args()
        totals(args)
    except KeyboardInterrupt:
        raise SystemExit(3)  # You know, 3, like 'C', like Ctrl-C!

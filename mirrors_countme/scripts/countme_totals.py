#!/usr/bin/python3

import argparse

from ..totals import totals
from ..version import __version__

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
        help="Update totals from raw data (from countme_parse_access_log)",
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


def cli():
    try:
        args = parse_args()
        totals(
            countme_totals=args.countme_totals,
            countme_raw=args.countme_raw,
            progress=args.progress,
            csv_dump=args.csv_dump,
        )
    except KeyboardInterrupt:
        raise SystemExit(3)  # You know, 3, like 'C', like Ctrl-C!

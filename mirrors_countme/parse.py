from .progress import ReadProgress


def parse_from_iterator(
    logfiles,
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

    for logf in logfiles:
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
        ReadProgress(logs, display=progress),
        writer=writer,
        matcher=matcher,
        matchmode=matchmode,
        header=header,
        sqlite=sqlite,
        dupcheck=dupcheck,
        index=index,
    )

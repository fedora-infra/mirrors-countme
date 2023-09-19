from .progress import ReadProgress


# Given a match iter, we replace the None values with ""
def _convert_none_members(miter):
    for item in miter:
        if None in item:
            n = []
            for val in item:
                if val is None:
                    val = ""
                n.append(val)
            item = tuple(n)
        yield item


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

    if index:
        writer.write_index()

    for logf in logfiles:
        # Make an iterator object for the matching log lines
        match_iter = iter(matcher(logf))

        # WORKAROUND: filter or blank out match items with missing values
        if matchmode == "countme":
            match_iter = _convert_none_members(match_iter)

        if dupcheck:
            # Duplicate data check (for sqlite output)
            for item in match_iter:
                if writer.has_item(item):  # if it's already in the db...
                    continue  # skip to next log

                writer.write_item(item)  # insert it into the db
            writer.commit()
        else:
            # Write matching items (sqlite does commit at end, or rollback on error)
            writer.write_items(match_iter)


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

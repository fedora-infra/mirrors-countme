from countme.progress import ReadProgress


def parse(args=None):
    if args.header or args.sqlite:
        args.writer.write_header()

    for logf in ReadProgress(args.logs, display=args.progress):
        # Make an iterator object for the matching log lines
        match_iter = iter(args.matcher(logf))

        # TEMP WORKAROUND: filter out match items with missing values
        if args.matchmode == "countme":
            match_iter = filter(lambda i: None not in i, match_iter)

        # Duplicate data check (for sqlite output)
        if args.dupcheck:
            try:
                item = next(match_iter)  # grab first matching item
            except StopIteration:
                # If there is no next match, keep going
                continue
            if args.writer.has_item(item):  # if it's already in the db...
                continue  # skip to next log
            else:  # otherwise
                args.writer.write_item(item)  # insert it into the db

        # Write matching items (sqlite does commit at end, or rollback on error)
        args.writer.write_items(match_iter)

    if args.index:
        args.writer.write_index()

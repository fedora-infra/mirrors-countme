DAY_LEN = 24 * 60 * 60
WEEK_LEN = 7 * DAY_LEN
COUNTME_EPOCH = 345600  # =00:00:00 Mon Jan 5 00:00:00 1970 (UTC)
MONTHIDX = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
# TODO: this should probably move into the module somewhere..
LOG_JITTER_WINDOW = 600

# Feb 11 2020 was the date that we branched F32 from Rawhide, so we've decided
# to use that as the starting week for countme data.
COUNTME_START_TIME = 1581292800  # =Mon Feb 10 00:00:00 2020 (UTC)
COUNTME_START_WEEKNUM = 2614

# And here's how you convert a weeknum to a human-readable date
COUNTME_EPOCH_ORDINAL = 719167

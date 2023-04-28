# mirrors-countme

Parse http `access_log` data, find DNF `countme` requests, and output
structured data that lets us estimate the number of people using various
Fedora releases.

See [Changes/DNF Better Counting] for more info about the `countme` feature in
general.

## How it works

The short version:

* Starting in Fedora 32, DNF adds "countme=N" to one random HTTP request
  per week for each repo that has its `countme` setting enabled.
* `parse-access-log.py` parses logs from mirrors.fedoraproject.org, finds
  those requests, and yields the following information:
    * request timestamp, repo & arch
    * client OS name, version, variant, and arch
    * client "age", from 1-4: 1 week, 1 month, 6 months, or >6 months.
* We use that data to make cool charts & graphs and estimate how many Fedora
  users there are and what they're using.

## Technical details

### Client behavior & configuration

DNF 4.2.9 added the `countme` option, which [dnf.conf(5)] describes like so:

>    Determines whether a special flag should be added to a single, randomly
>    chosen metalink/mirrorlist query each week.
>    This allows the repository owner to estimate the number of systems
>    consuming it, by counting such queries over a week's time, which is much
>    more accurate than just counting unique IP addresses (which is subject to
>    both overcounting and undercounting due to short DHCP leases and NAT,
>    respectively).
>
>    The flag is a simple "countme=N" parameter appended to the metalink and
>    mirrorlist URL, where N is an integer representing the "longevity" bucket
>    this system belongs to.
>    The following 4 buckets are defined, based on how many full weeks have
>    passed since the beginning of the week when this system was installed: 1 =
>    first week, 2 = first month (2-4 weeks), 3 = six months (5-24 weeks) and 4
>    = more than six months (> 24 weeks).
>    This information is meant to help distinguish short-lived installs from
>    long-term ones, and to gather other statistics about system lifecycle.

Note that the default is False, because we don't want to enable this for every
repo you have configured.

Starting with Fedora 32, we set `countme=1` in Fedora official repo configs:

```
[updates]
name=Fedora $releasever - $basearch - Updates
#baseurl=http://download.example/pub/fedora/linux/updates/$releasever/Everything/$basearch/
metalink=https://mirrors.fedoraproject.org/metalink?repo=updates-released-f$releasever&arch=$basearch
enabled=1
countme=1
repo_gpgcheck=0
type=rpm
gpgcheck=1
metadata_expire=6h
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-fedora-$releasever-$basearch
skip_if_unavailable=False
```

This means that the default configuration only adds "countme=N" when using
official Fedora repos, which are all done via HTTPS connections to
mirrors.fedoraproject.org. "countme=N" does _not_ get added in subsequent
requests to the chosen mirror(s).

### Privacy, randomization, and user counting

DNF makes a serious effort to keep the `countme` data anonymous _and_ accurate
by only sending `countme` with one _random_ request to each enabled repo _per
week_. So how does it decide when the week starts, and how does it choose
which request?

First, all clients use the same "week": Week 0 started at timestamp 345600
(Mon 05 Jan 1970 00:00:00 - the first Monday of POSIX time), and weeks are
exactly 604800 (7&times;24&times;60&times;60) seconds long.

Second, all clients have the same random chance - currently 1:4 - to send
`countme` with any request in a given week. Once it's been sent, the client
won't send another `countme` for that repo for the rest of the week.

The default update interval for the `updates` repo is 6 hours, which means
that clients who use `dnf-makecache.service` will probably send `countme`
sometime in the first 24 hours of a given week - and nothing for the rest of
the week.

This means that _daily_ totals of users are unreliably variable, since the
start of the week will have more `countme` requests than the end of the week.
But the weekly totals should be a consistent, representative sample of the
total population.

For more details on how libdnf handles the randomization, see
[libdnf/repo/Repo.cpp:addCountmeFlag()].

### Data collected

The only data we look at is in the HTTP request itself. Our log lines are in
the standard Combined Log Format, like so[^IPvBeefy]:

```
240.159.140.173 - - [29/Mar/2020:16:04:28 +0000] "GET /metalink?repo=fedora-modular-32&arch=x86_64&countme=1 HTTP/2.0" 200 18336 "-" "libdnf (Fedora 32; workstation; Linux.x86_64)"
```


We only look at log lines where the request is "GET", the query string includes
"countme=N", the result is 200 or 302, and the User-Agent string matches the
libdnf User-Agent header.

The only data we use are the timestamp, the query parameters (`repo`, `arch`,
`countme`), and the libdnf User-Agent data.

#### libdnf User-Agent data

As in the log line above, the User-Agent header that libdnf sends looks like this:

```
User-Agent: libdnf (Fedora 32; workstation; Linux.x86_64)
```

This string is assembled in [libdnf/utils/os-release.cpp:getUserAgent()] and
the format is as follows:

```
{product} ({os_name} {os_version}; {os_variant}; {os_canon}.{os_arch})
```

where the values are:

`product`
:  "libdnf"

`os_name`
:  [/etc/os-release] `NAME`

`os_version`
:  [/etc/os-release] `VERSION_ID`

`os_variant`
:  [/etc/os-release] `VARIANT_ID`

`os_canon`
:  rpm `%_os` (via libdnf `getCanonOS()`)

`os_arch`
:  rpm `%_arch` (via libdnf `getBaseArch()`)

(Older versions of libdnf sent `libdnf/{LIBDNF_VERSION}` for the `product`,
but the version string was dropped in libdnf 0.37.2 due to privacy concerns;
see [libdnf commit d8d0984].)

#### `repo=`, `arch=`, `countme=`

The `repo=` and `arch=` values are exactly what's set in the URL in the `.repo`
file.

`repo` is whatever string appears after `repo=` in the repo's `metalink` URL.
The values that are accepted for `repo` are determined by [mirrormanager];
see [mirrormanager2/lib/repomap.py] for some of the gnarly details there.

`arch` is usually set as `arch=$basearch`, which means that `os_arch` and
`repo_arch` are usually the same value. But it _is_ valid for a client to
use a repo with an `arch=` that's different from rpm's `%_arch` - for example,
an i686 system could use an i386 repo - so `repo_arch` and `os_arch` _may_ be
different values.

`countme`, as discussed in [dnf.conf(5)], is a value from 1 to 4 indicating
the "age" of the system, counted in _full_ weeks since the system was
first installed. The values are:

1. One week or less (0-1 weeks)
2. Up to one month (2-4 weeks)
3. Up to six months (5-24 weeks)
4. More than six months (25+ weeks)

These are defined in [libdnf/repo/Repo.cpp:COUNTME\_BUCKETS].


## OK but how do we actually use it in Fedora?

Because the raw log data contains IP and timestamps that could be used to
track or identify users, we run the parsing and counting inside private parts
of the Fedora infrastructure and only publish the anonymous aggregate data.

In practice, this is a three-part process:

1. Run `countme-update-rawdb.sh` daily to parse log data into `rawdb`
  * `rawdb` is a SQLite database of structured data for each `countme` hit
  * Kept private since it contains IP addresses and timestamps
  * Typical log data: ~6GB/day
  * Typical parsing time: ~5min (Intel Core i7-6770HQ, 2.60GHz)
  * Typical rawdb size: ~8MB/day for F32; I'd guess keeping 1 year of data for
    3 concurrent releases would take about 10GB.
  * Retaining historical data lets us quickly recalculate counts if we
    discover significant errors due to misconfigured/malicious clients
2. Run `countme-update-totals.sh` to read `rawdb` and update `totalsdb`
  * Counts up hits for each week, grouped by:
    * System info: `os_name`, `os_version`, `os_variant`, `os_arch`, `sys_age`
    * Repo requested: `repo_tag`, `repo_arch`
  * Only generates data for weeks where we have complete log data
  * No timestamps or IP addresses
  * Typical parsing time: small, <=~5s
  * Typical totalsdb size: ~55KB/week (~700 rows/week) for F32
  * After update, (re)generate `totals.csv`
3. Publish updated `totals.db` and `totals.csv`
  * See https://data-analysis.fedoraproject.org/csv-reports/countme/
  * Might end up in different places/forms in the future
  * Can also run countme-weekly-totals-ui.sh command to see text data easily.

## Contributing

You need to be legally allowed to submit any contribution to this project.
What this means in detail is laid out at the [Developer Certificate of Origin]
website. The mechanism by which you certify this is adding a `Signed-off-by`
trailer to git commit log messages, you can do this by using the
`--signoff/-s` option to `git commit`.

[^IPvBeefy]: Don't worry, 240.159.140.173 is a fake IP address. Actually,
             it's the 4-byte UTF-8 encoding for &#x1f32d;, U+1F32D HOT DOG.

[Changes/DNF Better Counting]: https://fedoraproject.org/wiki/Changes/DNF_Better_Counting
[dnf.conf(5)]: https://dnf.readthedocs.io/en/latest/conf_ref.html
[/etc/os-release]: http://man7.org/linux/man-pages/man5/os-release.5.html
[mirrormanager]: https://github.com/fedora-infra/mirrormanager2
[mirrormanager2/lib/repomap.py]: https://github.com/fedora-infra/mirrormanager2/blob/master/mirrormanager2/lib/repomap.py
[libdnf commit d8d0984]: https://github.com/rpm-software-management/libdnf/commit/d8d0984
[libdnf/utils/os-release.cpp:getUserAgent()]: https://github.com/rpm-software-management/libdnf/blob/0.47.0/libdnf/utils/os-release.cpp#L108
[libdnf/repo/Repo.cpp:addCountmeFlag()]: https://github.com/rpm-software-management/libdnf/blob/0.47.0/libdnf/repo/Repo.cpp#L1051
[libdnf/repo/Repo.cpp:COUNTME\_BUCKETS]: https://github.com/rpm-software-management/libdnf/blob/0.47.0/libdnf/repo/Repo.cpp#L92
[Developer Certificate of Origin]: https://developercertificate.org

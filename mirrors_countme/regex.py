# mirrors_countme.regex - regexes for log matching and parsing
#
# Copyright Red Hat
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# Author: Will Woods <wwoods@redhat.com>

import re

__all__ = (
    "compile_log_regex",
    "LOG_RE",
    "LIBDNF_USER_AGENT_RE",
    "COUNTME_USER_AGENT_RE",
    "MIRRORS_LOG_RE",
    "COUNTME_LOG_RE",
    "LOG_DATE_RE",
)

# ===========================================================================
# ====== Regexes! Get your regexes here! ====================================
# ===========================================================================

# Log format, according to ansible/roles/httpd/proxy/templates/httpd.conf.j2:
#   LogFormat "%a %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
# That's the standard Combined Log Format, with numeric IPs (%a).
#
# Example log line:
#   240.159.140.173 - - [29/Mar/2020:16:04:28 +0000] "GET /metalink?repo=fedora-modular-32&arch=x86_64&countme=1 HTTP/2.0" 200 18336 "-" "libdnf (Fedora 32; workstation; Linux.x86_64)" # noqa
#
# Here it is as a Python regex, with a format placeholder for the actual field
# contents. Default field regexes are in LOG_PATTERN_FIELDS, below, and
# compile_log_regex() lets you construct more interesting regexes that only
# match the lines you care about.
# The request target is split into 'path' and 'query'; 'path' is always
# present but 'query' may be absent, depending on the value of 'query_match'.
# 'query_match' should be '?' (optional), '' (required), or '{0}' (absent).
LOG_PATTERN_FORMAT = (
    r"^"
    r"(?P<host>{host})\s"
    r"(?P<identity>{identity})\s"
    r"(?P<user>{user})\s"
    r"\[(?P<time>{time})\]\s"
    r'"(?P<method>{method})\s'
    r"(?P<path>{path})(?:\?(?P<query>{query})){query_match}"
    r'\s(?P<protocol>{protocol})"\s'
    r"(?P<status>{status})\s"
    r"(?P<nbytes>{nbytes})\s"
    r'"(?P<referrer>{referrer})"\s'
    r'"(?P<user_agent>{user_agent})"\s*'
    r"$"
)

# Pattern for a HTTP header token, as per RFC7230.
# Basically: all printable ASCII chars except '"(),/:;<=>?@[\]{}'
# (see https://tools.ietf.org/html/rfc7230#section-3.2.6)
HTTP_TOKEN_PATTERN = r"[\w\#$%^!&'*+.`|~-]+"

# Here's the default/fallback patterns for each field.
# Note that all fields are non-zero width except query, which is optional,
# and query_match, which should be '?', '', or '{0}', as described above.
LOG_PATTERN_FIELDS = {
    "host": "\\S+",
    "identity": "\\S+",
    "user": "\\S+",
    "time": ".+?",
    "method": HTTP_TOKEN_PATTERN,
    "path": "[^\\s\\?]+",
    "query": "\\S*",
    "query_match": "?",
    "protocol": "HTTP/\\d\\.\\d",
    "status": "\\d+",
    "nbytes": "\\d+|-",
    "referrer": '[^"]+',
    "user_agent": ".+?",
}

# A regex for libdnf/rpm-ostree user-agent strings.
# Examples:
#   "libdnf/0.35.5 (Fedora 32; workstation; Linux.x86_64)"
#   "libdnf (Fedora 32; generic; Linux.x86_64)"
#   "rpm-ostree (Fedora 33; coreos; Linux.x86_64)"
#
# The format, according to libdnf/utils/os-release.cpp:getUserAgent():
#   f"{USER_AGENT} ({os_name} {os_version}; {os_variant}; {os_canon}.{os_arch})"
# where:
#   USER_AGENT = "libdnf" or "libdnf/{LIBDNF_VERSION}" or "rpm-ostree"
#   os_name    = os-release NAME
#   os_version = os-release VERSION_ID
#   os_variant = os-release VARIANT_ID
#   os_canon   = rpm %_os (via libdnf getCanonOS())
#   os_arch    = rpm %_arch (via libdnf getBaseArch())
#
# (libdnf before 0.37.2 used "libdnf/{LIBDNF_VERSION}" as USER_AGENT, but the
# version number was dropped in commit d8d0984 due to privacy concerns.)
#
# For more info on the User-Agent header, see RFC7231, Section 5.5.3:
#   https://tools.ietf.org/html/rfc7231#section-5.5.3)
COUNTME_USER_AGENT_PATTERN = (
    r"(?P<product>(?:libdnf|rpm-ostree)(?:/(?P<product_version>\S+))?)\s+"
    r"\("
    r"(?P<os_name>.*)\s"
    r"(?P<os_version>[0-9a-z._-]*?);\s"
    r"(?P<os_variant>[0-9a-z._-]*);\s"
    r"(?P<os_canon>[\w./]+)\."
    r"(?P<os_arch>\w+)"
    r"\)"
)
COUNTME_USER_AGENT_RE = re.compile(COUNTME_USER_AGENT_PATTERN)
LIBDNF_USER_AGENT_RE = re.compile(COUNTME_USER_AGENT_PATTERN)


def compile_log_regex(flags=0, ascii=True, query_present=None, **kwargs):
    """
    Return a compiled re.Pattern object that should match lines in access_log,
    capturing each field (as listed in LOG_PATTERN_FIELDS) in its own group.

    The default regex to match each field is in LOG_PATTERN_FIELDS but you
    can supply your own custom regexes as keyword arguments, like so:

        mirror_request_pattern = compile_log_regex(path='/foo.*?')

    The `flags` argument is passed to `re.compile()`. Since access_log contents
    should (according to the httpd docs) be ASCII-only, that flag is added by
    default, but you can turn that off by adding 'ascii=False'.

    If `query_present` is True, then the regex only matches lines where the
    target resource has a query string - i.e. query is required.
    If False, it only matches lines *without* a query string.
    If None (the default), the query string is optional.
    """
    if ascii:  # pragma: no branch
        flags |= re.ASCII

    fields = LOG_PATTERN_FIELDS.copy()
    fields.update(kwargs)

    if query_present is not None:
        fields["query_match"] = "" if query_present else "{0}"

    pattern = LOG_PATTERN_FORMAT.format(**fields)

    return re.compile(pattern, flags=flags)


# Default matcher that should match any access.log line
LOG_RE = compile_log_regex()

# Compiled pattern to match all mirrorlist/metalink hits, like mirrorlist.py
MIRRORS_LOG_RE = compile_log_regex(path=r"/metalink|/mirrorlist")

# Compiled pattern for countme lines.
# We only count:
#   * GET requests for /metalink or /mirrorlist,
#   * with libdnf's User-Agent string (see above).
# (We used to count only query strings containing "&countme=\d+", this would not let us gather
# “traditional” unique IP statistics.)
COUNTME_LOG_RE = compile_log_regex(
    method="GET|HEAD",
    query_present=True,
    path=r"/metalink|/mirrorlist",
    status=r"200|302",
    user_agent=COUNTME_USER_AGENT_PATTERN,
)

# Regex for pulling the date out of a log line
LOG_DATE_RE = compile_log_regex(time=r"(?P<date>[^:]+):.*?")

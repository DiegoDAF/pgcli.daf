"""Hide secrets in text that is about to be printed or logged.

Connection strings and statements carry passwords in a handful of spellings,
and every place that logs or lists them used to know only some of those.
"""

import re

# The three places libpq lets a password live in a connection string.
_URI_USERINFO_PASSWORD = re.compile(r"://([^:/@]+):([^@]+)@")
_KEYWORD_PASSWORD = re.compile(r"(?i)(\bpassword\s*=\s*)('[^']*'|[^\s&]+)")
_QUERY_PASSWORD = re.compile(r"([?&]password=)[^&\s]+")
# CREATE ROLE ... PASSWORD 'x', ALTER USER ... PASSWORD 'x', OPTIONS (password 'x').
_ROLE_PASSWORD = re.compile(r"(PASSWORD\s+)'[^']*'", re.IGNORECASE)


def redact_sql(sql):
    """``sql`` with the password of a CREATE/ALTER ROLE hidden, for the log.

    The debug log records every statement; this keeps ``alter role x
    password '...'`` from writing the new password to disk.
    """
    return _ROLE_PASSWORD.sub(r"\1'***'", sql)


def redact_process_title(title):
    """The process title with every password hidden.

    ``ps`` shows the command line to every user of the machine. A password
    arrives as ``://user:pass@`` in a URL, as ``password=`` in a key=value
    string and, the one that used to slip through, as ``?password=`` in a
    URL's query string.
    """
    if "://" in title:
        # Per-URL, non-greedy character classes so EVERY password is masked.
        # A greedy ":(.*):(.*)@" would span across multiple connection URLs and
        # leak all but the last password in the process title (visible via `ps`).
        title = _URI_USERINFO_PASSWORD.sub(r"://\1:xxxx@", title)
        title = _QUERY_PASSWORD.sub(r"\1xxxx", title)
    elif "=" in title:
        title = re.sub(r"password=(.+?)((\s[a-zA-Z]+=)|$)", r"password=xxxx\2", title)
    return title


def redact_dsn(dsn):
    """A connection string fit to print.

    Hides the password in each of libpq's spellings: ``://user:pass@`` in a
    URI, ``password=...`` in a key=value string and in a URI's query string.
    ``--list-dsn`` only knew the first one and printed the other two as they
    were.
    """
    dsn = _URI_USERINFO_PASSWORD.sub(r"://\1:***@", dsn)
    return _KEYWORD_PASSWORD.sub(r"\1***", dsn)

"""Hide secrets in text that is about to be printed or logged.

Connection strings and statements carry passwords in a handful of spellings,
and every place that logs or lists them used to know only some of those.
"""

import re

# The three places libpq lets a password live in a connection string.
_URI_USERINFO_PASSWORD = re.compile(r"://([^:/@]+):([^@]+)@")
_KEYWORD_PASSWORD = re.compile(r"(?i)(\bpassword\s*=\s*)('[^']*'|[^\s&]+)")


def redact_dsn(dsn):
    """A connection string fit to print.

    Hides the password in each of libpq's spellings: ``://user:pass@`` in a
    URI, ``password=...`` in a key=value string and in a URI's query string.
    ``--list-dsn`` only knew the first one and printed the other two as they
    were.
    """
    dsn = _URI_USERINFO_PASSWORD.sub(r"://\1:***@", dsn)
    return _KEYWORD_PASSWORD.sub(r"\1***", dsn)

"""Hide secrets in text that is about to be printed or logged.

Connection strings and statements carry passwords in a handful of spellings,
and every place that logs or lists them used to know only some of those.
"""

import re


def _libpq_secret_keywords():
    """The connection keywords libpq itself marks as secrets.

    libpq flags them with dispchar '*': password, sslpassword and, since
    PostgreSQL 18, oauth_client_secret. They are read from the library so a
    new one follows it; the three known ones are the floor.
    """
    known = {"password", "sslpassword", "oauth_client_secret"}
    try:
        from psycopg import pq

        known |= {option.keyword.decode() for option in pq.Conninfo.get_defaults() if option.dispchar == b"*"}
    except Exception:
        pass
    return sorted(known)


SECRET_KEYWORDS = _libpq_secret_keywords()
_SECRETS = "|".join(re.escape(keyword) for keyword in SECRET_KEYWORDS)

# The places libpq lets a password live in a connection string. The userinfo
# of a URI ends at the first "/" or "@", as in libpq's parser, so a "@" later
# in the query string does not turn the host into a password; the user may be
# empty (postgresql://:pass@host/db).
_URI_USERINFO_PASSWORD = re.compile(r"://([^:/@]*):([^/@]+)@")
# ?password= / &sslpassword= in a URI's query: the value ends at "&".
_QUERY_PASSWORD = re.compile(rf"(?i)([?&](?:{_SECRETS})=)[^&\s]*")
# password=... in a key=value string, quoted ('it\'s') or not (ends at a
# blank; a quote never belongs to an unquoted value). The lookbehind leaves
# the URI query form to the pattern above and accepts getopt's attached
# "-dpassword=...".
_KEYWORD_PASSWORD = re.compile(rf"(?i)(?:(?<![?&\w])|(?<=-d))((?:{_SECRETS})\s*=\s*)('(?:[^'\\]|\\.)*'|[^\s']+)")
# CREATE ROLE ... PASSWORD 'x': the literal may be 'it''s', E'a\'b', U&'...',
# $$x$$ or $tag$x$tag$, may follow the keyword with no blank or with
# comments, and adjacent literals on following lines are one string. The tag
# group always participates (it may be empty) so the backreference closes a
# plain $$ too.
_ROLE_PASSWORD = re.compile(
    r"(?i)\b(PASSWORD)((?:\s|/\*.*?\*/|--[^\n]*)*)"
    r"((?:E'(?:[^'\\]|\\.|'')*'|U&'(?:[^']|'')*'|'(?:[^']|'')*')(?:\s*\n\s*'(?:[^']|'')*')*"
    r"|\$(?P<tag>[^\W\d]\w*|)\$.*?\$(?P=tag)\$)",
    re.DOTALL,
)
# What is left of an unquoted key=value password that had blanks in it, once
# its first word is masked: up to the next " keyword=", the next option or
# the end. libpq would stop at the blank; whoever reads ps does not.
_TITLE_TAIL = re.compile(r"(=xxxx)((?:\s[^\s=-]+)+)(?=\s[a-zA-Z_]+=|\s-|$)")


def redact_dsn(dsn):
    """A connection string fit to print.

    Hides every keyword libpq marks as a secret (``password``,
    ``sslpassword``, ``oauth_client_secret``) in each of its spellings:
    ``://user:pass@`` in a URI, ``keyword=`` in a key=value string and in a
    URI's query string. ``--list-dsn`` only knew the first one and printed
    the other two as they were.
    """
    dsn = _URI_USERINFO_PASSWORD.sub(r"://\1:***@", dsn)
    dsn = _QUERY_PASSWORD.sub(r"\1***", dsn)
    return _KEYWORD_PASSWORD.sub(r"\1***", dsn)


def redact_sql(sql):
    """``sql`` with its passwords hidden, for the log.

    The debug log records every statement: the literal of ``alter role x
    password '...'``, whichever way it is quoted, and the connection strings
    inside ``create subscription ... connection '...'`` or ``dblink('...')``.
    A ``where password = 'x'`` loses its value too; it is a log.
    """
    return redact_dsn(_ROLE_PASSWORD.sub(r"\1\2'***'", sql))


def redact_text(text):
    """Free text that may quote a statement or a connection string.

    An error message carries the server's ``LINE 1: ...`` echo of the
    statement, a traceback carries the message, a status of a failed
    statement is the message: the ``sql:`` part of a log line was masked and
    the ``error:`` part of the same line handed the secret back.
    """
    return redact_sql(str(text))


def redact_process_title(title):
    """The process title with every password hidden.

    ``ps`` shows the command line to every user of the machine. A password
    arrives as ``://user:pass@`` in a URL, as ``?password=`` in a URL's query
    string or as ``password=`` in a key=value string, and a command line can
    carry several of these at once (a key=value DSN next to
    ``--ssh-tunnel ssh://...``), so every form is masked every time.
    """
    title = _URI_USERINFO_PASSWORD.sub(r"://\1:xxxx@", title)
    title = _QUERY_PASSWORD.sub(r"\1xxxx", title)
    title = _KEYWORD_PASSWORD.sub(r"\1xxxx", title)
    return _TITLE_TAIL.sub(r"\1", title)

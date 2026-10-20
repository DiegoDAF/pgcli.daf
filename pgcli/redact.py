"""Hide secrets in text that is about to be printed or logged.

Connection strings and statements carry passwords in a handful of spellings,
and every place that logs or lists them used to know only some of those.
"""

import re

# The places libpq lets a password live in a connection string. The
# userinfo of a URI ends at the first "/" or "@", as in libpq's parser, so a
# "@" later in the query string does not turn the host into a password.
_URI_USERINFO_PASSWORD = re.compile(r"://([^:/@]+):([^/@]+)@")
# ?password= / &password= / ?sslpassword= in a URI's query: the value ends at "&".
_QUERY_PASSWORD = re.compile(r"(?i)([?&](?:ssl)?password=)[^&\s]*")
# password=... in a key=value string, quoted ('it\'s') or not (ends at a
# blank); the lookbehind leaves the URI query form to the pattern above.
_KEYWORD_PASSWORD = re.compile(r"(?i)(?<![?&\w])((?:ssl)?password\s*=\s*)('(?:[^'\\]|\\.)*'|\S+)")
# CREATE ROLE ... PASSWORD 'x', 'it''s', E'a\'b', $$x$$ or $tag$x$tag$; ALTER
# USER; OPTIONS (password 'x'). The tag group always participates (it may be
# empty) so the backreference closes a plain $$ too.
_ROLE_PASSWORD = re.compile(
    r"(?i)(PASSWORD\s+)(E'(?:[^'\\]|\\.|'')*'|'(?:[^']|'')*'|\$(?P<tag>[A-Za-z_][A-Za-z0-9_]*|)\$.*?\$(?P=tag)\$)",
    re.DOTALL,
)


def redact_dsn(dsn):
    """A connection string fit to print.

    Hides the password in each of libpq's spellings: ``://user:pass@`` in a
    URI, ``password=`` or ``sslpassword=`` in a key=value string and in a
    URI's query string. ``--list-dsn`` only knew the first one and printed
    the other two as they were.
    """
    dsn = _URI_USERINFO_PASSWORD.sub(r"://\1:***@", dsn)
    dsn = _QUERY_PASSWORD.sub(r"\1***", dsn)
    return _KEYWORD_PASSWORD.sub(r"\1***", dsn)


def redact_sql(sql):
    """``sql`` with the password of a CREATE/ALTER ROLE hidden, for the log.

    The debug log records every statement; this keeps ``alter role x
    password '...'`` from writing the new password to disk, whichever way
    the literal is quoted.
    """
    return _ROLE_PASSWORD.sub(r"\1'***'", sql)


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
    # An unquoted key=value password runs up to the next " keyword=" or the
    # end, so a password with a blank in it is hidden whole.
    return re.sub(r"(?<![?&\w])password=(.+?)((\s[a-zA-Z]+=)|$)", r"password=xxxx\2", title)

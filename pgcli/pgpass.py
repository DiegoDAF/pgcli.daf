"""Minimal ``.pgpass`` reader replicating libpq's lookup semantics.

We need this because pgcli rewrites the connection ``port`` to the SSH tunnel's
random local port before libpq performs its own ``.pgpass`` lookup. libpq then
matches the file against that random port and fails for entries with an explicit
port (only ``*`` port entries work). By resolving the password ourselves using
the ORIGINAL host:port we sidestep that limitation. See FIX_PGPASS_SSH_PORT.

Simplifications relative to libpq (documented on purpose):
- No "localhost == default unix socket" host aliasing: our case is a real TCP
  hostname tunneled to 127.0.0.1, so exact-or-``*`` matching is correct.
- Windows ``%APPDATA%\\postgresql\\pgpass.conf`` is out of scope (we are Linux).
"""

import os
import stat


def _split_pgpass_line(line):
    """Split a .pgpass line into fields, honoring backslash escaping.

    libpq treats ``\\`` as an escape: ``\\:`` is a literal colon and ``\\\\`` is a
    literal backslash. Fields are separated by unescaped colons.
    """
    fields = []
    current = []
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "\\" and i + 1 < len(line):
            current.append(line[i + 1])
            i += 2
            continue
        if ch == ":":
            fields.append("".join(current))
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    fields.append("".join(current))
    return fields


def _pgpass_path():
    """Return the path libpq would use: PGPASSFILE env, else ~/.pgpass."""
    env = os.environ.get("PGPASSFILE")
    if env:
        return os.path.expanduser(env)
    return os.path.expanduser("~/.pgpass")


def _has_safe_permissions(path):
    """libpq ignores .pgpass if it is group/world accessible (must be <= 0600)."""
    try:
        mode = os.stat(path).st_mode
    except OSError:
        return False
    if not stat.S_ISREG(mode):
        return False
    # Reject if any group/other permission bit is set.
    return (stat.S_IMODE(mode) & 0o077) == 0


def _field_matches(entry, value):
    return entry == "*" or entry == value


def lookup_password(host, port, database, user, path=None):
    """Return the password from .pgpass matching (host, port, database, user).

    First matching line wins (libpq behavior). ``port`` is compared as a string.
    Returns ``None`` if there is no match, or the file is missing or has unsafe
    permissions.
    """
    path = path or _pgpass_path()
    if not _has_safe_permissions(path):
        return None
    host = host or "localhost"
    port = str(port or "5432")
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.rstrip("\n")
                if not line or line.lstrip().startswith("#"):
                    continue
                parts = _split_pgpass_line(line)
                if len(parts) < 5:
                    continue
                e_host, e_port, e_db, e_user = parts[0], parts[1], parts[2], parts[3]
                # An unescaped ':' in the password would split into extra parts;
                # rejoin them as a pragmatic recovery (escapes are already resolved).
                e_pass = ":".join(parts[4:]) if len(parts) > 5 else parts[4]
                if (
                    _field_matches(e_host, host)
                    and _field_matches(e_port, port)
                    and _field_matches(e_db, database)
                    and _field_matches(e_user, user)
                ):
                    return e_pass
    except OSError:
        return None
    return None

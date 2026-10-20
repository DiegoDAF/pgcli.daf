"""Connection arguments shared by the pg_dump and pg_dumpall wrappers.

Both wrappers have to answer the same two questions: where is the server the
user asked for, and how do I say "the tunnel" instead once one is up. They used
to answer them with a byte-identical copy of these functions, so a fix in one
could silently miss the other.

The rules below were measured against pg_dump 18, not assumed:

* What a ``-d`` connection string states wins over ``-h``/``-p``.
* What it omits falls back to ``-h``/``-p``.

So a connection string that names the real host has to be rewritten; leaving it
alone and appending ``-h 127.0.0.1`` sends the dump straight past the tunnel.
"""

import os
from typing import List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit, parse_qsl, unquote

from psycopg.conninfo import conninfo_to_dict, make_conninfo

URI_PREFIXES = ("postgresql://", "postgres://")

# Keywords that decide which server libpq actually dials.
HOST_KEYWORDS = ("host=", "hostaddr=")


def is_uri(value: str) -> bool:
    """True for the URI form of a connection string."""
    return value.startswith(URI_PREFIXES)


def is_connstring(value: str) -> bool:
    """True for either connection-string form, URI or keyword/value."""
    return is_uri(value) or any(kw in value for kw in HOST_KEYWORDS) or "port=" in value


def _coerce_port(value: str, source: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{source} is not a valid port number: {value!r}") from None


def _split_userinfo(netloc: str) -> Tuple[str, str]:
    """Split a URI authority into its userinfo and host:port halves.

    The userinfo is returned verbatim, percent-encoding and all, because
    rebuilding it from parsed pieces would change the credentials.
    """
    if "@" in netloc:
        userinfo, _, hostport = netloc.rpartition("@")
        return userinfo + "@", hostport
    return "", netloc


def _split_hostport(hostport: str) -> Tuple[str, Optional[str]]:
    """Split ``host:port`` and unwrap an IPv6 literal's brackets."""
    if hostport.startswith("["):
        host, _, rest = hostport.partition("]")
        host = host[1:]
        port = rest[1:] if rest.startswith(":") else None
        return host, port or None
    host, sep, port = hostport.rpartition(":")
    if not sep:
        return hostport, None
    return host, port or None


def _join_hostport(host: str, port: int) -> str:
    """Rebuild ``host:port``, bracketing an IPv6 literal."""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:{port}"


def _reject_ambiguous_uri(uri: str) -> None:
    """Refuse the URIs whose target this code cannot rewrite honestly."""
    parts = urlsplit(uri)
    _, hostport = _split_userinfo(parts.netloc)
    if "," in hostport:
        raise ValueError(
            f"cannot route a multi-host connection string through an SSH tunnel: {uri!r}. "
            "Name a single host, or pass the tunnel target with -h and -p."
        )
    for key, _value in parse_qsl(parts.query, keep_blank_values=True):
        # libpq lets these query parameters override the URI's own host and
        # port, so after the rewrite pg_dump would dial them and not the tunnel.
        if key in ("host", "hostaddr", "port"):
            raise ValueError(
                f"cannot route a connection string that sets {key!r} in its query string "
                f"through an SSH tunnel: {uri!r}. Put the host and port in the URI itself, "
                "or pass the tunnel target with -h and -p."
            )


def uri_host_port(uri: str) -> Tuple[Optional[str], Optional[int]]:
    """The host and port a URI names, or ``None`` for whichever it omits.

    A multi-host URI (``a:5432,b:5433``) reports its first host: that is
    enough to decide whether a tunnel applies, and only then is the URI
    refused (``_reject_ambiguous_uri`` in the rewrite). Refusing it here
    rejected dumps that never needed a tunnel, while pg_dump itself takes
    several hosts.
    """
    _, hostport = _split_userinfo(urlsplit(uri).netloc)
    if not hostport:
        return None, None
    host, port = _split_hostport(hostport.split(",", 1)[0])
    return (host or None), (_coerce_port(port, "the URI") if port is not None else None)


def rewrite_uri(uri: str, host: str, port: int) -> str:
    """Point a URI at ``host:port``, keeping everything else verbatim."""
    _reject_ambiguous_uri(uri)
    parts = urlsplit(uri)
    userinfo, _ = _split_userinfo(parts.netloc)
    return urlunsplit((
        parts.scheme,
        userinfo + _join_hostport(host, port),
        parts.path,
        parts.query,
        parts.fragment,
    ))


def connstring_host_port(value: str) -> Tuple[Optional[str], Optional[int]]:
    """The host and port a keyword/value connection string names.

    Like uri_host_port(), a multi-host ``host=a,b`` reports its first host and
    port; the string is refused only if a tunnel then has to rewrite it.
    """
    host: Optional[str] = None
    port: Optional[int] = None
    for part in value.split():
        if part.startswith("host="):
            host = part.split("=", 1)[1].split(",", 1)[0]
        elif part.startswith("hostaddr=") and host is None:
            host = part.split("=", 1)[1].split(",", 1)[0]
        elif part.startswith("port="):
            port = _coerce_port(part.split("=", 1)[1].split(",", 1)[0], "the connection string")
    return host, port


def _reject_ambiguous_connstring(value: str) -> None:
    """Refuse a keyword/value string that names several hosts.

    Rewriting ``host=a,b`` to the tunnel's ``127.0.0.1`` would keep only one
    of them and open the tunnel towards "a,b", which no SSH server resolves.
    """
    for part in value.split():
        if part.startswith(("host=", "hostaddr=", "port=")) and "," in part:
            raise ValueError(
                f"cannot route a multi-host connection string through an SSH tunnel: {value!r}. "
                "Name a single host, or pass the tunnel target with -h and -p."
            )


def rewrite_connstring(value: str, host: str, port: int) -> str:
    """Point a keyword/value connection string at ``host:port``."""
    _reject_ambiguous_connstring(value)
    parts = []
    for part in value.split():
        if part.startswith("host="):
            parts.append(f"host={host}")
        elif part.startswith("hostaddr="):
            # hostaddr skips DNS and is what libpq dials. Leaving the real
            # address here walks past the tunnel even with host= rewritten.
            parts.append(f"hostaddr={host}")
        elif part.startswith("port="):
            parts.append(f"port={port}")
        else:
            parts.append(part)
    return " ".join(parts)


def parse_connection_args(args: List[str]) -> tuple:
    """Find the server the user asked for, without consuming the arguments.

    Returns ``(host, port, remaining_args, has_host, has_port)``. The remaining
    arguments are the originals, unchanged: the caller still needs them to run
    the dump directly when no tunnel is involved.
    """
    host = os.environ.get("PGHOST") or "localhost"
    env_port = os.environ.get("PGPORT")
    port = _coerce_port(env_port, "PGPORT") if env_port else 5432
    remaining_args: List[str] = []
    has_host = False
    has_port = False

    def read_dbname(value: str) -> None:
        nonlocal host, port, has_host, has_port
        if is_uri(value):
            uri_host, uri_port = uri_host_port(value)
            if uri_host:
                host, has_host = uri_host, True
            if uri_port is not None:
                port, has_port = uri_port, True
        elif is_connstring(value):
            cs_host, cs_port = connstring_host_port(value)
            if cs_host:
                host, has_host = cs_host, True
            if cs_port is not None:
                port, has_port = cs_port, True

    i = 0
    while i < len(args):
        arg = args[i]

        if arg == "--host" and i + 1 < len(args):
            host = args[i + 1]
            has_host = True
            remaining_args.extend([arg, args[i + 1]])
            i += 2
            continue
        if arg.startswith("--host="):
            host = arg.split("=", 1)[1]
            has_host = True
            remaining_args.append(arg)
            i += 1
            continue

        if arg == "--port" and i + 1 < len(args):
            port = _coerce_port(args[i + 1], "--port")
            has_port = True
            remaining_args.extend([arg, args[i + 1]])
            i += 2
            continue
        if arg.startswith("--port="):
            port = _coerce_port(arg.split("=", 1)[1], "--port")
            has_port = True
            remaining_args.append(arg)
            i += 1
            continue

        if arg == "--dbname" and i + 1 < len(args):
            read_dbname(args[i + 1])
            remaining_args.extend([arg, args[i + 1]])
            i += 2
            continue
        if arg.startswith("--dbname="):
            read_dbname(arg.split("=", 1)[1])
            remaining_args.append(arg)
            i += 1
            continue

        # -h HOST, -hHOST and the flag clusters getopt accepts (-ch HOST, -chHOST).
        letter, attached, _flags = _short_option_value(arg)
        if letter in ("h", "p", "d"):
            if attached:
                value, consumed = attached, 1
            elif i + 1 < len(args):
                value, consumed = args[i + 1], 2
            else:
                remaining_args.append(arg)  # dangling option: pg_dump reports it
                i += 1
                continue
            if letter == "h":
                host, has_host = value, True
            elif letter == "p":
                port, has_port = _coerce_port(value, "-p"), True
            else:
                read_dbname(value)
            remaining_args.extend(args[i : i + consumed])
            i += consumed
            continue

        remaining_args.append(arg)
        i += 1

    return host, port, remaining_args, has_host, has_port


def _short_option_value(arg: str) -> Tuple[Optional[str], str, str]:
    """Split a short-option argument the way getopt does.

    For ``-h x``, ``-hx``, ``-ch x`` and ``-chx`` returns ``(letter, attached,
    flags)``: the first letter that takes a value, the value attached to it
    (``""`` when it is the next argument) and the flags that precede it
    (``"c"`` above, ``""`` for a bare ``-h``). Anything else, long options
    included, gives ``(None, "", "")``.

    The tunnel decision used to read only ``-h HOST``, so
    ``pgcli_dump -hdb.example.com`` and ``pgcli_dump -ch db.example.com``
    dialed the host directly and skipped the tunnel.
    """
    if len(arg) < 2 or not arg.startswith("-") or arg.startswith("--"):
        return None, "", ""
    for j, letter in enumerate(arg[1:], 1):
        if letter in SHORT_OPTIONS_WITH_VALUE:
            return letter, arg[j + 1 :], arg[1:j]
    return None, "", ""


def build_tunneled_args(
    original_args: List[str],
    tunnel_host: str,
    tunnel_port: int,
    original_host: Optional[str],
    original_port: Optional[int],
    has_host: bool,
    has_port: bool,
) -> List[str]:
    """Rewrite the connection arguments to point at the local end of the tunnel.

    ``original_host`` and ``original_port`` are kept for the callers that
    pass them; the rewrite itself only needs to know whether the arguments
    named a host and a port.
    """
    new_args: List[str] = []
    # A rewritten connection string pins both host and port on its own, so the
    # trailing -h/-p would be redundant at best and contradictory at worst.
    rewrote_uri = False
    i = 0

    def rewrite_dbname(value: str) -> str:
        nonlocal rewrote_uri
        if is_uri(value):
            # A URI the rewrite cannot honour (several hosts, host= or port=
            # in its query) is refused here, where a tunnel is actually in play.
            _reject_ambiguous_uri(value)
            if uri_host_port(value)[0]:
                rewrote_uri = True
                return rewrite_uri(value, tunnel_host, tunnel_port)
            return value
        if is_connstring(value):
            return rewrite_connstring(value, tunnel_host, tunnel_port)
        return value

    while i < len(original_args):
        arg = original_args[i]

        if arg == "--host":
            new_args.extend([arg, tunnel_host])
            i += 2
            continue
        if arg.startswith("--host="):
            new_args.append(f"--host={tunnel_host}")
            i += 1
            continue

        if arg == "--port":
            new_args.extend([arg, str(tunnel_port)])
            i += 2
            continue
        if arg.startswith("--port="):
            new_args.append(f"--port={tunnel_port}")
            i += 1
            continue

        if arg == "--dbname" and i + 1 < len(original_args):
            new_args.extend([arg, rewrite_dbname(original_args[i + 1])])
            i += 2
            continue
        if arg.startswith("--dbname="):
            new_args.append(f"--dbname={rewrite_dbname(arg.split('=', 1)[1])}")
            i += 1
            continue

        # -h HOST, -hHOST, -ch HOST, -chHOST: the flags stay, the value changes.
        letter, attached, flags = _short_option_value(arg)
        if letter in ("h", "p", "d") and (attached or i + 1 < len(original_args)):
            value = attached if attached else original_args[i + 1]
            if letter == "h":
                value = tunnel_host
            elif letter == "p":
                value = str(tunnel_port)
            else:
                value = rewrite_dbname(value)
            if flags:
                new_args.append("-" + flags)
            new_args.extend(["-" + letter, value])
            i += 1 if attached else 2
            continue

        new_args.append(arg)
        i += 1

    if not rewrote_uri:
        if not has_host:
            new_args.extend(["-h", tunnel_host])
        if not has_port:
            new_args.extend(["-p", str(tunnel_port)])

    return new_args


def uri_user_dbname(uri: str) -> Tuple[Optional[str], Optional[str]]:
    """The user and database a URI names, percent-decoded the way libpq reads them."""
    parts = urlsplit(uri)
    userinfo, _ = _split_userinfo(parts.netloc)
    user = None
    if userinfo:
        # Strip the trailing "@", then keep only what precedes the password.
        user = unquote(userinfo[:-1].split(":", 1)[0]) or None
    dbname = unquote(parts.path.lstrip("/")) or None
    return user, dbname


def connstring_user_dbname(value: str) -> Tuple[Optional[str], Optional[str]]:
    """The user and database a keyword/value connection string names."""
    user = None
    dbname = None
    for part in value.split():
        if part.startswith("user="):
            user = part.split("=", 1)[1] or None
        elif part.startswith("dbname="):
            dbname = part.split("=", 1)[1] or None
    return user, dbname


def dbname_identity(value: str) -> Tuple[Optional[str], Optional[str]]:
    """The user and database behind a -d value, in any of its three forms."""
    if is_uri(value):
        return uri_user_dbname(value)
    if is_connstring(value):
        return connstring_user_dbname(value)
    # A bare database name.
    return None, value or None


# Options of pg_dump and pg_dumpall that take a value, from their PostgreSQL 18
# --help. The value is either attached (-tfoo, -Fc, --table=foo) or the next
# argument (-t foo, --table foo); an argument that is no option's value is an
# operand (the database name), wherever it sits, since both tools use getopt.
SHORT_OPTIONS_WITH_VALUE = frozenset("dfFjZeEnNStThpUl")
LONG_OPTIONS_WITH_VALUE = frozenset({
    "--compress",
    "--database",
    "--dbname",
    "--encoding",
    "--exclude-database",
    "--exclude-extension",
    "--exclude-schema",
    "--exclude-table",
    "--exclude-table-and-children",
    "--exclude-table-data",
    "--exclude-table-data-and-children",
    "--extension",
    "--extra-float-digits",
    "--file",
    "--filter",
    "--format",
    "--host",
    "--include-foreign-data",
    "--jobs",
    "--lock-wait-timeout",
    "--port",
    "--restrict-key",
    "--role",
    "--rows-per-insert",
    "--schema",
    "--section",
    "--snapshot",
    "--superuser",
    "--sync-method",
    "--table",
    "--table-and-children",
    "--username",
})


def getopt_items(args: List[str]) -> List[Tuple[Optional[str], Optional[str]]]:
    """Read pg_dump-style arguments the way getopt does, keeping their order.

    Options come out as (name, value), with value None for a flag; operands as
    (None, argument). A flag cluster (-Ot x) yields one item per letter.
    """
    items: List[Tuple[Optional[str], Optional[str]]] = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--":
            items.extend((None, rest) for rest in args[i + 1 :])
            break
        if arg.startswith("--"):
            name, eq, value = arg.partition("=")
            if eq:
                items.append((name, value))
            elif name in LONG_OPTIONS_WITH_VALUE and i + 1 < len(args):
                i += 1
                items.append((name, args[i]))
            else:
                items.append((name, None))
        elif arg.startswith("-") and len(arg) > 1:
            # Flags up to the first letter that takes a value, whose value is
            # the rest of the argument or the next one.
            for j, letter in enumerate(arg[1:], 1):
                if letter in SHORT_OPTIONS_WITH_VALUE:
                    value = arg[j + 1 :]
                    if not value and i + 1 < len(args):
                        i += 1
                        value = args[i]
                    items.append(("-" + letter, value))
                    break
                items.append(("-" + letter, None))
        else:
            items.append((None, arg))
        i += 1
    return items


def _argv_from_items(items: List[Tuple[Optional[str], Optional[str]]]) -> List[str]:
    """Write getopt_items() back as arguments, operands last (after -- if needed)."""
    argv: List[str] = []
    operands: List[str] = []
    for name, value in items:
        if name is None:
            operands.append(value or "")
        elif value is None:
            argv.append(name)
        elif name.startswith("--"):
            argv.append(f"{name}={value}")
        else:
            argv.extend([name, value])
    if any(operand.startswith("-") for operand in operands):
        argv.append("--")
    return argv + operands


def apply_dsn_alias(args: List[str], conninfo: str) -> Tuple[List[str], Optional[str]]:
    """Turn a resolved ``--dsn`` alias into the dump tool's own arguments.

    The alias is the connection, as with ``pgcli --dsn``: its connection string,
    SSL settings included, goes to the dump tool as ``-d``. ``-h``, ``-p`` and
    ``-U`` on the command line override its host, port and user, and a
    database operand replaces its database. ``-d`` as well would name the
    connection twice and is refused.

    Returns the new arguments and the alias's password, if it has one, which
    the caller passes in PGPASSWORD: arguments show up in ps.
    """
    items = getopt_items(args)
    if any(name in ("-d", "--dbname") for name, _ in items):
        raise ValueError("--dsn already names the connection: use either --dsn or -d/--dbname, not both.")
    params = conninfo_to_dict(conninfo)
    overrides = {"-h": "host", "--host": "host", "-p": "port", "--port": "port", "-U": "user", "--username": "user"}
    kept = []
    database_seen = False
    for name, value in items:
        if name in overrides and value:
            params[overrides[name]] = value
        elif name is None and not database_seen:
            params["dbname"] = value
            database_seen = True
        else:
            kept.append((name, value))
    password = params.pop("password", None)
    return ["-d", make_conninfo(**params)] + _argv_from_items(kept), password

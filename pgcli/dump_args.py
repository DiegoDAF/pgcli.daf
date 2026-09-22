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
        if key in ("host", "hostaddr"):
            raise ValueError(
                f"cannot route a connection string that sets {key!r} in its query string "
                f"through an SSH tunnel: {uri!r}. Put the host in the URI itself, "
                "or pass the tunnel target with -h and -p."
            )


def uri_host_port(uri: str) -> Tuple[Optional[str], Optional[int]]:
    """The host and port a URI names, or ``None`` for whichever it omits."""
    _reject_ambiguous_uri(uri)
    _, hostport = _split_userinfo(urlsplit(uri).netloc)
    if not hostport:
        return None, None
    host, port = _split_hostport(hostport)
    return (host or None), (_coerce_port(port, "the URI") if port is not None else None)


def rewrite_uri(uri: str, host: str, port: int) -> str:
    """Point a URI at ``host:port``, keeping everything else verbatim."""
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
    """The host and port a keyword/value connection string names."""
    host: Optional[str] = None
    port: Optional[int] = None
    for part in value.split():
        if part.startswith("host="):
            host = part.split("=", 1)[1]
        elif part.startswith("hostaddr=") and host is None:
            host = part.split("=", 1)[1]
        elif part.startswith("port="):
            port = _coerce_port(part.split("=", 1)[1], "the connection string")
    return host, port


def rewrite_connstring(value: str, host: str, port: int) -> str:
    """Point a keyword/value connection string at ``host:port``."""
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

        if arg in ("-h", "--host") and i + 1 < len(args):
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

        if arg in ("-p", "--port") and i + 1 < len(args):
            port = _coerce_port(args[i + 1], "-p")
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

        if arg in ("-d", "--dbname") and i + 1 < len(args):
            read_dbname(args[i + 1])
            remaining_args.extend([arg, args[i + 1]])
            i += 2
            continue
        if arg.startswith("--dbname="):
            read_dbname(arg.split("=", 1)[1])
            remaining_args.append(arg)
            i += 1
            continue

        remaining_args.append(arg)
        i += 1

    return host, port, remaining_args, has_host, has_port


def build_tunneled_args(
    original_args: List[str],
    tunnel_host: str,
    tunnel_port: int,
    original_host: str,
    original_port: int,
    has_host: bool,
    has_port: bool,
) -> List[str]:
    """Rewrite the connection arguments to point at the local end of the tunnel."""
    new_args: List[str] = []
    # A rewritten connection string pins both host and port on its own, so the
    # trailing -h/-p would be redundant at best and contradictory at worst.
    rewrote_uri = False
    i = 0

    def rewrite_dbname(value: str) -> str:
        nonlocal rewrote_uri
        if is_uri(value):
            if uri_host_port(value)[0]:
                rewrote_uri = True
                return rewrite_uri(value, tunnel_host, tunnel_port)
            return value
        if is_connstring(value):
            return rewrite_connstring(value, tunnel_host, tunnel_port)
        return value

    while i < len(original_args):
        arg = original_args[i]

        if arg in ("-h", "--host"):
            new_args.extend([arg, tunnel_host])
            i += 2
            continue
        if arg.startswith("--host="):
            new_args.append(f"--host={tunnel_host}")
            i += 1
            continue

        if arg in ("-p", "--port"):
            new_args.extend([arg, str(tunnel_port)])
            i += 2
            continue
        if arg.startswith("--port="):
            new_args.append(f"--port={tunnel_port}")
            i += 1
            continue

        if arg in ("-d", "--dbname") and i + 1 < len(original_args):
            new_args.extend([arg, rewrite_dbname(original_args[i + 1])])
            i += 2
            continue
        if arg.startswith("--dbname="):
            new_args.append(f"--dbname={rewrite_dbname(arg.split('=', 1)[1])}")
            i += 1
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

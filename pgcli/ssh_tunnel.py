"""
SSH Tunnel helper module for pgcli tools.

This module provides reusable SSH tunnel functionality that can be used
by pgcli, pgcli_dump, pgcli_dumpall, and other tools.

Uses native Paramiko for SSH tunneling (no sshtunnel dependency).
"""

import atexit
import getpass
import glob
import io
import logging
import os
import re
import select
import shlex
import socket
import socketserver
import sys
import threading
from typing import Any, Optional, Tuple, cast
from urllib.parse import urlparse

import paramiko

from .redact import redact_dsn

SSH_TUNNEL_SUPPORT = True


class SSHTunnelError(Exception):
    """The tunnel could not be opened.

    Raised instead of exiting the process: at startup the callers exit with
    the message, while \\c and the automatic reconnect report it and keep the
    session they already have. ``str()`` is the text to show the user.
    """

    def __str__(self):
        reason = self.args[0] if self.args else "unknown error"
        return f"SSH tunnel error: {reason}"


# ssh gives up at this nesting depth with "Too many recursive configuration
# includes" (readconf.c). Keeping the same number also bounds an include loop.
_SSH_INCLUDE_MAX_DEPTH = 16

# ssh reads the user's ~/.ssh/config first and then this file; the first value
# obtained for each keyword wins (ssh_config(5)).
_SYSTEM_SSH_CONFIG = "/etc/ssh/ssh_config"


def _split_ssh_arguments(text):
    """Split an ssh_config argument list the way OpenSSH's argv_split() does.

    Whitespace separates, quotes group, and a backslash escapes only a quote,
    another backslash or (outside quotes) a space; any other backslash is kept.
    shlex.split() dropped every backslash, so on Windows
    ``Include C:\\Users\\me\\.ssh\\conf.d\\*`` turned into a path that does not exist.
    """
    args, current, quote, in_arg = [], [], None, False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "\\" and i + 1 < n and (text[i + 1] in "\\'\"" or (quote is None and text[i + 1] == " ")):
            current.append(text[i + 1])
            in_arg = True
            i += 2
            continue
        if quote is None and ch in " \t":
            if in_arg:
                args.append("".join(current))
                current, in_arg = [], False
        elif quote is None and ch in "'\"":
            quote, in_arg = ch, True
        elif quote is not None and ch == quote:
            quote = None
        else:
            current.append(ch)
            in_arg = True
        i += 1
    if in_arg:
        args.append("".join(current))
    return args


def _expand_ssh_includes(path: str, depth: int = 0, logger: Optional[logging.Logger] = None, base_dir: Optional[str] = None) -> str:
    """Return the text of an ssh_config with its ``Include`` directives expanded.

    paramiko's ``SSHConfig`` ignores ``Include`` entirely, so a Host block that
    lives in ``~/.ssh/config.d/`` is invisible to it even though ssh honors it.
    This inlines the included files, following what ssh does (ssh_config(5)):

    * a directive may list several patterns, and each is globbed and sorted;
    * a relative path is taken from ``base_dir``: ``~/.ssh`` (the default) for
      the user's config, ``/etc/ssh`` for the system one;
    * a pattern matching nothing, or a file that cannot be read, is not an
      error and is skipped;
    * the enclosing ``Host``/``Match`` context is restored afterwards, so an
      included file that opens its own blocks does not capture the directives
      that follow the ``Include`` in the parent.
    """
    logger = logger or logging.getLogger(__name__)
    if depth > _SSH_INCLUDE_MAX_DEPTH:
        logger.warning("Too many recursive SSH config includes at %s", path)
        return ""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()
    except OSError as e:
        logger.debug("Could not read SSH config include %s: %s", path, e)
        return ""

    out: "list[str]" = []
    context: Optional[str] = None  # last Host/Match line seen in THIS file
    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()
        if lowered.startswith(("host ", "match ")) or lowered in ("host", "match"):
            context = line
            out.append(line)
            continue
        if not lowered.startswith("include ") and lowered != "include":
            out.append(line)
            continue

        included: "list[str]" = []
        for pattern in _split_ssh_arguments(stripped[len("include") :].strip()):
            pattern = os.path.expanduser(pattern)
            if not os.path.isabs(pattern):
                pattern = os.path.join(base_dir or os.path.expanduser("~/.ssh"), pattern)
            for name in sorted(glob.glob(pattern)):
                if os.path.isfile(name):
                    included.append(_expand_ssh_includes(name, depth + 1, logger, base_dir))
        text = "\n".join(part for part in included if part)
        if context is not None and text:
            # Inside a Host/Match block ssh processes the include only when that
            # block matches, and it restores the context afterwards. A flat
            # config cannot express that condition for blocks the included file
            # opens, so keep only the part that does apply to the parent (the
            # directives before its first block) and drop the rest.
            kept = []
            for included_line in text.splitlines():
                if included_line.strip().lower().startswith(("host ", "match ")):
                    logger.debug(
                        "Ignoring Host/Match blocks from %s: the include sits inside %r",
                        stripped,
                        context.strip(),
                    )
                    break
                kept.append(included_line)
            text = "\n".join(kept)
        if not text.strip():
            continue
        out.append(f"# Include: {stripped}")
        out.append(text)
    return "\n".join(out)


# Stand-ins for %%, %r and %p in a ProxyCommand while paramiko parses it, put
# back by _fill_proxy_command_tokens() once the real user and port are known.
_PROXY_TOKENS = {"%%": "\x01pct\x01", "%r": "\x01user\x01", "%p": "\x01port\x01"}


def _protect_proxy_command_tokens(text: str) -> str:
    """Hide %%, %r and %p in ProxyCommand lines from paramiko.

    ssh expands %r and %p with the user and port it actually connects with,
    the command line (here, the tunnel URL) first; paramiko uses the Host
    block's User and Port, and turns a literal %% into a broken token. %h and
    %n are left to paramiko, which expands them the way ssh does.
    """
    out = []
    for line in text.splitlines():
        if re.match(r"\s*proxycommand[\s=]", line, re.IGNORECASE):
            line = re.sub(r"%[%rp]", lambda m: _PROXY_TOKENS[m.group(0)], line)
        out.append(line)
    return "\n".join(out)


def _fill_proxy_command_tokens(command: str, user: str, port: int) -> str:
    """Put the effective user and port where %r and %p stood, and % for %%."""
    command = command.replace(_PROXY_TOKENS["%r"], user).replace(_PROXY_TOKENS["%p"], str(port))
    return command.replace(_PROXY_TOKENS["%%"], "%")


def _lookup_ssh_config(hostname: str, user_path: str, system_path: str, logger: logging.Logger) -> "dict[str, Any]":
    """Look ``hostname`` up the way ssh does: the user's config, then the system one.

    Both get their Include directives inlined first. The system file goes after
    a ``Host *`` line, so directives at its top level stay global instead of
    landing in whatever Host block the user's file ends with. A broken user
    config raises, for the caller to report; a system file paramiko cannot
    parse is skipped with a warning rather than losing the user's config too.
    A ProxyCommand comes back with stand-ins for %%, %r and %p, for
    _fill_proxy_command_tokens().
    """
    user_text = _expand_ssh_includes(user_path, logger=logger) if os.path.isfile(user_path) else ""
    system_text = ""
    if os.path.isfile(system_path):
        system_text = _expand_ssh_includes(system_path, logger=logger, base_dir=os.path.dirname(system_path))
    user_text = _protect_proxy_command_tokens(user_text)
    system_text = _protect_proxy_command_tokens(system_text)

    user_config = paramiko.SSHConfig()
    user_config.parse(io.StringIO(user_text))
    if system_text.strip():
        try:
            combined = paramiko.SSHConfig()
            combined.parse(io.StringIO(f"{user_text}\nHost *\n{system_text}\n"))
            return dict(combined.lookup(hostname))
        except Exception as e:
            logger.warning("Could not read %s, using %s only: %s", system_path, user_path, e)
    return dict(user_config.lookup(hostname))


def _parse_jump_hop(hop: str) -> Tuple[Optional[str], str, Optional[int]]:
    """Split one ProxyJump hop into (user, host, port).

    Accepts what ssh accepts: ``[user@]host[:port]``, ``[user@][v6addr]:port``
    and the ``ssh://user@host:port`` URI form.
    """
    hop = hop.strip()
    port: Optional[int] = None
    if hop.startswith("ssh://"):
        parsed = urlparse(hop)
        try:
            port = parsed.port
        except ValueError:  # "ssh://h:abc" or a port out of range; ssh rejects the config
            port = None
        return parsed.username, parsed.hostname or "", port
    user: Optional[str] = None
    if "@" in hop:
        user, hop = hop.rsplit("@", 1)
    host = hop
    if hop.startswith("["):
        host, _, rest = hop[1:].partition("]")
        if rest.startswith(":") and rest[1:].isdigit():
            port = int(rest[1:])
    elif hop.count(":") == 1:
        # A single colon is host:port; more than one is a bare IPv6 literal.
        name, _, port_str = hop.partition(":")
        if port_str.isdigit():
            host, port = name, int(port_str)
    return user, host, port


def _proxy_command_from_proxyjump(proxyjump: str, target_host: str, target_port: int) -> Optional[str]:
    """Build the ProxyCommand that ssh itself derives from a ProxyJump directive.

    ssh dials the LAST hop with ``-W [target]:port`` and hands the earlier hops
    back to itself as ``-J`` so it recurses through them (ssh.c, "Setting
    implicit ProxyCommand from ProxyJump"). ``none`` yields None.
    """
    # ssh strips a trailing "# comment" from the value; paramiko keeps it.
    proxyjump = proxyjump.split("#", 1)[0]
    hops = [h.strip() for h in proxyjump.split(",") if h.strip()]
    if not hops or proxyjump.strip().lower() == "none":
        return None
    user, host, port = _parse_jump_hop(hops[-1])
    if not host:
        return None
    argv = ["ssh"]
    if user:
        argv += ["-l", user]
    if port:
        argv += ["-p", str(port)]
    if len(hops) > 1:
        argv += ["-J", ",".join(hops[:-1])]
    argv += ["-W", f"[{target_host}]:{target_port}", host]
    return shlex.join(argv)


def _proxy_command_from_host_config(host_config: "dict[str, Any]", target_host: str, target_port: int) -> Optional[str]:
    """Pick the proxy for a host the way ssh does.

    The first ProxyCommand or ProxyJump directive read wins (SSHConfig.lookup()
    keeps that order; ``ProxyJump none`` is skipped). A ProxyJump has to be
    translated here because paramiko returns it verbatim instead of turning it
    into a ProxyCommand, so without this the tunnel would dial the final host
    directly and die on DNS.
    """
    for key, value in host_config.items():
        if key == "proxycommand":
            # paramiko stores "ProxyCommand none" as None. Like in ssh it still
            # wins over a later ProxyJump: that is how one host is excluded
            # from a wildcard jump, so it must dial directly.
            return str(value) if value else None
        if key == "proxyjump" and value:
            command = _proxy_command_from_proxyjump(value, target_host, target_port)
            if command:
                return command
    return None


class _ChannelOpenFailure(logging.Filter):
    """Keep the reason the SSH server gave for refusing a forwarded channel.

    paramiko logs it ("Secsh channel 0 open FAILED: Name or service not known:
    Connect failed") but raises only ``ChannelException(2, 'Connect failed')``,
    which drops the part that says what went wrong on the far side.
    """

    _PATTERN = re.compile(r"open FAILED: (.*): [^:]*$")

    def __init__(self):
        super().__init__()
        self.reason: Optional[str] = None

    def filter(self, record):
        match = self._PATTERN.search(record.getMessage())
        if match:
            self.reason = match.group(1)
        return True


def _bind_forward_handler(transport, remote_host, remote_port, ssh_hostname, logger, log_channel):
    """Return a _ForwardHandler subclass bound to one tunnel.

    The transport is moved to its own log channel under ``pgcli`` so that
    paramiko's raw "Secsh channel ... open FAILED" line goes to pgcli's log
    instead of the terminal, and so the reason it carries can be reported in a
    message that names the hosts involved.
    """
    transport.set_log_channel(log_channel)
    channel_logger = logging.getLogger(log_channel)
    # Same level paramiko's own logger effectively had (no handler, so only
    # warnings and up surfaced); keeps per-connection debug chatter out of
    # pgcli's log.
    channel_logger.setLevel(logging.WARNING)
    channel_failure = _ChannelOpenFailure()
    channel_logger.addFilter(channel_failure)
    return type(
        "BoundForwardHandler",
        (_ForwardHandler,),
        {
            "ssh_transport": transport,
            "remote_host": remote_host,
            "remote_port": remote_port,
            "ssh_hostname": ssh_hostname,
            "logger": logger,
            "channel_failure": channel_failure,
        },
    )


class _ForwardHandler(socketserver.StreamRequestHandler):
    """Handles a single forwarded connection through the SSH tunnel.

    Class attributes are set dynamically by _bind_forward_handler().
    """

    ssh_transport: paramiko.Transport
    remote_host: str
    remote_port: int
    ssh_hostname: str = "the SSH server"
    logger: logging.Logger
    channel_failure: Optional[_ChannelOpenFailure] = None

    def handle(self):
        try:
            channel = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.remote_host, self.remote_port),
                self.request.getpeername(),
            )
        except Exception as e:
            # Without this the user only sees libpq's "server closed the
            # connection unexpectedly", which points at the database rather
            # than at the SSH server that could not reach it (for example a
            # typo in the DSN host, which only the far side fails to resolve).
            reason = (self.channel_failure and self.channel_failure.reason) or getattr(e, "text", None) or str(e)
            message = f"SSH tunnel: {self.ssh_hostname} could not open a connection to {self.remote_host}:{self.remote_port}: {reason}"
            self.logger.error(message)
            hint = f"Check the database host and port in the DSN, and that {self.ssh_hostname} can resolve and reach them."
            sys.stderr.write(f"{message}\n{hint}\n")
            sys.stderr.flush()
            return

        if channel is None:
            self.logger.error("SSH channel open was rejected by server")
            return

        self.logger.debug("SSH channel opened to %s:%d", self.remote_host, self.remote_port)

        try:
            while True:
                r, _, _ = select.select([self.request, channel], [], [], 1.0)
                if self.request in r:
                    data = self.request.recv(4096)
                    if not data:
                        break
                    channel.sendall(data)
                if channel in r:
                    data = channel.recv(4096)
                    if not data:
                        break
                    self.request.sendall(data)
        except (OSError, EOFError):
            pass
        finally:
            channel.close()


class _NativeSSHTunnel:
    """Native Paramiko SSH tunnel implementation.

    Replaces sshtunnel.SSHTunnelForwarder with direct Paramiko usage.
    Binds a local TCP port and forwards connections through an SSH channel.
    """

    HOST_KEY_POLICIES = {
        "auto-add": paramiko.AutoAddPolicy,
        "warn": paramiko.WarningPolicy,
        "reject": paramiko.RejectPolicy,
    }

    def __init__(
        self,
        ssh_hostname: str,
        ssh_port: int,
        remote_host: str,
        remote_port: int,
        ssh_username: Optional[str] = None,
        ssh_password: Optional[str] = None,
        ssh_proxy: Optional[Any] = None,
        allow_agent: bool = True,
        key_filenames: Optional[list] = None,
        host_key_policy: str = "auto-add",
        logger: Optional[logging.Logger] = None,
        passphrase: Optional[str] = None,
        secret_provider: Optional[Any] = None,
        secret_saver: Optional[Any] = None,
        ssh_proxy_command: Optional[str] = None,
    ):
        self.ssh_hostname = ssh_hostname
        self.ssh_port = ssh_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_proxy = ssh_proxy
        # ProxyCommand as a string so a fresh paramiko.ProxyCommand can be built
        # per connect attempt (the proxy socket is single-use; reusing it after
        # a failed auth would break the keyring retry).
        self.ssh_proxy_command = ssh_proxy_command
        self.allow_agent = allow_agent
        self.key_filenames = key_filenames
        self.host_key_policy = host_key_policy
        self.logger = logger or logging.getLogger(__name__)
        # Optional secret persistence (e.g. OS keyring) for key passphrase /
        # SSH password when no agent is available.
        self.passphrase = passphrase
        # secret_provider(context) -> Optional[(kind, secret, should_save)]
        # secret_saver(context, kind, secret) -> None
        self.secret_provider = secret_provider
        self.secret_saver = secret_saver

        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._server: Optional[socketserver.ThreadingTCPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._is_active = False

    @property
    def is_active(self) -> bool:
        # Started and not stopped is not enough: the SSH connection under the
        # tunnel can drop (bastion restart, laptop asleep), and then every
        # forwarded connection fails.
        if not self._is_active or self._ssh_client is None:
            return False
        transport = self._ssh_client.get_transport()
        return bool(transport is not None and transport.is_active())

    @property
    def local_bind_port(self) -> Optional[int]:
        if self._server:
            return self._server.server_address[1]
        return None

    def _build_client(self) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        policy_cls = self.HOST_KEY_POLICIES.get(self.host_key_policy, paramiko.AutoAddPolicy)
        client.set_missing_host_key_policy(policy_cls())
        return client

    def _base_connect_kwargs(self) -> "dict[str, Any]":
        connect_kwargs: dict[str, Any] = {
            "hostname": self.ssh_hostname,
            "port": self.ssh_port,
            "username": self.ssh_username,
            "allow_agent": self.allow_agent,
            "look_for_keys": False,
            "compress": False,
            "timeout": 10,
        }
        if self.key_filenames:
            connect_kwargs["key_filename"] = self.key_filenames
        if self.ssh_password:
            connect_kwargs["password"] = self.ssh_password
        if self.passphrase:
            connect_kwargs["passphrase"] = self.passphrase
        if self.ssh_proxy_command:
            # Fresh proxy socket per attempt (single-use), so the retry works.
            connect_kwargs["sock"] = paramiko.ProxyCommand(self.ssh_proxy_command)
        elif self.ssh_proxy:
            connect_kwargs["sock"] = self.ssh_proxy
        return connect_kwargs

    def start(self):
        """Start SSH connection and local forwarding server."""
        self._ssh_client = self._build_client()
        connect_kwargs = self._base_connect_kwargs()

        try:
            self._ssh_client.connect(**connect_kwargs)
        except (paramiko.PasswordRequiredException, paramiko.AuthenticationException) as e:
            # Agent / unencrypted-key auth failed. If a secret provider is
            # available (interactive pgcli), ask it for the key passphrase or
            # SSH password (from keyring or a prompt) and retry once.
            if not self.secret_provider:
                raise
            context = {
                "hostname": self.ssh_hostname,
                "username": self.ssh_username,
                "port": self.ssh_port,
                "key_filenames": self.key_filenames,
            }
            result = self.secret_provider(context)
            if not result:
                raise e
            kind, secret, should_save = result
            self.logger.debug("Retrying SSH auth with provided %s", kind)
            retry_kwargs = self._base_connect_kwargs()
            retry_kwargs["passphrase" if kind == "passphrase" else "password"] = secret
            # Fresh client: a failed connect leaves the previous one unusable.
            self._ssh_client.close()
            self._ssh_client = self._build_client()
            self._ssh_client.connect(**retry_kwargs)
            if should_save and self.secret_saver:
                self.secret_saver(context, kind, secret)
        self.logger.debug("SSH connection established to %s:%d", self.ssh_hostname, self.ssh_port)

        handler_class = _bind_forward_handler(
            self._ssh_client.get_transport(),
            self.remote_host,
            self.remote_port,
            self.ssh_hostname,
            self.logger,
            f"{__name__}.paramiko.{id(self)}",
        )

        self._server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), handler_class)
        self._server.daemon_threads = True

        self._server_thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
        )
        self._server_thread.start()
        self._is_active = True

        self.logger.debug(
            "Local forwarding server started on 127.0.0.1:%d -> %s:%d",
            self.local_bind_port,
            self.remote_host,
            self.remote_port,
        )

    def stop(self):
        """Stop the forwarding server and SSH connection."""
        self._is_active = False
        if self._server:
            # shutdown() waits for serve_forever() to return: only when the
            # forwarding thread runs, or it never comes back (a thread that
            # could not be started, for example).
            if self._server_thread is not None and self._server_thread.is_alive():
                self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None
        self._server_thread = None


class SSHTunnelManager:
    """Manages SSH tunnel connections for database tools."""

    def __init__(
        self,
        ssh_tunnel_url: Optional[str] = None,
        ssh_tunnel_config: Optional[dict] = None,
        dsn_ssh_tunnel_config: Optional[dict] = None,
        logger: Optional[logging.Logger] = None,
        allow_agent: bool = True,
        host_key_policy: str = "auto-add",
        secret_provider: Optional[Any] = None,
        secret_saver: Optional[Any] = None,
    ):
        """
        Initialize SSH tunnel manager.

        Args:
            ssh_tunnel_url: Explicit SSH tunnel URL (e.g., ssh://user@host:port)
            ssh_tunnel_config: Dict of host_regex -> tunnel_url mappings
            dsn_ssh_tunnel_config: Dict of dsn_regex -> tunnel_url mappings
            logger: Logger instance for debug output
            allow_agent: Whether to allow SSH agent for key authentication (default True)
            host_key_policy: SSH host key policy: 'auto-add', 'warn', or 'reject' (default 'auto-add')
            secret_provider: Optional callable to obtain a key passphrase / SSH
                password when agent auth fails. Receives a context dict and
                returns (kind, secret, should_save) or None.
            secret_saver: Optional callable to persist a secret after a
                successful retry. Receives (context, kind, secret).
        """
        self.ssh_tunnel_url = ssh_tunnel_url
        self.ssh_tunnel_config = ssh_tunnel_config or {}
        self.dsn_ssh_tunnel_config = dsn_ssh_tunnel_config or {}
        self.logger = logger or logging.getLogger(__name__)
        self.tunnel: Optional[_NativeSSHTunnel] = None
        self.allow_agent = allow_agent
        self.host_key_policy = host_key_policy
        self.secret_provider = secret_provider
        self.secret_saver = secret_saver

    def find_tunnel_url(
        self,
        host: Optional[str] = None,
        dsn_alias: Optional[str] = None,
    ) -> Optional[str]:
        """
        Find matching SSH tunnel URL from config.

        Args:
            host: Database host to match against ssh_tunnel_config
            dsn_alias: DSN alias to match against dsn_ssh_tunnel_config

        Returns:
            Matching tunnel URL or None
        """
        # First, check if we already have an explicit URL
        if self.ssh_tunnel_url:
            return self.ssh_tunnel_url

        # Check DSN-based tunnel config
        if dsn_alias and self.dsn_ssh_tunnel_config:
            for dsn_regex, tunnel_url in self.dsn_ssh_tunnel_config.items():
                if self._matches(dsn_regex, dsn_alias, "[dsn ssh tunnels]"):
                    self.logger.debug(
                        "Found SSH tunnel for DSN '%s' matching '%s': %s",
                        dsn_alias,
                        dsn_regex,
                        redact_dsn(tunnel_url),  # ssh://user:password@bastion is allowed here
                    )
                    return cast(str, tunnel_url)

        # Check host-based tunnel config
        if host and self.ssh_tunnel_config:
            for host_regex, tunnel_url in self.ssh_tunnel_config.items():
                if self._matches(host_regex, host, "[ssh tunnels]"):
                    self.logger.debug(
                        "Found SSH tunnel for host '%s' matching '%s': %s",
                        host,
                        host_regex,
                        redact_dsn(tunnel_url),
                    )
                    return cast(str, tunnel_url)

        return None

    def _matches(self, pattern: str, value: str, section: str) -> bool:
        """re.fullmatch() that reports a bad pattern instead of crashing on it.

        The keys of [ssh tunnels] and [dsn ssh tunnels] are regular
        expressions written by hand; one unbalanced bracket used to be an
        uncaught re.error at every connection.
        """
        try:
            return re.fullmatch(pattern, value) is not None
        except re.error as e:
            message = f"Ignoring the {section} entry {pattern!r}: not a valid regular expression ({e})"
            self.logger.warning(message)
            # pgcli's logger goes to a file; without this line the user only
            # sees the direct connection fail with an unrelated libpq error.
            # The wrappers' loggers already print to stderr: no second line.
            prints = any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in self.logger.handlers)
            if not prints:
                sys.stderr.write(f"Warning: {message}\n")
            return False

    def start_tunnel(
        self,
        host: str,
        port: int = 5432,
        dsn_alias: Optional[str] = None,
    ) -> Tuple[str, int]:
        """
        Start SSH tunnel if configured.

        Args:
            host: Remote database host
            port: Remote database port (default 5432)
            dsn_alias: Optional DSN alias for config lookup

        Returns:
            Tuple of (local_host, local_port) to connect to.
            If no tunnel is needed, returns (host, port) unchanged.

        Raises:
            SystemExit: If tunnel is configured but paramiko is missing
        """
        tunnel_url = self.find_tunnel_url(host=host, dsn_alias=dsn_alias)

        if not tunnel_url:
            self.logger.debug("No SSH tunnel configured for host=%s, dsn=%s", host, dsn_alias)
            return host, port

        # Add protocol if missing
        if "://" not in tunnel_url:
            tunnel_url = f"ssh://{tunnel_url}"

        tunnel_info = urlparse(tunnel_url)
        ssh_hostname = tunnel_info.hostname
        try:
            url_port = tunnel_info.port
        except ValueError as e:
            # "bastion:2222x" or a port out of range: urlparse raises here,
            # which was a traceback for the wrappers and at startup.
            raise SSHTunnelError(f"invalid port in tunnel URL {redact_dsn(tunnel_url)!r}: {e}") from e
        ssh_port = url_port or 22
        ssh_username = tunnel_info.username
        proxycommand = None

        # Read SSH config for username/port/proxycommand/identityfile.
        # IdentityFile entries are read in order (host-specific first, wildcard after)
        # and passed as key_filename to paramiko. Paramiko tries each key in order,
        # skipping any that fail (e.g. passphrase-protected without agent).
        # look_for_keys remains False to prevent blind scanning of ~/.ssh/.
        # Auth order: key_filename (specific->wildcard) -> agent -> password
        ssh_config_path = os.path.expanduser("~/.ssh/config")
        ssh_config_error = None
        key_filenames = []
        if ssh_hostname and (os.path.isfile(ssh_config_path) or os.path.isfile(_SYSTEM_SSH_CONFIG)):
            try:
                host_config = _lookup_ssh_config(ssh_hostname, ssh_config_path, _SYSTEM_SSH_CONFIG, self.logger)
                ssh_hostname = host_config.get("hostname", ssh_hostname)
                if not ssh_username:
                    ssh_username = host_config.get("user")
                if not url_port and "port" in host_config:
                    ssh_port = int(host_config["port"])
                identity_files = host_config.get("identityfile", [])
                key_filenames = [os.path.expanduser(f) for f in identity_files if os.path.isfile(os.path.expanduser(f))]
                if key_filenames:
                    self.logger.debug("SSH identity files from config: %s", key_filenames)
                proxycommand = _proxy_command_from_host_config(host_config, ssh_hostname, ssh_port)
            except Exception as e:
                ssh_config_error = str(e)
                self.logger.warning("Could not read SSH config: %s", e)

        if not ssh_username:
            ssh_username = getpass.getuser()

        if proxycommand:
            # Now that the user and port are settled, the way ssh settles them.
            proxycommand = _fill_proxy_command_tokens(proxycommand, ssh_username, ssh_port)
            self.logger.debug("SSH proxy command from config: %s", proxycommand)

        if not ssh_hostname:
            self.logger.error("SSH tunnel URL has no hostname: %s", redact_dsn(tunnel_url))
            raise SSHTunnelError(f"no hostname in URL '{redact_dsn(tunnel_url)}'")

        self.logger.debug(
            "Creating SSH tunnel: %s@%s:%d -> %s:%d (allow_agent=%s, key_files=%d)",
            ssh_username,
            ssh_hostname,
            ssh_port,
            host,
            int(port),
            self.allow_agent,
            len(key_filenames),
        )

        tunnel = None
        try:
            tunnel = _NativeSSHTunnel(
                ssh_hostname=ssh_hostname,
                ssh_port=ssh_port,
                remote_host=host,
                remote_port=int(port),
                ssh_username=ssh_username,
                ssh_password=tunnel_info.password,
                ssh_proxy_command=proxycommand,
                allow_agent=self.allow_agent,
                key_filenames=key_filenames or None,
                host_key_policy=self.host_key_policy,
                logger=self.logger,
                secret_provider=self.secret_provider,
                secret_saver=self.secret_saver,
            )
            tunnel.start()
            if not tunnel.is_active:
                raise Exception(f"SSH tunnel failed to start (is_active={tunnel.is_active})")
            # Only now: a tunnel that failed to come up must not replace the
            # one \c or the reconnect still rely on.
            self.tunnel = tunnel

            self.logger.debug("SSH tunnel verified active")
        except socket.gaierror as e:
            # Name the host: a bare "Name or service not known" sent the last
            # investigation to DNS when the real defect was an unread ProxyJump.
            if ssh_config_error:
                hint = f" (~/.ssh/config could not be read: {ssh_config_error}; the host was dialed directly)"
            elif proxycommand:
                hint = ""
            else:
                # Say what was looked at, so a ProxyJump living somewhere else
                # is not the last thing anyone suspects.
                hint = (
                    " (paramiko found no ProxyJump/ProxyCommand for it in ~/.ssh/config"
                    f" or {_SYSTEM_SSH_CONFIG}, Include files followed, so the host was dialed directly)"
                )
            msg = f"could not resolve SSH host '{ssh_hostname}' from this machine{hint}: {e}"
            self.logger.error("SSH tunnel failed: %s", msg)
            raise SSHTunnelError(msg) from e
        except KeyboardInterrupt:
            # Ctrl-C at the passphrase prompt of a second auth attempt: the
            # first attempt's SSH transport is still open to the bastion.
            if tunnel is not None:
                tunnel.stop()
            raise
        except Exception as e:
            self.logger.error("SSH tunnel failed: %s", str(e))
            if tunnel is not None:
                # An SSH session opened before the forwarding failed must not linger.
                tunnel.stop()
            raise SSHTunnelError(str(e)) from e

        atexit.register(self.stop_tunnel)

        local_port = tunnel.local_bind_port
        if local_port is None:
            raise SSHTunnelError("SSH tunnel started but local port is not available")
        self.logger.debug("SSH tunnel ready, local port: %d", local_port)

        return "127.0.0.1", local_port

    def stop_tunnel(self):
        """Stop the SSH tunnel, also when its SSH connection already dropped,
        so its local forwarding server does not linger."""
        if self.tunnel:
            self.logger.debug("Stopping SSH tunnel")
            self.tunnel.stop()
            self.tunnel = None


def get_tunnel_manager_from_config(
    config: dict,
    ssh_tunnel_url: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> SSHTunnelManager:
    """
    Create an SSHTunnelManager from pgcli config.

    Args:
        config: Loaded pgcli config (from get_config())
        ssh_tunnel_url: Optional explicit SSH tunnel URL
        logger: Optional logger instance

    Returns:
        Configured SSHTunnelManager instance
    """
    # Extract allow_agent from ssh tunnels config (default True)
    ssh_tunnels_config = config.get("ssh tunnels", {})
    allow_agent = str(ssh_tunnels_config.get("allow_agent", "True")).lower() == "true"
    host_key_policy = str(ssh_tunnels_config.get("host_key_policy", "auto-add")).lower()

    return SSHTunnelManager(
        ssh_tunnel_url=ssh_tunnel_url,
        ssh_tunnel_config=ssh_tunnels_config,
        dsn_ssh_tunnel_config=config.get("dsn ssh tunnels"),
        logger=logger,
        allow_agent=allow_agent,
        host_key_policy=host_key_policy,
    )

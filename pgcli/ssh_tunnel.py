"""
SSH Tunnel helper module for pgcli tools.

This module provides reusable SSH tunnel functionality that can be used
by pgcli, pgcli_dump, pgcli_dumpall, and other tools.

Uses native Paramiko for SSH tunneling (no sshtunnel dependency).
"""

import atexit
import getpass
import logging
import os
import re
import select
import socketserver
import sys
import threading
from typing import Any, Optional, Tuple, cast
from urllib.parse import urlparse

import click
import paramiko

SSH_TUNNEL_SUPPORT = True


class _ForwardHandler(socketserver.StreamRequestHandler):
    """Handles a single forwarded connection through the SSH tunnel.

    Class attributes are set dynamically by _NativeSSHTunnel via type().
    """

    ssh_transport: paramiko.Transport
    remote_host: str
    remote_port: int
    logger: logging.Logger

    def handle(self):
        try:
            channel = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.remote_host, self.remote_port),
                self.request.getpeername(),
            )
        except Exception as e:
            self.logger.error("Failed to open SSH channel: %s", e)
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
    ):
        self.ssh_hostname = ssh_hostname
        self.ssh_port = ssh_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_proxy = ssh_proxy
        self.allow_agent = allow_agent
        self.key_filenames = key_filenames
        self.host_key_policy = host_key_policy
        self.logger = logger or logging.getLogger(__name__)

        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._server: Optional[socketserver.ThreadingTCPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._is_active = False

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def local_bind_port(self) -> Optional[int]:
        if self._server:
            return self._server.server_address[1]
        return None

    def start(self):
        """Start SSH connection and local forwarding server."""
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.load_system_host_keys()
        policy_cls = self.HOST_KEY_POLICIES.get(self.host_key_policy, paramiko.AutoAddPolicy)
        self._ssh_client.set_missing_host_key_policy(policy_cls())

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
        if self.ssh_proxy:
            connect_kwargs["sock"] = self.ssh_proxy

        self._ssh_client.connect(**connect_kwargs)
        self.logger.debug("SSH connection established to %s:%d", self.ssh_hostname, self.ssh_port)

        handler_class = type(
            "BoundForwardHandler",
            (_ForwardHandler,),
            {
                "ssh_transport": self._ssh_client.get_transport(),
                "remote_host": self.remote_host,
                "remote_port": self.remote_port,
                "logger": self.logger,
            },
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
            self._server.shutdown()
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
        """
        self.ssh_tunnel_url = ssh_tunnel_url
        self.ssh_tunnel_config = ssh_tunnel_config or {}
        self.dsn_ssh_tunnel_config = dsn_ssh_tunnel_config or {}
        self.logger = logger or logging.getLogger(__name__)
        self.tunnel: Optional[_NativeSSHTunnel] = None
        self.allow_agent = allow_agent
        self.host_key_policy = host_key_policy

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
                if re.fullmatch(dsn_regex, dsn_alias):
                    self.logger.debug(
                        "Found SSH tunnel for DSN '%s' matching '%s': %s",
                        dsn_alias,
                        dsn_regex,
                        tunnel_url,
                    )
                    return cast(str, tunnel_url)

        # Check host-based tunnel config
        if host and self.ssh_tunnel_config:
            for host_regex, tunnel_url in self.ssh_tunnel_config.items():
                if re.fullmatch(host_regex, host):
                    self.logger.debug(
                        "Found SSH tunnel for host '%s' matching '%s': %s",
                        host,
                        host_regex,
                        tunnel_url,
                    )
                    return cast(str, tunnel_url)

        return None

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
        ssh_port = tunnel_info.port or 22
        ssh_username = tunnel_info.username
        ssh_proxy = None

        # Read SSH config for username/port/proxycommand/identityfile.
        # IdentityFile entries are read in order (host-specific first, wildcard after)
        # and passed as key_filename to paramiko. Paramiko tries each key in order,
        # skipping any that fail (e.g. passphrase-protected without agent).
        # look_for_keys remains False to prevent blind scanning of ~/.ssh/.
        # Auth order: key_filename (specific->wildcard) -> agent -> password
        ssh_config_path = os.path.expanduser("~/.ssh/config")
        key_filenames = []
        if ssh_hostname and os.path.isfile(ssh_config_path):
            try:
                ssh_config = paramiko.SSHConfig()
                with open(ssh_config_path) as f:
                    ssh_config.parse(f)
                host_config = ssh_config.lookup(ssh_hostname)
                ssh_hostname = host_config.get("hostname", ssh_hostname)
                if not ssh_username:
                    ssh_username = host_config.get("user")
                if not tunnel_info.port and "port" in host_config:
                    ssh_port = int(host_config["port"])
                proxycommand = host_config.get("proxycommand")
                if proxycommand:
                    ssh_proxy = paramiko.ProxyCommand(proxycommand)
                identity_files = host_config.get("identityfile", [])
                key_filenames = [os.path.expanduser(f) for f in identity_files
                                 if os.path.isfile(os.path.expanduser(f))]
                if key_filenames:
                    self.logger.debug("SSH identity files from config: %s", key_filenames)
            except Exception as e:
                self.logger.warning("Could not read SSH config: %s", e)

        if not ssh_username:
            ssh_username = getpass.getuser()

        self.logger.debug(
            "Creating SSH tunnel: %s@%s:%d -> %s:%d (allow_agent=%s, key_files=%d)",
            ssh_username, ssh_hostname, ssh_port, host, int(port),
            self.allow_agent, len(key_filenames),
        )

        try:
            tunnel = _NativeSSHTunnel(
                ssh_hostname=ssh_hostname,
                ssh_port=ssh_port,
                remote_host=host,
                remote_port=int(port),
                ssh_username=ssh_username,
                ssh_password=tunnel_info.password,
                ssh_proxy=ssh_proxy,
                allow_agent=self.allow_agent,
                key_filenames=key_filenames or None,
                host_key_policy=self.host_key_policy,
                logger=self.logger,
            )
            tunnel.start()
            self.tunnel = tunnel

            if not tunnel.is_active:
                raise Exception(f"SSH tunnel failed to start (is_active={tunnel.is_active})")

            self.logger.debug("SSH tunnel verified active")
        except Exception as e:
            self.logger.error("SSH tunnel failed: %s", str(e))
            click.secho(f"SSH tunnel error: {e}", err=True, fg="red")
            sys.exit(1)

        atexit.register(self.stop_tunnel)

        local_port = tunnel.local_bind_port
        self.logger.debug("SSH tunnel ready, local port: %d", local_port)

        return "127.0.0.1", local_port

    def stop_tunnel(self):
        """Stop the SSH tunnel if running."""
        if self.tunnel and self.tunnel.is_active:
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

"""
SSH Tunnel helper module for pgcli tools.

This module provides reusable SSH tunnel functionality that can be used
by pgcli, pgcli_dump, pgcli_dumpall, and other tools.
"""

import atexit
import logging
import re
import sys
from typing import Any, Optional, Tuple, cast
from urllib.parse import urlparse

import click

try:
    import sshtunnel

    SSH_TUNNEL_SUPPORT = True
except ImportError:
    SSH_TUNNEL_SUPPORT = False


class SSHTunnelManager:
    """Manages SSH tunnel connections for database tools."""

    def __init__(
        self,
        ssh_tunnel_url: Optional[str] = None,
        ssh_tunnel_config: Optional[dict] = None,
        dsn_ssh_tunnel_config: Optional[dict] = None,
        logger: Optional[logging.Logger] = None,
        allow_agent: bool = True,
    ):
        """
        Initialize SSH tunnel manager.

        Args:
            ssh_tunnel_url: Explicit SSH tunnel URL (e.g., ssh://user@host:port)
            ssh_tunnel_config: Dict of host_regex -> tunnel_url mappings
            dsn_ssh_tunnel_config: Dict of dsn_regex -> tunnel_url mappings
            logger: Logger instance for debug output
            allow_agent: Whether to allow SSH agent for key authentication (default True)
        """
        self.ssh_tunnel_url = ssh_tunnel_url
        self.ssh_tunnel_config = ssh_tunnel_config or {}
        self.dsn_ssh_tunnel_config = dsn_ssh_tunnel_config or {}
        self.logger = logger or logging.getLogger(__name__)
        self.tunnel: Optional[Any] = None
        self.allow_agent = allow_agent

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
                if re.search(host_regex, host):
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
            SystemExit: If tunnel is configured but sshtunnel package is missing
        """
        tunnel_url = self.find_tunnel_url(host=host, dsn_alias=dsn_alias)

        if not tunnel_url:
            self.logger.debug("No SSH tunnel configured for host=%s, dsn=%s", host, dsn_alias)
            return host, port

        # Verify sshtunnel is available
        if not SSH_TUNNEL_SUPPORT:
            click.secho(
                'Cannot open SSH tunnel, "sshtunnel" package was not found. '
                "Please install pgcli with `pip install pgcli[sshtunnel]` if you want SSH tunnel support.",
                err=True,
                fg="red",
            )
            sys.exit(1)

        # Add protocol if missing
        if "://" not in tunnel_url:
            tunnel_url = f"ssh://{tunnel_url}"

        tunnel_info = urlparse(tunnel_url)
        params = {
            "local_bind_address": ("127.0.0.1",),
            "remote_bind_address": (host, int(port)),
            "ssh_address_or_host": (tunnel_info.hostname, tunnel_info.port or 22),
            "logger": self.logger,
            "ssh_config_file": "~/.ssh/config",
            "allow_agent": self.allow_agent,
            "host_pkey_directories": [],  # Don't scan ~/.ssh/ for keys, use ssh-agent only
            "compression": False,
        }

        if tunnel_info.username:
            params["ssh_username"] = tunnel_info.username
        if tunnel_info.password:
            params["ssh_password"] = tunnel_info.password

        # Hack: sshtunnel adds a console handler to the logger, so we revert handlers.
        logger_handlers = self.logger.handlers.copy()
        try:
            log_params = {k: ("***" if k == "ssh_password" else v) for k, v in params.items()}
            self.logger.debug("Creating SSH tunnel with params: %r", log_params)
            tunnel = sshtunnel.SSHTunnelForwarder(**params)
            self.tunnel = tunnel
            self.logger.debug("SSH tunnel created, calling start()...")
            tunnel.start()
            self.logger.debug("SSH tunnel start() returned, is_active: %s", tunnel.is_active)

            if not tunnel.is_active:
                raise Exception(f"SSH tunnel failed to start (is_active={tunnel.is_active})")

            self.logger.debug("SSH tunnel verified active")
        except Exception as e:
            self.logger.handlers = logger_handlers
            self.logger.error("SSH tunnel failed: %s", str(e))
            click.secho(f"SSH tunnel error: {e}", err=True, fg="red")
            sys.exit(1)

        self.logger.handlers = logger_handlers
        atexit.register(self.stop_tunnel)

        local_port = tunnel.local_bind_ports[0]
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

    return SSHTunnelManager(
        ssh_tunnel_url=ssh_tunnel_url,
        ssh_tunnel_config=ssh_tunnels_config,
        dsn_ssh_tunnel_config=config.get("dsn ssh tunnels"),
        logger=logger,
        allow_agent=allow_agent,
    )

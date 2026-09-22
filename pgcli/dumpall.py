"""
pgcli_dumpall - A wrapper around pg_dumpall with SSH tunnel support.

This tool provides the same functionality as pg_dumpall but adds support for
SSH tunnels using pgcli's configuration.
"""

import logging
import os
import subprocess
import sys
from typing import Optional

import click

from .config import get_config
from .ssh_tunnel import get_tunnel_manager_from_config
from .dump import get_password_from_pgpass, parse_user_and_database
from .dump_args import (  # re-exported: both wrappers and their tests import these
    build_tunneled_args,
    parse_connection_args,
)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Setup logging for pgcli_dumpall."""
    logger = logging.getLogger("pgcli_dumpall")
    handler = logging.StreamHandler()
    if verbose:
        logger.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    else:
        logger.setLevel(logging.WARNING)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def find_pg_dumpall() -> str:
    """Find pg_dumpall executable in PATH."""
    # Check common locations
    paths_to_check = [
        "/usr/bin/pg_dumpall",
        "/usr/local/bin/pg_dumpall",
        "/usr/pgsql-17/bin/pg_dumpall",
        "/usr/pgsql-16/bin/pg_dumpall",
        "/usr/pgsql-15/bin/pg_dumpall",
        "/usr/pgsql-14/bin/pg_dumpall",
    ]

    # First check PATH
    for path in os.environ.get("PATH", "").split(os.pathsep):
        pg_dumpall_path = os.path.join(path, "pg_dumpall")
        if os.path.isfile(pg_dumpall_path) and os.access(pg_dumpall_path, os.X_OK):
            return pg_dumpall_path

    # Then check common locations
    for path in paths_to_check:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path

    return "pg_dumpall"  # Fall back to PATH lookup


@click.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
        "allow_interspersed_args": True,
    }
)
@click.option(
    "--ssh-tunnel",
    "ssh_tunnel",
    default=None,
    help="SSH tunnel URL (e.g., ssh://user@host:port). If not provided, uses pgcli config.",
)
@click.option(
    "--dsn",
    "dsn_alias",
    default=None,
    help="DSN alias from pgcli config (for SSH tunnel lookup).",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Verbose mode (show SSH tunnel debug info).",
)
@click.pass_context
def cli(ctx, ssh_tunnel: Optional[str], dsn_alias: Optional[str], verbose: bool):
    """
    pg_dumpall wrapper with SSH tunnel support.

    This command wraps pg_dumpall and adds SSH tunnel functionality using
    pgcli's configuration. All pg_dumpall options are passed through.

    Examples:

        # Use SSH tunnel from pgcli config
        pgcli_dumpall -h mydb.example.com > cluster_backup.sql

        # Explicit SSH tunnel
        pgcli_dumpall --ssh-tunnel user@bastion.example.com -h mydb > backup.sql

        # Use DSN alias for tunnel lookup, dump only globals
        pgcli_dumpall --dsn production -g -f globals.sql
    """
    logger = setup_logging(verbose)

    # Get all extra arguments (pg_dumpall options)
    pg_dumpall_args = ctx.args

    # Load pgcli config
    try:
        config = get_config()
    except Exception as e:
        logger.warning("Could not load pgcli config: %s", e)
        config = {}

    # Parse connection arguments
    host, port, remaining_args, has_host, has_port = parse_connection_args(pg_dumpall_args)
    logger.debug("Parsed connection: host=%s, port=%d", host, port)

    # Setup SSH tunnel manager
    tunnel_manager = get_tunnel_manager_from_config(
        config,
        ssh_tunnel_url=ssh_tunnel,
        logger=logger,
    )

    # Try to start tunnel
    tunnel_host, tunnel_port = tunnel_manager.start_tunnel(
        host=host,
        port=port,
        dsn_alias=dsn_alias,
    )

    # Build final pg_dumpall command
    pg_dumpall_path = find_pg_dumpall()
    logger.debug("Using pg_dumpall: %s", pg_dumpall_path)

    # Prepare environment (may need to set PGPASSWORD for tunneled connections)
    env = os.environ.copy()

    if tunnel_host != host or tunnel_port != port:
        # Tunnel is active, modify connection args
        logger.debug("SSH tunnel active: %s:%d -> %s:%d", host, port, tunnel_host, tunnel_port)
        final_args = build_tunneled_args(
            remaining_args,
            tunnel_host,
            tunnel_port,
            host,
            port,
            has_host,
            has_port,
        )

        # Look up password from .pgpass using ORIGINAL host (not tunneled)
        # This is needed because pg_dumpall will see 127.0.0.1 but .pgpass has the real host
        if "PGPASSWORD" not in env:
            user, _ = parse_user_and_database(pg_dumpall_args)
            # pg_dumpall connects to all databases, use wildcard
            logger.debug("Looking up password for %s@%s:%d/*", user, host, port)
            password = get_password_from_pgpass(host, port, "*", user)
            if password:
                logger.debug("Found password in .pgpass for original host")
                env["PGPASSWORD"] = password
    else:
        # No tunnel, use original args
        final_args = pg_dumpall_args

    # Execute pg_dumpall
    cmd = [pg_dumpall_path] + final_args
    logger.debug("Executing: %s", " ".join(cmd))

    try:
        result = subprocess.run(cmd, env=env)
        sys.exit(result.returncode)
    except FileNotFoundError:
        click.secho(
            f"Error: pg_dumpall not found at '{pg_dumpall_path}'. Please ensure PostgreSQL client tools are installed.",
            err=True,
            fg="red",
        )
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
    finally:
        tunnel_manager.stop_tunnel()


def main():
    """Entry point for pgcli_dumpall."""
    cli()


if __name__ == "__main__":
    main()

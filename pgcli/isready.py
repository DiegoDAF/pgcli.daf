"""
pgcli_isready - A wrapper around pg_isready with SSH tunnel support.

This tool provides the same functionality as pg_isready but adds support for
SSH tunnels using pgcli's configuration.
"""

import logging
import os
import subprocess
import sys
from typing import List, Optional

import click

from .config import get_config
from .dsnaliases import DsnAliases
from .dump_args import apply_dsn_alias, parse_connection_args  # noqa: F401  re-exported, the tests import it from here
from .dump_args import build_tunneled_args as _build_tunneled_args
from .ssh_tunnel import SSHTunnelError, get_tunnel_manager_from_config


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Setup logging for pgcli_isready."""
    logger = logging.getLogger("pgcli_isready")
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


def find_pg_isready() -> str:
    """Find pg_isready executable in PATH."""
    paths_to_check = [
        "/usr/bin/pg_isready",
        "/usr/local/bin/pg_isready",
        "/usr/pgsql-17/bin/pg_isready",
        "/usr/pgsql-16/bin/pg_isready",
        "/usr/pgsql-15/bin/pg_isready",
        "/usr/pgsql-14/bin/pg_isready",
    ]

    for path in os.environ.get("PATH", "").split(os.pathsep):
        pg_isready_path = os.path.join(path, "pg_isready")
        if os.path.isfile(pg_isready_path) and os.access(pg_isready_path, os.X_OK):
            return pg_isready_path

    for path in paths_to_check:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path

    return "pg_isready"


def build_tunneled_args(
    original_args: List[str],
    tunnel_host: str,
    tunnel_port: int,
    has_host: bool,
    has_port: bool,
) -> List[str]:
    """Rewrite the connection arguments to point at the local end of the tunnel.

    The same rewrite as the dump wrappers' (pg_isready takes the same -h, -p
    and -d, connection strings included); this keeps the signature the
    wrapper has always had.
    """
    return _build_tunneled_args(original_args, tunnel_host, tunnel_port, None, None, has_host, has_port)


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
    help="DSN alias from pgcli config: the server to check, as with pgcli --dsn (it also picks the SSH tunnel).",
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
    pg_isready wrapper with SSH tunnel support.

    This command wraps pg_isready and adds SSH tunnel functionality using
    pgcli's configuration. All pg_isready options are passed through.

    Exit codes: 0=accepting connections, 1=rejecting, 2=no response, 3=no attempt.

    Examples:

        # Check if remote DB is accepting connections via SSH tunnel
        pgcli_isready -h mydb.example.com

        # Explicit SSH tunnel
        pgcli_isready --ssh-tunnel user@bastion -h mydb.internal

        # The DSN alias is the server to check, as with pgcli --dsn
        pgcli_isready --dsn production

        # With timeout
        pgcli_isready -h mydb.example.com -t 5
    """
    logger = setup_logging(verbose)

    pg_isready_args = ctx.args

    try:
        config = get_config()
    except Exception as e:
        logger.warning("Could not load pgcli config: %s", e)
        config = {}

    # --dsn names the server itself, like pgcli --dsn. pg_isready never
    # authenticates, so the alias's password is simply dropped. Bad
    # arguments (a port that is not a number) are one line, not a traceback.
    try:
        if dsn_alias:
            conninfo = DsnAliases.from_config(config).get(dsn_alias)
            if conninfo is None:
                raise ValueError(
                    f"Could not find a DSN with alias {dsn_alias}. Please check the \"[alias_dsn]\" section in pgclirc or dsn.d/ directory."
                )
            pg_isready_args, _ = apply_dsn_alias(pg_isready_args, conninfo)
        host, port, remaining_args, has_host, has_port = parse_connection_args(pg_isready_args)
    except ValueError as e:
        click.secho(f"Error: {e}", err=True, fg="red")
        sys.exit(1)
    logger.debug("Parsed connection: host=%s, port=%d", host, port)

    tunnel_manager = get_tunnel_manager_from_config(
        config,
        ssh_tunnel_url=ssh_tunnel,
        logger=logger,
    )

    try:
        tunnel_host, tunnel_port = tunnel_manager.start_tunnel(
            host=host,
            port=port,
            dsn_alias=dsn_alias,
        )
    except SSHTunnelError as e:
        click.secho(str(e), err=True, fg="red")
        sys.exit(1)

    pg_isready_path = find_pg_isready()
    logger.debug("Using pg_isready: %s", pg_isready_path)

    if tunnel_host != host or tunnel_port != port:
        logger.debug("SSH tunnel active: %s:%d -> %s:%d", host, port, tunnel_host, tunnel_port)
        final_args = build_tunneled_args(
            remaining_args,
            tunnel_host,
            tunnel_port,
            has_host,
            has_port,
        )
    else:
        final_args = pg_isready_args

    cmd = [pg_isready_path] + final_args
    logger.debug("Executing: %s", " ".join(cmd))

    try:
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    except FileNotFoundError:
        click.secho(
            f"Error: pg_isready not found at '{pg_isready_path}'. Please ensure PostgreSQL client tools are installed.",
            err=True,
            fg="red",
        )
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
    finally:
        tunnel_manager.stop_tunnel()


def main():
    """Entry point for pgcli_isready."""
    cli()


if __name__ == "__main__":
    main()

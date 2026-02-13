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
from .ssh_tunnel import get_tunnel_manager_from_config


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


def parse_connection_args(args: List[str]) -> tuple:
    """
    Parse connection-related arguments from the command line.

    Returns:
        Tuple of (host, port, remaining_args, has_host, has_port)
    """
    host = os.environ.get("PGHOST", "localhost")
    port = int(os.environ.get("PGPORT", 5432))
    remaining_args = []
    has_host = False
    has_port = False

    i = 0
    while i < len(args):
        arg = args[i]

        if arg in ("-h", "--host"):
            if i + 1 < len(args):
                host = args[i + 1]
                has_host = True
                remaining_args.extend([arg, args[i + 1]])
                i += 2
                continue
        elif arg.startswith("--host="):
            host = arg.split("=", 1)[1]
            has_host = True
            remaining_args.append(arg)
            i += 1
            continue

        if arg in ("-p", "--port"):
            if i + 1 < len(args):
                port = int(args[i + 1])
                has_port = True
                remaining_args.extend([arg, args[i + 1]])
                i += 2
                continue
        elif arg.startswith("--port="):
            port = int(arg.split("=", 1)[1])
            has_port = True
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
    has_host: bool,
    has_port: bool,
) -> List[str]:
    """Build new argument list with tunneled connection parameters."""
    new_args = []
    i = 0

    while i < len(original_args):
        arg = original_args[i]

        if arg in ("-h", "--host"):
            new_args.extend(["-h", tunnel_host])
            i += 2
            continue
        elif arg.startswith("--host="):
            new_args.append(f"--host={tunnel_host}")
            i += 1
            continue

        if arg in ("-p", "--port"):
            new_args.extend(["-p", str(tunnel_port)])
            i += 2
            continue
        elif arg.startswith("--port="):
            new_args.append(f"--port={tunnel_port}")
            i += 1
            continue

        new_args.append(arg)
        i += 1

    if not has_host:
        new_args.extend(["-h", tunnel_host])
    if not has_port:
        new_args.extend(["-p", str(tunnel_port)])

    return new_args


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
        allow_interspersed_args=True,
    )
)
@click.option(
    "--ssh-tunnel",
    "ssh_tunnel",
    default=None,
    help="SSH tunnel URL (e.g., ssh://user@host:port). "
    "If not provided, uses pgcli config.",
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
    pg_isready wrapper with SSH tunnel support.

    This command wraps pg_isready and adds SSH tunnel functionality using
    pgcli's configuration. All pg_isready options are passed through.

    Exit codes: 0=accepting connections, 1=rejecting, 2=no response, 3=no attempt.

    Examples:

        # Check if remote DB is accepting connections via SSH tunnel
        pgcli_isready -h mydb.example.com

        # Explicit SSH tunnel
        pgcli_isready --ssh-tunnel user@bastion -h mydb.internal

        # Use DSN alias for tunnel lookup
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

    host, port, remaining_args, has_host, has_port = parse_connection_args(pg_isready_args)
    logger.debug("Parsed connection: host=%s, port=%d", host, port)

    tunnel_manager = get_tunnel_manager_from_config(
        config,
        ssh_tunnel_url=ssh_tunnel,
        logger=logger,
    )

    tunnel_host, tunnel_port = tunnel_manager.start_tunnel(
        host=host,
        port=port,
        dsn_alias=dsn_alias,
    )

    pg_isready_path = find_pg_isready()
    logger.debug("Using pg_isready: %s", pg_isready_path)

    if tunnel_host != host or tunnel_port != port:
        logger.debug("SSH tunnel active: %s:%d -> %s:%d", host, port, tunnel_host, tunnel_port)
        final_args = build_tunneled_args(
            remaining_args, tunnel_host, tunnel_port, has_host, has_port,
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
            f"Error: pg_isready not found at '{pg_isready_path}'. "
            "Please ensure PostgreSQL client tools are installed.",
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

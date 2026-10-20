"""
pgcli_dump - A wrapper around pg_dump with SSH tunnel support.

This tool provides the same functionality as pg_dump but adds support for
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
from .pgpass import has_safe_permissions, lookup_password, pgpass_path
from .redact import redact_dsn
from .ssh_tunnel import SSHTunnelError, get_tunnel_manager_from_config
from .dump_args import (  # re-exported: both wrappers and their tests import these
    apply_dsn_alias,
    build_tunneled_args,
    dbname_identity,
    getopt_items,
    parse_connection_args,
)


def get_password_from_pgpass(host: str, port: int, database: str, user: str) -> Optional[str]:
    """Return the password for these connection values from the password file.

    The lookup is pgpass.lookup_password(), the same one pgcli uses and the
    way libpq reads the file: PGPASSFILE or ~/.pgpass, ``\\:`` and ``\\\\``
    escapes, each field matched exactly or by a whole ``*``, first match wins.
    These wrappers used to keep a looser reader of their own (fnmatch globs,
    no escapes, no PGPASSFILE), so they could disagree with psql and pgcli.
    """
    path = pgpass_path()
    if not os.path.exists(path):
        return None

    # libpq refuses a password file that others can read, and says so. Without
    # this the wrappers would authenticate with a password psql itself would
    # have ignored, which is the kind of difference that only shows up when
    # somebody tightens the file and the backup stops working.
    if not has_safe_permissions(path):
        click.secho(
            f'WARNING: password file "{path}" has group or world access; permissions should be u=rw (0600) or less',
            err=True,
            fg="yellow",
        )
        return None

    password: Optional[str] = lookup_password(host, port, database, user, path=path)
    return password


def parse_user_and_database(args: List[str]) -> tuple:
    """
    Parse user and database from command line arguments.

    Returns:
        Tuple of (user, database)
    """
    user = os.environ.get("PGUSER", "postgres")
    database = os.environ.get("PGDATABASE", "*")

    items = getopt_items(args)
    operands = [value for name, value in items if name is None and value is not None]
    for name, value in items:
        if name in ("-U", "--username") and value:
            user = value
        # -d/--dbname in all three forms: bare name, keyword/value connection
        # string, or URI. .pgpass is asked about whatever comes out of here, so
        # reading only the bare form means a tunnelled dump looks up the wrong
        # user and never finds its password.
        elif name in ("-d", "--dbname") and value:
            cs_user, cs_db = dbname_identity(value)
            if cs_user:
                user = cs_user
            if cs_db:
                database = cs_db

    # The database operand, if any. pg_dump refuses one together with -d ("too
    # many command-line arguments"), so the two never both apply.
    if operands:
        cs_user, cs_db = dbname_identity(operands[0])
        if cs_user:
            user = cs_user
        if cs_db:
            database = cs_db

    return user, database


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Setup logging for pgcli_dump."""
    logger = logging.getLogger("pgcli_dump")
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


def find_pg_dump() -> str:
    """Find pg_dump executable in PATH."""
    # Check common locations
    paths_to_check = [
        "/usr/bin/pg_dump",
        "/usr/local/bin/pg_dump",
        "/usr/pgsql-17/bin/pg_dump",
        "/usr/pgsql-16/bin/pg_dump",
        "/usr/pgsql-15/bin/pg_dump",
        "/usr/pgsql-14/bin/pg_dump",
    ]

    # First check PATH
    for path in os.environ.get("PATH", "").split(os.pathsep):
        pg_dump_path = os.path.join(path, "pg_dump")
        if os.path.isfile(pg_dump_path) and os.access(pg_dump_path, os.X_OK):
            return pg_dump_path

    # Then check common locations
    for path in paths_to_check:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path

    return "pg_dump"  # Fall back to PATH lookup


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
    help="DSN alias from pgcli config: the connection to dump, as with pgcli --dsn (it also picks the SSH tunnel).",
)
@click.option(
    "--tunnel-verbose",
    "verbose",
    is_flag=True,
    help="Show SSH tunnel debug info. -v and --verbose go to the dump tool, as its own verbose mode.",
)
@click.pass_context
def cli(ctx, ssh_tunnel: Optional[str], dsn_alias: Optional[str], verbose: bool):
    """
    pg_dump wrapper with SSH tunnel support.

    This command wraps pg_dump and adds SSH tunnel functionality using
    pgcli's configuration. All pg_dump options are passed through.

    Examples:

        # Use SSH tunnel from pgcli config
        pgcli_dump -h mydb.example.com mydb > backup.sql

        # Explicit SSH tunnel
        pgcli_dump --ssh-tunnel user@bastion.example.com -h mydb mydb > backup.sql

        # The DSN alias is the connection, as with pgcli --dsn
        pgcli_dump --dsn production mydb -F c -f backup.dump
    """
    logger = setup_logging(verbose)

    # Get all extra arguments (pg_dump options)
    pg_dump_args = ctx.args

    # Load pgcli config
    try:
        config = get_config()
    except Exception as e:
        logger.warning("Could not load pgcli config: %s", e)
        config = {}

    # Parse connection arguments
    # --dsn names the connection itself, like pgcli --dsn; bad arguments are
    # reported in one line instead of a traceback.
    alias_password = None
    try:
        if dsn_alias:
            conninfo = DsnAliases.from_config(config).get(dsn_alias)
            if conninfo is None:
                raise ValueError(
                    f"Could not find a DSN with alias {dsn_alias}. Please check the \"[alias_dsn]\" section in pgclirc or dsn.d/ directory."
                )
            pg_dump_args, alias_password = apply_dsn_alias(pg_dump_args, conninfo)
        host, port, remaining_args, has_host, has_port = parse_connection_args(pg_dump_args)
    except ValueError as e:
        click.secho(f"Error: {e}", err=True, fg="red")
        sys.exit(1)
    logger.debug("Parsed connection: host=%s, port=%d", host, port)

    # Setup SSH tunnel manager
    tunnel_manager = get_tunnel_manager_from_config(
        config,
        ssh_tunnel_url=ssh_tunnel,
        logger=logger,
    )

    # Try to start tunnel
    try:
        tunnel_host, tunnel_port = tunnel_manager.start_tunnel(
            host=host,
            port=port,
            dsn_alias=dsn_alias,
        )
    except SSHTunnelError as e:
        click.secho(str(e), err=True, fg="red")
        sys.exit(1)

    # Build final pg_dump command
    pg_dump_path = find_pg_dump()
    logger.debug("Using pg_dump: %s", pg_dump_path)

    # Prepare environment (may need to set PGPASSWORD for tunneled connections)
    env = os.environ.copy()
    if alias_password and "PGPASSWORD" not in env:
        env["PGPASSWORD"] = alias_password

    if tunnel_host != host or tunnel_port != port:
        # Tunnel is active, modify connection args
        logger.debug("SSH tunnel active: %s:%d -> %s:%d", host, port, tunnel_host, tunnel_port)
        try:
            final_args = build_tunneled_args(
                remaining_args,
                tunnel_host,
                tunnel_port,
                host,
                port,
                has_host,
                has_port,
            )
        except ValueError as e:
            # A connection string the tunnel cannot rewrite (several hosts,
            # host= or port= in a URI's query): one line, not a traceback.
            tunnel_manager.stop_tunnel()
            click.secho(f"Error: {e}", err=True, fg="red")
            sys.exit(1)

        # Look up password from .pgpass using ORIGINAL host (not tunneled)
        # This is needed because pg_dump will see 127.0.0.1 but .pgpass has the real host
        if "PGPASSWORD" not in env:
            user, database = parse_user_and_database(pg_dump_args)
            logger.debug("Looking up password for %s@%s:%d/%s", user, host, port, database)
            password = get_password_from_pgpass(host, port, database, user)
            if password:
                logger.debug("Found password in .pgpass for original host")
                env["PGPASSWORD"] = password
    else:
        # No tunnel, use original args
        final_args = pg_dump_args

    # Execute pg_dump
    cmd = [pg_dump_path] + final_args
    # -d may carry the password; --tunnel-verbose must not print it.
    logger.debug("Executing: %s", redact_dsn(" ".join(cmd)))

    try:
        result = subprocess.run(cmd, env=env)
        sys.exit(result.returncode)
    except FileNotFoundError:
        click.secho(
            f"Error: pg_dump not found at '{pg_dump_path}'. Please ensure PostgreSQL client tools are installed.",
            err=True,
            fg="red",
        )
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
    finally:
        tunnel_manager.stop_tunnel()


def main():
    """Entry point for pgcli_dump."""
    cli()


if __name__ == "__main__":
    main()

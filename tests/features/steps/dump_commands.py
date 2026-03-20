"""Behave step definitions for pgcli_dump and pgcli_dumpall commands."""

import subprocess
from behave import when, then


@when("we run pgcli_dump with {options}")
def step_run_pgcli_dump(context, options):
    """Run pgcli_dump with given options."""
    cmd = f"pgcli_dump {options}"
    context.cmd = cmd
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        context.exit_code = result.returncode
        context.stdout = result.stdout
        context.stderr = result.stderr
    except subprocess.TimeoutExpired:
        context.exit_code = -1
        context.stdout = ""
        context.stderr = "Command timed out"


@when("we run pgcli_dumpall with {options}")
def step_run_pgcli_dumpall(context, options):
    """Run pgcli_dumpall with given options."""
    cmd = f"pgcli_dumpall {options}"
    context.cmd = cmd
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        context.exit_code = result.returncode
        context.stdout = result.stdout
        context.stderr = result.stderr
    except subprocess.TimeoutExpired:
        context.exit_code = -1
        context.stdout = ""
        context.stderr = "Command timed out"


@then("we see pgcli_dump help output")
def step_see_pgcli_dump_help(context):
    """Verify pgcli_dump help output is shown."""
    output = context.stdout + context.stderr
    assert "pg_dump wrapper with SSH tunnel support" in output, f"Expected help text not found in: {output}"
    assert "--ssh-tunnel" in output, f"Expected --ssh-tunnel option not found in: {output}"


@then("we see pgcli_dumpall help output")
def step_see_pgcli_dumpall_help(context):
    """Verify pgcli_dumpall help output is shown."""
    output = context.stdout + context.stderr
    assert "pg_dumpall wrapper with SSH tunnel support" in output, f"Expected help text not found in: {output}"
    assert "--ssh-tunnel" in output, f"Expected --ssh-tunnel option not found in: {output}"


@then("pgcli_dump exits successfully")
def step_pgcli_dump_exits_successfully(context):
    """Verify pgcli_dump exits with code 0."""
    assert context.exit_code == 0, f"Expected exit code 0, got {context.exit_code}. stderr: {context.stderr}"


@then("pgcli_dumpall exits successfully")
def step_pgcli_dumpall_exits_successfully(context):
    """Verify pgcli_dumpall exits with code 0."""
    assert context.exit_code == 0, f"Expected exit code 0, got {context.exit_code}. stderr: {context.stderr}"


@then("we see pg_dump version output")
def step_see_pg_dump_version(context):
    """Verify pg_dump version output is shown."""
    output = context.stdout + context.stderr
    # pg_dump --version outputs something like "pg_dump (PostgreSQL) 16.1"
    assert "pg_dump" in output.lower() or "postgresql" in output.lower(), f"Expected pg_dump version info not found in: {output}"


@then("we see pg_dumpall version output")
def step_see_pg_dumpall_version(context):
    """Verify pg_dumpall version output is shown."""
    output = context.stdout + context.stderr
    # pg_dumpall --version outputs something like "pg_dumpall (PostgreSQL) 16.1"
    assert "pg_dumpall" in output.lower() or "postgresql" in output.lower(), f"Expected pg_dumpall version info not found in: {output}"


@then("pgcli_dump attempts database connection")
def step_pgcli_dump_attempts_connection(context):
    """Verify pgcli_dump attempted to connect (may fail without valid DB)."""
    # The command should have run pg_dump, which may fail with connection error
    # but that's expected - we just verify it tried
    output = context.stdout + context.stderr
    # Accept either success or connection error (means pg_dump was invoked)
    connection_attempted = (
        context.exit_code == 0
        or "connection" in output.lower()
        or "password" in output.lower()
        or "could not connect" in output.lower()
        or "fe_sendauth" in output.lower()
        or "ssl" in output.lower()
    )
    assert connection_attempted, f"Expected connection attempt, got exit_code={context.exit_code}, output: {output}"


@then("pgcli_dumpall attempts database connection")
def step_pgcli_dumpall_attempts_connection(context):
    """Verify pgcli_dumpall attempted to connect (may fail without valid DB)."""
    output = context.stdout + context.stderr
    connection_attempted = (
        context.exit_code == 0
        or "connection" in output.lower()
        or "password" in output.lower()
        or "could not connect" in output.lower()
        or "fe_sendauth" in output.lower()
        or "ssl" in output.lower()
    )
    assert connection_attempted, f"Expected connection attempt, got exit_code={context.exit_code}, output: {output}"


@then("we see ssh-tunnel option in help")
def step_see_ssh_tunnel_in_help(context):
    """Verify --ssh-tunnel option is shown in help."""
    output = context.stdout + context.stderr
    assert "--ssh-tunnel" in output, f"Expected --ssh-tunnel option not found in: {output}"


@then("we see dsn option in help")
def step_see_dsn_in_help(context):
    """Verify --dsn option is shown in help."""
    output = context.stdout + context.stderr
    assert "--dsn" in output, f"Expected --dsn option not found in: {output}"

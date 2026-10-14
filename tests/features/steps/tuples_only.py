"""
Steps for testing -t/--tuples-only option behavioral tests.
"""

import subprocess
from behave import when, then


@when('we run pgcli with "{options}"')
def step_run_pgcli_with_options(context, options):
    """Run pgcli with specified options."""
    # Split options into individual arguments, handling quoted strings
    import shlex

    args = shlex.split(options)

    cmd = [
        "pgcli",
        "-h",
        context.conf["host"],
        "-p",
        str(context.conf["port"]),
        "-U",
        context.conf["user"],
        "-d",
        context.conf["dbname"],
        "--less-chatty",  # Suppress intro/goodbye messages
    ] + args

    try:
        context.cmd_output = subprocess.check_output(cmd, cwd=context.package_root, stderr=subprocess.STDOUT, timeout=10)
        context.exit_code = 0
    except subprocess.CalledProcessError as e:
        context.cmd_output = e.output
        context.exit_code = e.returncode
    except subprocess.TimeoutExpired:
        context.cmd_output = b"Command timed out"
        context.exit_code = -1


@then("we see only the data rows")
def step_see_only_data_rows(context):
    """Verify that output contains only data rows (no headers, borders, or status)."""
    output = context.cmd_output.decode('utf-8').strip()

    # Should not have table borders or formatting characters
    assert "+-" not in output, f"Expected no table borders, but got: {output}"
    assert not output.startswith("|"), f"Expected no table pipes, but got: {output}"

    # Should have some output (the data)
    assert len(output) > 0, f"Expected data output, but got empty: {output}"


@then('we don\'t see "{text}"')
def step_dont_see_text(context, text):
    """Verify that specified text is NOT in the output."""
    output = context.cmd_output.decode('utf-8')
    assert text not in output, f"Expected NOT to see '{text}' in output, but got: {output}"


@then('we see "{text}" in the output')
def step_see_text_in_output(context, text):
    """Verify that specified text IS in the output."""
    output = context.cmd_output.decode('utf-8')
    assert text in output, f"Expected to see '{text}' in output, but got: {output}"


@then("we see tab-separated values")
def step_see_tab_separated_values(context):
    """Verify that output contains tab-separated values."""
    output = context.cmd_output.decode('utf-8').strip()

    # Should contain tabs
    assert "\t" in output, f"Expected tab-separated values, but got: {output}"

    # Should not have table borders
    assert "+-" not in output, f"Expected no table borders, but got: {output}"
    assert "|" not in output, f"Expected no table pipes, but got: {output}"


@then("we see multiple data rows")
def step_see_multiple_data_rows(context):
    """Verify that output contains multiple rows of data."""
    output = context.cmd_output.decode('utf-8').strip()
    lines = output.split('\n')

    # Filter out empty lines
    data_lines = [line for line in lines if line.strip()]

    # Should have multiple rows
    assert len(data_lines) >= 3, f"Expected at least 3 data rows, but got {len(data_lines)}: {output}"

    # Should not have table formatting
    assert "+-" not in output, f"Expected no table borders, but got: {output}"


@when('we run pgcli separating stderr with "{options}"')
def step_run_pgcli_separating_stderr(context, options):
    """Run pgcli keeping stdout and stderr apart, to check where errors go."""
    import shlex

    cmd = [
        "pgcli",
        "-h",
        context.conf["host"],
        "-p",
        str(context.conf["port"]),
        "-U",
        context.conf["user"],
        "-d",
        context.conf["dbname"],
        "--less-chatty",
    ] + shlex.split(options)
    result = subprocess.run(cmd, cwd=context.package_root, capture_output=True, timeout=10)
    context.cmd_stdout = result.stdout.decode()
    context.cmd_stderr = result.stderr.decode()
    context.exit_code = result.returncode


@then('we see "{text}" on stderr')
def step_see_on_stderr(context, text):
    assert text in context.cmd_stderr, f"Expected {text!r} on stderr, got: {context.cmd_stderr!r}"


@then('we don\'t see "{text}" on stdout')
def step_not_on_stdout(context, text):
    assert text not in context.cmd_stdout, f"Did not expect {text!r} on stdout, got: {context.cmd_stdout!r}"

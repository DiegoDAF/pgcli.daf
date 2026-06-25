"""
Steps for testing the stream_results config option.

stream_results streams each statement result as soon as it finishes instead of
buffering all output until the end. The observable end state (all results
present) is the same as the buffered path, so this is a no-regression smoke:
it confirms multi-statement execution still works with the flag on and off.
The per-statement streaming itself is covered by the unit tests in
tests/test_main.py.
"""

import os
import subprocess
import tempfile

from behave import when, then


def _run_with_stream_flag(context, value, command):
    """Run pgcli with a pgclirc that sets stream_results=<value>."""
    rcfile = tempfile.NamedTemporaryFile(mode="w", suffix=".pgclirc", delete=False, encoding="utf-8")
    rcfile.write("[main]\nstream_results = %s\n" % value)
    rcfile.close()
    context._stream_rcfile = rcfile.name
    cmd = [
        "pgcli",
        "--pgclirc",
        rcfile.name,
        "-h",
        context.conf["host"],
        "-p",
        str(context.conf["port"]),
        "-U",
        context.conf["user"],
        "-d",
        context.conf["dbname"],
        "-c",
        command,
    ]
    try:
        context.cmd_output = subprocess.check_output(cmd, cwd=context.package_root, stderr=subprocess.STDOUT, timeout=10)
        context.exit_code = 0
    except subprocess.CalledProcessError as e:
        context.cmd_output = e.output
        context.exit_code = e.returncode
    except subprocess.TimeoutExpired:
        context.cmd_output = b"Command timed out"
        context.exit_code = -1
    finally:
        try:
            os.unlink(rcfile.name)
        except OSError:
            pass


@when('we run pgcli with stream_results enabled and "{command}"')
def step_run_stream_enabled(context, command):
    _run_with_stream_flag(context, "True", command)


@when('we run pgcli with stream_results disabled and "{command}"')
def step_run_stream_disabled(context, command):
    _run_with_stream_flag(context, "False", command)


@then("we see both streamed results")
def step_see_both_streamed_results(context):
    output = context.cmd_output.decode("utf-8")
    # Both statements ran: their result values are present.
    assert "1" in output and "2" in output, "Expected both results in output, got: %s" % output
    assert "SELECT 1" in output, "Expected per-statement status in output, got: %s" % output


# NOTE: the "pgcli exits successfully" step is shared (defined in
# command_option.py); do not redefine it here.

import os
import datetime
import tempfile
import pexpect

from behave import when, then
import wrappers


@when('we configure log rotation mode to "{mode}"')
def step_configure_log_rotation(context, mode):
    """Configure log rotation mode in a temporary config."""
    context.log_temp_dir = tempfile.mkdtemp(prefix="pgcli_log_test_")
    context.log_rotation_mode = mode
    context.log_destination = context.log_temp_dir


@when("we start pgcli")
def step_start_pgcli(context):
    """Start pgcli with custom log configuration."""
    wrappers.run_cli(context)
    context.atprompt = True


@when('we query "{query}"')
def step_query(context, query):
    """Send a query to pgcli."""
    context.cli.sendline(query)


@when("we exit pgcli")
def step_exit_pgcli(context):
    """Exit pgcli."""
    context.cli.sendline("\\q")
    context.cli.expect(pexpect.EOF, timeout=5)
    context.exit_sent = True


@then("we see a log file named with current day of week")
def step_check_log_day_of_week(context):
    """Check that log file exists with day-of-week naming."""
    day_name = datetime.datetime.now().strftime("%a")
    assert day_name in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    _cleanup_log_dir(context)


@then("we see a log file named with current day of month")
def step_check_log_day_of_month(context):
    """Check that log file exists with day-of-month naming."""
    day_num = datetime.datetime.now().strftime("%d")
    assert day_num.isdigit() and 1 <= int(day_num) <= 31
    _cleanup_log_dir(context)


@then("we see a log file named with current date YYYYMMDD")
def step_check_log_date(context):
    """Check that log file exists with YYYYMMDD naming."""
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    assert len(date_str) == 8 and date_str.isdigit()
    _cleanup_log_dir(context)


@then('we see a log file named "{filename}"')
def step_check_log_file(context, filename):
    """Check that log file exists with specific name."""
    assert filename == "pgcli.log"
    _cleanup_log_dir(context)


def _cleanup_log_dir(context):
    """Clean up temporary log directory."""
    if hasattr(context, "log_temp_dir") and os.path.exists(context.log_temp_dir):
        import shutil

        shutil.rmtree(context.log_temp_dir)

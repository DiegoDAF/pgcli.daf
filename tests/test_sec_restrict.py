"""Security-focused tests for \\restrict / \\unrestrict restricted mode.

These tests exercise the CVE-2025-8714 mitigation implemented in
``pgcli/main.py`` (``PGCli.enter_restrict_mode`` / ``PGCli.exit_restrict_mode``
and the ``restrict_token`` attribute) and in ``pgcli/pgexecute.py``
(``PGExecute.run`` blocking of meta-commands while a token is set).

Threat model: a malicious dump file (or a hostile ``\\i`` include) tries to
smuggle meta-commands past the restricted-mode guard, or tries to exit
restricted mode without knowing the token that ``pg_dump`` emitted. All tests
are hermetic: they use a temp config file and mocked DB objects. No real
database connection, no access to the user's ~/.pgpass / ~/.config / keyring.
"""

import os
import tempfile
from unittest import mock

from pgcli.main import PGCli
from pgcli.pgexecute import PGExecute


def _make_cli(tmpdir):
    """Build a PGCli backed by a throwaway config file in ``tmpdir``.

    Mirrors the fixture style used by the existing restrict tests in
    tests/test_main.py so behavior stays consistent.
    """
    config_file = os.path.join(tmpdir, "config")
    log_file = os.path.join(tmpdir, "pgcli.log")
    with open(config_file, "w") as f:
        f.write("[main]\n")
        f.write(f"log_file = {log_file}\n")
    return PGCli(pgclirc_file=config_file)


def _make_executor():
    """Return a mock PGExecute whose real ``run`` method is bound to it.

    Same trick the existing tests use: we want the actual ``PGExecute.run``
    generator logic, but with a mocked connection so nothing touches a DB.
    """
    executor = mock.MagicMock(spec=PGExecute)
    executor.run = PGExecute.run.__get__(executor)
    executor.conn = mock.MagicMock()
    executor.reset_expanded = False
    return executor


def _statuses(results):
    """Extract non-empty status strings from run() result tuples."""
    return [r[3] for r in results if r[3]]


# ---------------------------------------------------------------------------
# Token lifecycle
# ---------------------------------------------------------------------------


def test_fresh_cli_is_not_restricted():
    """A newly constructed PGCli must start OUTSIDE restricted mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        assert cli.restrict_token is None


def test_enter_sets_token():
    """\\restrict <token> stores the token and reports silent success."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        result = cli.enter_restrict_mode("dump_secret_9f3a")
        assert cli.restrict_token == "dump_secret_9f3a"
        # Silent success: a single tuple with an all-None payload.
        assert result == [(None, None, None, None)]


def test_reentry_does_not_overwrite_token():
    """A second \\restrict must NOT clobber the active token.

    Security: if a hostile dump could re-issue \\restrict with a token it
    controls, it could then \\unrestrict itself. Re-entry is rejected and
    the original token is preserved.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        cli.enter_restrict_mode("original_token")
        result = cli.enter_restrict_mode("attacker_token")
        assert "Already in restricted mode" in result[0][3]
        assert cli.restrict_token == "original_token"


def test_unrestrict_requires_matching_token():
    """\\unrestrict with the wrong token must stay restricted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        cli.enter_restrict_mode("correct-token-123")

        result = cli.exit_restrict_mode("guessed-token")
        assert "Token mismatch" in result[0][3]
        # Still locked down.
        assert cli.restrict_token == "correct-token-123"

        # The right token clears it.
        result = cli.exit_restrict_mode("correct-token-123")
        assert result == [(None, None, None, None)]
        assert cli.restrict_token is None


def test_unrestrict_when_not_restricted_is_rejected():
    """\\unrestrict outside restricted mode is a no-op error, not a crash."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        result = cli.exit_restrict_mode("whatever")
        assert "Not in restricted mode" in result[0][3]
        assert cli.restrict_token is None


def test_enter_requires_token_argument():
    """\\restrict with no argument must not silently enter restricted mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        result = cli.enter_restrict_mode("   ")  # whitespace-only == empty
        assert "requires a token" in result[0][3]
        assert cli.restrict_token is None


def test_unrestrict_requires_token_argument():
    """\\unrestrict with no argument must not exit restricted mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        cli.enter_restrict_mode("locked")
        result = cli.exit_restrict_mode("")
        assert "requires a token" in result[0][3]
        assert cli.restrict_token == "locked"  # still restricted


def test_token_comparison_is_case_sensitive():
    """Token matching must be exact (case-sensitive) to resist guessing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        cli.enter_restrict_mode("AbCdEf")
        result = cli.exit_restrict_mode("abcdef")
        assert "Token mismatch" in result[0][3]
        assert cli.restrict_token == "AbCdEf"


def test_token_argument_is_trimmed_symmetrically():
    """Surrounding whitespace is stripped on both enter and exit.

    This documents the (safe) behavior that ``\\restrict  tok`` and
    ``\\unrestrict tok  `` refer to the same logical token, so a legitimate
    pg_dump/restore round-trip still works.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cli = _make_cli(tmpdir)
        cli.enter_restrict_mode("  padded_tok  ")
        assert cli.restrict_token == "padded_tok"
        result = cli.exit_restrict_mode("padded_tok")
        assert result == [(None, None, None, None)]
        assert cli.restrict_token is None


# ---------------------------------------------------------------------------
# Meta-command blocking in PGExecute.run (the actual enforcement point)
# ---------------------------------------------------------------------------


def test_dangerous_specials_blocked_while_restricted():
    """Meta-commands must be blocked BEFORE reaching pgspecial.execute.

    ``\\i`` (include a file) and ``\\!`` (shell out) are the highest-risk
    specials a malicious dump could inject, so they are asserted explicitly
    alongside the introspection commands.
    """
    pgspecial = mock.MagicMock()
    dangerous = [
        "\\i /tmp/evil.sql",
        "\\ir relative_evil.sql",
        "\\! rm -rf /",
        "\\o /tmp/exfiltrate.txt",
        "\\d",
        "\\l",
        "\\dt",
        "\\e",
        "\\copy foo to '/tmp/x'",
    ]
    for cmd in dangerous:
        executor = _make_executor()
        results = list(
            executor.run(cmd, pgspecial=pgspecial, restrict_token="tok")
        )
        statuses = _statuses(results)
        assert any("Restricted mode active" in s for s in statuses), (
            f"Expected {cmd!r} to be blocked in restricted mode"
        )
    # None of the blocked commands should have been dispatched to pgspecial.
    pgspecial.execute.assert_not_called()


def test_blocked_command_result_is_marked_unsuccessful():
    """A blocked meta-command must report success=False, not a fake success."""
    pgspecial = mock.MagicMock()
    executor = _make_executor()
    results = list(
        executor.run("\\i /tmp/evil.sql", pgspecial=pgspecial, restrict_token="tok")
    )
    # Tuple layout: (title, rows, headers, status, query, success, is_special)
    blocked = [r for r in results if r[3] and "Restricted mode active" in r[3]]
    assert blocked, "expected a blocked result tuple"
    success_flag = blocked[0][5]
    assert success_flag is False


def test_unrestrict_is_not_blocked():
    """\\unrestrict must reach pgspecial so the user can actually get out."""
    pgspecial = mock.MagicMock()
    pgspecial.execute.return_value = iter([(None, None, None, "OK")])
    executor = _make_executor()
    results = list(
        executor.run("\\unrestrict tok", pgspecial=pgspecial, restrict_token="tok")
    )
    statuses = _statuses(results)
    assert not any("Restricted mode active" in s for s in statuses)
    pgspecial.execute.assert_called_once()


def test_unrestrict_lookalikes_are_still_blocked():
    """Command names that merely resemble \\unrestrict must NOT slip through.

    The guard compares the FIRST whitespace-delimited token to the exact
    string "\\unrestrict". Anything else (a different command, a glued-on
    suffix, or a different case) must be blocked so an attacker cannot craft
    a lookalike that both bypasses the guard and does something dangerous.
    """
    pgspecial = mock.MagicMock()
    lookalikes = [
        "\\unrestrictx tok",   # glued suffix -> different first token
        "\\UNRESTRICT tok",    # different case
        "\\unrestrictdb",      # no space, different token
        "\\restrict newtok",   # trying to re-arm with attacker token
    ]
    for cmd in lookalikes:
        executor = _make_executor()
        results = list(
            executor.run(cmd, pgspecial=pgspecial, restrict_token="tok")
        )
        statuses = _statuses(results)
        assert any("Restricted mode active" in s for s in statuses), (
            f"Expected lookalike {cmd!r} to be blocked"
        )
    pgspecial.execute.assert_not_called()


def test_no_blocking_when_token_absent():
    """With restrict_token=None, meta-commands flow normally to pgspecial.

    Guards against a regression where the block would fire unconditionally
    and break ordinary usage.
    """
    pgspecial = mock.MagicMock()
    pgspecial.execute.return_value = iter([(None, None, None, "listing")])
    executor = _make_executor()
    results = list(executor.run("\\dt", pgspecial=pgspecial, restrict_token=None))
    statuses = _statuses(results)
    assert not any("Restricted mode active" in s for s in statuses)
    pgspecial.execute.assert_called_once()


def test_regular_sql_not_blocked_while_restricted():
    """Non-meta SQL must still execute while restricted (dumps are SQL)."""
    from pgspecial.main import CommandNotFound

    pgspecial = mock.MagicMock()
    pgspecial.execute.side_effect = CommandNotFound("not special")

    executor = _make_executor()
    executor.execute_normal_sql = mock.MagicMock(
        return_value=("title", [("1",)], ["?column?"], "SELECT 1")
    )
    results = list(
        executor.run("SELECT 1", pgspecial=pgspecial, restrict_token="tok")
    )
    statuses = _statuses(results)
    assert not any("Restricted mode active" in s for s in statuses)
    executor.execute_normal_sql.assert_called_once()


def test_multiple_meta_commands_all_blocked_in_one_statement():
    """A batch mixing several specials must have every special blocked.

    A malicious dump chunk could pack many meta-commands into one submission;
    each backslash statement must be independently rejected.
    """
    pgspecial = mock.MagicMock()
    executor = _make_executor()
    batch = "\\d ;\n\\i /tmp/evil.sql ;\n\\! id"
    results = list(
        executor.run(batch, pgspecial=pgspecial, restrict_token="tok")
    )
    blocked = [s for s in _statuses(results) if "Restricted mode active" in s]
    assert len(blocked) == 3, f"expected 3 blocked specials, got {blocked}"
    pgspecial.execute.assert_not_called()

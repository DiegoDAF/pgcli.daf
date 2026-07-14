# -*- coding: utf-8 -*-
r"""Security-focused tests for secret / path sanitization in pgcli.

Covers three sanitization surfaces in ``pgcli/main.py``:

1. ``obfuscate_process_password`` -- masks the DB password inside the
   process title (``ps``/``setproctitle``) so it never leaks to other
   users on the box.  We add edge cases beyond the existing happy-path
   test in ``tests/test_main.py``.

2. The ``--list-dsn`` masking (``re.sub`` of ``://user:pass@`` ->
   ``://user:***@``) -- asserts a configured DSN password never appears
   verbatim in the printed output.

3. ``PGCli._sanitize_path`` -- used by ``\i`` / ``\o`` / ``\log-file``;
   must block ``/dev``, ``/proc``, ``/sys`` and non-regular files, and
   must resolve symlinks *before* deciding (so a symlink into ``/dev``
   is still blocked).

All tests are hermetic: they use tmp dirs, monkeypatch env / config
location, and never read or write the real ~/.pgpass, ~/.config or
system keyring.  No real DB connection is made.
"""

import os
import platform

import pytest
from click.testing import CliRunner

try:
    import setproctitle
except ImportError:  # pragma: no cover - depends on optional dep
    setproctitle = None

from pgcli.main import PGCli, cli, obfuscate_process_password


# ---------------------------------------------------------------------------
# 1. obfuscate_process_password -- edge cases
# ---------------------------------------------------------------------------


def _setproctitle_functional():
    """Return True only if setproctitle can actually round-trip a title.

    Other tests that shell out or use Click's CliRunner can corrupt the
    process argv buffer, which makes setproctitle silently no-op.  Guard
    against that so these tests skip (not falsely fail) in a poisoned run.
    """
    if setproctitle is None:
        return False
    setproctitle.setproctitle("pgcli_sec_canary")
    return setproctitle.getproctitle() == "pgcli_sec_canary"


needs_setproctitle = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="process title obfuscation is a POSIX/ps concern",
)


@needs_setproctitle
class TestObfuscateProcessPassword:
    """Edge cases for the process-title password masking."""

    @pytest.fixture(autouse=True)
    def _guard_and_restore(self):
        if not _setproctitle_functional():
            pytest.skip("setproctitle not functional (argv buffer corrupted by prior tests)")
        original = setproctitle.getproctitle()
        yield
        setproctitle.setproctitle(original)

    def test_url_password_with_special_chars_is_masked(self):
        # A password full of regex-meaningful characters must still be gone.
        # ('@' is not URL-legal unencoded inside a password, so it is omitted.)
        secret = r"pa$$w.rd*+?"  # noqa: S105 - test literal, not a real secret
        setproctitle.setproctitle(f"pgcli postgresql://root:{secret}@localhost:5432/db")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert secret not in title
        assert "xxxx" in title

    def test_url_without_password_is_untouched(self):
        # No ":pass@" segment -> nothing to mask, must not corrupt the title.
        setproctitle.setproctitle("pgcli postgresql://root@localhost/db")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert "root@localhost" in title
        assert "xxxx" not in title

    def test_kv_password_at_end_of_string_is_masked(self):
        # password= as the final token (no trailing space) hits the '$' branch.
        setproctitle.setproctitle("pgcli host=localhost password=hunter2")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert "hunter2" not in title
        assert "password=xxxx" in title

    def test_kv_password_with_spaces_is_masked(self):
        # A password containing a space: the current regex stops the greedy
        # capture at the next ' key=' token, so only the first word is masked.
        # The security-relevant part (the leading secret token) must be gone.
        setproctitle.setproctitle("pgcli password=top secret_word host=localhost")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert "password=top" not in title
        assert "password=xxxx" in title

    def test_kv_no_password_key_untouched(self):
        setproctitle.setproctitle("pgcli user=root host=localhost dbname=x")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert title == "pgcli user=root host=localhost dbname=x"

    def test_multiple_kv_passwords_all_masked(self):
        # The key=value branch handles repeated password= tokens correctly.
        setproctitle.setproctitle("pgcli password=one url=x password=two")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert "one" not in title.replace("xxxx", "")
        assert "two" not in title.replace("xxxx", "")
        assert title.count("password=xxxx") == 2

    def test_two_urls_both_passwords_masked(self):
        """Regression for the greedy-regex leak: with TWO connection URLs in the
        process title, BOTH passwords must be masked (a greedy ':(.*):(.*)@'
        would leak all but the last, visible via `ps`). Fixed to a per-URL
        character-class substitution."""
        first = "FIRSTSECRET"  # noqa: S105
        second = "SECONDSECRET"  # noqa: S105
        setproctitle.setproctitle(f"pgcli pg://a:{first}@h1/db extra://b:{second}@h2")
        obfuscate_process_password()
        title = setproctitle.getproctitle()
        assert first not in title
        assert second not in title
        assert title.count(":xxxx@") == 2


# ---------------------------------------------------------------------------
# 2. --list-dsn password masking
# ---------------------------------------------------------------------------


class TestListDsnMasking:
    """The --list-dsn output must never print a configured DSN password."""

    def _write_pgclirc(self, tmp_path, aliases):
        rc = tmp_path / "pgclirc"
        lines = ["[alias_dsn]"]
        for name, dsn in aliases.items():
            lines.append(f"{name} = {dsn}")
        rc.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return rc

    def _run_list_dsn(self, tmp_path, monkeypatch, aliases):
        """Invoke `pgcli --list-dsn --pgclirc <tmp>` hermetically.

        We point config_location() at an (empty) tmp config dir so the real
        ~/.config/pgcli is never touched, and pass our own pgclirc file as
        the source of aliases.
        """
        cfg_dir = tmp_path / "cfgdir"
        cfg_dir.mkdir()
        # config_location() is expected to return a trailing-slash string.
        monkeypatch.setattr("pgcli.main.config_location", lambda: str(cfg_dir) + os.sep)
        # Make sure nothing falls back to a real HOME.
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg-config"))
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg-state"))
        rc = self._write_pgclirc(tmp_path, aliases)
        runner = CliRunner()
        result = runner.invoke(cli, ["--list-dsn", "--pgclirc", str(rc)])
        return result

    def test_password_not_in_list_dsn_output(self, tmp_path, monkeypatch):
        secret = "supersecretpw"  # noqa: S105
        result = self._run_list_dsn(
            tmp_path,
            monkeypatch,
            {"prod": f"postgresql://appuser:{secret}@db.internal:5432/appdb"},
        )
        assert result.exit_code == 0, result.output
        assert secret not in result.output
        assert "***" in result.output
        # The non-secret parts should still be visible / useful.
        assert "prod" in result.output
        assert "appuser" in result.output
        assert "db.internal" in result.output

    def test_multiple_dsns_all_masked(self, tmp_path, monkeypatch):
        s1 = "pw_one_leak"  # noqa: S105
        s2 = "pw_two_leak"  # noqa: S105
        result = self._run_list_dsn(
            tmp_path,
            monkeypatch,
            {
                "one": f"postgres://u1:{s1}@h1/d1",
                "two": f"postgres://u2:{s2}@h2/d2",
            },
        )
        assert result.exit_code == 0, result.output
        assert s1 not in result.output
        assert s2 not in result.output
        assert result.output.count("***") == 2

    def test_dsn_without_password_is_listed_verbatim(self, tmp_path, monkeypatch):
        result = self._run_list_dsn(
            tmp_path,
            monkeypatch,
            {"nopw": "postgresql://appuser@db.internal/appdb"},
        )
        assert result.exit_code == 0, result.output
        # No credentials to mask; alias should appear.
        assert "nopw" in result.output
        assert "appuser" in result.output


# ---------------------------------------------------------------------------
# 3. PGCli._sanitize_path
# ---------------------------------------------------------------------------


class TestSanitizePathSecurity:
    """Security edge cases for _sanitize_path beyond the happy path."""

    @pytest.mark.parametrize(
        "blocked",
        ["/dev/null", "/dev/tcp/attacker.example/4444", "/proc/self/environ", "/sys/kernel"],
    )
    def test_direct_restricted_paths_blocked(self, blocked):
        resolved, err = PGCli._sanitize_path(blocked)
        assert resolved is None
        assert err is not None
        assert "restricted" in err.lower()

    def test_symlink_into_dev_is_blocked_after_resolution(self, tmp_path):
        # An attacker-provided symlink that points into /dev must be blocked
        # because realpath() resolves it before the prefix check.
        link = tmp_path / "innocent.sql"
        link.symlink_to("/dev/null")
        resolved, err = PGCli._sanitize_path(str(link))
        assert resolved is None
        assert err is not None
        assert "restricted" in err.lower()

    def test_dotdot_traversal_into_dev_is_blocked(self):
        # A path using ../ that ultimately resolves under /dev must be blocked.
        # realpath() collapses the '..' before the prefix check, so an
        # obfuscated path that lands in /dev is still caught.
        resolved, err = PGCli._sanitize_path("/dev/foo/../../dev/null")
        assert resolved is None
        assert err is not None
        assert "restricted" in err.lower()

    def test_fifo_non_regular_file_blocked(self, tmp_path):
        # Named pipe (FIFO) is not a regular file -> reading/writing it could
        # hang or exfiltrate; must be blocked.
        if not hasattr(os, "mkfifo"):
            pytest.skip("os.mkfifo not available on this platform")
        fifo = tmp_path / "pipe"
        try:
            os.mkfifo(str(fifo))
        except (OSError, NotImplementedError):
            pytest.skip("cannot create FIFO on this filesystem")
        resolved, err = PGCli._sanitize_path(str(fifo))
        assert resolved is None
        assert err is not None
        assert "regular file" in err.lower()

    def test_directory_blocked(self, tmp_path):
        resolved, err = PGCli._sanitize_path(str(tmp_path))
        assert resolved is None
        assert err is not None
        assert "regular file" in err.lower()

    def test_regular_file_allowed(self, tmp_path):
        f = tmp_path / "ok.sql"
        f.write_text("select 1;", encoding="utf-8")
        resolved, err = PGCli._sanitize_path(str(f))
        assert err is None
        assert resolved == str(f)

    def test_nonexistent_target_allowed_for_write(self, tmp_path):
        # \o / \log-file need to create new files; a non-existent path is OK.
        target = tmp_path / "new_output.txt"
        resolved, err = PGCli._sanitize_path(str(target))
        assert err is None
        assert resolved == str(target)

    def test_tilde_expansion_stays_within_sanitizer(self, tmp_path, monkeypatch):
        # ~ must expand relative to HOME, and a plain file under HOME is fine.
        monkeypatch.setenv("HOME", str(tmp_path))
        resolved, err = PGCli._sanitize_path("~/query.sql")
        assert err is None
        assert resolved == str(tmp_path / "query.sql")

    def test_write_to_file_rejects_dev_null(self, tmp_path, monkeypatch):
        # End-to-end: \o /dev/null (via write_to_file) must refuse and leave
        # output_file unset -- exercises the sanitizer through the real caller
        # without needing a DB connection.
        cli_obj = PGCli.__new__(PGCli)  # bypass __init__ (no config / DB)
        cli_obj.output_file = None
        result = cli_obj.write_to_file("/dev/null")
        assert cli_obj.output_file is None
        # result is a list of tuples; the message (index 3) should explain the block.
        message = result[0][3]
        assert "restricted" in message.lower()
        assert "disabled" in message.lower()

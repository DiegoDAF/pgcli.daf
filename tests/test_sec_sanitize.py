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

from utils import posix_only
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
    @posix_only
    def test_direct_restricted_paths_blocked(self, blocked):
        resolved, err = PGCli._sanitize_path(blocked)
        assert resolved is None
        assert err is not None
        assert "restricted" in err.lower()

    @posix_only
    def test_symlink_into_dev_is_blocked_after_resolution(self, tmp_path):
        # An attacker-provided symlink that points into /dev must be blocked
        # because realpath() resolves it before the prefix check.
        link = tmp_path / "innocent.sql"
        link.symlink_to("/dev/null")
        resolved, err = PGCli._sanitize_path(str(link))
        assert resolved is None
        assert err is not None
        assert "restricted" in err.lower()

    @posix_only
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
        monkeypatch.setenv("USERPROFILE", str(tmp_path))  # what Windows reads
        resolved, err = PGCli._sanitize_path("~/query.sql")
        assert err is None
        assert resolved == str(tmp_path / "query.sql")

    @posix_only
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


@pytest.mark.parametrize(
    "dsn, shown",
    [
        ("postgresql://appuser:s3cret@db.internal/appdb", "postgresql://appuser:***@db.internal/appdb"),
        ("host=db.internal dbname=appdb user=appuser password=s3cret", "host=db.internal dbname=appdb user=appuser password=***"),
        ("host=db.internal password='s3 cret' dbname=appdb", "host=db.internal password=*** dbname=appdb"),
        (
            "postgresql://appuser@db.internal/appdb?password=s3cret&sslmode=require",
            "postgresql://appuser@db.internal/appdb?password=***&sslmode=require",
        ),
        (
            "postgresql://appuser@db.internal/appdb?sslmode=require&password=s3cret",
            "postgresql://appuser@db.internal/appdb?sslmode=require&password=***",
        ),
        ("postgresql://appuser@db.internal/appdb", "postgresql://appuser@db.internal/appdb"),
        # sslpassword is the client key's passphrase: a secret too.
        ("host=db.internal sslkey=/k sslpassword=keypass dbname=appdb", "host=db.internal sslkey=/k sslpassword=*** dbname=appdb"),
        ("postgresql://u@db.internal/x?sslpassword=keypass", "postgresql://u@db.internal/x?sslpassword=***"),
        # An unquoted key=value password ends at a blank, not at "&".
        ("host=db.internal password=ab&cd dbname=appdb", "host=db.internal password=*** dbname=appdb"),
        # libpq's \' escape inside a quoted value.
        ("host=x password='it\\'s' dbname=y", "host=x password=*** dbname=y"),
        # "@" in the query string is not a userinfo separator: libpq ends the
        # userinfo at the first "/" or "@".
        (
            "postgresql://db.internal:5432/appdb?application_name=diego@laptop",
            "postgresql://db.internal:5432/appdb?application_name=diego@laptop",
        ),
    ],
)
def test_redact_dsn_knows_every_spelling(dsn, shown):
    # --list-dsn only masked ://user:pass@; a key=value alias or a URL with
    # ?password= printed the secret.
    from pgcli.redact import redact_dsn

    assert redact_dsn(dsn) == shown


def test_list_dsn_masks_key_value_and_query_string_passwords(tmp_path, monkeypatch):
    from click.testing import CliRunner
    from pgcli.main import cli

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg-config"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg-state"))
    rc = tmp_path / "pgclirc"
    rc.write_text(
        "[alias_dsn]\nkv = host=db.internal dbname=appdb user=appuser password=s3cret\n"
        "urlq = postgresql://appuser@db.internal/appdb?password=s3cret\n",
        encoding="utf-8",
    )
    result = CliRunner().invoke(cli, ["--list-dsn", "--pgclirc", str(rc)])
    assert result.exit_code == 0, result.output
    assert "s3cret" not in result.output
    assert result.output.count("***") == 2


@pytest.mark.parametrize(
    "title, shown",
    [
        ("pgcli postgresql://u@h/db?password=s3cret&sslmode=require", "pgcli postgresql://u@h/db?password=xxxx&sslmode=require"),
        ("pgcli -d postgresql://u@h/db?sslmode=require&password=s3cret", "pgcli -d postgresql://u@h/db?sslmode=require&password=xxxx"),
        ("pgcli postgresql://u:s3cret@h/db", "pgcli postgresql://u:xxxx@h/db"),
        ("pgcli host=h password=s3cret dbname=x", "pgcli host=h password=xxxx dbname=x"),
        ("pgcli postgresql://u:a@h/db postgresql://v:b@h/db", "pgcli postgresql://u:xxxx@h/db postgresql://v:xxxx@h/db"),
        # A key=value DSN next to this fork's own --ssh-tunnel URL: both forms
        # on one command line, so neither may be skipped.
        (
            "pgcli host=h password=s3cret dbname=x --ssh-tunnel ssh://me@bastion",
            "pgcli host=h password=xxxx dbname=x --ssh-tunnel ssh://me@bastion",
        ),
        ("pgcli user=root password=top secret host=localhost", "pgcli user=root password=xxxx host=localhost"),
    ],
)
def test_redact_process_title(title, shown):
    # ps shows the command line to everyone on the machine. A password given
    # as ?password= in a URL's query string used to stay there.
    from pgcli.redact import redact_process_title

    assert redact_process_title(title) == shown


@pytest.mark.parametrize(
    "sql, shown",
    [
        ("alter role bob password 's3cret'", "alter role bob password '***'"),
        ("ALTER ROLE bob PASSWORD E'a\\'b'", "ALTER ROLE bob PASSWORD '***'"),
        ("alter role bob password $$s3cret$$", "alter role bob password '***'"),
        ("alter role bob password $q$s3c$ret$q$", "alter role bob password '***'"),
        ("alter role bob password 'it''s'", "alter role bob password '***'"),
        ("create user mapping for u server s options (password 'pw')", "create user mapping for u server s options (password '***')"),
        ("alter role bob password null", "alter role bob password null"),
    ],
)
def test_redact_sql_hides_every_quoting_of_the_literal(sql, shown):
    from pgcli.redact import redact_sql

    assert redact_sql(sql) == shown


@pytest.mark.parametrize(
    "dsn, shown",
    [
        # libpq accepts an empty user.
        ("postgresql://:s3cret@db.internal/appdb", "postgresql://:***@db.internal/appdb"),
        # The third keyword libpq marks as a secret (PostgreSQL 18).
        ("host=db.internal oauth_client_secret=s3cret dbname=appdb", "host=db.internal oauth_client_secret=*** dbname=appdb"),
        ("postgresql://u@db.internal/appdb?oauth_client_secret=s3cret", "postgresql://u@db.internal/appdb?oauth_client_secret=***"),
        # getopt's attached form keeps the "d" glued to the keyword.
        ("pg_dump -dpassword=s3cret host=db.internal", "pg_dump -dpassword=*** host=db.internal"),
        # Two secrets in one string; IPv6 with userinfo; an empty quoted value.
        ("host=db.internal password=s3cret sslpassword=keypass", "host=db.internal password=*** sslpassword=***"),
        ("postgresql://u:s3cret@[::1]:5432/db", "postgresql://u:***@[::1]:5432/db"),
        ("host=db.internal password='' dbname=appdb", "host=db.internal password=*** dbname=appdb"),
        # An unquoted value inside a SQL literal keeps the literal's closing quote.
        ("'host=h password=s3cret'", "'host=h password=***'"),
    ],
)
def test_redact_dsn_more_spellings(dsn, shown):
    from pgcli.redact import redact_dsn

    assert redact_dsn(dsn) == shown


@pytest.mark.parametrize(
    "title, shown",
    [
        ("pgcli host=h sslpassword=keypass dbname=x", "pgcli host=h sslpassword=xxxx dbname=x"),
        ("pgcli host=h password = s3cret dbname=x", "pgcli host=h password = xxxx dbname=x"),
        ("pgcli host=h oauth_client_secret=s3cret -c select", "pgcli host=h oauth_client_secret=xxxx -c select"),
        ("pgcli postgresql://:s3cret@h/db", "pgcli postgresql://:xxxx@h/db"),
        ("pgcli --ssh-tunnel ssh://me:s3cret@bastion host=h dbname=x", "pgcli --ssh-tunnel ssh://me:xxxx@bastion host=h dbname=x"),
        # The arguments after a masked password stay in the title.
        (
            "pgcli host=h password=s3cret --ssh-tunnel ssh://me@bastion -c select",
            "pgcli host=h password=xxxx --ssh-tunnel ssh://me@bastion -c select",
        ),
        ("pgcli 'host=h password=s p' -c x", "pgcli 'host=h password=xxxx -c x"),
    ],
)
def test_redact_process_title_more_spellings(title, shown):
    from pgcli.redact import redact_process_title

    assert redact_process_title(title) == shown


@pytest.mark.parametrize(
    "sql, shown",
    [
        ("alter role bob password's3cret'", "alter role bob password'***'"),
        ("alter role bob password /* c */ 's3cret'", "alter role bob password /* c */ '***'"),
        ("alter role bob password -- c\n's3cret'", "alter role bob password -- c\n'***'"),
        ("alter role bob password U&'s3cret'", "alter role bob password '***'"),
        ("alter role bob password 's3cret1'\n's3cret2'", "alter role bob password '***'"),
        ("alter role bob password $é$s3cret$é$", "alter role bob password '***'"),
        ("alter server s options (set password 's3cret')", "alter server s options (set password '***')"),
        ("alter server s options (add password 's3cret')", "alter server s options (add password '***')"),
        # Connection strings inside SQL carry passwords too.
        (
            "create subscription s connection 'host=h dbname=d password=s3cret' publication p",
            "create subscription s connection 'host=h dbname=d password=***' publication p",
        ),
        (
            "select * from dblink('host=h password=s3cret dbname=d', 'select 1') as t(a int)",
            "select * from dblink('host=h password=*** dbname=d', 'select 1') as t(a int)",
        ),
        (
            "create subscription s connection 'postgresql://u:s3cret@h/d' publication p",
            "create subscription s connection 'postgresql://u:***@h/d' publication p",
        ),
        # Not a password literal.
        ("set password_encryption = 'scram-sha-256'", "set password_encryption = 'scram-sha-256'"),
        ("select password_hash from t", "select password_hash from t"),
    ],
)
def test_redact_sql_more_spellings(sql, shown):
    from pgcli.redact import redact_sql

    assert redact_sql(sql) == shown


def test_redact_text_hides_the_statement_echoed_by_a_server_error():
    # The server's message carries LINE 1: <statement>; an error line that
    # masked its "sql:" part handed the secret back in its "error:" part.
    from pgcli.redact import redact_text

    message = 'unrecognized role option "loginn"\nLINE 1: alter role bob password \'s3cret\' loginn\n        ^'
    assert "s3cret" not in redact_text(message)
    assert "password '***'" in redact_text(message)
    assert "s3cret" not in redact_text(Exception(message))


def test_the_secret_keywords_come_from_libpq():
    from pgcli.redact import SECRET_KEYWORDS

    assert {"password", "sslpassword", "oauth_client_secret"} <= set(SECRET_KEYWORDS)

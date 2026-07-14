"""Security-focused tests for the ``.pgpass`` reader (pgcli/pgpass.py).

These assertions guard the security-sensitive behavior of the parser:

- refuses to read a file that is group/world accessible (libpq's 0600 rule),
- resolves backslash escapes correctly so a crafted line cannot smuggle an
  unintended field boundary,
- treats ``*`` wildcards for host/port/db/user as intended,
- recovers a password that itself contains a colon,
- ignores a missing file or a non-regular file (e.g. a directory), and
- honors the ``PGPASSFILE`` env override through ``_pgpass_path``.

Every test is hermetic: it uses pytest's ``tmp_path`` / ``monkeypatch`` and a
throwaway file. The real ``~/.pgpass`` is never read or written.
"""

import os
import stat

import pytest

from pgcli.pgpass import (
    _has_safe_permissions,
    _pgpass_path,
    _split_pgpass_line,
    lookup_password,
)


def _write_pgpass(tmp_path, contents, mode=0o600, name="pgpass"):
    p = tmp_path / name
    p.write_text(contents)
    os.chmod(p, mode)
    return str(p)


# --------------------------------------------------------------------------- #
# Permission handling (the core security control)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "mode",
    [
        0o640,  # group read
        0o604,  # other read
        0o660,  # group read+write
        0o606,  # other read+write
        0o644,  # typical too-open default
        0o777,  # world writable/exec
        0o601,  # only other-exec set
    ],
)
def test_rejects_group_or_world_accessible_file(tmp_path, mode):
    """Any group/other permission bit must make the reader return None."""
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n", mode=mode)
    assert lookup_password("h", 5432, "db", "u", path=path) is None


@pytest.mark.parametrize("mode", [0o600, 0o400, 0o700, 0o500])
def test_accepts_owner_only_permissions(tmp_path, mode):
    """Owner-only permission sets (no g/o bits) are accepted."""
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n", mode=mode)
    assert lookup_password("h", 5432, "db", "u", path=path) == "secret"


def test_tightening_permissions_re_enables_read(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n", mode=0o644)
    assert lookup_password("h", 5432, "db", "u", path=path) is None
    os.chmod(path, 0o600)
    assert lookup_password("h", 5432, "db", "u", path=path) == "secret"


def test_has_safe_permissions_predicate(tmp_path):
    safe = _write_pgpass(tmp_path, "x\n", mode=0o600, name="safe")
    unsafe = _write_pgpass(tmp_path, "x\n", mode=0o640, name="unsafe")
    assert _has_safe_permissions(safe) is True
    assert _has_safe_permissions(unsafe) is False


# --------------------------------------------------------------------------- #
# Missing / non-regular files
# --------------------------------------------------------------------------- #


def test_missing_file_returns_none(tmp_path):
    assert lookup_password("h", 5432, "db", "u", path=str(tmp_path / "does_not_exist")) is None


def test_directory_is_not_a_regular_file(tmp_path):
    """A directory (even with 0700) must be rejected: not a regular file."""
    d = tmp_path / "adir"
    d.mkdir()
    os.chmod(d, 0o700)
    assert _has_safe_permissions(str(d)) is False
    assert lookup_password("h", 5432, "db", "u", path=str(d)) is None


def test_fifo_is_not_a_regular_file(tmp_path):
    """A named pipe is not a regular file and must be rejected."""
    fifo = tmp_path / "afifo"
    os.mkfifo(fifo, 0o600)
    assert stat.S_ISFIFO(os.stat(fifo).st_mode)
    assert _has_safe_permissions(str(fifo)) is False
    assert lookup_password("h", 5432, "db", "u", path=str(fifo)) is None


# --------------------------------------------------------------------------- #
# Backslash escaping (prevents field-boundary smuggling)
# --------------------------------------------------------------------------- #


def test_escaped_colon_is_literal_colon(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:pa\\:ss\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pa:ss"


def test_escaped_backslash_is_literal_backslash(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:pa\\\\ss\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pa\\ss"


def test_escaped_colon_in_host_field(tmp_path):
    """A host containing an escaped colon must be matched as one field, so the
    line does not silently shift the password into a different column."""
    path = _write_pgpass(tmp_path, "ho\\:st:5432:db:u:secret\n")
    assert lookup_password("ho:st", 5432, "db", "u", path=path) == "secret"
    # And a plain "ho" host must NOT match it.
    assert lookup_password("ho", 5432, "db", "u", path=path) is None


def test_split_line_resolves_escapes():
    assert _split_pgpass_line("h:5432:db:u:pa\\:ss") == [
        "h",
        "5432",
        "db",
        "u",
        "pa:ss",
    ]
    assert _split_pgpass_line("h:5432:db:u:pa\\\\ss") == [
        "h",
        "5432",
        "db",
        "u",
        "pa\\ss",
    ]
    # Unescaped colon really is a separator.
    assert _split_pgpass_line("h:5432:db:u:pa:ss") == [
        "h",
        "5432",
        "db",
        "u",
        "pa",
        "ss",
    ]


def test_trailing_backslash_is_literal(tmp_path):
    """A trailing lone backslash (no char to escape) is kept verbatim."""
    assert _split_pgpass_line("h:5432:db:u:pass\\") == [
        "h",
        "5432",
        "db",
        "u",
        "pass\\",
    ]
    path = _write_pgpass(tmp_path, "h:5432:db:u:pass\\\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pass\\"


# --------------------------------------------------------------------------- #
# Password containing colons (unescaped -> recovery path)
# --------------------------------------------------------------------------- #


def test_password_with_unescaped_colons_recovered(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:pa:ss:word\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pa:ss:word"


def test_password_single_escaped_colon_no_recovery(tmp_path):
    """Exactly 5 fields (colon escaped) uses parts[4] directly, not rejoin."""
    path = _write_pgpass(tmp_path, "h:5432:db:u:pa\\:ss\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pa:ss"


# --------------------------------------------------------------------------- #
# Wildcard matching
# --------------------------------------------------------------------------- #


def test_wildcard_host(tmp_path):
    path = _write_pgpass(tmp_path, "*:5432:db:u:secret\n")
    assert lookup_password("anyhost", 5432, "db", "u", path=path) == "secret"


def test_wildcard_port(tmp_path):
    path = _write_pgpass(tmp_path, "h:*:db:u:secret\n")
    assert lookup_password("h", 65535, "db", "u", path=path) == "secret"


def test_wildcard_database(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:*:u:secret\n")
    assert lookup_password("h", 5432, "anydb", "u", path=path) == "secret"


def test_wildcard_user(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:*:secret\n")
    assert lookup_password("h", 5432, "db", "anyuser", path=path) == "secret"


def test_all_wildcards(tmp_path):
    path = _write_pgpass(tmp_path, "*:*:*:*:secret\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "secret"


def test_wildcard_is_not_regex_or_prefix(tmp_path):
    """``*`` is only the whole-field wildcard, not a partial/glob match."""
    path = _write_pgpass(tmp_path, "prod*:5432:db:u:secret\n")
    # A literal "prod*" host field only matches an equally literal value.
    assert lookup_password("production", 5432, "db", "u", path=path) is None
    assert lookup_password("prod*", 5432, "db", "u", path=path) == "secret"


# --------------------------------------------------------------------------- #
# PGPASSFILE env override via _pgpass_path
# --------------------------------------------------------------------------- #


def test_pgpassfile_env_override_used(tmp_path, monkeypatch):
    path = _write_pgpass(tmp_path, "h:5432:db:u:fromenv\n")
    monkeypatch.setenv("PGPASSFILE", path)
    # path=None -> falls back to _pgpass_path() which reads PGPASSFILE.
    assert lookup_password("h", 5432, "db", "u") == "fromenv"


def test_pgpass_path_prefers_env(tmp_path, monkeypatch):
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "custom.pgpass"))
    assert _pgpass_path() == str(tmp_path / "custom.pgpass")


def test_pgpass_path_expands_tilde_in_env(monkeypatch):
    monkeypatch.setenv("PGPASSFILE", "~/somewhere/pgpass")
    result = _pgpass_path()
    assert "~" not in result
    assert result.endswith("/somewhere/pgpass")
    assert os.path.isabs(result)


def test_pgpass_path_default_without_env(monkeypatch):
    monkeypatch.delenv("PGPASSFILE", raising=False)
    assert _pgpass_path() == os.path.expanduser("~/.pgpass")


def test_env_override_also_enforces_permissions(tmp_path, monkeypatch):
    """A PGPASSFILE pointed at a too-open file is still rejected."""
    path = _write_pgpass(tmp_path, "h:5432:db:u:fromenv\n", mode=0o644)
    monkeypatch.setenv("PGPASSFILE", path)
    assert lookup_password("h", 5432, "db", "u") is None

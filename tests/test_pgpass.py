"""Tests for the ``.pgpass`` parser (pgcli/pgpass.py).

These never touch the real ``~/.pgpass``: they use pytest's ``tmp_path`` and a
throwaway file passed explicitly or via the ``PGPASSFILE`` env var.
"""

import os

import pytest

from pgcli.pgpass import lookup_password


def _write_pgpass(tmp_path, contents, mode=0o600):
    p = tmp_path / "pgpass"
    p.write_text(contents)
    os.chmod(p, mode)
    return str(p)


def test_exact_match(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "secret"


def test_wildcard_port(tmp_path):
    path = _write_pgpass(tmp_path, "h:*:db:u:secret\n")
    assert lookup_password("h", 54321, "db", "u", path=path) == "secret"


def test_wildcard_host(tmp_path):
    path = _write_pgpass(tmp_path, "*:5432:db:u:secret\n")
    assert lookup_password("anyhost", 5432, "db", "u", path=path) == "secret"


def test_no_match_returns_none(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n")
    assert lookup_password("h", 5432, "other_db", "u", path=path) is None
    assert lookup_password("h", 5432, "db", "other_user", path=path) is None


def test_first_match_wins(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:first\nh:5432:db:u:second\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "first"


def test_comment_and_blank_lines_skipped(tmp_path):
    contents = "# a comment\n\n  # indented comment\nh:5432:db:u:secret\n"
    path = _write_pgpass(tmp_path, contents)
    assert lookup_password("h", 5432, "db", "u", path=path) == "secret"


def test_escaping_colon_in_password(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:pa\\:ss\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pa:ss"


def test_escaping_backslash_in_password(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:pa\\\\ss\n")
    assert lookup_password("h", 5432, "db", "u", path=path) == "pa\\ss"


def test_pgpassfile_env_override(tmp_path, monkeypatch):
    path = _write_pgpass(tmp_path, "h:5432:db:u:fromenv\n")
    monkeypatch.setenv("PGPASSFILE", path)
    # path=None -> must fall back to PGPASSFILE
    assert lookup_password("h", 5432, "db", "u") == "fromenv"


def test_unsafe_permissions_ignored(tmp_path):
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n", mode=0o644)
    assert lookup_password("h", 5432, "db", "u", path=path) is None
    # Tightening the permissions makes it match again.
    os.chmod(path, 0o600)
    assert lookup_password("h", 5432, "db", "u", path=path) == "secret"


def test_missing_file_returns_none(tmp_path):
    assert lookup_password("h", 5432, "db", "u", path=str(tmp_path / "nope")) is None


def test_port_compared_as_string(tmp_path):
    # int port arg and string port field must match.
    path = _write_pgpass(tmp_path, "h:5432:db:u:secret\n")
    assert lookup_password("h", "5432", "db", "u", path=path) == "secret"


def test_default_host_and_port(tmp_path):
    path = _write_pgpass(tmp_path, "localhost:5432:db:u:secret\n")
    # host="" -> localhost, port=None -> 5432
    assert lookup_password("", None, "db", "u", path=path) == "secret"

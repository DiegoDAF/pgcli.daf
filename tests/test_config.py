import io
import os
import stat

import pytest

from pgcli.config import ensure_dir_exists, skip_initial_comment


def test_ensure_file_parent(tmpdir):
    subdir = tmpdir.join("subdir")
    rcfile = subdir.join("rcfile")
    ensure_dir_exists(str(rcfile))


def test_ensure_existing_dir(tmpdir):
    rcfile = str(tmpdir.mkdir("subdir").join("rcfile"))

    # should just not raise
    ensure_dir_exists(rcfile)


def test_ensure_other_create_error(tmpdir):
    subdir = tmpdir.join('subdir"')
    rcfile = subdir.join("rcfile")

    # trigger an  oserror that isn't "directory already exists"
    os.chmod(str(tmpdir), stat.S_IREAD)

    with pytest.raises(OSError):
        ensure_dir_exists(str(rcfile))


@pytest.mark.parametrize(
    "text, skipped_lines",
    (
        ("abc\n", 1),
        ("#[section]\ndef\n[section]", 2),
        ("[section]", 0),
    ),
)
def test_skip_initial_comment(text, skipped_lines):
    assert skip_initial_comment(io.StringIO(text)) == skipped_lines


def test_state_location_respects_xdg(monkeypatch):
    from pgcli.config import state_location

    monkeypatch.setenv("XDG_STATE_HOME", "/xdg/state")
    assert state_location() == "/xdg/state/pgcli/"


@pytest.mark.skipif(os.name == "nt", reason="POSIX path")
def test_state_location_default(monkeypatch):
    from pgcli.config import state_location

    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    monkeypatch.setenv("HOME", "/home/tester")
    assert state_location() == "/home/tester/.local/state/pgcli/"


def test_migrate_state_file_moves_from_config_to_state(tmp_path):
    from pgcli.config import migrate_state_file

    cfg = tmp_path / "config"
    cfg.mkdir()
    st = tmp_path / "state"
    (cfg / "history").write_text("old-history")

    new = migrate_state_file("history", config_dir=str(cfg) + "/", state_dir=str(st) + "/")

    assert new == str(st / "history")
    assert (st / "history").read_text() == "old-history"
    assert not (cfg / "history").exists()  # moved, not copied


def test_migrate_state_file_does_not_clobber_newer(tmp_path):
    from pgcli.config import migrate_state_file

    cfg = tmp_path / "config"
    cfg.mkdir()
    st = tmp_path / "state"
    st.mkdir()
    (cfg / "history").write_text("old")
    (st / "history").write_text("current")

    migrate_state_file("history", config_dir=str(cfg) + "/", state_dir=str(st) + "/")

    assert (st / "history").read_text() == "current"  # untouched
    assert (cfg / "history").exists()  # old left in place


def test_migrate_state_file_missing_old_is_noop(tmp_path):
    from pgcli.config import migrate_state_file

    cfg = tmp_path / "config"
    cfg.mkdir()
    st = tmp_path / "state"
    new = migrate_state_file("history", config_dir=str(cfg) + "/", state_dir=str(st) + "/")

    assert new == str(st / "history")
    assert not (st / "history").exists()

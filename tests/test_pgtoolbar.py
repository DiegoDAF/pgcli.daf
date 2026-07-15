from unittest.mock import MagicMock

from pgcli.pgtoolbar import create_toolbar_tokens_func
from pgcli.main import PGCli


def _toolbar_text(cli):
    cli.pgexecute = MagicMock()
    cli.pgexecute.failed_transaction.return_value = False
    cli.pgexecute.valid_transaction.return_value = False
    tokens = create_toolbar_tokens_func(cli)()
    return "".join(t[1] for t in tokens)


def test_toolbar_shows_paste_mode_off(tmpdir):
    cli = PGCli(pgclirc_file=str(tmpdir.join("rcfile")))
    assert "[F6] Paste: OFF" in _toolbar_text(cli)


def test_toolbar_shows_paste_mode_on(tmpdir):
    rc = tmpdir.join("rcfile")
    rc.write("[main]\npaste_mode = True\n")
    cli = PGCli(pgclirc_file=str(rc))
    assert "[F6] Paste: ON" in _toolbar_text(cli)

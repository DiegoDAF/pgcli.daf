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


# The tests above build a real PGCli because the config is what they exercise.
# Everything below is about the toolbar's own branching, so it uses a light fake:
# the toolbar is a pure function of the pgcli object plus the running app.

import pytest
from unittest.mock import patch
from prompt_toolkit.key_binding.vi_state import InputMode

from pgcli.pgtoolbar import _get_vi_mode, vi_modes


def fake_cli(**overrides):
    """A pgcli whose every toolbar input is off unless asked otherwise."""
    cli = MagicMock()
    defaults = {
        "multi_line": False,
        "multiline_mode": "normal",
        "vi_mode": False,
        "explain_mode": False,
        "explain_summary": False,
        "paste_mode": False,
    }
    defaults.update(overrides)
    for name, value in defaults.items():
        setattr(cli, name, value)
    cli.completer.smart_completion = overrides.get("smart_completion", True)
    cli.pgexecute.auto_commit = overrides.get("auto_commit", True)
    cli.pgexecute.failed_transaction.return_value = overrides.get("failed_transaction", False)
    cli.pgexecute.valid_transaction.return_value = overrides.get("valid_transaction", False)
    cli.completion_refresher.is_refreshing.return_value = overrides.get("is_refreshing", False)
    return cli


def toolbar(app=None, **overrides):
    """Render the toolbar as plain text. Without an app, get_app() raises."""
    if app is None:
        return "".join(t[1] for t in create_toolbar_tokens_func(fake_cli(**overrides))())
    with patch("pgcli.pgtoolbar.get_app", return_value=app):
        return "".join(t[1] for t in create_toolbar_tokens_func(fake_cli(**overrides))())


def app_with(selection=None, input_mode=InputMode.INSERT):
    app = MagicMock()
    app.current_buffer.selection_state = selection
    app.vi_state.input_mode = input_mode
    return app


class TestToggles:
    @pytest.mark.parametrize(
        "flag, on, off",
        [
            ("smart_completion", "[F2] Smart Completion: ON", "[F2] Smart Completion: OFF"),
            ("multi_line", "[F3] Multiline: ON", "[F3] Multiline: OFF"),
            ("paste_mode", "[F6] Paste: ON", "[F6] Paste: OFF"),
        ],
    )
    def test_each_toggle_shows_its_state(self, flag, on, off):
        assert on in toolbar(app=app_with(), **{flag: True})
        assert off in toolbar(app=app_with(), **{flag: False})


class TestMultilineHint:
    def test_safe_mode_names_the_key_that_executes(self):
        text = toolbar(app=app_with(), multi_line=True, multiline_mode="safe")
        assert "[Esc] [Enter] to execute" in text

    def test_normal_mode_names_the_semicolon(self):
        text = toolbar(app=app_with(), multi_line=True, multiline_mode="normal")
        assert "Semi-colon [;] will end the line" in text

    def test_no_hint_at_all_outside_multiline(self):
        text = toolbar(app=app_with(), multi_line=False, multiline_mode="safe")
        assert "to execute" not in text
        assert "will end the line" not in text


class TestEditingMode:
    def test_emacs_is_named_when_vi_is_off(self):
        assert "[F4] Emacs-mode" in toolbar(app=app_with(), vi_mode=False)

    @pytest.mark.parametrize("mode, letter", sorted(vi_modes.items(), key=lambda kv: kv[1]))
    def test_vi_shows_which_sub_mode_is_active(self, mode, letter):
        text = toolbar(app=app_with(input_mode=mode), vi_mode=True)
        assert f"[F4] Vi-mode ({letter})" in text

    def test_the_vi_mode_letter_comes_from_the_app(self):
        with patch("pgcli.pgtoolbar.get_app", return_value=app_with(input_mode=InputMode.NAVIGATION)):
            assert _get_vi_mode() == "N"


class TestExplainMode:
    def test_off_by_default(self):
        assert "[F5] Explain: OFF" in toolbar(app=app_with())

    def test_on_without_summary(self):
        assert "[F5] Explain: ON " in toolbar(app=app_with(), explain_mode=True)

    def test_on_with_summary(self):
        text = toolbar(app=app_with(), explain_mode=True, explain_summary=True)
        assert "[F5] Explain: ON+SUMMARY" in text

    def test_summary_alone_does_not_claim_explain_is_on(self):
        # F5 cycles OFF -> ON -> ON+SUMMARY, so summary without explain should
        # never happen; if it does, the toolbar must not lie about it.
        assert "[F5] Explain: OFF" in toolbar(app=app_with(), explain_summary=True)


class TestAutocommit:
    def test_nothing_is_shown_while_autocommit_is_on(self):
        # ON is the default, so saying so every time would be noise.
        assert "Autocommit" not in toolbar(app=app_with(), auto_commit=True)

    def test_off_is_flagged(self):
        assert "Autocommit: OFF" in toolbar(app=app_with(), auto_commit=False)


class TestRunSelectionHint:
    def test_shown_only_while_something_is_selected(self):
        assert "[F9] Run selection" in toolbar(app=app_with(selection=MagicMock()))
        assert "[F9] Run selection" not in toolbar(app=app_with(selection=None))

    def test_the_toolbar_works_with_no_running_app(self):
        # Outside a running application get_app() does not raise: it returns a
        # DummyApplication whose selection_state is None. So the hint is simply
        # absent, which is what the toolbar shows at startup and under pytest.
        text = toolbar(app=None)
        assert "[F2] Smart Completion" in text
        assert "[F9] Run selection" not in text

    def test_a_broken_app_does_not_take_the_toolbar_down(self):
        # The hint is contextual decoration. Whatever goes wrong reaching for
        # the selection, the rest of the toolbar still has to render.
        with patch("pgcli.pgtoolbar.get_app", side_effect=RuntimeError("no app")):
            text = "".join(t[1] for t in create_toolbar_tokens_func(fake_cli())())
        assert "[F2] Smart Completion" in text
        assert "[F5] Explain: OFF" in text
        assert "[F9] Run selection" not in text


class TestTransactionState:
    def test_a_failed_transaction_is_announced(self):
        assert "Failed transaction" in toolbar(app=app_with(), failed_transaction=True)

    def test_an_open_transaction_is_announced(self):
        assert "Transaction" in toolbar(app=app_with(), valid_transaction=True)

    def test_neither_is_announced_when_idle(self):
        assert "Transaction" not in toolbar(app=app_with())

    def test_refreshing_completions_is_announced(self):
        assert "Refreshing completions" in toolbar(app=app_with(), is_refreshing=True)

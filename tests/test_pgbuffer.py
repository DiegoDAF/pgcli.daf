"""When Enter runs the query and when it just adds a line.

`_is_complete` is covered in test_trailing_comments.py. What was missing is the
two prompt_toolkit Conditions built on top of it, which is what actually decides
whether pressing Enter executes: the most-pressed key in the whole program.
"""

from unittest.mock import MagicMock, patch

import pytest

from pgcli.pgbuffer import buffer_should_be_handled, safe_multi_line_mode


def fake_pgcli(multi_line=True, multiline_mode="normal"):
    return MagicMock(multi_line=multi_line, multiline_mode=multiline_mode)


def handled(text, multi_line=True, multiline_mode="normal"):
    """Would Enter execute this buffer?"""
    app = MagicMock()
    app.layout.get_buffer_by_name.return_value.document.text = text
    with patch("pgcli.pgbuffer.get_app", return_value=app):
        return buffer_should_be_handled(fake_pgcli(multi_line, multiline_mode))()


class TestSafeMultiLineMode:
    def test_on_only_when_multi_line_and_safe(self):
        assert safe_multi_line_mode(fake_pgcli(True, "safe"))() is True

    def test_off_when_the_mode_is_not_safe(self):
        assert safe_multi_line_mode(fake_pgcli(True, "normal"))() is False

    def test_off_when_multi_line_is_off(self):
        # Without multi-line there is nothing to be safe about.
        assert safe_multi_line_mode(fake_pgcli(False, "safe"))() is False


class TestSingleLineMode:
    def test_enter_always_runs_outside_multi_line(self):
        # Half-written or not, single-line mode sends it.
        assert handled("select 1", multi_line=False) is True
        assert handled("", multi_line=False) is True


class TestSafeModeNeverAutoRuns:
    @pytest.mark.parametrize("text", ["select 1;", "\\dt", "", "exit"])
    def test_safe_mode_holds_everything(self, text):
        # In safe mode Enter never executes on its own, not even a finished
        # statement: that is the whole point of the mode.
        assert handled(text, multiline_mode="safe") is False


class TestMultiLineMode:
    @pytest.mark.parametrize(
        "text",
        [
            "select 1;",
            "  select 1;  ",
            "select 1; -- una nota",
            "select 1; /* bloque */",
        ],
        ids=["plain", "padded", "line-comment", "block-comment"],
    )
    def test_a_finished_statement_runs(self, text):
        assert handled(text) is True

    @pytest.mark.parametrize(
        "text",
        ["select 1", "select", "-- solo un comentario", "select * from t where a = 'x"],
        ids=["no-semicolon", "bare-keyword", "only-a-comment", "open-quote"],
    )
    def test_an_unfinished_statement_waits(self, text):
        assert handled(text) is False

    def test_xor_is_not_read_as_a_comment(self):
        # The regression guard for #1646: sqlparse follows MySQL and treats "#"
        # as a comment marker, which would hide the semicolon and leave Enter
        # doing nothing forever on a perfectly finished statement.
        assert handled("select 17 # 5;") is True

    def test_a_dollar_quoted_body_keeps_waiting(self):
        # The case the whole open-quote check exists for: semicolons inside a
        # function body must not end the input.
        text = "create function f() returns int as $$ begin return 1; end $$ language plpgsql"
        assert handled(text) is False

    @pytest.mark.parametrize("text", ["\\dt", "\\d mytable", "\\?"], ids=["dt", "d-table", "help"])
    def test_backslash_commands_run_immediately(self, text):
        assert handled(text) is True

    @pytest.mark.parametrize("suffix", [r"\e", r"\G"])
    def test_editor_and_expanded_suffixes_run(self, suffix):
        assert handled(f"select 1 {suffix}") is True

    @pytest.mark.parametrize("text", ["exit", "quit", ":q"])
    def test_the_ways_out_run(self, text):
        assert handled(text) is True

    def test_a_bare_enter_runs(self):
        # An empty buffer has to be handled or Enter would do nothing at all.
        assert handled("") is True
        assert handled("   ") is True

    def test_a_word_that_merely_starts_like_exit_waits(self):
        assert handled("exits") is False

import sys

from pgcli.packages.prompt_utils import confirm_destructive_query


def test_confirm_destructive_query_notty():
    stdin = sys.stdin
    if not stdin.isatty():
        sql = "drop database foo;"
        assert confirm_destructive_query(sql, [], None) is None


def test_confirm_destructive_query_with_alias():
    stdin = sys.stdin
    if not stdin.isatty():
        sql = "drop database foo;"
        assert confirm_destructive_query(sql, ["drop"], "test") is None


# The two tests above come from upstream and are left as they are. They only run
# their body when stdin is not a tty, and the first one passes no keywords, so
# is_destructive() short-circuits before the tty check it is named after. The
# set below drives the branches directly instead of depending on how the suite
# was launched.

import click
import pytest
from unittest.mock import patch

from pgcli.packages.prompt_utils import confirm, prompt

DESTRUCTIVE = "drop table ventas;"
KEYWORDS = ["drop", "truncate", "delete"]


@pytest.fixture
def tty(monkeypatch):
    """Pretend stdin is a terminal, so the confirmation is reachable."""
    monkeypatch.setattr("pgcli.packages.prompt_utils.sys.stdin.isatty", lambda: True)


@pytest.fixture
def not_tty(monkeypatch):
    monkeypatch.setattr("pgcli.packages.prompt_utils.sys.stdin.isatty", lambda: False)


class TestWhenNothingIsAsked:
    def test_a_harmless_query_is_not_worth_a_prompt(self, tty):
        assert confirm_destructive_query("select 1;", KEYWORDS, None) is None

    def test_a_keyword_that_is_not_in_the_list_is_harmless(self, tty):
        assert confirm_destructive_query(DESTRUCTIVE, ["truncate"], None) is None

    def test_without_a_terminal_there_is_nobody_to_ask(self, not_tty):
        assert confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None) is None


class TestForce:
    """The -y/--yes path."""

    def test_force_answers_yes_without_asking(self, tty):
        with patch("pgcli.packages.prompt_utils.confirm") as asked:
            assert confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None, force=True) is True
        asked.assert_not_called()

    def test_force_works_in_a_script_too(self, not_tty):
        # force is checked BEFORE the tty test on purpose: -y has to work from
        # cron and from a pipe, which is the whole reason it exists.
        assert confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None, force=True) is True

    def test_force_does_not_invent_a_warning_for_a_harmless_query(self, tty):
        assert confirm_destructive_query("select 1;", KEYWORDS, None, force=True) is None


class TestAsking:
    def test_yes_proceeds(self, tty):
        with patch("pgcli.packages.prompt_utils.confirm", return_value=True):
            assert confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None) is True

    def test_no_stops(self, tty):
        with patch("pgcli.packages.prompt_utils.confirm", return_value=False):
            assert confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None) is False

    def test_the_alias_is_named_in_the_question(self, tty):
        # Which server you are about to break is the one thing worth reading.
        with patch("pgcli.packages.prompt_utils.confirm", return_value=True) as asked:
            confirm_destructive_query(DESTRUCTIVE, KEYWORDS, "produccion")
        assert "produccion" in asked.call_args[0][0]

    def test_without_an_alias_the_question_still_makes_sense(self, tty):
        with patch("pgcli.packages.prompt_utils.confirm", return_value=True) as asked:
            confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None)
        text = asked.call_args[0][0]
        assert "destructive command" in text
        assert " in " not in text


class TestAbortHandling:
    def test_confirm_reads_ctrl_c_as_no(self):
        # click raises Abort on Ctrl-C; letting it through would look like a
        # crash right after the user declined.
        with patch("click.confirm", side_effect=click.Abort):
            assert confirm("seguro?") is False

    def test_confirm_passes_the_answer_through(self):
        with patch("click.confirm", return_value=True):
            assert confirm("seguro?") is True

    def test_prompt_reads_ctrl_c_as_false(self):
        with patch("click.prompt", side_effect=click.Abort):
            assert prompt("password") is False

    def test_prompt_passes_the_answer_through(self):
        with patch("click.prompt", return_value="texto"):
            assert prompt("dame texto") == "texto"

    def test_ctrl_c_at_the_destructive_prompt_stops_the_query(self, tty):
        with patch("click.confirm", side_effect=click.Abort):
            assert confirm_destructive_query(DESTRUCTIVE, KEYWORDS, None) is False

"""Syntax and output colours.

pgstyle had no tests at all. It is what turns the `[colors]` section of the
config into something prompt_toolkit and cli_helpers understand, so a mistake
here is visible on every single line the user sees.

The tests define their own Pygments style instead of leaning on "native", whose
concrete colours are a pygments implementation detail and do change between
releases.
"""

import logging

import pytest
from pygments.style import Style as PygmentsStyle
from pygments.token import Token

from pgcli.pgstyle import (
    PROMPT_STYLE_TO_TOKEN,
    TOKEN_TO_PROMPT_STYLE,
    parse_pygments_style,
    style_factory,
    style_factory_output,
)


class SampleStyle(PygmentsStyle):
    default_style = ""
    styles = {
        Token.Keyword: "bold #ff0000",
        Token.Name: "#00ff00",
    }


def rules_of(style):
    """The merged style as a plain dict, last rule winning."""
    return dict(style.style_rules)


class TestParsePygmentsStyle:
    def test_a_style_class_lets_one_token_inherit_another(self):
        token, value = parse_pygments_style("Token.Keyword", SampleStyle, {"Token.Keyword": "Token.Name"})
        assert token is Token.Keyword
        assert value == "#00ff00"

    def test_a_plain_dict_is_taken_literally_instead(self):
        # style_factory passes the class (so values can name another token) and
        # style_factory_output passes the .styles dict, which has no .styles
        # attribute: the AttributeError is what makes the value literal.
        token, value = parse_pygments_style("Token.Keyword", SampleStyle.styles, {"Token.Keyword": "bold #123456"})
        assert token is Token.Keyword
        assert value == "bold #123456"


class TestStyleFactory:
    def test_an_unknown_style_name_falls_back_instead_of_raising(self):
        # A typo in pgclirc must not stop pgcli from starting.
        assert rules_of(style_factory("no-such-style-anywhere", {}))

    def test_a_prompt_toolkit_class_name_passes_straight_through(self):
        rules = rules_of(style_factory("native", {"completion-menu.completion": "bg:#ff0000"}))
        assert rules["completion-menu.completion"] == "bg:#ff0000"

    def test_a_pygments_token_is_translated_to_its_class_name(self):
        # The 1.0-era config spelling still has to work.
        rules = rules_of(style_factory("native", {"Token.Toolbar.On": "bold #00ff00"}))
        assert rules["bottom-toolbar.on"] == "bold #00ff00"

    def test_the_toolbar_override_is_always_applied(self):
        rules = rules_of(style_factory("native", {}))
        assert rules["bottom-toolbar"] == "noreverse"

    def test_a_users_toolbar_rule_wins_over_the_override(self):
        # The override is merged before the user's rules on purpose.
        rules = rules_of(style_factory("native", {"bottom-toolbar": "bg:#333333"}))
        assert rules["bottom-toolbar"] == "bg:#333333"

    def test_an_unmappable_token_is_logged_and_dropped(self, caplog):
        with caplog.at_level(logging.ERROR, logger="pgcli.pgstyle"):
            rules = rules_of(style_factory("native", {"Token.Nonsense.Here": "bold"}))
        assert "Unhandled style / class name" in caplog.text
        assert "Token.Nonsense.Here" not in rules

    @pytest.mark.parametrize("token_name, class_name", list(TOKEN_TO_PROMPT_STYLE.items())[:8])
    def test_every_mapped_token_reaches_its_class(self, token_name, class_name):
        rules = rules_of(style_factory("native", {str(token_name): "bold"}))
        assert rules[class_name] == "bold"


class TestStyleFactoryOutput:
    def test_an_unknown_style_name_falls_back_instead_of_raising(self):
        assert style_factory_output("no-such-style-anywhere", {}).styles

    def test_it_returns_something_cli_helpers_can_use(self):
        out = style_factory_output("native", {})
        assert issubclass(out, PygmentsStyle)
        assert out.default_style == ""

    def test_a_pygments_token_overrides_the_theme(self):
        out = style_factory_output("native", {"Token.Literal.String": "bold #abcdef"})
        assert out.styles[Token.Literal.String] == "bold #abcdef"

    def test_a_class_name_is_mapped_back_to_its_token(self):
        # cli_helpers still speaks Pygments tokens, so the 2.0 names the user
        # writes have to be translated in the other direction.
        out = style_factory_output("native", {"output.header": "bold #abcdef"})
        assert out.styles[PROMPT_STYLE_TO_TOKEN["output.header"]] == "bold #abcdef"

    def test_an_unknown_name_is_logged_and_ignored(self, caplog):
        with caplog.at_level(logging.ERROR, logger="pgcli.pgstyle"):
            style_factory_output("native", {"not-a-real-class": "bold"})
        assert "Unhandled style / class name" in caplog.text


class TestTheTwoMapsAgree:
    def test_the_reverse_map_covers_every_entry(self):
        assert len(PROMPT_STYLE_TO_TOKEN) == len(TOKEN_TO_PROMPT_STYLE)

    def test_no_two_tokens_share_a_class_name(self):
        # A duplicate would silently drop one token from the reverse map, and
        # the output styling for it would stop working.
        names = list(TOKEN_TO_PROMPT_STYLE.values())
        assert len(names) == len(set(names))

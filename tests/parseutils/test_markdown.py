"""Pulling SQL back out of text pasted from a markdown document.

Pasting an answer from a chat brings the code fence along, and the server
answers with a syntax error pointing at the fence instead of at anything
useful. Backticks are never valid PostgreSQL, so a fence is unambiguous; the
prose case is deliberately more careful, and leaves the text alone whenever it
cannot find a line that starts a statement.
"""

from pgcli.packages.parseutils import strip_markdown


def test_fence_with_a_language_tag():
    assert strip_markdown("```sql\nselect 1;\n```") == ("select 1;", "markdown fence")


def test_fence_without_a_language_tag():
    assert strip_markdown("```\nselect 1;\n```") == ("select 1;", "markdown fence")


def test_tilde_fence():
    assert strip_markdown("~~~postgresql\nselect 1;\n~~~") == ("select 1;", "markdown fence")


def test_unclosed_fence():
    """pgcli submits at the semicolon, so the closing fence often arrives in a
    separate input and the opening one comes alone."""
    assert strip_markdown("```sql\nselect 1;") == ("select 1;", "markdown fence")


def test_a_lone_fence_becomes_empty():
    """The trailing ``` of a pasted block: dropped rather than sent."""
    assert strip_markdown("```") == ("", "markdown fence")
    assert strip_markdown("```\n```") == ("", "markdown fence")


def test_prose_before_the_fence_is_dropped():
    text = "Sure, this counts the rows:\n\n```sql\nselect count(*) from t;\n```"
    sql, removed = strip_markdown(text)
    assert sql == "select count(*) from t;"
    assert "surrounding text" in removed


def test_several_statements_inside_the_fence_are_kept():
    assert strip_markdown("```sql\nselect 1;\nselect 2;\n```")[0] == "select 1;\nselect 2;"


def test_prose_without_a_fence_stops_at_the_first_statement():
    sql, removed = strip_markdown("Here is the query you asked for:\n\nselect 1;")
    assert sql == "select 1;"
    assert "2 line(s)" in removed


# --- what must NOT be touched ---------------------------------------------


def test_plain_sql_is_untouched():
    for text in ("select 1;", "  select 1;", "SELECT 1;\nSELECT 2;"):
        assert strip_markdown(text) == (text, "")


def test_a_leading_comment_is_kept():
    """A comment is valid SQL and may carry meaning (hints, pgbouncer tags)."""
    for text in ("-- count them\nselect 1;", "/* block */\nselect 1;"):
        assert strip_markdown(text) == (text, "")


def test_backslash_commands_are_kept():
    assert strip_markdown("\\dt") == ("\\dt", "")
    assert strip_markdown("\\d+ mytable") == ("\\d+ mytable", "")


def test_a_parenthesised_query_is_kept():
    text = "(select 1) union (select 2);"
    assert strip_markdown(text) == (text, "")


def test_a_cte_is_kept():
    text = "with x as (select 1)\nselect * from x;"
    assert strip_markdown(text) == (text, "")


def test_text_with_no_statement_is_left_for_the_server_to_reject():
    """Nothing here starts a statement, so rather than guess, hand it over
    unchanged and let the error come from the server."""
    text = "this is not sql at all"
    assert strip_markdown(text) == (text, "")


def test_backticks_inside_a_string_are_not_a_fence():
    text = "select '```' as fence;"
    assert strip_markdown(text) == (text, "")


def test_empty_input():
    assert strip_markdown("") == ("", "")

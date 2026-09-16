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

import re
import sqlparse

sqlparse.engine.grouping.MAX_GROUPING_DEPTH = None
sqlparse.engine.grouping.MAX_GROUPING_TOKENS = None

BASE_KEYWORDS = [
    "drop",
    "shutdown",
    "delete",
    "truncate",
    "alter",
    "unconditional_update",
]
ALL_KEYWORDS = BASE_KEYWORDS + ["update"]


def query_starts_with(formatted_sql, prefixes):
    """Check if the query starts with any item from *prefixes*."""
    prefixes = [prefix.lower() for prefix in prefixes]
    return bool(formatted_sql) and formatted_sql.split()[0] in prefixes


def query_is_unconditional_update(query):
    """Check if the query starts with UPDATE and contains no top-level WHERE clause.

    Uses sqlparse's parse tree (rather than naive whitespace splitting) so that
    the word "where" appearing inside a string literal, comment, or a nested
    subquery doesn't get mistaken for an actual WHERE clause.
    """
    statements = sqlparse.parse(query)
    if not statements:
        return False
    statement = statements[0]

    first_token = statement.token_first(skip_cm=True)
    if first_token is None or first_token.ttype is not sqlparse.tokens.DML:
        return False
    if first_token.value.upper() != "UPDATE":
        return False

    return not any(isinstance(token, sqlparse.sql.Where) for token in statement.tokens)


def is_destructive(queries, keywords):
    """Returns if any of the queries in *queries* is destructive."""
    for query in sqlparse.split(queries):
        if query:
            formatted_sql = sqlparse.format(query.lower(), strip_comments=True).strip()
            if "unconditional_update" in keywords and query_is_unconditional_update(query):
                return True
            if query_starts_with(formatted_sql, keywords):
                return True
    return False


def parse_destructive_warning(warning_level):
    """Converts a deprecated destructive warning option to a list of command keywords."""
    if not warning_level:
        return []

    if not isinstance(warning_level, list):
        if "," in warning_level:
            return warning_level.split(",")
        warning_level = [warning_level]

    return {
        "true": ALL_KEYWORDS,
        "false": [],
        "all": ALL_KEYWORDS,
        "moderate": BASE_KEYWORDS,
        "off": [],
        "": [],
    }.get(warning_level[0], warning_level)


# Lines that open or close a markdown code fence: ``` or ~~~, optionally
# followed by a language tag. Neither character sequence means anything in
# PostgreSQL (a backtick is always a syntax error), so finding one at the
# start of a line is unambiguous.
_MARKDOWN_FENCE = re.compile(r"^\s*(?:`{3,}|~{3,})\s*[\w+-]*\s*$")

# A line that starts a statement pgcli can run: a SQL keyword, a backslash
# command, an opening parenthesis (``(select ...) union ...``) or a comment.
# Used only to find where the SQL begins in pasted prose; anything not matched
# here is left alone rather than guessed at.
_SQL_LINE_START = re.compile(
    r"""^\s*(?:
        --|/\*|\(|\\          # comment, parenthesised query, backslash command
        |(?:select|insert|update|delete|merge|with|values|table
          |create|alter|drop|truncate|comment|refresh|reindex|cluster|vacuum|analyze
          |grant|revoke|security
          |begin|start|commit|rollback|savepoint|release|end|abort
          |explain|copy|call|do|prepare|execute|deallocate
          |declare|fetch|move|close|listen|unlisten|notify
          |set|reset|show|discard|lock|checkpoint|import
          )\b
    )""",
    re.IGNORECASE | re.VERBOSE,
)


def strip_markdown(text):
    """Return ``(sql, removed)`` for text pasted out of a markdown document.

    Pasting an answer from a chat or an LLM brings the code fence along, and
    sometimes the prose above it. The server then answers with a syntax error
    pointing at the fence, which says nothing about the real problem. This
    pulls the SQL back out:

    * text inside a fence wins, because a fence states exactly where the code
      is and there is nothing to guess;
    * without a fence, leading lines are dropped up to the first one that
      starts a statement, and if no such line exists nothing is touched;
    * a lone fence becomes empty text, so it is not sent to the server.

    ``removed`` describes what was dropped, for the caller to report, and is
    empty when the text was left as it is.
    """
    lines = text.splitlines()
    if not lines:
        return text, ""

    fences = [i for i, line in enumerate(lines) if _MARKDOWN_FENCE.match(line)]
    if fences:
        opening = fences[0]
        closing = fences[1] if len(fences) > 1 else len(lines)
        inner = lines[opening + 1 : closing]
        if not [line for line in inner if line.strip()]:
            # Just a fence, or an empty block: nothing to run.
            return "", "markdown fence"
        what = "markdown fence" + (" and surrounding text" if opening > 0 else "")
        return "\n".join(inner), what

    for index, line in enumerate(lines):
        if _SQL_LINE_START.match(line):
            if index == 0:
                return text, ""  # already SQL: the common case, untouched
            return "\n".join(lines[index:]), "%d line(s) of text before the statement" % index
    return text, ""  # nothing that looks like SQL: let the server complain

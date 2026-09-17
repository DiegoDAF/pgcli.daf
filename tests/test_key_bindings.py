from unittest.mock import Mock

from prompt_toolkit.buffer import Buffer
from prompt_toolkit.document import Document
from prompt_toolkit.keys import Keys

from pgcli.key_bindings import pgcli_bindings


def _find_binding(kb, key):
    for b in kb.bindings:
        if b.keys == (key,):
            return b
    return None


def _make_pgcli():
    pgcli = Mock()
    pgcli.completer.smart_completion = True
    pgcli.multi_line = True
    pgcli.vi_mode = False
    pgcli.explain_mode = False
    pgcli.paste_mode = False
    return pgcli


def _buffer_with_selection(text, start, end):
    buff = Buffer()
    buff.set_document(Document(text, cursor_position=start))
    buff.start_selection()
    buff.cursor_position = end
    return buff


def test_f9_binding_registered_with_selection_filter():
    """F9 is bound and only fires while there is a selection."""
    kb = pgcli_bindings(_make_pgcli())
    binding = _find_binding(kb, Keys.F9)
    assert binding is not None


def test_f9_executes_only_selected_text():
    """Pressing F9 replaces the buffer with the selection and accepts it."""
    kb = pgcli_bindings(_make_pgcli())
    binding = _find_binding(kb, Keys.F9)

    # "select 1;\nselect 2;\nselect 3;" -> select the middle statement.
    text = "select 1;\nselect 2;\nselect 3;"
    start = text.index("select 2")
    buff = _buffer_with_selection(text, start, start + len("select 2"))
    buff.validate_and_handle = Mock()  # requires an active app; stub it out

    event = Mock()
    event.current_buffer = buff
    binding.handler(event)

    assert buff.text == "select 2"
    buff.validate_and_handle.assert_called_once()


def test_f9_ignores_whitespace_only_selection():
    """A selection that is only whitespace is not executed."""
    kb = pgcli_bindings(_make_pgcli())
    binding = _find_binding(kb, Keys.F9)

    text = "select 1;\n   \nselect 3;"
    start = text.index("\n") + 1
    buff = _buffer_with_selection(text, start, start + 3)  # the three spaces
    buff.validate_and_handle = Mock()

    event = Mock()
    event.current_buffer = buff
    binding.handler(event)

    buff.validate_and_handle.assert_not_called()


def _make_pgcli_for_explain(config_summary=False):
    pgcli = _make_pgcli()
    pgcli.explain_summary = config_summary
    pgcli.config = {"main": Mock(**{"as_bool.return_value": config_summary})}
    return pgcli


def _press_f5(pgcli, times=1):
    kb = pgcli_bindings(pgcli)
    binding = _find_binding(kb, Keys.F5)
    assert binding is not None
    for _ in range(times):
        binding.handler(Mock())
    return pgcli.explain_mode, pgcli.explain_summary


def test_f5_cycles_off_plan_and_summary():
    """One key, three states: off, the plan, the plan plus the analysis."""
    pgcli = _make_pgcli_for_explain()
    assert _press_f5(pgcli) == (True, False)  # plan
    assert _press_f5(pgcli) == (True, True)  # plan + summary
    assert _press_f5(pgcli) == (False, False)  # off
    assert _press_f5(pgcli) == (True, False)  # back to the plan


def test_f5_restores_the_configured_summary_when_turning_off():
    """Someone who set explain_summary = True gets it back on the next F5,
    instead of losing it because they cycled past the end."""
    pgcli = _make_pgcli_for_explain(config_summary=True)
    _press_f5(pgcli, times=3)  # plan, plan+summary, off
    assert (pgcli.explain_mode, pgcli.explain_summary) == (False, True)

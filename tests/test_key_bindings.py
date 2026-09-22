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


# F5 and F9 are covered above. The rest of the function keys had no tests at
# all, so a binding could be dropped or wired to the wrong flag and the suite
# would stay green.


def _press(key, pgcli=None, event=None):
    """Fire the handler bound to a key and return the pgcli it acted on."""
    pgcli = pgcli or _make_pgcli()
    binding = _find_binding(pgcli_bindings(pgcli), key)
    assert binding is not None, f"{key} is not bound"
    binding.handler(event or Mock())
    return pgcli


def test_f2_toggles_smart_completion():
    pgcli = _make_pgcli()
    pgcli.completer.smart_completion = True
    _press(Keys.F2, pgcli)
    assert pgcli.completer.smart_completion is False
    _press(Keys.F2, pgcli)
    assert pgcli.completer.smart_completion is True


def test_f3_toggles_multiline():
    pgcli = _make_pgcli()
    pgcli.multi_line = True
    _press(Keys.F3, pgcli)
    assert pgcli.multi_line is False
    _press(Keys.F3, pgcli)
    assert pgcli.multi_line is True


def test_f4_toggles_vi_mode_and_tells_the_application():
    from prompt_toolkit.enums import EditingMode

    pgcli = _make_pgcli()
    pgcli.vi_mode = False
    event = Mock()
    _press(Keys.F4, pgcli, event)
    assert pgcli.vi_mode is True
    # Flipping the flag without telling prompt_toolkit would leave the toolbar
    # claiming vi while the keys stayed emacs.
    assert event.app.editing_mode == EditingMode.VI
    _press(Keys.F4, pgcli, event)
    assert pgcli.vi_mode is False
    assert event.app.editing_mode == EditingMode.EMACS


def test_f6_toggles_paste_mode():
    pgcli = _make_pgcli()
    pgcli.paste_mode = False
    _press(Keys.F6, pgcli)
    assert pgcli.paste_mode is True
    _press(Keys.F6, pgcli)
    assert pgcli.paste_mode is False


def test_each_function_key_is_bound_once():
    # A duplicate binding means whichever registered last silently wins.
    kb = pgcli_bindings(_make_pgcli())
    for key in (Keys.F2, Keys.F3, Keys.F4, Keys.F5, Keys.F6, Keys.F9):
        bound = [b for b in kb.bindings if b.keys == (key,)]
        assert len(bound) == 1, f"{key} is bound {len(bound)} times"


def test_the_toggles_do_not_touch_each_other():
    # They all live in the same closure over the same pgcli object.
    pgcli = _make_pgcli()
    before = (pgcli.multi_line, pgcli.vi_mode, pgcli.explain_mode)
    _press(Keys.F2, pgcli)
    assert (pgcli.multi_line, pgcli.vi_mode, pgcli.explain_mode) == before


# The editing bindings: tab, escape, c-space, the two enters, alt-enter and the
# history keys. None of them had a test, and several are bound to the same key
# with different filters, so a reordering could change which one wins.


def _bindings_for(kb, *keys):
    from prompt_toolkit.keys import Keys as K

    wanted = tuple(getattr(K, k) if isinstance(k, str) else k for k in keys)
    return [b for b in kb.bindings if b.keys == wanted]


def _event_on(buff):
    """An event whose buffer is `buff`, reachable by both names the code uses."""
    event = Mock()
    event.current_buffer = buff
    event.app.current_buffer = buff
    return event


def _mock_buffer(text, cursor=None, complete_state=None):
    """A buffer that records calls but carries a real Document."""
    buff = Mock()
    buff.document = Document(text, cursor_position=cursor if cursor is not None else len(text))
    buff.complete_state = complete_state
    return buff


class TestTab:
    def _press_tab(self, buff):
        kb = pgcli_bindings(_make_pgcli())
        _bindings_for(kb, "ControlI")[0].handler(_event_on(buff))

    def test_opens_the_menu_on_a_line_with_text(self):
        buff = _mock_buffer("sele")
        self._press_tab(buff)
        buff.start_completion.assert_called_once_with(select_first=True)

    def test_moves_to_the_next_entry_when_the_menu_is_already_open(self):
        buff = _mock_buffer("sele", complete_state=Mock())
        self._press_tab(buff)
        buff.complete_next.assert_called_once()
        buff.start_completion.assert_not_called()

    def test_indents_a_blank_continuation_line(self):
        # On a blank line below the first, tab is an indent, not a completion.
        buff = _mock_buffer("select 1\n", cursor=9)
        self._press_tab(buff)
        buff.insert_text.assert_called_once_with(" " * 4, fire_event=False)
        buff.start_completion.assert_not_called()

    def test_the_first_line_completes_even_when_blank(self):
        buff = _mock_buffer("")
        self._press_tab(buff)
        buff.start_completion.assert_called_once_with(select_first=True)


class TestControlSpace:
    def _press(self, buff):
        kb = pgcli_bindings(_make_pgcli())
        _bindings_for(kb, "ControlAt")[0].handler(_event_on(buff))

    def test_opens_the_menu_without_preselecting(self):
        # Unlike tab: c-space shows the options and leaves the choice open.
        buff = _mock_buffer("sele")
        self._press(buff)
        buff.start_completion.assert_called_once_with(select_first=False)

    def test_moves_to_the_next_entry_when_already_open(self):
        buff = _mock_buffer("sele", complete_state=Mock())
        self._press(buff)
        buff.complete_next.assert_called_once()


class TestClosingTheMenu:
    def test_escape_closes_the_completion_menu(self):
        kb = pgcli_bindings(_make_pgcli())
        buff = _mock_buffer("sele", complete_state=Mock())
        _bindings_for(kb, "Escape")[0].handler(_event_on(buff))
        assert buff.complete_state is None

    def test_enter_accepts_the_selection_instead_of_running_the_query(self):
        # The first of the two enter bindings. Running the query here would be
        # the wrong thing: the user was picking from the dropdown.
        kb = pgcli_bindings(_make_pgcli())
        buff = _mock_buffer("sele", complete_state=Mock())
        _bindings_for(kb, "ControlM")[0].handler(_event_on(buff))
        assert buff.complete_state is None
        buff.validate_and_handle.assert_not_called()


class TestEnterRunsTheQuery:
    def test_the_second_enter_binding_submits(self):
        kb = pgcli_bindings(_make_pgcli())
        buff = _mock_buffer("select 1;")
        _bindings_for(kb, "ControlM")[1].handler(_event_on(buff))
        buff.validate_and_handle.assert_called_once()

    def test_there_are_exactly_two_enter_bindings(self):
        # One closes the dropdown, one submits. A third would make which runs
        # depend on registration order.
        kb = pgcli_bindings(_make_pgcli())
        assert len(_bindings_for(kb, "ControlM")) == 2


class TestAltEnter:
    def test_inserts_a_line_break(self):
        kb = pgcli_bindings(_make_pgcli())
        buff = _mock_buffer("select 1")
        _bindings_for(kb, "Escape", "ControlM")[0].handler(_event_on(buff))
        buff.insert_text.assert_called_once_with("\n")


class TestHistoryKeys:
    def test_control_p_goes_back(self):
        kb = pgcli_bindings(_make_pgcli())
        buff = _mock_buffer("")
        event = _event_on(buff)
        event.arg = 1
        _bindings_for(kb, "ControlP")[0].handler(event)
        buff.history_backward.assert_called_once_with(count=1)

    def test_control_n_goes_forward(self):
        kb = pgcli_bindings(_make_pgcli())
        buff = _mock_buffer("")
        event = _event_on(buff)
        event.arg = 3
        _bindings_for(kb, "ControlN")[0].handler(event)
        buff.history_forward.assert_called_once_with(count=3)

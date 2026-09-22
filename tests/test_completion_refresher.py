import time
import pytest
from unittest.mock import Mock, patch


@pytest.fixture
def refresher():
    from pgcli.completion_refresher import CompletionRefresher

    return CompletionRefresher()


def test_ctor(refresher):
    """
    Refresher object should contain a few handlers
    :param refresher:
    :return:
    """
    assert len(refresher.refreshers) > 0
    actual_handlers = list(refresher.refreshers.keys())
    expected_handlers = [
        "schemata",
        "tables",
        "views",
        "types",
        "databases",
        "roles",
        "settings",
        "casing",
        "functions",
    ]
    assert expected_handlers == actual_handlers


def test_refresh_called_once(refresher):
    """

    :param refresher:
    :return:
    """
    callbacks = Mock()
    pgexecute = Mock(**{"is_virtual_database.return_value": False})
    special = Mock()

    with patch.object(refresher, "_bg_refresh") as bg_refresh:
        actual = refresher.refresh(pgexecute, special, callbacks)
        time.sleep(1)  # Wait for the thread to work.
        assert len(actual) == 1
        assert len(actual[0]) == 4
        assert actual[0][3] == "Auto-completion refresh started in the background."
        bg_refresh.assert_called_with(pgexecute, special, callbacks, None, None)


def test_refresh_called_twice(refresher):
    """
    If refresh is called a second time, it should be restarted
    :param refresher:
    :return:
    """
    callbacks = Mock()

    pgexecute = Mock(**{"is_virtual_database.return_value": False})
    special = Mock()

    def dummy_bg_refresh(*args):
        time.sleep(3)  # seconds

    refresher._bg_refresh = dummy_bg_refresh

    actual1 = refresher.refresh(pgexecute, special, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert len(actual1) == 1
    assert len(actual1[0]) == 4
    assert actual1[0][3] == "Auto-completion refresh started in the background."

    actual2 = refresher.refresh(pgexecute, special, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert len(actual2) == 1
    assert len(actual2[0]) == 4
    assert actual2[0][3] == "Auto-completion refresh restarted."


def test_refresh_with_callbacks(refresher):
    """
    Callbacks must be called
    :param refresher:
    """
    callbacks = [Mock()]
    pgexecute = Mock(**{"is_virtual_database.return_value": False})
    pgexecute.extra_args = {}
    special = Mock()

    # Set refreshers to 0: we're not testing refresh logic here
    refresher.refreshers = {}
    refresher.refresh(pgexecute, special, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert callbacks[0].call_count == 1


# The nine individual refreshers had no tests: test_ctor only checked that they
# are registered under the right names, never that each one feeds the completer
# what it says it does. Neither did the branches of refresh() and _bg_refresh().


def _completer_and_executor():
    return Mock(), Mock()


class TestEachRefresherFeedsTheCompleter:
    def test_schemata(self):
        from pgcli.completion_refresher import refresh_schemata

        completer, executor = _completer_and_executor()
        refresh_schemata(completer, executor)
        completer.set_search_path.assert_called_once_with(executor.search_path())
        completer.extend_schemata.assert_called_once_with(executor.schemata())

    def test_tables_also_brings_columns_and_foreign_keys(self):
        from pgcli.completion_refresher import refresh_tables

        completer, executor = _completer_and_executor()
        refresh_tables(completer, executor)
        completer.extend_relations.assert_called_once_with(executor.tables(), kind="tables")
        completer.extend_columns.assert_called_once_with(executor.table_columns(), kind="tables")
        completer.extend_foreignkeys.assert_called_once_with(executor.foreignkeys())

    def test_views_are_tagged_as_views_not_tables(self):
        from pgcli.completion_refresher import refresh_views

        completer, executor = _completer_and_executor()
        refresh_views(completer, executor)
        completer.extend_relations.assert_called_once_with(executor.views(), kind="views")
        completer.extend_columns.assert_called_once_with(executor.view_columns(), kind="views")

    @pytest.mark.parametrize(
        "func_name, completer_method, executor_method",
        [
            ("refresh_types", "extend_datatypes", "datatypes"),
            ("refresh_databases", "extend_database_names", "databases"),
            ("refresh_roles", "extend_role_names", "roles"),
            ("refresh_settings", "extend_setting_names", "settings"),
            ("refresh_functions", "extend_functions", "functions"),
        ],
    )
    def test_the_simple_ones_delegate(self, func_name, completer_method, executor_method):
        import pgcli.completion_refresher as cr

        completer, executor = _completer_and_executor()
        getattr(cr, func_name)(completer, executor)
        getattr(completer, completer_method).assert_called_once_with(getattr(executor, executor_method)())


class TestCasingRefresher:
    def test_nothing_happens_without_a_casing_file(self):
        from pgcli.completion_refresher import refresh_casing

        completer, executor = _completer_and_executor()
        completer.casing_file = None
        refresh_casing(completer, executor)
        completer.extend_casing.assert_not_called()

    def test_the_file_is_generated_from_the_server_when_asked(self, tmpdir):
        from pgcli.completion_refresher import refresh_casing

        completer, executor = _completer_and_executor()
        path = str(tmpdir.join("casing"))
        completer.casing_file = path
        completer.generate_casing_file = True
        executor.casing.return_value = ["SELECT", "FROM"]
        refresh_casing(completer, executor)
        with open(path) as f:
            assert f.read() == "SELECT\nFROM"
        completer.extend_casing.assert_called_once_with(["SELECT", "FROM"])

    def test_an_existing_file_is_read_and_not_regenerated(self, tmpdir):
        from pgcli.completion_refresher import refresh_casing

        completer, executor = _completer_and_executor()
        casing = tmpdir.join("casing")
        casing.write("Ventas\nClientes\n")
        completer.casing_file = str(casing)
        completer.generate_casing_file = True
        refresh_casing(completer, executor)
        executor.casing.assert_not_called()
        completer.extend_casing.assert_called_once_with(["Ventas", "Clientes"])

    def test_a_missing_file_is_not_generated_unless_asked(self, tmpdir):
        from pgcli.completion_refresher import refresh_casing

        completer, executor = _completer_and_executor()
        completer.casing_file = str(tmpdir.join("nope"))
        completer.generate_casing_file = False
        refresh_casing(completer, executor)
        executor.casing.assert_not_called()
        completer.extend_casing.assert_not_called()


class TestRefreshEntryPoint:
    def test_a_virtual_database_has_nothing_to_refresh(self, refresher):
        executor = Mock()
        executor.is_virtual_database.return_value = True
        result = refresher.refresh(executor, Mock(), lambda c: None)
        assert "can't be started" in result[0][3]
        assert refresher._completer_thread is None

    def test_a_second_refresh_restarts_the_running_one(self, refresher):
        executor = Mock()
        executor.is_virtual_database.return_value = False
        running = Mock()
        running.is_alive.return_value = True
        refresher._completer_thread = running

        result = refresher.refresh(executor, Mock(), lambda c: None)

        assert "restarted" in result[0][3]
        # The running thread is told to start over rather than a second one
        # being spawned against the same completer.
        assert refresher._restart_refresh.is_set()
        assert refresher._completer_thread is running

    def test_a_first_refresh_starts_a_daemon_thread(self, refresher):
        executor = Mock()
        executor.is_virtual_database.return_value = False
        with patch("pgcli.completion_refresher.threading.Thread") as thread_cls:
            result = refresher.refresh(executor, Mock(), lambda c: None)
        assert "started in the background" in result[0][3]
        thread_cls.assert_called_once()
        assert thread_cls.call_args[1]["name"] == "completion_refresh"
        # A non-daemon thread would keep pgcli alive after the user quits.
        assert refresher._completer_thread.daemon is True
        refresher._completer_thread.start.assert_called_once()

    def test_is_refreshing_is_false_before_anything_starts(self, refresher):
        assert not refresher.is_refreshing()


class TestBackgroundRefresh:
    def _run(self, refresher, **kwargs):
        """Drive _bg_refresh with the refreshers themselves stubbed out.

        The real ones are covered above. Leaving them in here would run
        refresh_casing against a Mock completer, where casing_file is a Mock
        and os.path.isfile raises TypeError: a failure about the test setup,
        not about the code under test.
        """
        from collections import OrderedDict

        executor = Mock()
        refreshers = kwargs.pop("refreshers", OrderedDict())
        with (
            patch("pgcli.completion_refresher.PGCompleter") as completer_cls,
            patch.object(type(refresher), "refreshers", refreshers),
        ):
            refresher._bg_refresh(executor, Mock(), kwargs.pop("callbacks", []), **kwargs)
        return executor, completer_cls.return_value

    def test_a_single_callback_does_not_have_to_be_a_list(self, refresher):
        seen = []
        self._run(refresher, callbacks=seen.append, settings={"single_connection": True})
        assert len(seen) == 1

    def test_every_callback_in_a_list_is_called(self, refresher):
        seen = []
        callbacks = [lambda c: seen.append("a"), lambda c: seen.append("b")]
        self._run(refresher, callbacks=callbacks, settings={"single_connection": True})
        assert seen == ["a", "b"]

    def test_single_connection_reuses_the_executor_and_leaves_it_open(self, refresher):
        executor, _ = self._run(refresher, settings={"single_connection": True})
        executor.copy.assert_not_called()
        executor.conn.close.assert_not_called()

    def test_otherwise_a_copy_is_made_and_closed_again(self, refresher):
        # The refresh runs on its own connection so it cannot disturb the
        # session the user is typing into; leaving it open would leak it.
        executor, _ = self._run(refresher, settings={})
        executor.copy.assert_called_once()
        executor.copy.return_value.conn.close.assert_called_once()

    def test_history_is_fed_to_the_completer_newest_last(self, refresher):
        history = Mock()
        history.get_strings.return_value = [f"select {i};" for i in range(150)]
        _, completer = self._run(refresher, history=history, settings={"single_connection": True})
        calls = [c[0][0] for c in completer.extend_query_history.call_args_list]
        # Only the last 100 are loaded, in order.
        assert len(calls) == 100
        assert calls[0] == "select 50;"
        assert calls[-1] == "select 149;"
        assert all(c[1]["is_init"] is True for c in completer.extend_query_history.call_args_list)

    def test_no_history_is_not_an_error(self, refresher):
        _, completer = self._run(refresher, history=None, settings={"single_connection": True})
        completer.extend_query_history.assert_not_called()

    def test_a_restart_request_runs_the_refreshers_again(self, refresher):
        # The restart flag is checked after each refresher, so the pass in
        # progress is abandoned and the whole set starts over.
        calls = []

        def first(completer, executor):
            calls.append("first")
            if len(calls) == 1:
                refresher._restart_refresh.set()

        def second(completer, executor):
            calls.append("second")

        from collections import OrderedDict

        self._run(
            refresher,
            settings={"single_connection": True},
            refreshers=OrderedDict([("a", first), ("b", second)]),
        )

        # First pass stops right after "first"; the second pass completes.
        assert calls == ["first", "first", "second"]
        assert not refresher._restart_refresh.is_set()

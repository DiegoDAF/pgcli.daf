import os
import pytest
from utils import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    create_db,
    db_connection,
    drop_tables,
)
import pgcli.main
import pgcli.pgexecute


@pytest.fixture(scope="function")
def connection():
    create_db("_test_db")
    connection = db_connection("_test_db")
    yield connection

    drop_tables(connection)
    connection.close()


@pytest.fixture
def cursor(connection):
    with connection.cursor() as cur:
        return cur


@pytest.fixture
def executor(connection):
    return pgcli.pgexecute.PGExecute(
        database="_test_db",
        user=POSTGRES_USER,
        host=POSTGRES_HOST,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        dsn=None,
        notify_callback=pgcli.main.notify_callback,
    )


@pytest.fixture
def exception_formatter():
    return lambda e: str(e)


@pytest.fixture(autouse=True)
def no_system_ssh_config(monkeypatch, tmp_path):
    # The SSH tunnel also reads /etc/ssh/ssh_config; point it at a file that
    # does not exist so no test depends on the machine it runs on.
    monkeypatch.setattr("pgcli.ssh_tunnel._SYSTEM_SSH_CONFIG", str(tmp_path / "no-system-ssh-config"))


@pytest.fixture(scope="session", autouse=True)
def temp_config(tmpdir_factory):
    # this function runs on start of test session.
    # use temporary directory for config home so user config will not be used
    os.environ["XDG_CONFIG_HOME"] = str(tmpdir_factory.mktemp("data"))
    # ... and for the state home: every PGCli() the tests build opens the
    # debug log there, and without this they appended to the developer's real
    # ~/.local/state/pgcli log and history.
    os.environ["XDG_STATE_HOME"] = str(tmpdir_factory.mktemp("state"))

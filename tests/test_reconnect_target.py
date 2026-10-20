"""Where pgcli connects after \\c to another server, or after a reconnect.

Over an SSH tunnel the connection carries host=<real server>, hostaddr=127.0.0.1
and the tunnel's local port. \\c to a different host used to keep that
hostaddr: it dialed 127.0.0.1:<new port>, a local server if one listens there,
while the prompt showed the new host.
"""

import socket
from unittest.mock import MagicMock, patch

import pytest
from psycopg import OperationalError

from pgcli.main import PGCli
from pgcli.pgexecute import PGExecute
from pgcli.ssh_tunnel import SSHTunnelError, _NativeSSHTunnel
from utils import POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER, dbtest


@pytest.fixture
def cli(tmpdir):
    cli = PGCli(pgclirc_file=str(tmpdir.join("rcfile")))
    cli.pgexecute = MagicMock(dbname="workforce", user="postgres", extra_args={"hostaddr": "127.0.0.1", "connect_timeout": 15})
    cli._target = ("workforce.internal", 5432)
    cli._tunnel_manager = MagicMock()
    cli._tunnel_manager.tunnel = MagicMock(name="old tunnel")
    return cli


def _tunnel_to(manager, local_port):
    """start_tunnel() that opens a new tunnel the way SSHTunnelManager does."""

    def start_tunnel(host, port=5432, dsn_alias=None):
        manager.tunnel = MagicMock(name="new tunnel")
        return "127.0.0.1", local_port

    return start_tunnel


def test_c_to_another_tunnelled_host_gets_its_own_endpoint(cli):
    old = cli._tunnel_manager.tunnel
    cli._tunnel_manager.start_tunnel.side_effect = _tunnel_to(cli._tunnel_manager, 50001)
    with patch("pgcli.main.pgpass.lookup_password", return_value="other-secret") as lookup:
        list(cli.change_db("postgres postgres other.internal 5432"))

    kwargs = cli.pgexecute.connect.call_args.kwargs
    assert (kwargs["host"], kwargs["hostaddr"], kwargs["port"]) == ("other.internal", "127.0.0.1", 50001)
    assert kwargs["reset_endpoint"] is True
    # .pgpass is asked about the real server, not the tunnel's local port.
    lookup.assert_called_once_with("other.internal", 5432, "postgres", "postgres")
    assert kwargs["password"] == "other-secret"
    old.stop.assert_called_once()
    assert cli._target == ("other.internal", 5432)


def test_c_to_a_host_without_a_tunnel_drops_the_old_hostaddr(cli):
    old = cli._tunnel_manager.tunnel
    cli._tunnel_manager.start_tunnel.return_value = ("other.internal", 5432)
    list(cli.change_db("postgres postgres other.internal 5432"))

    kwargs = cli.pgexecute.connect.call_args.kwargs
    assert (kwargs["host"], kwargs["port"]) == ("other.internal", 5432)
    assert "hostaddr" not in kwargs
    assert kwargs["reset_endpoint"] is True
    old.stop.assert_called_once()
    assert cli._tunnel_manager.tunnel is None


def test_failed_c_keeps_the_previous_tunnel(cli):
    old = cli._tunnel_manager.tunnel
    cli._tunnel_manager.start_tunnel.side_effect = _tunnel_to(cli._tunnel_manager, 50001)
    cli.pgexecute.connect.side_effect = OperationalError("no route")
    with patch("pgcli.main.pgpass.lookup_password", return_value=None):
        list(cli.change_db("postgres postgres other.internal 5432"))

    old.stop.assert_not_called()
    assert cli._tunnel_manager.tunnel is old
    assert cli._target == ("workforce.internal", 5432)


def test_c_naming_the_same_server_keeps_the_endpoint(cli):
    list(cli.change_db("otherdb postgres workforce.internal 5432"))

    cli._tunnel_manager.start_tunnel.assert_not_called()
    kwargs = cli.pgexecute.connect.call_args.kwargs
    assert kwargs["database"] == "otherdb"
    assert kwargs.get("host") is None and kwargs.get("port") is None
    assert kwargs["hostaddr"] == "127.0.0.1"


def test_reconnect_revives_a_dead_tunnel(cli):
    old = cli._tunnel_manager.tunnel
    old.is_active = False
    cli._tunnel_manager.start_tunnel.side_effect = _tunnel_to(cli._tunnel_manager, 50002)
    cli.auto_retry_closed_connection = False
    with patch("pgcli.main.confirm", return_value=False):
        cli._handle_server_closed_connection("select 1")

    old.stop.assert_called_once()
    cli._tunnel_manager.start_tunnel.assert_called_once_with(host="workforce.internal", port=5432, dsn_alias=cli.dsn_alias)
    assert cli.pgexecute.connect.call_args.kwargs == {"port": 50002}


def test_reconnect_with_a_live_tunnel_does_not_restart_it(cli):
    cli._tunnel_manager.tunnel.is_active = True
    cli.auto_retry_closed_connection = False
    with patch("pgcli.main.confirm", return_value=False):
        cli._handle_server_closed_connection("select 1")

    cli._tunnel_manager.start_tunnel.assert_not_called()
    assert cli.pgexecute.connect.call_args.kwargs == {}


def test_tunnel_is_inactive_once_its_ssh_transport_died():
    tunnel = _NativeSSHTunnel.__new__(_NativeSSHTunnel)
    tunnel._is_active = True
    tunnel._ssh_client = MagicMock()
    tunnel._ssh_client.get_transport.return_value.is_active.return_value = True
    assert tunnel.is_active
    tunnel._ssh_client.get_transport.return_value.is_active.return_value = False
    assert not tunnel.is_active


@pytest.mark.parametrize(
    "previous",
    [
        {
            "dbname": "postgres",
            "user": "postgres",
            "password": "old-secret",
            "host": "workforce.internal",
            "hostaddr": "127.0.0.1",
            "port": 50001,
        },
        {"dsn": "host=workforce.internal hostaddr=127.0.0.1 port=50001 dbname=postgres user=postgres password=old-secret"},
    ],
    ids=["params", "dsn"],
)
def test_reset_endpoint_does_not_carry_the_old_password_over(previous):
    # Like psql, the password is reused only while host, port and user stay
    # the same; another server gets its own from .pgpass or the prompt.
    from psycopg.conninfo import conninfo_to_dict

    executor = PGExecute.__new__(PGExecute)
    executor._conn_params = previous
    with patch("pgcli.pgexecute.psycopg.connect", side_effect=OperationalError("stop here")) as connect:
        with pytest.raises(OperationalError):
            executor.connect(host="other.internal", port=5432, reset_endpoint=True)

    params = conninfo_to_dict(connect.call_args.args[0])
    assert "password" not in params and "hostaddr" not in params
    assert (params["host"], params["port"]) == ("other.internal", "5432")


@dbtest
def test_reset_endpoint_drops_the_previous_server():
    # The first connection goes through hostaddr, the way a tunnel's does; the
    # host name itself does not even resolve.
    server_ip = socket.gethostbyname(POSTGRES_HOST)
    executor = PGExecute("postgres", POSTGRES_USER, POSTGRES_PASSWORD, "db.example.invalid", POSTGRES_PORT, None, hostaddr=server_ip)
    # The previous password is dropped with the endpoint; \c brings the new one.
    executor.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, password=POSTGRES_PASSWORD, reset_endpoint=True)
    assert "hostaddr" not in executor._conn_params
    assert executor._conn_params["host"] == POSTGRES_HOST
    with executor.conn.cursor() as cur:
        cur.execute("select 1")
        assert cur.fetchone()[0] == 1


@dbtest
def test_copy_follows_c_to_another_database_on_a_dsn_connection():
    # copy() rebuilt the executor from its parameters, and with a DSN the loose
    # dbname from \c was dropped: the completion refresher read the old database.
    from psycopg.conninfo import make_conninfo

    dsn = make_conninfo(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname="postgres", user=POSTGRES_USER)
    executor = PGExecute(dsn=dsn, password=POSTGRES_PASSWORD)
    executor.connect(database="template1")
    assert executor.conn.info.dbname == "template1"
    assert executor.copy().conn.info.dbname == "template1"


@dbtest
def test_reset_endpoint_keeps_a_dsn_connection_copyable():
    # With a DSN, connect() expects host and port inside it and drops loose
    # ones when copying; the completion refresher's copy() then dialed the
    # default 127.0.0.1:5432 after \c over a tunnel.
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    server_ip = socket.gethostbyname(POSTGRES_HOST)
    dsn = make_conninfo(host="db.example.invalid", hostaddr=server_ip, port=POSTGRES_PORT, dbname="postgres", user=POSTGRES_USER)
    executor = PGExecute(dsn=dsn, password=POSTGRES_PASSWORD)
    executor.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, hostaddr=server_ip, password=POSTGRES_PASSWORD, reset_endpoint=True)

    clone = executor.copy()
    params = conninfo_to_dict(clone._conn_params["dsn"])
    assert (params["host"], params["port"], params["hostaddr"]) == (POSTGRES_HOST, str(POSTGRES_PORT), server_ip)
    with clone.conn.cursor() as cur:
        cur.execute("select 1")
        assert cur.fetchone()[0] == 1


def test_a_failed_tunnel_on_c_keeps_the_session(cli, capsys):
    # start_tunnel() used to sys.exit(1) on failure. Fine at startup, fatal
    # once \c opened tunnels too: SystemExit sailed through change_db and the
    # REPL died with the session.
    old = cli._tunnel_manager.tunnel
    cli._tunnel_manager.start_tunnel.side_effect = SSHTunnelError("Unable to connect to port 22 on bastion")
    list(cli.change_db("postgres postgres other.internal 5432"))

    captured = capsys.readouterr()
    assert "SSH tunnel error: Unable to connect to port 22 on bastion" in captured.err
    assert "Previous connection kept" in captured.out
    cli.pgexecute.connect.assert_not_called()
    assert cli._tunnel_manager.tunnel is old
    assert cli._target == ("workforce.internal", 5432)


def test_a_failed_tunnel_on_reconnect_is_reported_and_tried_again_later(cli, capsys):
    old = cli._tunnel_manager.tunnel
    old.is_active = False
    cli._tunnel_manager.start_tunnel.side_effect = SSHTunnelError("bastion down")
    cli.auto_retry_closed_connection = False
    cli._handle_server_closed_connection("select 1")

    captured = capsys.readouterr()
    assert "Reconnect Failed" in captured.out
    assert "SSH tunnel error: bastion down" in captured.err
    cli.pgexecute.connect.assert_not_called()
    # The dead tunnel stays registered: the next reconnect tries the bastion
    # again instead of dialing the dead local port for good.
    assert cli._tunnel_manager.tunnel is old

    cli._tunnel_manager.start_tunnel.side_effect = _tunnel_to(cli._tunnel_manager, 50003)
    with patch("pgcli.main.confirm", return_value=False):
        cli._handle_server_closed_connection("select 1")
    assert cli.pgexecute.connect.call_args.kwargs == {"port": 50003}


def test_a_failed_tunnel_at_startup_still_exits(tmpdir, capsys):
    cli = PGCli(pgclirc_file=str(tmpdir.join("rcfile")))
    with patch.object(cli._tunnel_manager, "start_tunnel", side_effect=SSHTunnelError("no route to bastion")):
        with pytest.raises(SystemExit) as e:
            cli.connect(database="db", host="db.internal", user="me", port=5432)
    assert e.value.code == 1
    assert "SSH tunnel error: no route to bastion" in capsys.readouterr().err

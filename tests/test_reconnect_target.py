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
from pgcli.ssh_tunnel import _NativeSSHTunnel
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


@dbtest
def test_reset_endpoint_drops_the_previous_server():
    # The first connection goes through hostaddr, the way a tunnel's does; the
    # host name itself does not even resolve.
    server_ip = socket.gethostbyname(POSTGRES_HOST)
    executor = PGExecute("postgres", POSTGRES_USER, POSTGRES_PASSWORD, "db.example.invalid", POSTGRES_PORT, None, hostaddr=server_ip)
    executor.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, reset_endpoint=True)
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
    executor.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, hostaddr=server_ip, reset_endpoint=True)

    clone = executor.copy()
    params = conninfo_to_dict(clone._conn_params["dsn"])
    assert (params["host"], params["port"], params["hostaddr"]) == (POSTGRES_HOST, str(POSTGRES_PORT), server_ip)
    with clone.conn.cursor() as cur:
        cur.execute("select 1")
        assert cur.fetchone()[0] == 1

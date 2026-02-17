import logging
import os
from unittest.mock import patch, MagicMock, ANY, call

import pytest
from configobj import ConfigObj
from click.testing import CliRunner

from pgcli.main import cli, notify_callback, PGCli
from pgcli.pgexecute import PGExecute
from pgcli.ssh_tunnel import (
    SSHTunnelManager,
    get_tunnel_manager_from_config,
    SSH_TUNNEL_SUPPORT,
    _NativeSSHTunnel,
)


# =============================================================================
# Fixtures
# =============================================================================

TUNNEL_LOCAL_PORT = 1111


@pytest.fixture
def mock_tunnel_manager():
    """Mock SSHTunnelManager for main.py integration tests."""
    with patch("pgcli.main.SSHTunnelManager") as mock_cls:
        mock_mgr = MagicMock(spec=SSHTunnelManager)
        mock_mgr.start_tunnel.return_value = ("127.0.0.1", TUNNEL_LOCAL_PORT)
        mock_tunnel = MagicMock()
        mock_tunnel.local_bind_port = TUNNEL_LOCAL_PORT
        mock_tunnel.is_active = True
        mock_mgr.tunnel = mock_tunnel
        mock_cls.return_value = mock_mgr
        yield mock_cls, mock_mgr


@pytest.fixture
def mock_pgexecute() -> MagicMock:
    with patch.object(PGExecute, "__init__", return_value=None) as mock_pgexecute:
        yield mock_pgexecute


@pytest.fixture
def mock_native_tunnel():
    """Mock paramiko + socketserver for SSHTunnelManager unit tests."""
    with patch("pgcli.ssh_tunnel.paramiko.SSHClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_transport = MagicMock()
        mock_client.get_transport.return_value = mock_transport
        mock_client_cls.return_value = mock_client

        with patch("pgcli.ssh_tunnel.socketserver.ThreadingTCPServer") as mock_srv_cls:
            mock_server = MagicMock()
            mock_server.server_address = ("127.0.0.1", 12345)
            mock_server.daemon_threads = True
            mock_srv_cls.return_value = mock_server

            with patch("pgcli.ssh_tunnel.threading.Thread") as mock_thread_cls:
                mock_thread = MagicMock()
                mock_thread_cls.return_value = mock_thread

                yield {
                    "client_cls": mock_client_cls,
                    "client": mock_client,
                    "transport": mock_transport,
                    "server_cls": mock_srv_cls,
                    "server": mock_server,
                    "thread_cls": mock_thread_cls,
                    "thread": mock_thread,
                }


# =============================================================================
# Layer 1: main.py integration tests (mock SSHTunnelManager)
# =============================================================================


def test_ssh_tunnel(mock_tunnel_manager, mock_pgexecute: MagicMock) -> None:
    mock_cls, mock_mgr = mock_tunnel_manager

    # Test with just a host
    tunnel_url = "some.host"
    db_params = {
        "database": "dbname",
        "host": "db.host",
        "user": "db_user",
        "passwd": "db_passwd",
    }

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(**db_params)

    # SSHTunnelManager should be created with the tunnel URL
    mock_cls.assert_called_once()
    init_kwargs = mock_cls.call_args[1]
    assert init_kwargs["ssh_tunnel_url"] == tunnel_url

    # start_tunnel should be called with the db host/port
    mock_mgr.start_tunnel.assert_called_once_with(
        host="db.host", port=5432, dsn_alias=None,
    )

    # PGExecute should get original host, tunnel port, and hostaddr
    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    assert call_args[0] == db_params["database"]
    assert call_args[3] == db_params["host"]  # Original host preserved
    assert call_args[4] == TUNNEL_LOCAL_PORT
    assert call_kwargs.get("hostaddr") == "127.0.0.1"

    mock_cls.reset_mock()
    mock_mgr.reset_mock()
    mock_pgexecute.reset_mock()

    # Test with a full url and with a specific db port
    tunnel_url = "ssh://tunnel_user:tunnel_pass@some.other.host:1022"
    db_params["port"] = 1234

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(**db_params)

    init_kwargs = mock_cls.call_args[1]
    assert init_kwargs["ssh_tunnel_url"] == tunnel_url

    mock_mgr.start_tunnel.assert_called_once_with(
        host="db.host", port=1234, dsn_alias=None,
    )

    call_args, call_kwargs = mock_pgexecute.call_args
    assert call_args[3] == db_params["host"]  # Original host preserved
    assert call_args[4] == TUNNEL_LOCAL_PORT
    assert call_kwargs.get("hostaddr") == "127.0.0.1"

    mock_cls.reset_mock()
    mock_mgr.reset_mock()
    mock_pgexecute.reset_mock()

    # Test with DSN
    dsn = f"user={db_params['user']} password={db_params['passwd']} host={db_params['host']} port={db_params['port']}"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(dsn=dsn)

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    dsn_arg = call_args[5]  # DSN is the 6th positional argument
    assert f"host={db_params['host']}" in dsn_arg
    assert "hostaddr=127.0.0.1" in dsn_arg
    assert f"port={TUNNEL_LOCAL_PORT}" in dsn_arg


def test_cli_with_tunnel() -> None:
    runner = CliRunner()
    tunnel_url = "mytunnel"
    with patch.object(PGCli, "__init__", autospec=True, return_value=None) as mock_pgcli:
        runner.invoke(cli, ["--ssh-tunnel", tunnel_url])
        mock_pgcli.assert_called_once()
        call_args, call_kwargs = mock_pgcli.call_args
        assert call_kwargs["ssh_tunnel_url"] == tunnel_url


def test_config(tmpdir: os.PathLike, mock_tunnel_manager, mock_pgexecute: MagicMock) -> None:
    mock_cls, mock_mgr = mock_tunnel_manager
    pgclirc = str(tmpdir.join("rcfile"))

    tunnel_user = "tunnel_user"
    tunnel_passwd = "tunnel_pass"
    tunnel_host = "tunnel.host"
    tunnel_port = 1022
    tunnel_url = f"{tunnel_user}:{tunnel_passwd}@{tunnel_host}:{tunnel_port}"

    tunnel2_url = "tunnel2.host"

    config = ConfigObj()
    config.filename = pgclirc
    config["ssh tunnels"] = {}
    config["ssh tunnels"][r".*\.com"] = tunnel_url
    config["ssh tunnels"][r"hello-.*"] = tunnel2_url
    config.write()

    # Unmatched host: start_tunnel returns unchanged host/port
    mock_mgr.start_tunnel.return_value = ("unmatched.host", 5432)
    pgcli = PGCli(pgclirc_file=pgclirc)
    pgcli.connect(host="unmatched.host")
    # SSHTunnelManager should have been created with the config
    init_kwargs = mock_cls.call_args[1]
    assert r".*\.com" in init_kwargs["ssh_tunnel_config"]
    assert r"hello-.*" in init_kwargs["ssh_tunnel_config"]
    mock_cls.reset_mock()
    mock_mgr.reset_mock()
    mock_pgexecute.reset_mock()

    # Matched host: start_tunnel returns tunnel address
    mock_mgr.start_tunnel.return_value = ("127.0.0.1", TUNNEL_LOCAL_PORT)
    pgcli = PGCli(pgclirc_file=pgclirc)
    pgcli.connect(host="matched.host.com")
    mock_mgr.start_tunnel.assert_called_once_with(
        host="matched.host.com", port=5432, dsn_alias=None,
    )
    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    assert call_kwargs.get("hostaddr") == "127.0.0.1"


def test_ssh_tunnel_with_uri(mock_tunnel_manager, mock_pgexecute: MagicMock) -> None:
    """Test that connect_uri passes DSN for .pgpass compatibility"""
    mock_cls, mock_mgr = mock_tunnel_manager
    tunnel_url = "tunnel.host"
    uri = "postgresql://testuser@db.example.com:5432/testdb"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect_uri(uri)

    mock_mgr.start_tunnel.assert_called_once()
    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args

    dsn_arg = call_args[5]
    assert dsn_arg
    assert "host=db.example.com" in dsn_arg
    assert "hostaddr=127.0.0.1" in dsn_arg
    assert f"port={TUNNEL_LOCAL_PORT}" in dsn_arg
    assert "user=testuser" in dsn_arg
    assert "dbname=testdb" in dsn_arg


def test_ssh_tunnel_preserves_original_host_for_pgpass(
    mock_tunnel_manager, mock_pgexecute: MagicMock
) -> None:
    """Test that original hostname is preserved for .pgpass lookup"""
    mock_cls, mock_mgr = mock_tunnel_manager
    tunnel_url = "tunnel.host"
    original_host = "production-db.aws.amazonaws.com"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(database="mydb", host=original_host, user="admin")

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    assert call_args[3] == original_host
    assert call_kwargs.get("hostaddr") == "127.0.0.1"


def test_ssh_tunnel_with_dsn_string(
    mock_tunnel_manager, mock_pgexecute: MagicMock
) -> None:
    """Test SSH tunnel with DSN connection string"""
    mock_cls, mock_mgr = mock_tunnel_manager
    tunnel_url = "tunnel.host"
    dsn = "host=db.prod.com port=5432 dbname=myapp user=appuser"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(dsn=dsn)

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    dsn_arg = call_args[5]
    assert "host=db.prod.com" in dsn_arg
    assert "hostaddr=127.0.0.1" in dsn_arg
    assert f"port={TUNNEL_LOCAL_PORT}" in dsn_arg


def test_no_ssh_tunnel_does_not_set_hostaddr(mock_pgexecute: MagicMock) -> None:
    """Test that hostaddr is not set when SSH tunnel is not used"""
    pgcli = PGCli()
    pgcli.connect(database="mydb", host="localhost", user="user")

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    assert "hostaddr" not in call_kwargs


def test_ssh_tunnel_with_port_in_dsn(
    mock_tunnel_manager, mock_pgexecute: MagicMock
) -> None:
    """Test that custom port in DSN is handled correctly with SSH tunnel"""
    mock_cls, mock_mgr = mock_tunnel_manager
    tunnel_url = "tunnel.host"
    dsn = "postgresql://user@db.example.com:6543/testdb"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect_uri(dsn)

    # Verify start_tunnel was called with the original port from DSN
    mock_mgr.start_tunnel.assert_called_once_with(
        host="db.example.com", port=6543, dsn_alias=None,
    )

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    dsn_arg = call_args[5]
    assert f"port={TUNNEL_LOCAL_PORT}" in dsn_arg


def test_connect_uri_without_ssh_tunnel(mock_pgexecute: MagicMock) -> None:
    """Test that connect_uri works correctly without SSH tunnel"""
    uri = "postgresql://testuser:testpass@localhost:5432/testdb"

    pgcli = PGCli()
    pgcli.connect_uri(uri)

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    dsn_arg = call_args[5]
    assert uri == dsn_arg
    assert "hostaddr" not in call_kwargs


# =============================================================================
# Layer 2: SSHTunnelManager unit tests (mock paramiko + socketserver)
# =============================================================================


class TestSSHTunnelManager:
    """Tests for SSHTunnelManager class."""

    def test_init_with_explicit_url(self):
        """Test initialization with explicit SSH tunnel URL."""
        manager = SSHTunnelManager(ssh_tunnel_url="ssh://user@host:22")
        assert manager.ssh_tunnel_url == "ssh://user@host:22"
        assert manager.tunnel is None

    def test_init_with_config(self):
        """Test initialization with config dictionaries."""
        ssh_config = {".*\\.prod\\.example\\.com": "bastion.example.com"}
        dsn_config = {"prod-.*": "ssh://user@bastion:22"}

        manager = SSHTunnelManager(
            ssh_tunnel_config=ssh_config,
            dsn_ssh_tunnel_config=dsn_config,
        )
        assert manager.ssh_tunnel_config == ssh_config
        assert manager.dsn_ssh_tunnel_config == dsn_config

    def test_find_tunnel_url_explicit(self):
        """Test that explicit URL takes precedence."""
        manager = SSHTunnelManager(
            ssh_tunnel_url="ssh://explicit@host:22",
            ssh_tunnel_config={".*": "ssh://config@host:22"},
        )
        url = manager.find_tunnel_url(host="anyhost.com")
        assert url == "ssh://explicit@host:22"

    def test_find_tunnel_url_dsn_match(self):
        """Test DSN-based tunnel URL lookup."""
        manager = SSHTunnelManager(
            dsn_ssh_tunnel_config={
                "prod-.*": "ssh://prod-bastion:22",
                "staging-.*": "ssh://staging-bastion:22",
            }
        )
        url = manager.find_tunnel_url(dsn_alias="prod-main")
        assert url == "ssh://prod-bastion:22"

    def test_find_tunnel_url_host_match(self):
        """Test host-based tunnel URL lookup."""
        manager = SSHTunnelManager(
            ssh_tunnel_config={
                ".*\\.prod\\.example\\.com": "ssh://prod-bastion:22",
                ".*\\.staging\\.example\\.com": "ssh://staging-bastion:22",
            }
        )
        url = manager.find_tunnel_url(host="db1.prod.example.com")
        assert url == "ssh://prod-bastion:22"

    def test_find_tunnel_url_no_match(self):
        """Test when no tunnel matches."""
        manager = SSHTunnelManager(
            ssh_tunnel_config={".*\\.prod\\.example\\.com": "ssh://bastion:22"}
        )
        url = manager.find_tunnel_url(host="localhost")
        assert url is None

    def test_find_tunnel_url_no_partial_host_match(self):
        """Test that partial hostname matches are rejected (re.fullmatch)."""
        manager = SSHTunnelManager(
            ssh_tunnel_config={"prod": "ssh://bastion:22"}
        )
        assert manager.find_tunnel_url(host="nonprod") is None
        assert manager.find_tunnel_url(host="prod.extra.com") is None
        assert manager.find_tunnel_url(host="prod") == "ssh://bastion:22"

    def test_find_tunnel_url_no_partial_dsn_match(self):
        """Test that partial DSN matches are rejected (re.fullmatch)."""
        manager = SSHTunnelManager(
            dsn_ssh_tunnel_config={"prod": "ssh://bastion:22"}
        )
        assert manager.find_tunnel_url(dsn_alias="nonprod") is None
        assert manager.find_tunnel_url(dsn_alias="prod-extra") is None
        assert manager.find_tunnel_url(dsn_alias="prod") == "ssh://bastion:22"

    def test_find_tunnel_url_dsn_takes_precedence(self):
        """Test that DSN match takes precedence over host match."""
        manager = SSHTunnelManager(
            ssh_tunnel_config={".*": "ssh://host-bastion:22"},
            dsn_ssh_tunnel_config={"mydsn": "ssh://dsn-bastion:22"},
        )
        url = manager.find_tunnel_url(host="anyhost.com", dsn_alias="mydsn")
        assert url == "ssh://dsn-bastion:22"

    def test_start_tunnel_no_config(self):
        """Test start_tunnel returns original host/port when no tunnel configured."""
        manager = SSHTunnelManager()
        host, port = manager.start_tunnel(host="db.example.com", port=5432)
        assert host == "db.example.com"
        assert port == 5432
        assert manager.tunnel is None

    def test_start_tunnel_with_config(self, mock_native_tunnel):
        """Test start_tunnel creates and starts native tunnel."""
        manager = SSHTunnelManager(
            ssh_tunnel_url="ssh://user@bastion.example.com:22",
            logger=logging.getLogger("test"),
        )

        host, port = manager.start_tunnel(host="db.internal", port=5432)

        assert host == "127.0.0.1"
        assert port == 12345  # from mock server_address
        assert manager.tunnel is not None
        assert manager.tunnel.is_active

        # Verify SSH connection params
        mock_native_tunnel["client"].connect.assert_called_once()
        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert connect_kwargs["hostname"] == "bastion.example.com"
        assert connect_kwargs["port"] == 22
        assert connect_kwargs["username"] == "user"
        assert connect_kwargs["allow_agent"] is True
        assert connect_kwargs["look_for_keys"] is False

        # Verify ThreadingTCPServer created on port 0 (auto-assign)
        mock_native_tunnel["server_cls"].assert_called_once()
        srv_args = mock_native_tunnel["server_cls"].call_args[0]
        assert srv_args[0] == ("127.0.0.1", 0)

        # Verify background thread started
        mock_native_tunnel["thread"].start.assert_called_once()

    def test_start_tunnel_with_password(self, mock_native_tunnel):
        """Test start_tunnel passes SSH password from URL."""
        manager = SSHTunnelManager(
            ssh_tunnel_url="ssh://user:s3cret@bastion:22",
            logger=logging.getLogger("test"),
        )

        host, port = manager.start_tunnel(host="db.internal", port=5432)

        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert connect_kwargs["password"] == "s3cret"

    def test_stop_tunnel_no_tunnel(self):
        """Test stop_tunnel when no tunnel exists."""
        manager = SSHTunnelManager()
        manager.stop_tunnel()  # Should not raise

    def test_stop_tunnel_active(self):
        """Test stop_tunnel when tunnel is active."""
        mock_tunnel = MagicMock(spec=_NativeSSHTunnel)
        mock_tunnel.is_active = True

        manager = SSHTunnelManager()
        manager.tunnel = mock_tunnel
        manager.stop_tunnel()

        mock_tunnel.stop.assert_called_once()
        assert manager.tunnel is None


class TestNativeSSHTunnel:
    """Tests for _NativeSSHTunnel class."""

    def test_start_and_stop(self, mock_native_tunnel):
        """Test tunnel start/stop lifecycle."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
            allow_agent=True,
            logger=logging.getLogger("test"),
        )

        assert not tunnel.is_active
        assert tunnel.local_bind_port is None

        tunnel.start()

        assert tunnel.is_active
        assert tunnel.local_bind_port == 12345
        mock_native_tunnel["client"].load_system_host_keys.assert_called_once()
        mock_native_tunnel["client"].set_missing_host_key_policy.assert_called_once()

        tunnel.stop()

        assert not tunnel.is_active
        mock_native_tunnel["server"].shutdown.assert_called_once()
        mock_native_tunnel["client"].close.assert_called_once()

    def test_look_for_keys_disabled(self, mock_native_tunnel):
        """Test that look_for_keys=False prevents scanning ~/.ssh/ for keys."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
        )
        tunnel.start()

        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert connect_kwargs["look_for_keys"] is False

    def test_allow_agent_configurable(self, mock_native_tunnel):
        """Test that allow_agent is passed through."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            allow_agent=False,
        )
        tunnel.start()

        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert connect_kwargs["allow_agent"] is False

    def test_proxy_command_passed(self, mock_native_tunnel):
        """Test that ssh_proxy is passed as sock parameter."""
        mock_proxy = MagicMock()
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_proxy=mock_proxy,
        )
        tunnel.start()

        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert connect_kwargs["sock"] is mock_proxy


class TestGetTunnelManagerFromConfig:
    """Tests for get_tunnel_manager_from_config function."""

    def test_empty_config(self):
        """Test with empty config."""
        manager = get_tunnel_manager_from_config({})
        assert manager.ssh_tunnel_url is None
        assert manager.ssh_tunnel_config == {}
        assert manager.dsn_ssh_tunnel_config == {}

    def test_with_ssh_tunnels_config(self):
        """Test with ssh tunnels section in config."""
        config = {
            "ssh tunnels": {
                ".*\\.prod\\.example\\.com": "ssh://bastion:22",
            }
        }
        manager = get_tunnel_manager_from_config(config)
        assert manager.ssh_tunnel_config == config["ssh tunnels"]

    def test_with_dsn_ssh_tunnels_config(self):
        """Test with dsn ssh tunnels section in config."""
        config = {
            "dsn ssh tunnels": {
                "prod-.*": "ssh://bastion:22",
            }
        }
        manager = get_tunnel_manager_from_config(config)
        assert manager.dsn_ssh_tunnel_config == config["dsn ssh tunnels"]

    def test_with_explicit_url(self):
        """Test that explicit URL overrides config."""
        config = {
            "ssh tunnels": {".*": "ssh://config-bastion:22"},
        }
        manager = get_tunnel_manager_from_config(
            config, ssh_tunnel_url="ssh://explicit-bastion:22"
        )
        assert manager.ssh_tunnel_url == "ssh://explicit-bastion:22"

    def test_with_custom_logger(self):
        """Test with custom logger."""
        logger = logging.getLogger("custom")
        manager = get_tunnel_manager_from_config({}, logger=logger)
        assert manager.logger == logger

    def test_allow_agent_from_config(self):
        """Test allow_agent is read from config."""
        config = {"ssh tunnels": {"allow_agent": "False"}}
        manager = get_tunnel_manager_from_config(config)
        assert manager.allow_agent is False

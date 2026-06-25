import logging
import os
from unittest.mock import patch, MagicMock, mock_open

import paramiko
import pytest
from configobj import ConfigObj
from click.testing import CliRunner

from pgcli.main import cli, PGCli
from pgcli.pgexecute import PGExecute
from pgcli.ssh_tunnel import (
    SSHTunnelManager,
    get_tunnel_manager_from_config,
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
        host="db.host",
        port=5432,
        dsn_alias=None,
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
        host="db.host",
        port=1234,
        dsn_alias=None,
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
        host="matched.host.com",
        port=5432,
        dsn_alias=None,
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


def test_ssh_tunnel_preserves_original_host_for_pgpass(mock_tunnel_manager, mock_pgexecute: MagicMock) -> None:
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


def test_ssh_tunnel_with_dsn_string(mock_tunnel_manager, mock_pgexecute: MagicMock) -> None:
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


def test_ssh_tunnel_with_port_in_dsn(mock_tunnel_manager, mock_pgexecute: MagicMock) -> None:
    """Test that custom port in DSN is handled correctly with SSH tunnel"""
    mock_cls, mock_mgr = mock_tunnel_manager
    tunnel_url = "tunnel.host"
    dsn = "postgresql://user@db.example.com:6543/testdb"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect_uri(dsn)

    # Verify start_tunnel was called with the original port from DSN
    mock_mgr.start_tunnel.assert_called_once_with(
        host="db.example.com",
        port=6543,
        dsn_alias=None,
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
        manager = SSHTunnelManager(ssh_tunnel_config={".*\\.prod\\.example\\.com": "ssh://bastion:22"})
        url = manager.find_tunnel_url(host="localhost")
        assert url is None

    def test_find_tunnel_url_no_partial_host_match(self):
        """Test that partial hostname matches are rejected (re.fullmatch)."""
        manager = SSHTunnelManager(ssh_tunnel_config={"prod": "ssh://bastion:22"})
        assert manager.find_tunnel_url(host="nonprod") is None
        assert manager.find_tunnel_url(host="prod.extra.com") is None
        assert manager.find_tunnel_url(host="prod") == "ssh://bastion:22"

    def test_find_tunnel_url_no_partial_dsn_match(self):
        """Test that partial DSN matches are rejected (re.fullmatch)."""
        manager = SSHTunnelManager(dsn_ssh_tunnel_config={"prod": "ssh://bastion:22"})
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

    def test_key_filenames_passed_to_connect(self, mock_native_tunnel):
        """Test that key_filenames are passed as key_filename to connect()."""
        key_files = ["/home/user/.ssh/id_ed25519", "/home/user/.ssh/id_rsa"]
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
            key_filenames=key_files,
        )
        tunnel.start()

        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert connect_kwargs["key_filename"] == key_files
        assert connect_kwargs["look_for_keys"] is False  # Still disabled

    def test_no_key_filenames_omits_key_filename(self, mock_native_tunnel):
        """Test that key_filename is NOT passed when key_filenames is None."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
        )
        tunnel.start()

        connect_kwargs = mock_native_tunnel["client"].connect.call_args[1]
        assert "key_filename" not in connect_kwargs

    def test_host_key_policy_auto_add(self, mock_native_tunnel):
        """Test that auto-add policy sets AutoAddPolicy."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            host_key_policy="auto-add",
        )
        tunnel.start()
        policy_arg = mock_native_tunnel["client"].set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, paramiko.AutoAddPolicy)

    def test_host_key_policy_warn(self, mock_native_tunnel):
        """Test that warn policy sets WarningPolicy."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            host_key_policy="warn",
        )
        tunnel.start()
        policy_arg = mock_native_tunnel["client"].set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, paramiko.WarningPolicy)

    def test_host_key_policy_reject(self, mock_native_tunnel):
        """Test that reject policy sets RejectPolicy."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            host_key_policy="reject",
        )
        tunnel.start()
        policy_arg = mock_native_tunnel["client"].set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, paramiko.RejectPolicy)

    def test_host_key_policy_default_is_auto_add(self, mock_native_tunnel):
        """Test that default policy is auto-add."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
        )
        tunnel.start()
        policy_arg = mock_native_tunnel["client"].set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, paramiko.AutoAddPolicy)

    def test_host_key_policy_invalid_falls_back_to_auto_add(self, mock_native_tunnel):
        """Test that invalid policy name falls back to AutoAddPolicy."""
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            host_key_policy="nonsense",
        )
        tunnel.start()
        policy_arg = mock_native_tunnel["client"].set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, paramiko.AutoAddPolicy)


class TestSSHTunnelIdentityFile:
    """Tests for IdentityFile reading from SSH config."""

    def _make_manager_with_ssh_config(self, mock_native_tunnel, host_config, tunnel_url="ssh://bastion.example.com"):
        """Helper: create manager, mock SSH config lookup, run start_tunnel."""
        mock_ssh_config = MagicMock()
        mock_ssh_config.lookup.return_value = host_config

        # Determine which identity files "exist" on disk
        existing_files = set(host_config.get("_existing_files", host_config.get("identityfile", [])))
        existing_files.add("~/.ssh/config")  # SSH config always exists

        manager = SSHTunnelManager(
            ssh_tunnel_url=tunnel_url,
            logger=logging.getLogger("test"),
        )

        with (
            patch("pgcli.ssh_tunnel.os.path.expanduser", side_effect=lambda p: p),
            patch("pgcli.ssh_tunnel.os.path.isfile", side_effect=lambda p: p in existing_files),
            patch("pgcli.ssh_tunnel.paramiko.SSHConfig") as mock_config_cls,
            patch("builtins.open", mock_open(read_data="")),
        ):
            mock_config_cls.return_value = mock_ssh_config
            host, port = manager.start_tunnel(host="db.internal", port=5432)

        return mock_native_tunnel["client"].connect.call_args[1]

    def test_start_tunnel_reads_identity_files(self, mock_native_tunnel):
        """Test that start_tunnel reads IdentityFile from SSH config and passes to connect."""
        host_config = {
            "hostname": "bastion.example.com",
            "user": "tunneluser",
            "identityfile": ["/home/user/.ssh/id_ed25519_specific", "/home/user/.ssh/id_rsa_wildcard"],
        }

        connect_kwargs = self._make_manager_with_ssh_config(mock_native_tunnel, host_config)

        assert "key_filename" in connect_kwargs
        assert connect_kwargs["key_filename"] == [
            "/home/user/.ssh/id_ed25519_specific",
            "/home/user/.ssh/id_rsa_wildcard",
        ]
        assert connect_kwargs["look_for_keys"] is False

    def test_start_tunnel_skips_nonexistent_identity_files(self, mock_native_tunnel):
        """Test that nonexistent IdentityFile entries are filtered out."""
        host_config = {
            "hostname": "bastion.example.com",
            "identityfile": ["/home/user/.ssh/id_ed25519_exists", "/home/user/.ssh/id_rsa_missing"],
            "_existing_files": ["/home/user/.ssh/id_ed25519_exists"],  # only this one exists
        }

        connect_kwargs = self._make_manager_with_ssh_config(mock_native_tunnel, host_config)

        assert "key_filename" in connect_kwargs
        assert connect_kwargs["key_filename"] == ["/home/user/.ssh/id_ed25519_exists"]

    def test_start_tunnel_no_identity_files_omits_key_filename(self, mock_native_tunnel):
        """Test that key_filename is omitted when SSH config has no IdentityFile."""
        host_config = {
            "hostname": "bastion.example.com",
            "user": "tunneluser",
        }

        connect_kwargs = self._make_manager_with_ssh_config(mock_native_tunnel, host_config)

        assert "key_filename" not in connect_kwargs

    def test_identity_file_order_preserved(self, mock_native_tunnel):
        """Test that IdentityFile order is preserved (host-specific first, wildcard after)."""
        host_config = {
            "hostname": "bastion.example.com",
            "identityfile": [
                "/home/user/.ssh/id_ed25519_host",  # host-specific (first)
                "/home/user/.ssh/id_ed25519_global",  # wildcard (second)
            ],
        }

        connect_kwargs = self._make_manager_with_ssh_config(mock_native_tunnel, host_config)

        assert connect_kwargs["key_filename"] == [
            "/home/user/.ssh/id_ed25519_host",
            "/home/user/.ssh/id_ed25519_global",
        ]


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
        manager = get_tunnel_manager_from_config(config, ssh_tunnel_url="ssh://explicit-bastion:22")
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

    def test_host_key_policy_from_config(self):
        """Test host_key_policy is read from config."""
        config = {"ssh tunnels": {"host_key_policy": "reject"}}
        manager = get_tunnel_manager_from_config(config)
        assert manager.host_key_policy == "reject"

    def test_host_key_policy_default(self):
        """Test host_key_policy defaults to auto-add."""
        manager = get_tunnel_manager_from_config({})
        assert manager.host_key_policy == "auto-add"


class TestSSHTunnelSecretEscalation:
    """Tests for saving/reusing the SSH tunnel passphrase / password (keyring).

    When agent / unencrypted-key auth fails, _NativeSSHTunnel asks a
    secret_provider for a key passphrase or SSH password and retries once,
    optionally persisting the secret via secret_saver.
    """

    def test_passphrase_escalation_retries_and_does_not_save_on_keyring_hit(self, mock_native_tunnel):
        """A passphrase from the provider (keyring hit -> should_save False) is
        used on retry, and the saver is not called."""
        mock_native_tunnel["client"].connect.side_effect = [
            paramiko.PasswordRequiredException("encrypted key"),
            None,  # retry succeeds
        ]
        provider = MagicMock(return_value=("passphrase", "s3cr3t", False))
        saver = MagicMock()

        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
            key_filenames=["/home/user/.ssh/id_rsa"],
            secret_provider=provider,
            secret_saver=saver,
            logger=logging.getLogger("test"),
        )
        tunnel.start()

        assert tunnel.is_active
        assert mock_native_tunnel["client"].connect.call_count == 2
        retry_kwargs = mock_native_tunnel["client"].connect.call_args_list[1][1]
        assert retry_kwargs["passphrase"] == "s3cr3t"
        provider.assert_called_once()
        saver.assert_not_called()

    def test_password_escalation_saves_when_requested(self, mock_native_tunnel):
        """A freshly prompted password (should_save True) triggers the saver."""
        mock_native_tunnel["client"].connect.side_effect = [
            paramiko.AuthenticationException("auth failed"),
            None,
        ]
        provider = MagicMock(return_value=("password", "hunter2", True))
        saver = MagicMock()

        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
            secret_provider=provider,
            secret_saver=saver,
            logger=logging.getLogger("test"),
        )
        tunnel.start()

        assert tunnel.is_active
        retry_kwargs = mock_native_tunnel["client"].connect.call_args_list[1][1]
        assert retry_kwargs["password"] == "hunter2"
        saver.assert_called_once()
        ctx, kind, secret = saver.call_args[0]
        assert kind == "password"
        assert secret == "hunter2"

    def test_no_provider_reraises(self, mock_native_tunnel):
        """Without a provider the auth error propagates unchanged."""
        mock_native_tunnel["client"].connect.side_effect = paramiko.AuthenticationException("nope")
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
            logger=logging.getLogger("test"),
        )
        with pytest.raises(paramiko.AuthenticationException):
            tunnel.start()

    def test_provider_returns_none_reraises(self, mock_native_tunnel):
        """If the provider declines (None), the original error is raised."""
        mock_native_tunnel["client"].connect.side_effect = paramiko.PasswordRequiredException("enc")
        provider = MagicMock(return_value=None)
        tunnel = _NativeSSHTunnel(
            ssh_hostname="bastion",
            ssh_port=22,
            remote_host="db.internal",
            remote_port=5432,
            ssh_username="testuser",
            key_filenames=["/home/user/.ssh/id_rsa"],
            secret_provider=provider,
            logger=logging.getLogger("test"),
        )
        with pytest.raises(paramiko.PasswordRequiredException):
            tunnel.start()

    def test_proxy_command_rebuilt_per_attempt_on_retry(self, mock_native_tunnel):
        """ProxyCommand (single-use socket) must be rebuilt on the retry so the
        keyring passphrase escalation works behind a ProxyCommand."""
        mock_native_tunnel["client"].connect.side_effect = [
            paramiko.PasswordRequiredException("encrypted key"),
            None,
        ]
        provider = MagicMock(return_value=("passphrase", "pp", False))
        with patch("pgcli.ssh_tunnel.paramiko.ProxyCommand") as mock_proxy:
            tunnel = _NativeSSHTunnel(
                ssh_hostname="bastion",
                ssh_port=22,
                remote_host="db.internal",
                remote_port=5432,
                ssh_username="testuser",
                key_filenames=["/home/user/.ssh/id_rsa"],
                ssh_proxy_command="corkscrew proxy 8080 %h %p",
                secret_provider=provider,
                logger=logging.getLogger("test"),
            )
            tunnel.start()
        # One ProxyCommand built per connect attempt (initial + retry).
        assert mock_proxy.call_count == 2


class TestPGCliSSHSecretProvider:
    """Tests for PGCli._ssh_tunnel_secret_provider / _secret_saver / keyring key."""

    def _cli(self, tmpdir):
        rcfile = str(tmpdir.join("rcfile"))
        return PGCli(pgclirc_file=rcfile)

    def test_keyring_key_naming(self, tmpdir):
        cli = self._cli(tmpdir)
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        assert cli._ssh_tunnel_keyring_key(ctx, "passphrase") == "ssh-tunnel-passphrase:/k/id_rsa"
        assert cli._ssh_tunnel_keyring_key(ctx, "password") == "ssh-tunnel-password:u@h:22"

    def test_provider_keyring_hit_no_prompt(self, tmpdir):
        cli = self._cli(tmpdir)
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_get_password", return_value="stored-pp"),
            patch("pgcli.main.getpass") as mock_getpass,
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        assert result == ("passphrase", "stored-pp", False)
        mock_getpass.assert_not_called()

    def test_provider_prompt_with_save_flag(self, tmpdir):
        cli = self._cli(tmpdir)
        cli.ssh_tunnel_save_password = True
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": None}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_get_password", return_value=""),
            patch("pgcli.main.getpass", return_value="typed-pw"),
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        # No key_filenames -> password; save flag on -> should_save True
        assert result == ("password", "typed-pw", True)

    def test_provider_prompt_asks_when_flag_off(self, tmpdir):
        cli = self._cli(tmpdir)
        cli.ssh_tunnel_save_password = False
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_get_password", return_value=""),
            patch("pgcli.main.getpass", return_value="typed-pp"),
            patch("pgcli.main.confirm", return_value=True) as mock_confirm,
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        assert result == ("passphrase", "typed-pp", True)
        mock_confirm.assert_called_once()

    def test_saver_sets_keyring(self, tmpdir):
        cli = self._cli(tmpdir)
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_set_password") as mock_set,
        ):
            cli._ssh_tunnel_secret_saver(ctx, "passphrase", "pp")
        mock_set.assert_called_once_with("ssh-tunnel-passphrase:/k/id_rsa", "pp")

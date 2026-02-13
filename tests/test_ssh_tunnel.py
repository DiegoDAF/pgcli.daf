import os
from unittest.mock import patch, MagicMock, ANY

import pytest
from configobj import ConfigObj
from click.testing import CliRunner
from sshtunnel import SSHTunnelForwarder

from pgcli.main import cli, notify_callback, PGCli
from pgcli.pgexecute import PGExecute


@pytest.fixture
def mock_ssh_tunnel_forwarder() -> MagicMock:
    mock_ssh_tunnel_forwarder = MagicMock(SSHTunnelForwarder, local_bind_ports=[1111], autospec=True)
    with patch(
        "pgcli.main.sshtunnel.SSHTunnelForwarder",
        return_value=mock_ssh_tunnel_forwarder,
    ) as mock:
        yield mock


@pytest.fixture
def mock_pgexecute() -> MagicMock:
    with patch.object(PGExecute, "__init__", return_value=None) as mock_pgexecute:
        yield mock_pgexecute


def test_ssh_tunnel(mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock) -> None:
    # Test with just a host
    tunnel_url = "some.host"
    db_params = {
        "database": "dbname",
        "host": "db.host",
        "user": "db_user",
        "passwd": "db_passwd",
    }
    expected_tunnel_params = {
        "local_bind_address": ("127.0.0.1",),
        "remote_bind_address": (db_params["host"], 5432),
        "ssh_address_or_host": (tunnel_url, 22),
        "logger": ANY,
        "ssh_config_file": None,
        "allow_agent": True,
        "host_pkey_directories": [],
        "compression": False,
        "ssh_username": ANY,  # from SSH config or getuser()
    }

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(**db_params)

    mock_ssh_tunnel_forwarder.assert_called_once_with(**expected_tunnel_params)
    mock_ssh_tunnel_forwarder.return_value.start.assert_called_once()
    mock_pgexecute.assert_called_once()

    call_args, call_kwargs = mock_pgexecute.call_args
    # With SSH tunnel, host should be preserved for .pgpass lookup
    # and hostaddr should be set to 127.0.0.1 for actual connection
    assert call_args == (
        db_params["database"],
        db_params["user"],
        db_params["passwd"],
        db_params["host"],  # Original host preserved
        pgcli.ssh_tunnel.local_bind_ports[0],
        "",
        notify_callback,
    )
    # Verify hostaddr is passed in kwargs
    assert call_kwargs.get("hostaddr") == "127.0.0.1"
    mock_ssh_tunnel_forwarder.reset_mock()
    mock_pgexecute.reset_mock()

    # Test with a full url and with a specific db port
    tunnel_user = "tunnel_user"
    tunnel_passwd = "tunnel_pass"
    tunnel_host = "some.other.host"
    tunnel_port = 1022
    tunnel_url = f"ssh://{tunnel_user}:{tunnel_passwd}@{tunnel_host}:{tunnel_port}"
    db_params["port"] = 1234

    expected_tunnel_params["remote_bind_address"] = (
        db_params["host"],
        db_params["port"],
    )
    expected_tunnel_params["ssh_address_or_host"] = (tunnel_host, tunnel_port)
    expected_tunnel_params["ssh_username"] = tunnel_user
    expected_tunnel_params["ssh_password"] = tunnel_passwd

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(**db_params)

    mock_ssh_tunnel_forwarder.assert_called_once_with(**expected_tunnel_params)
    mock_ssh_tunnel_forwarder.return_value.start.assert_called_once()
    mock_pgexecute.assert_called_once()

    call_args, call_kwargs = mock_pgexecute.call_args
    # With SSH tunnel, host should be preserved for .pgpass lookup
    # and hostaddr should be set to 127.0.0.1 for actual connection
    assert call_args == (
        db_params["database"],
        db_params["user"],
        db_params["passwd"],
        db_params["host"],  # Original host preserved
        pgcli.ssh_tunnel.local_bind_ports[0],
        "",
        notify_callback,
    )
    # Verify hostaddr is passed in kwargs
    assert call_kwargs.get("hostaddr") == "127.0.0.1"
    mock_ssh_tunnel_forwarder.reset_mock()
    mock_pgexecute.reset_mock()

    # Test with DSN
    dsn = f"user={db_params['user']} password={db_params['passwd']} host={db_params['host']} port={db_params['port']}"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(dsn=dsn)

    # With SSH tunnel + DSN, host is preserved and hostaddr is added
    # This allows .pgpass to work with the original hostname
    mock_ssh_tunnel_forwarder.assert_called_once_with(**expected_tunnel_params)
    mock_pgexecute.assert_called_once()

    call_args, call_kwargs = mock_pgexecute.call_args
    # The DSN should contain the original host, the tunnel port, and hostaddr
    dsn_arg = call_args[5]  # DSN is the 6th positional argument
    assert f"host={db_params['host']}" in dsn_arg
    assert f"hostaddr=127.0.0.1" in dsn_arg
    assert f"port={pgcli.ssh_tunnel.local_bind_ports[0]}" in dsn_arg


def test_cli_with_tunnel() -> None:
    runner = CliRunner()
    tunnel_url = "mytunnel"
    with patch.object(PGCli, "__init__", autospec=True, return_value=None) as mock_pgcli:
        runner.invoke(cli, ["--ssh-tunnel", tunnel_url])
        mock_pgcli.assert_called_once()
        call_args, call_kwargs = mock_pgcli.call_args
        assert call_kwargs["ssh_tunnel_url"] == tunnel_url


def test_config(tmpdir: os.PathLike, mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock) -> None:
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

    # Unmatched host
    pgcli = PGCli(pgclirc_file=pgclirc)
    pgcli.connect(host="unmatched.host")
    mock_ssh_tunnel_forwarder.assert_not_called()

    # Host matching first tunnel
    pgcli = PGCli(pgclirc_file=pgclirc)
    pgcli.connect(host="matched.host.com")
    mock_ssh_tunnel_forwarder.assert_called_once()
    call_args, call_kwargs = mock_ssh_tunnel_forwarder.call_args
    assert call_kwargs["ssh_address_or_host"] == (tunnel_host, tunnel_port)
    assert call_kwargs["ssh_username"] == tunnel_user
    assert call_kwargs["ssh_password"] == tunnel_passwd
    mock_ssh_tunnel_forwarder.reset_mock()

    # Host matching second tunnel
    pgcli = PGCli(pgclirc_file=pgclirc)
    pgcli.connect(host="hello-i-am-matched")
    mock_ssh_tunnel_forwarder.assert_called_once()

    call_args, call_kwargs = mock_ssh_tunnel_forwarder.call_args
    assert call_kwargs["ssh_address_or_host"] == (tunnel2_url, 22)
    mock_ssh_tunnel_forwarder.reset_mock()

    # Host matching both tunnels (will use the first one matched)
    pgcli = PGCli(pgclirc_file=pgclirc)
    pgcli.connect(host="hello-i-am-matched.com")
    mock_ssh_tunnel_forwarder.assert_called_once()

    call_args, call_kwargs = mock_ssh_tunnel_forwarder.call_args
    assert call_kwargs["ssh_address_or_host"] == (tunnel_host, tunnel_port)
    assert call_kwargs["ssh_username"] == tunnel_user
    assert call_kwargs["ssh_password"] == tunnel_passwd


def test_ssh_tunnel_with_uri(mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock) -> None:
    """Test that connect_uri passes DSN for .pgpass compatibility"""
    tunnel_url = "tunnel.host"
    uri = "postgresql://testuser@db.example.com:5432/testdb"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect_uri(uri)

    # Verify SSH tunnel was created
    mock_ssh_tunnel_forwarder.assert_called_once()
    mock_ssh_tunnel_forwarder.return_value.start.assert_called_once()

    # Verify PGExecute was called
    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args

    # The DSN should be passed (6th positional argument)
    dsn_arg = call_args[5]
    assert dsn_arg  # DSN should not be empty
    assert "host=db.example.com" in dsn_arg
    assert "hostaddr=127.0.0.1" in dsn_arg
    assert f"port={pgcli.ssh_tunnel.local_bind_ports[0]}" in dsn_arg
    assert "user=testuser" in dsn_arg
    assert "dbname=testdb" in dsn_arg


def test_ssh_tunnel_preserves_original_host_for_pgpass(
    mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock
) -> None:
    """Test that original hostname is preserved for .pgpass lookup"""
    tunnel_url = "tunnel.host"
    original_host = "production-db.aws.amazonaws.com"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(database="mydb", host=original_host, user="admin")

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args

    # Host argument should be the original hostname, not 127.0.0.1
    assert call_args[3] == original_host

    # hostaddr should be 127.0.0.1 for actual connection
    assert call_kwargs.get("hostaddr") == "127.0.0.1"


def test_ssh_tunnel_with_dsn_string(
    mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock
) -> None:
    """Test SSH tunnel with DSN connection string"""
    tunnel_url = "tunnel.host"
    dsn = "host=db.prod.com port=5432 dbname=myapp user=appuser"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(dsn=dsn)

    mock_ssh_tunnel_forwarder.assert_called_once()
    mock_pgexecute.assert_called_once()

    call_args, call_kwargs = mock_pgexecute.call_args
    dsn_arg = call_args[5]

    # DSN should preserve original host and add hostaddr
    assert "host=db.prod.com" in dsn_arg
    assert "hostaddr=127.0.0.1" in dsn_arg
    # Port should be changed to tunnel port
    assert f"port={pgcli.ssh_tunnel.local_bind_ports[0]}" in dsn_arg


def test_no_ssh_tunnel_does_not_set_hostaddr(mock_pgexecute: MagicMock) -> None:
    """Test that hostaddr is not set when SSH tunnel is not used"""
    pgcli = PGCli()
    pgcli.connect(database="mydb", host="localhost", user="user")

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args

    # hostaddr should not be in kwargs when no SSH tunnel
    assert "hostaddr" not in call_kwargs


def test_ssh_tunnel_with_port_in_dsn(
    mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock
) -> None:
    """Test that custom port in DSN is handled correctly with SSH tunnel"""
    tunnel_url = "tunnel.host"
    dsn = "postgresql://user@db.example.com:6543/testdb"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect_uri(dsn)

    # Verify tunnel remote_bind_address uses the original port
    call_args, call_kwargs = mock_ssh_tunnel_forwarder.call_args
    assert call_kwargs["remote_bind_address"] == ("db.example.com", 6543)

    # Verify connection uses tunnel local port
    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args
    dsn_arg = call_args[5]
    assert f"port={pgcli.ssh_tunnel.local_bind_ports[0]}" in dsn_arg


def test_ssh_tunnel_reads_config_without_identity_file(
    mock_ssh_tunnel_forwarder: MagicMock, mock_pgexecute: MagicMock
) -> None:
    """Test that SSH tunnel reads SSH config for user/port/proxy but does NOT
    pass ssh_config_file to sshtunnel (to avoid IdentityFile passphrase prompts)."""
    tunnel_url = "tunnel.host"

    pgcli = PGCli(ssh_tunnel_url=tunnel_url)
    pgcli.connect(database="db", host="remote.host", user="user")

    # Verify SSHTunnelForwarder was called with ssh_config_file=None
    # (we read the config ourselves to avoid sshtunnel picking up IdentityFile)
    call_args, call_kwargs = mock_ssh_tunnel_forwarder.call_args
    assert call_kwargs["ssh_config_file"] is None
    assert call_kwargs["host_pkey_directories"] == []
    assert call_kwargs["allow_agent"] is True
    assert call_kwargs["compression"] is False
    assert "ssh_username" in call_kwargs


def test_connect_uri_without_ssh_tunnel(mock_pgexecute: MagicMock) -> None:
    """Test that connect_uri works correctly without SSH tunnel"""
    uri = "postgresql://testuser:testpass@localhost:5432/testdb"

    pgcli = PGCli()
    pgcli.connect_uri(uri)

    mock_pgexecute.assert_called_once()
    call_args, call_kwargs = mock_pgexecute.call_args

    # DSN should be passed
    dsn_arg = call_args[5]
    assert uri == dsn_arg

    # hostaddr should not be set without SSH tunnel
    assert "hostaddr" not in call_kwargs


# =============================================================================
# Tests for the standalone SSHTunnelManager class (pgcli/ssh_tunnel.py)
# =============================================================================

import logging
from pgcli.ssh_tunnel import (
    SSHTunnelManager,
    get_tunnel_manager_from_config,
    SSH_TUNNEL_SUPPORT,
)


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
        # "prod" should NOT match "nonprod" or "prod.extra.com"
        assert manager.find_tunnel_url(host="nonprod") is None
        assert manager.find_tunnel_url(host="prod.extra.com") is None
        # But should match exactly "prod"
        assert manager.find_tunnel_url(host="prod") == "ssh://bastion:22"

    def test_find_tunnel_url_no_partial_dsn_match(self):
        """Test that partial DSN matches are rejected (re.fullmatch)."""
        manager = SSHTunnelManager(
            dsn_ssh_tunnel_config={"prod": "ssh://bastion:22"}
        )
        # "prod" should NOT match "nonprod" or "prod-extra"
        assert manager.find_tunnel_url(dsn_alias="nonprod") is None
        assert manager.find_tunnel_url(dsn_alias="prod-extra") is None
        # But should match exactly "prod"
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

    @pytest.mark.skipif(not SSH_TUNNEL_SUPPORT, reason="sshtunnel not installed")
    def test_start_tunnel_with_config(self, mock_ssh_tunnel_forwarder):
        """Test start_tunnel creates and starts tunnel."""
        mock_ssh_tunnel_forwarder.return_value.is_active = True
        mock_ssh_tunnel_forwarder.return_value.local_bind_ports = [12345]

        manager = SSHTunnelManager(
            ssh_tunnel_url="ssh://user@bastion.example.com:22",
            logger=logging.getLogger("test"),
        )

        with patch("pgcli.ssh_tunnel.sshtunnel.SSHTunnelForwarder", mock_ssh_tunnel_forwarder):
            host, port = manager.start_tunnel(host="db.internal", port=5432)

        assert host == "127.0.0.1"
        assert port == 12345

    def test_stop_tunnel_no_tunnel(self):
        """Test stop_tunnel when no tunnel exists."""
        manager = SSHTunnelManager()
        manager.stop_tunnel()  # Should not raise

    @pytest.mark.skipif(not SSH_TUNNEL_SUPPORT, reason="sshtunnel not installed")
    def test_stop_tunnel_active(self):
        """Test stop_tunnel when tunnel is active."""
        mock_tunnel = MagicMock()
        mock_tunnel.is_active = True

        manager = SSHTunnelManager()
        manager.tunnel = mock_tunnel
        manager.stop_tunnel()

        mock_tunnel.stop.assert_called_once()
        assert manager.tunnel is None


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

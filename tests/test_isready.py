"""Tests for pgcli_isready wrapper."""

import os
import subprocess
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from pgcli.isready import (
    cli,
    find_pg_isready,
    parse_connection_args,
    build_tunneled_args,
    setup_logging,
)
from pgcli.ssh_tunnel import SSHTunnelManager


class TestParseConnectionArgs:
    """Tests for parse_connection_args function."""

    def test_parse_host_short_option(self):
        args = ["-h", "myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True
        assert has_port is False

    def test_parse_host_long_option(self):
        args = ["--host", "myhost.com"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True

    def test_parse_host_equals_format(self):
        args = ["--host=myhost.com"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True

    def test_parse_port_short_option(self):
        args = ["-p", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_parse_port_long_option(self):
        args = ["--port", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_parse_port_equals_format(self):
        args = ["--port=5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_default_values(self):
        args = ["-d", "mydb", "-t", "5"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "localhost"
        assert port == 5432
        assert has_host is False
        assert has_port is False
        assert remaining == ["-d", "mydb", "-t", "5"]

    def test_preserves_other_args(self):
        args = ["-h", "myhost", "-p", "5433", "-U", "user", "-t", "10", "-q"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost"
        assert port == 5433
        assert "-U" in remaining
        assert "-t" in remaining
        assert "-q" in remaining


class TestBuildTunneledArgs:
    """Tests for build_tunneled_args function."""

    def test_replace_host_short(self):
        args = ["-h", "original.host", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, True, False)
        assert result == ["-h", "127.0.0.1", "-d", "mydb", "-p", "12345"]

    def test_replace_port_short(self):
        args = ["-h", "original.host", "-p", "5432"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, True, True)
        assert result == ["-h", "127.0.0.1", "-p", "12345"]

    def test_replace_host_equals(self):
        args = ["--host=original.host"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, True, False)
        assert result == ["--host=127.0.0.1", "-p", "12345"]

    def test_add_host_port_when_missing(self):
        args = ["-d", "mydb", "-t", "5"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, False, False)
        assert "-h" in result
        assert "127.0.0.1" in result
        assert "-p" in result
        assert "12345" in result

    def test_preserves_other_options(self):
        args = ["-h", "host", "-U", "user", "-t", "10", "-q"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, True, False)
        assert "-U" in result
        assert "-t" in result
        assert "-q" in result


class TestFindExecutable:
    """Tests for find_pg_isready function."""

    def test_find_pg_isready_in_path(self):
        result = find_pg_isready()
        assert "pg_isready" in result


class TestIsreadyCli:
    """Tests for the CLI interface."""

    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "pg_isready wrapper" in result.output

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_passthrough_args(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("localhost", 5432)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(cli, ["-h", "myhost", "-p", "5432", "-t", "5"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-h" in cmd
        assert "-t" in cmd

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_with_ssh_tunnel_option(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            cli, ["--ssh-tunnel", "user@bastion", "-h", "db.internal"]
        )

        mock_manager.start_tunnel.assert_called_once()
        mock_manager.stop_tunnel.assert_called_once()

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_exit_code_passthrough(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("localhost", 5432)
        mock_tunnel_mgr.return_value = mock_manager

        for exit_code in [0, 1, 2, 3]:
            mock_run.return_value = MagicMock(returncode=exit_code)
            runner = CliRunner()
            result = runner.invoke(cli, ["-h", "localhost"])
            assert result.exit_code == exit_code


class TestSSHTunnelBehavior:
    """Tests for SSH tunnel integration."""

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_tunnel_modifies_host_and_port(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        runner.invoke(cli, ["-h", "remote.host", "-p", "5432"])

        cmd = mock_run.call_args[0][0]
        assert "127.0.0.1" in cmd
        assert "54321" in cmd

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_tunnel_cleanup_on_success(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        runner.invoke(cli, ["-h", "remote.host"])

        mock_manager.stop_tunnel.assert_called_once()

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_no_tunnel_preserves_original_args(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("myhost", 5432)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        runner.invoke(cli, ["-h", "myhost", "-p", "5432", "-t", "10"])

        cmd = mock_run.call_args[0][0]
        assert "myhost" in cmd
        assert "5432" in cmd
        assert "-t" in cmd

    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_tunnel_with_dsn_option(self, mock_tunnel_mgr, mock_config, mock_run):
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        runner.invoke(cli, ["--dsn", "production", "-h", "db.internal"])

        mock_manager.start_tunnel.assert_called_once_with(
            host="db.internal", port=5432, dsn_alias="production"
        )


class TestVerboseMode:
    """Tests for verbose/logging mode."""

    def test_setup_logging_verbose(self):
        import logging
        logger = setup_logging(verbose=True)
        assert logger.level == logging.DEBUG

    def test_setup_logging_non_verbose(self):
        import logging
        logger = setup_logging(verbose=False)
        assert logger.level == logging.WARNING


class TestIntegrationWithRealPgIsready:
    """Integration tests using the real pg_isready binary."""

    def test_pg_isready_version(self):
        result = subprocess.run(
            [find_pg_isready(), "--version"], capture_output=True, text=True
        )
        assert result.returncode == 0
        assert "pg_isready" in result.stdout


class TestErrorHandling:
    """Tests for error handling."""

    @patch("pgcli.isready.find_pg_isready")
    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_pg_isready_not_found(self, mock_tunnel_mgr, mock_config, mock_run, mock_find):
        mock_config.return_value = {}
        mock_find.return_value = "/nonexistent/pg_isready"
        mock_run.side_effect = FileNotFoundError()
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("localhost", 5432)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(cli, ["-h", "localhost"])
        assert result.exit_code == 1

    @patch("pgcli.isready.get_config")
    @patch("pgcli.isready.subprocess.run")
    @patch("pgcli.isready.get_tunnel_manager_from_config")
    def test_config_load_failure_continues(self, mock_tunnel_mgr, mock_run, mock_config):
        mock_config.side_effect = Exception("config error")
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("localhost", 5432)
        mock_tunnel_mgr.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(cli, ["-h", "localhost"])
        assert result.exit_code == 0

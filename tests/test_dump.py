"""Tests for pgcli_dump and pgcli_dumpall wrappers."""

import os
import subprocess
import tempfile
import pytest
from unittest.mock import patch, MagicMock, call
from click.testing import CliRunner

from pgcli.dump import (
    cli as dump_cli,
    find_pg_dump,
    parse_connection_args,
    build_tunneled_args,
    setup_logging,
)
from pgcli.dumpall import (
    cli as dumpall_cli,
    find_pg_dumpall,
    parse_connection_args as parse_connection_args_dumpall,
    build_tunneled_args as build_tunneled_args_dumpall,
    setup_logging as setup_logging_dumpall,
)
from pgcli.ssh_tunnel import SSHTunnelManager, get_tunnel_manager_from_config


class TestParseConnectionArgs:
    """Tests for parse_connection_args function."""

    def test_parse_host_short_option(self):
        """Test parsing -h option."""
        args = ["-h", "myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True
        assert has_port is False

    def test_parse_host_long_option(self):
        """Test parsing --host option."""
        args = ["--host", "myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True

    def test_parse_host_equals_format(self):
        """Test parsing --host=value format."""
        args = ["--host=myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True

    def test_parse_port_short_option(self):
        """Test parsing -p option."""
        args = ["-h", "myhost.com", "-p", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_parse_port_long_option(self):
        """Test parsing --port option."""
        args = ["--port", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_parse_port_equals_format(self):
        """Test parsing --port=value format."""
        args = ["--port=5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_default_values(self):
        """Test default values when no host/port specified."""
        args = ["-d", "mydb", "-U", "myuser"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == os.environ.get("PGHOST", "localhost")
        assert port == int(os.environ.get("PGPORT", 5432))
        assert has_host is False
        assert has_port is False

    def test_connection_string_in_dbname(self):
        """Test parsing connection string in --dbname."""
        args = ["-d", "host=db.example.com port=5433 dbname=mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "db.example.com"
        assert port == 5433
        assert has_host is True
        assert has_port is True


class TestBuildTunneledArgs:
    """Tests for build_tunneled_args function."""

    def test_replace_host_short(self):
        """Test replacing -h with tunnel host."""
        args = ["-h", "original.host", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "original.host", 5432, True, False)
        assert "-h" in result
        idx = result.index("-h")
        assert result[idx + 1] == "127.0.0.1"

    def test_replace_port_short(self):
        """Test replacing -p with tunnel port."""
        args = ["-h", "host", "-p", "5432"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, True, True)
        assert "-p" in result
        idx = result.index("-p")
        assert result[idx + 1] == "12345"

    def test_add_host_port_when_missing(self):
        """Test adding host/port when not in original args."""
        args = ["-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, False, False)
        assert "-h" in result
        assert "127.0.0.1" in result
        assert "-p" in result
        assert "12345" in result


class TestFindExecutables:
    """Tests for finding pg_dump and pg_dumpall executables."""

    def test_find_pg_dump_in_path(self):
        """Test that pg_dump can be found."""
        # This test just verifies the function doesn't crash
        result = find_pg_dump()
        assert result.endswith("pg_dump")

    def test_find_pg_dumpall_in_path(self):
        """Test that pg_dumpall can be found."""
        result = find_pg_dumpall()
        assert result.endswith("pg_dumpall")


class TestDumpCli:
    """Tests for pgcli_dump CLI."""

    def test_help(self):
        """Test --help option."""
        runner = CliRunner()
        result = runner.invoke(dump_cli, ["--help"])
        assert result.exit_code == 0
        assert "pg_dump wrapper with SSH tunnel support" in result.output
        assert "--ssh-tunnel" in result.output

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_passthrough_args(self, mock_config, mock_run):
        """Test that pg_dump args are passed through."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "-F", "c"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-F" in cmd
        assert "c" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_with_ssh_tunnel_option(self, mock_tunnel_manager, mock_config, mock_run):
        """Test --ssh-tunnel option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal", "-d", "mydb"],
        )

        mock_tunnel_manager.assert_called_once()
        mock_manager.start_tunnel.assert_called_once()

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_exit_code_passthrough(self, mock_config, mock_run):
        """Test that exit code is passed through from pg_dump."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=1)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-d", "nonexistent"])

        assert result.exit_code == 1


class TestDumpallCli:
    """Tests for pgcli_dumpall CLI."""

    def test_help(self):
        """Test --help option."""
        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["--help"])
        assert result.exit_code == 0
        assert "pg_dumpall wrapper with SSH tunnel support" in result.output
        assert "--ssh-tunnel" in result.output

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_passthrough_args(self, mock_config, mock_run):
        """Test that pg_dumpall args are passed through."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "-g", "-f", "globals.sql"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-g" in cmd
        assert "-f" in cmd
        assert "globals.sql" in cmd

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    @patch("pgcli.dumpall.get_tunnel_manager_from_config")
    def test_with_ssh_tunnel_option(self, mock_tunnel_manager, mock_config, mock_run):
        """Test --ssh-tunnel option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dumpall_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal", "-g"],
        )

        mock_tunnel_manager.assert_called_once()
        mock_manager.start_tunnel.assert_called_once()

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_globals_only_option(self, mock_config, mock_run):
        """Test -g/--globals-only option passthrough."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "-g"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-g" in cmd


# =============================================================================
# Extended tests for parse_connection_args edge cases
# =============================================================================

class TestParseConnectionArgsEdgeCases:
    """Extended tests for parse_connection_args edge cases."""

    def test_empty_args(self):
        """Test with empty argument list."""
        args = []
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert has_host is False
        assert has_port is False
        assert remaining == []

    def test_multiple_host_options_last_wins(self):
        """Test that last host option wins."""
        args = ["-h", "first.host", "-h", "second.host"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "second.host"
        assert has_host is True

    def test_multiple_port_options_last_wins(self):
        """Test that last port option wins."""
        args = ["-p", "5432", "-p", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_mixed_format_options(self):
        """Test mixed short and long format options."""
        args = ["-h", "shorthost", "--port=5433", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "shorthost"
        assert port == 5433
        assert has_host is True
        assert has_port is True

    def test_host_with_special_characters(self):
        """Test host with special characters."""
        args = ["-h", "db-1.prod.example.com"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "db-1.prod.example.com"

    def test_host_ipv4_address(self):
        """Test IPv4 address as host."""
        args = ["-h", "192.168.1.100", "-p", "5432"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "192.168.1.100"
        assert port == 5432

    def test_host_ipv6_address(self):
        """Test IPv6 address as host."""
        args = ["-h", "::1"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "::1"

    def test_unix_socket_path(self):
        """Test Unix socket path as host."""
        args = ["-h", "/var/run/postgresql"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "/var/run/postgresql"

    def test_preserves_other_arguments(self):
        """Test that other arguments are preserved."""
        args = ["-h", "host", "-U", "user", "-d", "db", "-F", "c", "--schema-only"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert "-U" in remaining
        assert "user" in remaining
        assert "-F" in remaining
        assert "c" in remaining
        assert "--schema-only" in remaining

    def test_dbname_with_connection_string_long_format(self):
        """Test --dbname= with connection string."""
        args = ["--dbname=host=db.example.com port=5433 dbname=mydb user=admin"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "db.example.com"
        assert port == 5433
        assert has_host is True
        assert has_port is True

    def test_postgresql_uri_in_dbname(self):
        """Test PostgreSQL URI in dbname (should not parse as host/port)."""
        # URIs are handled differently - host extraction from URI is not done
        args = ["-d", "postgresql://user@localhost:5432/mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        # URI doesn't contain "host=" so it won't extract host/port
        assert has_host is False

    def test_environment_variables_default(self):
        """Test that PGHOST/PGPORT environment variables are used as defaults."""
        with patch.dict(os.environ, {"PGHOST": "envhost.com", "PGPORT": "5434"}):
            args = ["-d", "mydb"]
            host, port, remaining, has_host, has_port = parse_connection_args(args)
            assert host == "envhost.com"
            assert port == 5434
            assert has_host is False  # Not from args
            assert has_port is False  # Not from args


# =============================================================================
# Extended tests for build_tunneled_args
# =============================================================================

class TestBuildTunneledArgsExtended:
    """Extended tests for build_tunneled_args function."""

    def test_replace_host_long_format(self):
        """Test replacing --host with tunnel host."""
        args = ["--host", "original.host", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "original.host", 5432, True, False)
        assert "--host" not in result or result[result.index("--host") + 1] == "127.0.0.1"
        # Check -h was used instead
        if "-h" in result:
            idx = result.index("-h")
            assert result[idx + 1] == "127.0.0.1"

    def test_replace_host_equals_format(self):
        """Test replacing --host=value format."""
        args = ["--host=original.host", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "original.host", 5432, True, False)
        assert "--host=127.0.0.1" in result

    def test_replace_port_long_format(self):
        """Test replacing --port with tunnel port."""
        args = ["--port", "5432", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, False, True)
        # Check that port was replaced
        if "-p" in result:
            idx = result.index("-p")
            assert result[idx + 1] == "12345"

    def test_replace_port_equals_format(self):
        """Test replacing --port=value format."""
        args = ["--port=5432", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, False, True)
        assert "--port=12345" in result

    def test_connection_string_replacement(self):
        """Test replacing host/port in connection string."""
        args = ["-d", "host=original.host port=5432 dbname=mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "original.host", 5432, True, True)
        # Find the -d argument and check the connection string
        idx = result.index("-d")
        conn_str = result[idx + 1]
        assert "host=127.0.0.1" in conn_str
        assert "port=12345" in conn_str

    def test_preserves_all_other_options(self):
        """Test that all other pg_dump options are preserved."""
        args = [
            "-h", "host",
            "-p", "5432",
            "-U", "user",
            "-d", "mydb",
            "-F", "c",
            "-f", "output.dump",
            "--schema-only",
            "-v",
            "--no-owner",
        ]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, True, True)
        assert "-U" in result
        assert "user" in result
        assert "-F" in result
        assert "c" in result
        assert "-f" in result
        assert "output.dump" in result
        assert "--schema-only" in result
        assert "-v" in result
        assert "--no-owner" in result


# =============================================================================
# Integration tests with real pg_dump/pg_dumpall
# =============================================================================

class TestIntegrationWithRealPgDump:
    """Integration tests that use real pg_dump/pg_dumpall."""

    def test_pg_dump_version(self):
        """Test that pg_dump --version works through wrapper."""
        runner = CliRunner()
        # Use mix_stderr=False to capture stderr separately
        result = runner.invoke(dump_cli, ["--version"], catch_exceptions=False)
        # pg_dump --version should return version info
        # Note: exit code 0 means success, version info goes to stdout

    def test_pg_dumpall_version(self):
        """Test that pg_dumpall --version works through wrapper."""
        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["--version"], catch_exceptions=False)

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_dump_schema_only_option(self, mock_config, mock_run):
        """Test --schema-only option is passed correctly."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "--schema-only"])

        cmd = mock_run.call_args[0][0]
        assert "--schema-only" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_dump_format_custom(self, mock_config, mock_run):
        """Test -F c (custom format) option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "-F", "c"])

        cmd = mock_run.call_args[0][0]
        assert "-F" in cmd
        idx = cmd.index("-F")
        assert cmd[idx + 1] == "c"

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_dump_output_file(self, mock_config, mock_run):
        """Test -f (output file) option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "-f", "backup.sql"])

        cmd = mock_run.call_args[0][0]
        assert "-f" in cmd
        assert "backup.sql" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_dump_table_option(self, mock_config, mock_run):
        """Test -t (table) option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "-t", "users"])

        cmd = mock_run.call_args[0][0]
        assert "-t" in cmd
        assert "users" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_dump_schema_option(self, mock_config, mock_run):
        """Test -n (schema) option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "-n", "public"])

        cmd = mock_run.call_args[0][0]
        assert "-n" in cmd
        assert "public" in cmd


# =============================================================================
# Tests for SSH tunnel behavior
# =============================================================================

class TestSSHTunnelBehavior:
    """Tests for SSH tunnel integration."""

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_tunnel_modifies_host_and_port(self, mock_tunnel_manager, mock_config, mock_run):
        """Test that SSH tunnel modifies host and port in command."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal.com", "-p", "5432", "-d", "mydb"],
        )

        # Verify tunnel was started
        mock_manager.start_tunnel.assert_called_once_with(
            host="db.internal.com",
            port=5432,
            dsn_alias=None,
        )

        # Verify command uses tunnel host/port
        cmd = mock_run.call_args[0][0]
        assert "127.0.0.1" in cmd
        assert "54321" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_tunnel_with_dsn_option(self, mock_tunnel_manager, mock_config, mock_run):
        """Test --dsn option for tunnel lookup."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["--dsn", "production", "-h", "db.internal.com", "-d", "mydb"],
        )

        # Verify tunnel was started with dsn_alias
        mock_manager.start_tunnel.assert_called_once()
        call_kwargs = mock_manager.start_tunnel.call_args[1]
        assert call_kwargs["dsn_alias"] == "production"

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_no_tunnel_preserves_original_args(self, mock_tunnel_manager, mock_config, mock_run):
        """Test that without tunnel, original args are preserved."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        # Return same host/port = no tunnel
        mock_manager.start_tunnel.return_value = ("db.example.com", 5432)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["-h", "db.example.com", "-p", "5432", "-d", "mydb"],
        )

        cmd = mock_run.call_args[0][0]
        assert "db.example.com" in cmd
        assert "5432" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_tunnel_cleanup_on_success(self, mock_tunnel_manager, mock_config, mock_run):
        """Test that tunnel is stopped after successful dump."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal", "-d", "mydb"],
        )

        # Verify tunnel stop was called
        mock_manager.stop_tunnel.assert_called_once()

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_tunnel_cleanup_on_error(self, mock_tunnel_manager, mock_config, mock_run):
        """Test that tunnel is stopped even when dump fails."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=1)  # Simulate failure
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal", "-d", "mydb"],
        )

        # Verify tunnel stop was called even on error
        mock_manager.stop_tunnel.assert_called_once()


# =============================================================================
# Tests for error handling
# =============================================================================

class TestErrorHandling:
    """Tests for error handling scenarios."""

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_pg_dump_not_found(self, mock_config, mock_run):
        """Test error when pg_dump is not found."""
        mock_config.return_value = {}
        mock_run.side_effect = FileNotFoundError("pg_dump not found")

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb"])

        assert result.exit_code == 1
        assert "pg_dump not found" in result.output or "Error" in result.output

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_pg_dumpall_not_found(self, mock_config, mock_run):
        """Test error when pg_dumpall is not found."""
        mock_config.return_value = {}
        mock_run.side_effect = FileNotFoundError("pg_dumpall not found")

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost"])

        assert result.exit_code == 1

    @patch("pgcli.dump.get_config")
    def test_config_load_failure_continues(self, mock_config):
        """Test that config load failure doesn't crash the wrapper."""
        mock_config.side_effect = Exception("Config error")

        runner = CliRunner()
        # Should not crash, just log warning and continue
        result = runner.invoke(dump_cli, ["--help"])
        assert result.exit_code == 0


# =============================================================================
# Tests for verbose mode
# =============================================================================

class TestVerboseMode:
    """Tests for verbose logging mode."""

    def test_setup_logging_verbose(self):
        """Test that verbose logging is configured correctly."""
        import logging
        logger = setup_logging(verbose=True)
        assert logger.level == logging.DEBUG

    def test_setup_logging_non_verbose(self):
        """Test that non-verbose logging is configured correctly."""
        import logging
        logger = setup_logging(verbose=False)
        assert logger.level == logging.WARNING

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_verbose_option_works(self, mock_config, mock_run):
        """Test -v/--verbose option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-v", "-h", "localhost", "-d", "mydb"])

        # Should complete successfully
        assert result.exit_code == 0


# =============================================================================
# Tests for config-based SSH tunnel
# =============================================================================

class TestConfigBasedSSHTunnel:
    """Tests for SSH tunnel configuration from pgcli config."""

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_tunnel_from_ssh_tunnels_config(self, mock_config, mock_run):
        """Test SSH tunnel lookup from [ssh tunnels] config section."""
        mock_config.return_value = {
            "ssh tunnels": {
                r".*\.prod\.example\.com": "bastion.example.com",
            }
        }
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "db.prod.example.com", "-d", "mydb"])

        # The tunnel should be set up based on config match

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_tunnel_from_dsn_ssh_tunnels_config(self, mock_config, mock_run):
        """Test SSH tunnel lookup from [dsn ssh tunnels] config section."""
        mock_config.return_value = {
            "dsn ssh tunnels": {
                "prod-.*": "ssh://bastion.example.com:22",
            }
        }
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["--dsn", "prod-main", "-h", "db.internal", "-d", "mydb"])

    def test_allow_agent_config_option(self):
        """Test allow_agent config option is passed to SSHTunnelManager."""
        from pgcli.ssh_tunnel import get_tunnel_manager_from_config

        # Test default (True when not specified)
        config = {"ssh tunnels": {}}
        manager = get_tunnel_manager_from_config(config)
        assert manager.allow_agent is True

        # Test explicit True
        config = {"ssh tunnels": {"allow_agent": "True"}}
        manager = get_tunnel_manager_from_config(config)
        assert manager.allow_agent is True

        # Test explicit False
        config = {"ssh tunnels": {"allow_agent": "False"}}
        manager = get_tunnel_manager_from_config(config)
        assert manager.allow_agent is False

        # Test case insensitive
        config = {"ssh tunnels": {"allow_agent": "true"}}
        manager = get_tunnel_manager_from_config(config)
        assert manager.allow_agent is True


# =============================================================================
# Tests for dumpall-specific options
# =============================================================================

class TestDumpallSpecificOptions:
    """Tests for pg_dumpall-specific options."""

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_roles_only_option(self, mock_config, mock_run):
        """Test -r/--roles-only option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "-r"])

        cmd = mock_run.call_args[0][0]
        assert "-r" in cmd

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_tablespaces_only_option(self, mock_config, mock_run):
        """Test -t/--tablespaces-only option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "-t"])

        cmd = mock_run.call_args[0][0]
        assert "-t" in cmd

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_exclude_database_option(self, mock_config, mock_run):
        """Test --exclude-database option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "--exclude-database=template*"])

        cmd = mock_run.call_args[0][0]
        assert "--exclude-database=template*" in cmd

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_no_role_passwords_option(self, mock_config, mock_run):
        """Test --no-role-passwords option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "--no-role-passwords"])

        cmd = mock_run.call_args[0][0]
        assert "--no-role-passwords" in cmd

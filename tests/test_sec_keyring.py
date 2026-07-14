"""Security-focused tests for SSH tunnel secret handling in pgcli/main.py.

These tests assert the security invariants around how pgcli builds keyring
entry names for SSH tunnel secrets, how it persists them, and that secrets
resolved from .pgpass (or entered by the user) are never leaked into the log
stream.

IMPORTANT (hermetic guarantees):
  * We patch ``pgcli.main.auth.keyring_get_password`` /
    ``pgcli.main.auth.keyring_set_password`` -- the HELPER functions -- rather
    than ``pgcli.main.auth.keyring``. ``PGCli.__init__`` re-runs
    ``auth.keyring_initialize`` on every instance, which would clobber a patch
    of the module-level ``keyring`` attribute. Patching the helpers means the
    REAL system keyring is never read or written, no matter what
    ``keyring_initialize`` decides.
  * No real DB connection is made (``PGExecute.__init__`` is mocked).
  * All config lives in a tmp rcfile; the real ~/.config is never touched.
  * ``pgpass.lookup_password`` is mocked, so the real ~/.pgpass is never read.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from pgcli.main import PGCli
from pgcli.pgexecute import PGExecute


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_pgexecute():
    """Prevent any real DB connection from PGCli.connect()."""
    with patch.object(PGExecute, "__init__", return_value=None) as m:
        yield m


@pytest.fixture
def cli(tmpdir):
    """A PGCli built against a throwaway rcfile (never touches ~/.config)."""
    rcfile = str(tmpdir.join("rcfile"))
    return PGCli(pgclirc_file=rcfile)


# =============================================================================
# Keyring key naming (secret scoping)
# =============================================================================


class TestKeyringKeyNaming:
    """The keyring key format determines how broadly a stored secret is shared.

    A passphrase belongs to an identity FILE (reusable across tunnels), while an
    SSH password must be scoped tightly to user@host:port so it is never reused
    for a different account/host.
    """

    def test_passphrase_key_is_scoped_to_identity_file(self, cli):
        ctx = {
            "username": "alice",
            "hostname": "bastion.example.com",
            "port": 22,
            "key_filenames": ["/home/alice/.ssh/id_ed25519"],
        }
        key = cli._ssh_tunnel_keyring_key(ctx, "passphrase")
        assert key == "ssh-tunnel-passphrase:/home/alice/.ssh/id_ed25519"

    def test_passphrase_key_uses_first_identity_file_only(self, cli):
        ctx = {
            "username": "alice",
            "hostname": "bastion",
            "port": 22,
            "key_filenames": ["/keys/first", "/keys/second"],
        }
        key = cli._ssh_tunnel_keyring_key(ctx, "passphrase")
        assert key == "ssh-tunnel-passphrase:/keys/first"
        assert "/keys/second" not in key

    def test_passphrase_key_falls_back_to_unknown(self, cli):
        # No identity file: must still produce a deterministic, non-crashing key
        # and must NOT leak host/user into a passphrase-typed key.
        ctx = {"username": "alice", "hostname": "h", "port": 22, "key_filenames": []}
        key = cli._ssh_tunnel_keyring_key(ctx, "passphrase")
        assert key == "ssh-tunnel-passphrase:unknown"

    def test_password_key_scoped_to_user_host_port(self, cli):
        ctx = {
            "username": "bob",
            "hostname": "db.internal",
            "port": 2222,
            "key_filenames": None,
        }
        key = cli._ssh_tunnel_keyring_key(ctx, "password")
        assert key == "ssh-tunnel-password:bob@db.internal:2222"

    def test_password_and_passphrase_namespaces_are_distinct(self, cli):
        """A password entry and a passphrase entry must never collide in the
        keyring, so one secret can't be served in place of the other."""
        ctx = {
            "username": "u",
            "hostname": "h",
            "port": 22,
            "key_filenames": ["/k/id_rsa"],
        }
        pw_key = cli._ssh_tunnel_keyring_key(ctx, "password")
        pp_key = cli._ssh_tunnel_keyring_key(ctx, "passphrase")
        assert pw_key != pp_key
        assert pw_key.startswith("ssh-tunnel-password:")
        assert pp_key.startswith("ssh-tunnel-passphrase:")

    def test_password_key_changes_with_host(self, cli):
        """Same user, different host -> different keyring entry (no cross-host
        password reuse)."""
        base = {"username": "u", "port": 22, "key_filenames": None}
        k1 = cli._ssh_tunnel_keyring_key({**base, "hostname": "host-a"}, "password")
        k2 = cli._ssh_tunnel_keyring_key({**base, "hostname": "host-b"}, "password")
        assert k1 != k2


# =============================================================================
# Secret saver: writes to the correct keyring entry (and only the keyring)
# =============================================================================


class TestSecretSaver:
    def test_saver_writes_passphrase_under_identity_file_key(self, cli):
        ctx = {
            "username": "u",
            "hostname": "h",
            "port": 22,
            "key_filenames": ["/k/id_rsa"],
        }
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_set_password") as mock_set,
        ):
            cli._ssh_tunnel_secret_saver(ctx, "passphrase", "topsecret-pp")
        mock_set.assert_called_once_with("ssh-tunnel-passphrase:/k/id_rsa", "topsecret-pp")

    def test_saver_writes_password_under_user_host_port_key(self, cli):
        ctx = {
            "username": "u",
            "hostname": "h",
            "port": 2222,
            "key_filenames": None,
        }
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_set_password") as mock_set,
        ):
            cli._ssh_tunnel_secret_saver(ctx, "password", "hunter2")
        mock_set.assert_called_once_with("ssh-tunnel-password:u@h:2222", "hunter2")

    def test_saver_noop_when_keyring_disabled(self, cli):
        """If keyring is disabled, the saver must NOT attempt to persist the
        secret anywhere (fails closed, no silent fallback)."""
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", None),
            patch("pgcli.main.auth.keyring_set_password") as mock_set,
        ):
            cli._ssh_tunnel_secret_saver(ctx, "passphrase", "pp")
        mock_set.assert_not_called()


# =============================================================================
# Secret provider: keyring lookup path (does not persist a keyring hit)
# =============================================================================


class TestSecretProvider:
    def test_keyring_hit_returns_should_save_false(self, cli):
        """A secret already in the keyring is reused with should_save=False, so
        it is never re-written (which could clobber / duplicate the entry)."""
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_get_password", return_value="stored-pp") as mock_get,
            patch("pgcli.main.auth.keyring_set_password") as mock_set,
            patch("pgcli.main.getpass") as mock_getpass,
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        assert result == ("passphrase", "stored-pp", False)
        # Looked up under the passphrase-scoped key, never prompted, never saved.
        mock_get.assert_called_once_with("ssh-tunnel-passphrase:/k/id_rsa")
        mock_getpass.assert_not_called()
        mock_set.assert_not_called()

    def test_provider_uses_password_kind_without_identity_file(self, cli):
        """Without an identity file the secret is an SSH password, looked up
        under the user@host:port key (not the passphrase namespace)."""
        cli.ssh_tunnel_save_password = True
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": None}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_get_password", return_value="") as mock_get,
            patch("pgcli.main.getpass", return_value="typed-pw"),
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        assert result == ("password", "typed-pw", True)
        mock_get.assert_called_once_with("ssh-tunnel-password:u@h:22")

    def test_provider_no_keyring_never_saves(self, cli):
        """With keyring disabled, a freshly typed secret is returned with
        should_save=False so it is not silently pushed to any store."""
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", None),
            patch("pgcli.main.getpass", return_value="typed-pp"),
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        assert result == ("passphrase", "typed-pp", False)

    def test_provider_aborts_on_interrupt(self, cli):
        """A Ctrl-C / EOF at the prompt returns None (abort) rather than an
        empty secret that could be mistaken for a valid one."""
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        with (
            patch("pgcli.main.auth.keyring", object()),
            patch("pgcli.main.auth.keyring_get_password", return_value=""),
            patch("pgcli.main.getpass", side_effect=KeyboardInterrupt),
        ):
            result = cli._ssh_tunnel_secret_provider(ctx)
        assert result is None


# =============================================================================
# Secrets must never be written to the log stream
# =============================================================================


class TestSecretsNotLogged:
    def test_provider_keyring_hit_does_not_log_secret(self, cli, caplog):
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        secret = "SUPER-SECRET-PASSPHRASE"
        with caplog.at_level(logging.DEBUG, logger="pgcli.main"):
            with (
                patch("pgcli.main.auth.keyring", object()),
                patch("pgcli.main.auth.keyring_get_password", return_value=secret),
            ):
                cli._ssh_tunnel_secret_provider(ctx)
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert secret not in joined

    def test_saver_does_not_log_secret(self, cli, caplog):
        ctx = {"username": "u", "hostname": "h", "port": 22, "key_filenames": ["/k/id_rsa"]}
        secret = "SUPER-SECRET-SAVED"
        with caplog.at_level(logging.DEBUG, logger="pgcli.main"):
            with (
                patch("pgcli.main.auth.keyring", object()),
                patch("pgcli.main.auth.keyring_set_password"),
            ):
                cli._ssh_tunnel_secret_saver(ctx, "passphrase", secret)
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert secret not in joined

    def test_pgpass_resolved_password_not_logged(self, mock_pgexecute, caplog, monkeypatch):
        """A password resolved from .pgpass while setting up an SSH tunnel must
        NOT appear anywhere in the log stream (it is the whole point of .pgpass
        that the secret stays out of logs / process listings)."""
        monkeypatch.delenv("PGPASSWORD", raising=False)

        secret = "PGPASS-RESOLVED-SECRET"

        # Mock the SSH tunnel so connect() takes the tunnel branch that resolves
        # .pgpass, without any real network / paramiko activity.
        tunnel_local_port = 1111
        with patch("pgcli.main.SSHTunnelManager") as mock_cls:
            mock_mgr = MagicMock()
            mock_mgr.start_tunnel.return_value = ("127.0.0.1", tunnel_local_port)
            mock_tunnel = MagicMock()
            mock_tunnel.local_bind_port = tunnel_local_port
            mock_tunnel.is_active = True
            mock_mgr.tunnel = mock_tunnel
            mock_cls.return_value = mock_mgr

            with caplog.at_level(logging.DEBUG, logger="pgcli.main"):
                with (
                    patch("pgcli.main.auth.keyring_get_password", return_value=""),
                    patch("pgcli.main.auth.keyring_set_password"),
                    patch("pgcli.main.pgpass.lookup_password", return_value=secret) as mock_lookup,
                ):
                    pgcli = PGCli(ssh_tunnel_url="tunnel.host")
                    pgcli.connect(database="mydb", host="realhost", user="myuser", port="5432")

        # Sanity: the .pgpass password really was resolved and handed to PGExecute
        # (3rd positional arg), so this test is actually exercising the secret path.
        mock_lookup.assert_called_once_with("realhost", "5432", "mydb", "myuser")
        assert mock_pgexecute.call_args[0][2] == secret

        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert secret not in joined

"""Connection-argument parsing and tunnel rewriting for pgcli_dump and pgcli_dumpall.

Every test runs against both wrappers. The two modules carried a byte-identical
copy of these functions for a long time, and only the pgcli.dump copy was ever
tested, so a fix could land in one and miss the other. Parametrising over both
keeps them honest whether they share an implementation or not.
"""

import logging
import os

import pytest

from pgcli import dump, dumpall


MODULES = [dump, dumpall]
IDS = ["dump", "dumpall"]


@pytest.fixture(params=MODULES, ids=IDS)
def mod(request):
    """The wrapper module under test."""
    return request.param


@pytest.fixture
def clean_pg_env(monkeypatch):
    """No PG* in the environment, so defaults are the documented ones."""
    for var in ("PGHOST", "PGPORT"):
        monkeypatch.delenv(var, raising=False)


def tunnel(mod, args, tunnel_host="127.0.0.1", tunnel_port=49999):
    """Run a full parse + rewrite cycle, the way the CLI does it."""
    host, port, remaining, has_host, has_port = mod.parse_connection_args(args)
    final = mod.build_tunneled_args(remaining, tunnel_host, tunnel_port, host, port, has_host, has_port)
    return host, port, final


class TestDefaults:
    def test_no_connection_args_uses_libpq_defaults(self, mod, clean_pg_env):
        host, port, remaining, has_host, has_port = mod.parse_connection_args(["mydb"])
        assert (host, port) == ("localhost", 5432)
        assert has_host is False
        assert has_port is False
        assert remaining == ["mydb"]

    def test_pghost_and_pgport_are_honored(self, mod, monkeypatch):
        monkeypatch.setenv("PGHOST", "env.example.com")
        monkeypatch.setenv("PGPORT", "6432")
        host, port, _, has_host, has_port = mod.parse_connection_args([])
        assert (host, port) == ("env.example.com", 6432)
        # They came from the environment, not the command line: pg_dump will
        # read them too, so nothing has to be appended for the direct case.
        assert has_host is False
        assert has_port is False

    def test_explicit_args_win_over_the_environment(self, mod, monkeypatch):
        monkeypatch.setenv("PGHOST", "env.example.com")
        monkeypatch.setenv("PGPORT", "6432")
        host, port, _, _, _ = mod.parse_connection_args(["-h", "cli.example.com", "-p", "5433"])
        assert (host, port) == ("cli.example.com", 5433)


class TestFlagForms:
    @pytest.mark.parametrize(
        "args",
        [
            ["-h", "db.example.com"],
            ["--host", "db.example.com"],
            ["--host=db.example.com"],
        ],
        ids=["short", "long", "equals"],
    )
    def test_every_host_spelling_is_recognised(self, mod, clean_pg_env, args):
        host, _, _, has_host, _ = mod.parse_connection_args(args)
        assert host == "db.example.com"
        assert has_host is True

    @pytest.mark.parametrize(
        "args",
        [
            ["-p", "5433"],
            ["--port", "5433"],
            ["--port=5433"],
        ],
        ids=["short", "long", "equals"],
    )
    def test_every_port_spelling_is_recognised(self, mod, clean_pg_env, args):
        _, port, _, _, has_port = mod.parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_unrelated_args_are_passed_through_in_order(self, mod, clean_pg_env):
        args = ["-Fc", "-Z", "9", "-t", "public.orders", "-h", "db.example.com", "mydb"]
        _, _, remaining, _, _ = mod.parse_connection_args(args)
        assert remaining == args

    def test_rewrite_keeps_unrelated_args_in_order(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["-Fc", "-t", "public.orders", "-h", "db.example.com", "mydb"])
        assert final == ["-Fc", "-t", "public.orders", "-h", "127.0.0.1", "mydb", "-p", "49999"]

    def test_rewrite_adds_host_and_port_when_absent(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["mydb"])
        assert final == ["mydb", "-h", "127.0.0.1", "-p", "49999"]

    def test_rewrite_does_not_duplicate_host_and_port(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["-h", "db.example.com", "-p", "5432", "mydb"])
        assert final.count("-h") == 1
        assert final.count("-p") == 1
        assert final == ["-h", "127.0.0.1", "-p", "49999", "mydb"]

    def test_rewrite_preserves_the_equals_spelling(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["--host=db.example.com", "--port=5432", "mydb"])
        assert final == ["--host=127.0.0.1", "--port=49999", "mydb"]


class TestKeywordValueConnstring:
    def test_host_and_port_are_read_from_the_connstring(self, mod, clean_pg_env):
        args = ["-d", "host=db.example.com port=6432 dbname=mydb"]
        host, port, _, has_host, has_port = mod.parse_connection_args(args)
        assert (host, port) == ("db.example.com", 6432)
        assert has_host is True
        assert has_port is True

    def test_connstring_is_rewritten_onto_the_tunnel(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["-d", "host=db.example.com port=6432 dbname=mydb"])
        assert final == ["-d", "host=127.0.0.1 port=49999 dbname=mydb"]

    def test_rewrite_keeps_the_other_keywords(self, mod, clean_pg_env):
        # Measured against pg_dump 18: what the connstring states wins over
        # -h/-p, but what it omits falls back to them. A connstring with no
        # port therefore takes the tunnel port from an appended -p.
        _, _, final = tunnel(mod, ["-d", "host=db.example.com dbname=mydb user=reader sslmode=require"])
        assert final == [
            "-d",
            "host=127.0.0.1 dbname=mydb user=reader sslmode=require",
            "-p",
            "49999",
        ]

    def test_hostaddr_is_rewritten_too(self, mod, clean_pg_env):
        # hostaddr skips DNS and is what libpq actually dials. Leaving it on the
        # real address sends pg_dump straight past the tunnel.
        _, _, final = tunnel(mod, ["-d", "host=db.example.com hostaddr=10.0.0.9 dbname=mydb"])
        assert "hostaddr=10.0.0.9" not in final[1]
        assert "hostaddr=127.0.0.1" in final[1]

    def test_dbname_without_a_connstring_is_left_alone(self, mod, clean_pg_env):
        host, _, _, has_host, _ = mod.parse_connection_args(["-d", "mydb"])
        assert host == "localhost"
        assert has_host is False
        _, _, final = tunnel(mod, ["-d", "mydb"])
        assert final == ["-d", "mydb", "-h", "127.0.0.1", "-p", "49999"]


class TestUriConnstring:
    """A URI in -d used to be parsed as nothing and rewritten as nothing.

    The tunnel was then built to "localhost" and pg_dump, which lets the URI win
    over -h/-p, dialled the real host directly. Both halves are silent.
    """

    @pytest.mark.parametrize("scheme", ["postgresql", "postgres"])
    def test_host_and_port_are_read_from_the_uri(self, mod, clean_pg_env, scheme):
        args = ["-d", f"{scheme}://reader@db.example.com:6432/mydb"]
        host, port, _, has_host, has_port = mod.parse_connection_args(args)
        assert (host, port) == ("db.example.com", 6432)
        assert has_host is True
        assert has_port is True

    def test_uri_without_a_port_falls_back_to_the_default(self, mod, clean_pg_env):
        host, port, _, has_host, has_port = mod.parse_connection_args(["-d", "postgres://db.example.com/mydb"])
        assert (host, port) == ("db.example.com", 5432)
        assert has_host is True
        assert has_port is False

    def test_uri_is_rewritten_onto_the_tunnel(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["-d", "postgresql://reader@db.example.com:6432/mydb"])
        assert final == ["-d", "postgresql://reader@127.0.0.1:49999/mydb"]

    def test_rewritten_uri_does_not_also_get_host_flags(self, mod, clean_pg_env):
        # The URI wins over -h/-p in libpq, so appending them would be both
        # redundant and misleading.
        _, _, final = tunnel(mod, ["-d", "postgresql://db.example.com/mydb"])
        assert "-h" not in final
        assert "-p" not in final

    def test_rewrite_preserves_userinfo_and_query(self, mod, clean_pg_env):
        _, _, final = tunnel(
            mod,
            ["-d", "postgresql://reader:s3cr3t@db.example.com:6432/mydb?sslmode=require"],
        )
        assert final == ["-d", "postgresql://reader:s3cr3t@127.0.0.1:49999/mydb?sslmode=require"]

    def test_percent_encoded_userinfo_survives_verbatim(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["-d", "postgresql://r%40der:p%40ss@db.example.com/mydb"])
        assert final == ["-d", "postgresql://r%40der:p%40ss@127.0.0.1:49999/mydb"]

    def test_uppercase_host_is_not_lowercased(self, mod, clean_pg_env):
        host, _, _, _, _ = mod.parse_connection_args(["-d", "postgresql://DB.Example.COM/mydb"])
        assert host == "DB.Example.COM"

    def test_ipv6_literal_is_read_and_rewritten(self, mod, clean_pg_env):
        args = ["-d", "postgresql://[2001:db8::1234]:6432/mydb"]
        host, port, _, _, _ = mod.parse_connection_args(args)
        assert (host, port) == ("2001:db8::1234", 6432)
        _, _, final = tunnel(mod, args)
        assert final == ["-d", "postgresql://127.0.0.1:49999/mydb"]

    def test_dbname_equals_uri_form_is_handled(self, mod, clean_pg_env):
        args = ["--dbname=postgresql://db.example.com:6432/mydb"]
        host, port, _, _, _ = mod.parse_connection_args(args)
        assert (host, port) == ("db.example.com", 6432)
        _, _, final = tunnel(mod, args)
        assert final == ["--dbname=postgresql://127.0.0.1:49999/mydb"]

    def test_uri_with_no_host_at_all_is_left_alone(self, mod, clean_pg_env):
        # postgresql:///mydb means "the local socket"; there is nothing to tunnel.
        host, _, _, has_host, _ = mod.parse_connection_args(["-d", "postgresql:///mydb"])
        assert host == "localhost"
        assert has_host is False


class TestUrisWeRefuseToGuessAt:
    """Refusing loudly beats rewriting half of a connection string."""

    @pytest.mark.parametrize(
        "uri",
        [
            "postgresql://a.example.com:5432,b.example.com:5432/mydb",
            "postgresql:///mydb?host=db.example.com",
            "postgresql://db.example.com/mydb?hostaddr=10.0.0.9",
        ],
        ids=["multihost", "host-in-query", "hostaddr-in-query"],
    )
    def test_ambiguous_uri_is_reported_not_guessed(self, mod, clean_pg_env, uri):
        with pytest.raises(ValueError) as excinfo:
            mod.parse_connection_args(["-d", uri])
        assert "tunnel" in str(excinfo.value).lower()


class TestMalformedInput:
    def test_host_flag_without_a_value_is_not_a_crash(self, mod, clean_pg_env):
        host, _, remaining, has_host, _ = mod.parse_connection_args(["-h"])
        assert host == "localhost"
        assert has_host is False
        assert remaining == ["-h"]

    def test_port_flag_without_a_value_is_not_a_crash(self, mod, clean_pg_env):
        _, port, remaining, _, has_port = mod.parse_connection_args(["-p"])
        assert port == 5432
        assert has_port is False
        assert remaining == ["-p"]

    @pytest.mark.parametrize("args", [["-p", "not-a-number"], ["--port=not-a-number"]])
    def test_non_numeric_port_says_so(self, mod, clean_pg_env, args):
        with pytest.raises(ValueError) as excinfo:
            mod.parse_connection_args(args)
        assert "not-a-number" in str(excinfo.value)

    def test_non_numeric_pgport_says_so(self, mod, monkeypatch):
        monkeypatch.setenv("PGPORT", "garbage")
        with pytest.raises(ValueError) as excinfo:
            mod.parse_connection_args([])
        assert "PGPORT" in str(excinfo.value)

    def test_empty_pgport_falls_back_to_the_default(self, mod, monkeypatch):
        # An unset variable written as "PGPORT=" in a profile is common enough
        # that it must not take the whole dump down.
        monkeypatch.setenv("PGPORT", "")
        _, port, _, _, _ = mod.parse_connection_args([])
        assert port == 5432


class TestBothWrappersAgree:
    """The two modules must not drift apart again."""

    CASES = [
        ["-h", "db.example.com", "-p", "6432", "mydb"],
        ["--host=db.example.com", "--port=6432"],
        ["-d", "host=db.example.com port=6432 dbname=mydb"],
        ["-d", "postgresql://reader@db.example.com:6432/mydb"],
        ["-Fc", "-t", "public.orders", "mydb"],
        ["-h"],
    ]

    @pytest.mark.parametrize("args", CASES, ids=range(len(CASES)))
    def test_dump_and_dumpall_produce_the_same_answer(self, clean_pg_env, args):
        assert dump.parse_connection_args(list(args)) == dumpall.parse_connection_args(list(args))
        assert tunnel(dump, list(args)) == tunnel(dumpall, list(args))


class TestUserAndDatabase:
    """What the wrappers tell .pgpass to look for.

    A tunnelled dump asks .pgpass for the ORIGINAL host, because pg_dump will
    only ever see 127.0.0.1. If the user or the database name is wrong, the
    lookup quietly finds nothing and the dump fails to authenticate, or matches
    a wildcard line meant for somebody else.
    """

    @pytest.fixture(autouse=True)
    def clean_identity_env(self, monkeypatch):
        for var in ("PGUSER", "PGDATABASE"):
            monkeypatch.delenv(var, raising=False)

    def test_flags_are_read(self):
        assert dump.parse_user_and_database(["-U", "reader", "ventas"]) == ("reader", "ventas")

    def test_equals_spelling_is_read(self):
        assert dump.parse_user_and_database(["--username=reader", "--dbname=ventas"]) == (
            "reader",
            "ventas",
        )

    def test_defaults_when_nothing_is_given(self):
        assert dump.parse_user_and_database([]) == ("postgres", "*")

    def test_environment_is_the_fallback(self, monkeypatch):
        monkeypatch.setenv("PGUSER", "envuser")
        monkeypatch.setenv("PGDATABASE", "envdb")
        assert dump.parse_user_and_database([]) == ("envuser", "envdb")

    def test_flags_win_over_the_environment(self, monkeypatch):
        monkeypatch.setenv("PGUSER", "envuser")
        assert dump.parse_user_and_database(["-U", "reader"])[0] == "reader"

    def test_option_value_is_not_mistaken_for_the_database(self):
        # "custom.sql" is the value of -f, not the database.
        assert dump.parse_user_and_database(["-f", "custom.sql"])[1] == "*"

    def test_user_and_database_come_out_of_a_uri(self):
        assert dump.parse_user_and_database(["-d", "postgresql://reader@db.example.com:6432/ventas"]) == ("reader", "ventas")

    def test_uri_with_a_query_string_is_read_the_same_way(self):
        assert dump.parse_user_and_database(["-d", "postgresql://reader@db.example.com/ventas?sslmode=require"]) == ("reader", "ventas")

    def test_uri_password_is_not_taken_for_the_username(self):
        assert dump.parse_user_and_database(["-d", "postgresql://reader:s3cr3t@db.example.com/ventas"]) == ("reader", "ventas")

    def test_percent_encoded_username_is_decoded(self):
        # libpq percent-decodes the userinfo, so .pgpass must be asked about
        # the decoded name.
        assert dump.parse_user_and_database(["-d", "postgresql://r%40der@db.example.com/v"])[0] == ("r@der")

    def test_uri_without_a_database_keeps_the_wildcard(self):
        assert dump.parse_user_and_database(["-d", "postgresql://reader@db.example.com/"])[1] == "*"

    def test_user_and_database_come_out_of_a_keyword_value_connstring(self):
        assert dump.parse_user_and_database(["-d", "host=db.example.com dbname=ventas user=reader"]) == ("reader", "ventas")

    def test_a_uri_is_never_used_as_a_database_name(self):
        # It used to be: "-d <uri>" with no query string set database to the
        # whole URI, which matches nothing in .pgpass.
        _, database = dump.parse_user_and_database(["-d", "postgresql://db.example.com:6432/ventas"])
        assert "://" not in database

    def test_dumpall_shares_the_same_parser(self):
        assert dumpall.parse_user_and_database is dump.parse_user_and_database


class TestPgpassLookup:
    """Reading the password for the ORIGINAL host, before the tunnel renames it."""

    @pytest.fixture
    def pgpass(self, tmp_path, monkeypatch):
        """Write a .pgpass and point Path.home() at it."""

        def write(*lines, mode=0o600):
            path = tmp_path / ".pgpass"
            path.write_text("".join(line + "\n" for line in lines))
            path.chmod(mode)
            monkeypatch.setattr(dump.Path, "home", staticmethod(lambda: tmp_path))
            return path

        return write

    def test_a_world_readable_pgpass_is_refused(self, pgpass, capsys):
        """libpq ignores a password file others can read and warns about it.
        The wrappers used to read it anyway, so pgcli_dump could authenticate
        with a password psql would have refused."""
        pgpass("db.example.com:5432:ventas:reader:s3cr3t", mode=0o644)
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None
        err = capsys.readouterr().err
        assert "group or world access" in err and "0600" in err

    def test_a_group_readable_pgpass_is_refused(self, pgpass):
        """0640 is enough for libpq to refuse it: any group bit counts."""
        pgpass("db.example.com:5432:ventas:reader:s3cr3t", mode=0o640)
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None

    def test_an_owner_only_pgpass_is_read_without_a_warning(self, pgpass, capsys):
        pgpass("db.example.com:5432:ventas:reader:s3cr3t", mode=0o600)
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "s3cr3t"
        assert capsys.readouterr().err == ""

    def test_a_stricter_pgpass_is_still_read(self, pgpass):
        """0400 is stricter than 0600, so libpq accepts it and so do we."""
        pgpass("db.example.com:5432:ventas:reader:s3cr3t", mode=0o400)
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "s3cr3t"

    def test_exact_match(self, pgpass):
        pgpass("db.example.com:5432:ventas:reader:s3cr3t")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "s3cr3t"

    def test_no_pgpass_at_all(self, tmp_path, monkeypatch):
        monkeypatch.setattr(dump.Path, "home", staticmethod(lambda: tmp_path))
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None

    def test_wildcards_match_everything(self, pgpass):
        pgpass("*:*:*:*:universal")
        assert dump.get_password_from_pgpass("any.host", 9999, "anydb", "anyone") == "universal"

    def test_a_different_port_does_not_match(self, pgpass):
        pgpass("db.example.com:5432:ventas:reader:s3cr3t")
        assert dump.get_password_from_pgpass("db.example.com", 6432, "ventas", "reader") is None

    def test_a_different_user_does_not_match(self, pgpass):
        pgpass("db.example.com:5432:ventas:reader:s3cr3t")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "writer") is None

    def test_first_matching_line_wins(self, pgpass):
        pgpass(
            "db.example.com:5432:ventas:reader:specific",
            "*:*:*:*:fallback",
        )
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == ("specific")

    def test_comments_and_blank_lines_are_skipped(self, pgpass):
        pgpass(
            "# produccion",
            "",
            "   ",
            "db.example.com:5432:ventas:reader:s3cr3t",
        )
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "s3cr3t"

    def test_short_lines_are_skipped_not_fatal(self, pgpass):
        pgpass(
            "db.example.com:5432:ventas",
            "db.example.com:5432:ventas:reader:s3cr3t",
        )
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "s3cr3t"

    def test_a_password_with_colons_is_kept_whole(self, pgpass):
        pgpass("db.example.com:5432:ventas:reader:a:b:c")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "a:b:c"

    def test_wildcard_database_covers_the_dumpall_case(self, pgpass):
        # pg_dumpall touches every database, so it asks with "*".
        pgpass("db.example.com:5432:*:reader:s3cr3t")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "*", "reader") == "s3cr3t"

    def test_an_unreadable_pgpass_is_not_fatal(self, pgpass):
        path = pgpass("db.example.com:5432:ventas:reader:s3cr3t")
        path.chmod(0o000)
        try:
            assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None
        finally:
            path.chmod(0o600)


class TestRemainingCorners:
    def test_tunnel_host_that_is_an_ipv6_literal_gets_bracketed(self, mod, clean_pg_env):
        _, _, final = tunnel(mod, ["-d", "postgresql://db.example.com/mydb"], tunnel_host="::1", tunnel_port=49999)
        assert final == ["-d", "postgresql://[::1]:49999/mydb"]

    def test_hostaddr_alone_is_enough_to_find_the_target(self, mod, clean_pg_env):
        host, _, _, has_host, _ = mod.parse_connection_args(["-d", "hostaddr=10.0.0.9 dbname=x"])
        assert host == "10.0.0.9"
        assert has_host is True

    def test_socket_uri_is_passed_through_untouched(self, mod, clean_pg_env):
        # postgresql:///mydb has no host to rewrite; it must not be mangled,
        # and the tunnel flags still get appended for the caller to use.
        _, _, final = tunnel(mod, ["-d", "postgresql:///mydb"])
        assert final == ["-d", "postgresql:///mydb", "-h", "127.0.0.1", "-p", "49999"]


class TestTunneledRunEndToEnd:
    """The whole path: parse, start the tunnel, find the password, run.

    These are the lines that decide whether a tunnelled backup authenticates,
    and they are the ones a URI used to walk straight past.
    """

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        """Cleared before the test body runs, so a test can set PGPASSWORD itself."""
        for var in ("PGPASSWORD", "PGUSER", "PGDATABASE", "PGHOST", "PGPORT"):
            monkeypatch.delenv(var, raising=False)

    @pytest.fixture
    def run_wrapper(self, tmp_path, monkeypatch):
        """Invoke a wrapper CLI with the tunnel, config and pg_dump* all faked."""
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner

        def run(mod, argv, pgpass_lines=(), tunnel=("127.0.0.1", 54321)):
            if pgpass_lines:
                (tmp_path / ".pgpass").write_text("".join(line + "\n" for line in pgpass_lines))
                (tmp_path / ".pgpass").chmod(0o600)
            monkeypatch.setattr(dump.Path, "home", staticmethod(lambda: tmp_path))

            manager = MagicMock()
            manager.start_tunnel.return_value = tunnel
            with (
                patch.object(mod, "get_config", return_value={}),
                patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
                patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)) as srun,
            ):
                result = CliRunner().invoke(mod.cli, argv)
            return result, manager, srun

        return run

    def test_uri_target_reaches_the_tunnel_and_the_command(self, mod, run_wrapper):
        result, manager, srun = run_wrapper(mod, ["-d", "postgresql://reader@db.internal:6432/ventas"])
        assert result.exit_code == 0
        # The tunnel is built to the host the URI named, not to localhost.
        manager.start_tunnel.assert_called_once_with(host="db.internal", port=6432, dsn_alias=None)
        cmd = srun.call_args[0][0]
        assert "postgresql://reader@127.0.0.1:54321/ventas" in cmd
        assert "db.internal" not in " ".join(cmd)

    def test_password_is_looked_up_for_the_original_host(self, mod, run_wrapper):
        result, _, srun = run_wrapper(
            mod,
            ["-d", "postgresql://reader@db.internal:6432/ventas"],
            pgpass_lines=["db.internal:6432:*:reader:s3cr3t"],
        )
        assert result.exit_code == 0
        assert srun.call_args[1]["env"]["PGPASSWORD"] == "s3cr3t"

    def test_a_pgpass_line_for_the_tunnel_endpoint_is_not_used(self, mod, run_wrapper):
        # 127.0.0.1 is where pg_dump connects, but it is not who the user is.
        result, _, srun = run_wrapper(
            mod,
            ["-d", "postgresql://reader@db.internal:6432/ventas"],
            pgpass_lines=["127.0.0.1:54321:*:reader:wrong"],
        )
        assert result.exit_code == 0
        assert "PGPASSWORD" not in srun.call_args[1]["env"]

    def test_an_existing_pgpassword_is_left_alone(self, mod, run_wrapper, monkeypatch):
        monkeypatch.setenv("PGPASSWORD", "from-the-environment")
        result, _, srun = run_wrapper(
            mod,
            ["-h", "db.internal", "mydb"],
            pgpass_lines=["db.internal:5432:*:postgres:from-pgpass"],
        )
        assert result.exit_code == 0
        assert srun.call_args[1]["env"]["PGPASSWORD"] == "from-the-environment"

    def test_without_a_tunnel_the_arguments_are_untouched(self, mod, run_wrapper):
        result, _, srun = run_wrapper(mod, ["-h", "db.internal", "-p", "5432", "mydb"], tunnel=("db.internal", 5432))
        assert result.exit_code == 0
        cmd = srun.call_args[0][0]
        assert cmd[1:] == ["-h", "db.internal", "-p", "5432", "mydb"]


class TestWrapperPlumbing:
    """The bits around the dump: logging, finding the binary, surviving a bad config.

    test_dump.py covers these for pgcli_dump only; pgcli_dumpall has the same
    code and had none of the coverage.
    """

    def test_verbose_turns_on_debug_logging(self, mod):
        logger = mod.setup_logging(verbose=True)
        try:
            assert logger.level == logging.DEBUG
            assert logger.handlers[-1].level == logging.DEBUG
        finally:
            logger.handlers.clear()

    def test_quiet_is_the_default(self, mod):
        logger = mod.setup_logging(verbose=False)
        try:
            assert logger.level == logging.WARNING
        finally:
            logger.handlers.clear()

    def test_the_binary_is_found_on_path(self, mod, monkeypatch, tmp_path):
        name = "pg_dump" if mod is dump else "pg_dumpall"
        fake = tmp_path / name
        fake.write_text("#!/bin/sh\n")
        fake.chmod(0o755)
        monkeypatch.setenv("PATH", str(tmp_path))
        finder = mod.find_pg_dump if mod is dump else mod.find_pg_dumpall
        assert finder() == str(fake)

    def test_a_versioned_install_is_found_when_path_has_nothing(self, mod, monkeypatch, tmp_path):
        name = "pg_dump" if mod is dump else "pg_dumpall"
        monkeypatch.setenv("PATH", str(tmp_path / "empty"))
        # Only the versioned path exists. A dev box usually has /usr/bin/pg_dump
        # too, and that one is checked first, so it has to be hidden here.
        known = f"/usr/pgsql-16/bin/{name}"
        monkeypatch.setattr(os.path, "isfile", lambda p: p == known)
        monkeypatch.setattr(os, "access", lambda p, m: p == known)
        finder = mod.find_pg_dump if mod is dump else mod.find_pg_dumpall
        assert finder() == known

    def test_falls_back_to_a_bare_name_when_nothing_is_found(self, mod, monkeypatch, tmp_path):
        name = "pg_dump" if mod is dump else "pg_dumpall"
        monkeypatch.setenv("PATH", str(tmp_path / "empty"))
        monkeypatch.setattr(os.path, "isfile", lambda p: False)
        finder = mod.find_pg_dump if mod is dump else mod.find_pg_dumpall
        assert finder() == name

    def test_an_unreadable_config_is_a_warning_not_a_crash(self, mod, monkeypatch, tmp_path):
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner

        for var in ("PGHOST", "PGPORT", "PGPASSWORD"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setattr(dump.Path, "home", staticmethod(lambda: tmp_path))
        manager = MagicMock()
        manager.start_tunnel.return_value = ("db.internal", 5432)
        with (
            patch.object(mod, "get_config", side_effect=OSError("boom")),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)),
        ):
            result = CliRunner().invoke(mod.cli, ["-h", "db.internal", "mydb"])
        assert result.exit_code == 0

    def test_main_delegates_to_the_cli(self, mod):
        from unittest.mock import patch

        with patch.object(mod, "cli") as fake_cli:
            mod.main()
        fake_cli.assert_called_once_with()

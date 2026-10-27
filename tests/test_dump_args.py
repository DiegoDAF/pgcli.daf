"""Connection-argument parsing and tunnel rewriting for pgcli_dump and pgcli_dumpall.

Every test runs against both wrappers. The two modules carried a byte-identical
copy of these functions for a long time, and only the pgcli.dump copy was ever
tested, so a fix could land in one and miss the other. Parametrising over both
keeps them honest whether they share an implementation or not.
"""

from utils import posix_only
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
            # libpq takes the query's port over the authority's (measured), so
            # the rewritten URI would send pg_dump to 127.0.0.1:<that port>,
            # a local server, instead of through the tunnel.
            "postgresql://db.example.com/mydb?port=5432",
            "postgresql://db.example.com:6543/mydb?port=5432",
        ],
        ids=["multihost", "host-in-query", "hostaddr-in-query", "port-in-query", "two-ports"],
    )
    def test_ambiguous_uri_is_reported_not_guessed(self, mod, clean_pg_env, uri):
        # Refused where the tunnel has to rewrite it, not before: pg_dump itself
        # takes several hosts, and a dump that needs no tunnel must run.
        with pytest.raises(ValueError) as excinfo:
            mod.build_tunneled_args(["-d", uri], "127.0.0.1", 40000, None, None, True, False)
        assert "tunnel" in str(excinfo.value).lower()
        host, _, _, has_host, _ = mod.parse_connection_args(["-d", uri])
        assert has_host is (uri.split("//", 1)[1][0] != "/")  # postgresql:///mydb names no host
        if has_host:
            assert "," not in host


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

    @pytest.mark.parametrize(
        "args",
        [
            ["-h", "dbx", "-t", "public.orders"],
            ["--table", "public.orders"],
            ["-n", "sales", "-N", "audit", "-T", "tmp_*", "-j", "4", "-Z", "9"],
            ["--exclude-table-data", "logs", "--section", "data", "--role", "dumper"],
        ],
        ids=["short", "long", "several-short", "several-long"],
    )
    def test_values_of_any_option_are_not_the_database(self, args):
        # Every pg_dump option that takes a value (pg_dump 18 --help), not just
        # -h/-p/-U/-d/-f/-F: their value is not a positional database name.
        assert dump.parse_user_and_database(args)[1] == "*"

    @pytest.mark.parametrize(
        "args",
        [
            ["-t", "public.orders", "ventas"],
            ["ventas", "-f", "out.sql"],
            ["ventas", "-t", "public.orders", "-F", "c"],
            ["-tpublic.orders", "-Fc", "ventas"],
            ["-Ot", "public.orders", "ventas"],
            ["--table=public.orders", "ventas", "--schema", "sales"],
            ["-f", "out.sql", "--", "ventas"],
        ],
        ids=["after-options", "first", "middle", "attached-values", "flag-cluster", "mixed", "after-double-dash"],
    )
    def test_the_database_is_found_wherever_it_is(self, args):
        # pg_dump uses getopt, which reorders arguments: the database name may
        # come before, between or after the options.
        assert dump.parse_user_and_database(args)[1] == "ventas"

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


class TestDsnAliasArguments:
    """--dsn ALIAS turns into the dump tool's own arguments."""

    ALIAS = "postgresql://reader@db.internal:6432/ventas?sslmode=verify-ca"

    def _conn(self, args):
        from psycopg.conninfo import conninfo_to_dict

        return conninfo_to_dict(args[args.index("-d") + 1])

    def test_the_alias_becomes_a_connection_string(self):
        from pgcli.dump_args import apply_dsn_alias

        args, password = apply_dsn_alias(["-F", "c", "-f", "out.dump"], self.ALIAS)
        assert self._conn(args) == {
            "host": "db.internal",
            "port": "6432",
            "dbname": "ventas",
            "user": "reader",
            "sslmode": "verify-ca",
        }
        assert args[2:] == ["-F", "c", "-f", "out.dump"]
        assert password is None

    def test_a_database_operand_replaces_the_aliases_database(self):
        from pgcli.dump_args import apply_dsn_alias

        args, _ = apply_dsn_alias(["otherdb", "-F", "c"], self.ALIAS)
        assert self._conn(args)["dbname"] == "otherdb"
        assert "otherdb" not in args[2:]

    @pytest.mark.parametrize(
        "flags",
        [["-h", "db2", "-p", "7000", "-U", "writer"], ["-hdb2", "--port=7000", "--username", "writer"]],
        ids=["separate", "attached"],
    )
    def test_host_port_and_user_on_the_command_line_win(self, flags):
        from pgcli.dump_args import apply_dsn_alias

        args, _ = apply_dsn_alias(flags + ["-t", "public.orders"], self.ALIAS)
        conn = self._conn(args)
        assert (conn["host"], conn["port"], conn["user"]) == ("db2", "7000", "writer")
        assert args[2:] == ["-t", "public.orders"]

    def test_a_password_in_the_alias_stays_out_of_the_command_line(self):
        # Arguments show up in ps; the wrapper hands the password over in
        # PGPASSWORD instead.
        from pgcli.dump_args import apply_dsn_alias

        args, password = apply_dsn_alias([], "postgresql://reader:s3cr3t@db.internal/ventas")
        assert password == "s3cr3t"
        assert "s3cr3t" not in " ".join(args)

    @pytest.mark.parametrize("flags", [["-d", "other"], ["--dbname=other"]])
    def test_dsn_and_dbname_together_are_refused(self, flags):
        from pgcli.dump_args import apply_dsn_alias

        with pytest.raises(ValueError) as excinfo:
            apply_dsn_alias(flags, self.ALIAS)
        assert "--dsn" in str(excinfo.value)


class TestPgpassLookup:
    """Reading the password for the ORIGINAL host, before the tunnel renames it."""

    @pytest.fixture
    def pgpass(self, tmp_path, monkeypatch):
        """Write a .pgpass and point Path.home() at it."""

        def write(*lines, mode=0o600):
            path = tmp_path / ".pgpass"
            path.write_text("".join(line + "\n" for line in lines))
            path.chmod(mode)
            monkeypatch.setenv("PGPASSFILE", str(path))
            return path

        return write

    @posix_only
    def test_a_world_readable_pgpass_is_refused(self, pgpass, capsys):
        """libpq ignores a password file others can read and warns about it.
        The wrappers used to read it anyway, so pgcli_dump could authenticate
        with a password psql would have refused."""
        pgpass("db.example.com:5432:ventas:reader:s3cr3t", mode=0o644)
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None
        err = capsys.readouterr().err
        assert "group or world access" in err and "0600" in err

    @posix_only
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
        monkeypatch.setenv("PGPASSFILE", str(tmp_path / "missing"))
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

    @posix_only
    def test_an_unreadable_pgpass_is_not_fatal(self, pgpass):
        path = pgpass("db.example.com:5432:ventas:reader:s3cr3t")
        path.chmod(0o000)
        try:
            assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None
        finally:
            path.chmod(0o600)

    # The wrappers now read the file the way pgcli and libpq do
    # (pgpass.lookup_password), instead of a second, looser reader.

    def test_an_escaped_colon_in_the_password(self, pgpass):
        # libpq: "\\:" is a literal colon, the backslash is not part of the password.
        pgpass("db.example.com:5432:ventas:reader:a\\:b")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "a:b"

    def test_an_escaped_colon_in_a_field(self, pgpass):
        pgpass("db.example.com:5432:ventas:odd\\:user:s3cr3t")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "odd:user") == "s3cr3t"

    def test_pgpassfile_is_honored(self, tmp_path, monkeypatch):
        elsewhere = tmp_path / "custom.pgpass"
        elsewhere.write_text("db.example.com:5432:ventas:reader:from-env\n")
        elsewhere.chmod(0o600)
        monkeypatch.setenv("PGPASSFILE", str(elsewhere))
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") == "from-env"

    def test_a_partial_glob_is_not_a_wildcard(self, pgpass):
        # libpq takes a field literally unless it is exactly "*", so psql and
        # pgcli never use this line; the wrappers no longer do either.
        pgpass("*.example.com:5432:ventas:reader:s3cr3t")
        assert dump.get_password_from_pgpass("db.example.com", 5432, "ventas", "reader") is None


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

        def run(mod, argv, pgpass_lines=(), tunnel=("127.0.0.1", 54321), config=None):
            if pgpass_lines:
                (tmp_path / ".pgpass").write_text("".join(line + "\n" for line in pgpass_lines))
                (tmp_path / ".pgpass").chmod(0o600)
            monkeypatch.setenv("PGPASSFILE", str(tmp_path / ".pgpass"))

            manager = MagicMock()
            manager.start_tunnel.return_value = tunnel
            with (
                patch.object(mod, "get_config", return_value=config or {}),
                patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
                patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)) as srun,
            ):
                result = CliRunner().invoke(mod.cli, argv)
            return result, manager, srun

        return run

    ALIAS_CONFIG = {
        "alias_dsn": {
            "prod": "postgresql://reader@db.internal:6432/ventas?sslmode=verify-ca&sslrootcert=/certs/rds.pem",
        }
    }

    def test_dsn_alias_is_the_connection(self, mod, run_wrapper):
        # Like pgcli --dsn: the alias names the host, port, user, database and
        # SSL settings, not only which tunnel to use.
        from psycopg.conninfo import conninfo_to_dict

        result, manager, srun = run_wrapper(
            mod,
            ["--dsn", "prod", "-f", "/dev/null"],
            # "*" for the database: pgcli_dumpall asks .pgpass with "*" by design.
            pgpass_lines=["db.internal:6432:*:reader:s3cr3t"],
            config=self.ALIAS_CONFIG,
        )
        assert result.exit_code == 0, result.output
        manager.start_tunnel.assert_called_once_with(host="db.internal", port=6432, dsn_alias="prod")
        cmd = srun.call_args[0][0]
        conn = conninfo_to_dict(cmd[cmd.index("-d") + 1])
        assert (conn["port"], conn["dbname"], conn["user"]) == ("54321", "ventas", "reader")
        assert (conn["sslmode"], conn["sslrootcert"]) == ("verify-ca", "/certs/rds.pem")
        assert srun.call_args[1]["env"]["PGPASSWORD"] == "s3cr3t"

    def test_unknown_dsn_alias_is_an_error(self, mod, run_wrapper):
        result, _, srun = run_wrapper(mod, ["--dsn", "nope"], config=self.ALIAS_CONFIG)
        assert result.exit_code == 1
        assert "Could not find a DSN with alias nope" in result.output
        srun.assert_not_called()

    @pytest.mark.parametrize(
        "argv",
        [["-d", "postgresql://h1,h2/db"], ["-h", "db.internal", "-p", "abc"], ["--dsn", "prod", "-d", "other"]],
        ids=["multi-host-uri", "non-numeric-port", "dsn-and-d"],
    )
    def test_bad_arguments_are_a_message_not_a_traceback(self, mod, run_wrapper, argv):
        result, _, srun = run_wrapper(mod, argv, config=self.ALIAS_CONFIG)
        assert result.exit_code == 1
        assert isinstance(result.exception, SystemExit)
        assert "Traceback" not in result.output
        assert result.output.strip()
        srun.assert_not_called()

    @pytest.mark.parametrize("flag", ["-v", "--verbose"])
    def test_verbose_reaches_pg_dump(self, mod, run_wrapper, flag):
        # pg_dump and pg_dumpall have their own -v/--verbose; the wrapper used to
        # take it for its tunnel debug output, so it never got through.
        result, _, srun = run_wrapper(mod, [flag, "-h", "db.internal", "mydb"])
        assert result.exit_code == 0
        assert flag in srun.call_args[0][0]

    def test_tunnel_verbose_is_the_wrappers_own_flag(self, mod, run_wrapper):
        from unittest.mock import patch

        with patch.object(mod, "setup_logging", wraps=mod.setup_logging) as setup:
            result, _, srun = run_wrapper(mod, ["--tunnel-verbose", "-h", "db.internal", "mydb"])
        assert result.exit_code == 0
        setup.assert_called_once_with(True)
        assert "--tunnel-verbose" not in srun.call_args[0][0]

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

    def test_tunnel_verbose_does_not_log_the_password(self, mod, run_wrapper, caplog):
        # --tunnel-verbose logged the child command line with the -d connection
        # string intact, password included.
        import logging

        with caplog.at_level(logging.DEBUG):
            result, _, _ = run_wrapper(
                mod, ["--tunnel-verbose", "-d", "postgresql://reader:s3cr3t@db.internal:6432/ventas", "-f", "/dev/null"]
            )
        assert result.exit_code == 0, result.output
        assert "Executing:" in caplog.text
        assert "s3cr3t" not in caplog.text
        assert "reader:***@127.0.0.1:54321" in caplog.text


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
        monkeypatch.setenv("PGPASSFILE", str(tmp_path / ".pgpass"))
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


class TestAttachedShortOptions:
    """getopt's -hHOST / -p5433 / -dDB forms. Only "-h HOST" was read, so
    "pgcli_dump -hdb.example.com mydb" dialed the host directly, no tunnel."""

    def test_parse_reads_the_attached_forms(self):
        from pgcli.dump_args import parse_connection_args

        host, port, remaining, has_host, has_port = parse_connection_args(["-hdb.example.com", "-p5433", "-dmydb", "-Fc"])
        assert (host, port, has_host, has_port) == ("db.example.com", 5433, True, True)
        assert remaining == ["-hdb.example.com", "-p5433", "-dmydb", "-Fc"]

    def test_parse_reads_a_connection_string_attached_to_d(self):
        from pgcli.dump_args import parse_connection_args

        host, port, _, has_host, has_port = parse_connection_args(["-dpostgresql://u@db.example.com:5433/mydb"])
        assert (host, port, has_host, has_port) == ("db.example.com", 5433, True, True)

    def test_build_rewrites_the_attached_forms(self):
        from pgcli.dump_args import build_tunneled_args

        args = ["-hdb.example.com", "-p5433", "-dpostgresql://u@db.example.com:5433/mydb", "-Fc"]
        assert build_tunneled_args(args, "127.0.0.1", 40000, "db.example.com", 5433, True, True) == [
            "-h", "127.0.0.1", "-p", "40000", "-d", "postgresql://u@127.0.0.1:40000/mydb", "-Fc",
        ]  # fmt: skip

    def test_a_bad_attached_port_is_an_error_not_a_traceback(self):
        from pgcli.dump_args import parse_connection_args

        with pytest.raises(ValueError, match="not a valid port number"):
            parse_connection_args(["-pabc"])

    def test_parse_reads_a_flag_cluster_ending_in_h(self):
        # getopt: -ch HOST is --clean plus -h HOST; -chHOST attaches the value.
        from pgcli.dump_args import parse_connection_args

        for args in (["-ch", "db.internal", "mydb"], ["-chdb.internal", "mydb"]):
            host, port, remaining, has_host, has_port = parse_connection_args(args)
            assert (host, has_host, has_port) == ("db.internal", True, False), args
            assert remaining == args

    def test_build_rewrites_a_flag_cluster_keeping_its_flags(self):
        from pgcli.dump_args import build_tunneled_args

        assert build_tunneled_args(["-ch", "db.internal", "mydb"], "127.0.0.1", 40000, None, None, True, False) == [
            "-c", "-h", "127.0.0.1", "mydb", "-p", "40000",
        ]  # fmt: skip
        assert build_tunneled_args(["-Ochdb.internal", "-p5433"], "127.0.0.1", 40000, None, None, True, True) == [
            "-Oc", "-h", "127.0.0.1", "-p", "40000",
        ]  # fmt: skip

    def test_a_dangling_short_option_is_left_to_pg_dump(self):
        from pgcli.dump_args import build_tunneled_args, parse_connection_args

        assert parse_connection_args(["mydb", "-h"])[2] == ["mydb", "-h"]
        assert build_tunneled_args(["mydb", "-h"], "127.0.0.1", 40000, None, None, False, False) == [
            "mydb",
            "-h",
            "-h",
            "127.0.0.1",
            "-p",
            "40000",
        ]


class TestMultiHostConnectionStrings:
    """pg_dump takes libpq's multi-host strings; only a tunnel cannot rewrite
    them. They were refused before the tunnel decision, so a plain dump with
    postgresql://a,b/db (no tunnel anywhere) died with a message about tunnels,
    while host=a,b was quietly rewritten to one host."""

    def test_parsing_reports_the_first_host_and_does_not_refuse(self):
        from pgcli.dump_args import parse_connection_args

        host, port, _, has_host, has_port = parse_connection_args(["-d", "postgresql://a:5432,b:5433/db"])
        assert (host, port, has_host, has_port) == ("a", 5432, True, True)
        host, port, _, has_host, has_port = parse_connection_args(["-d", "host=a,b port=5432,5433 dbname=db"])
        assert (host, port, has_host, has_port) == ("a", 5432, True, True)

    def test_the_rewrite_for_a_tunnel_refuses_both_forms(self):
        from pgcli.dump_args import build_tunneled_args

        for value in ("postgresql://a,b/db", "host=a,b dbname=db", "postgresql://a/db?host=c"):
            with pytest.raises(ValueError, match="through an SSH tunnel"):
                build_tunneled_args(["-d", value], "127.0.0.1", 40000, None, None, True, False)

    @pytest.mark.parametrize("value", ["postgresql://a,b/db", "host=a,b dbname=db"])
    def test_without_a_tunnel_the_dump_runs_untouched(self, value):
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner
        import pgcli.dump as mod

        manager = MagicMock()
        manager.start_tunnel.side_effect = lambda host, port, dsn_alias=None: (host, port)
        with (
            patch.object(mod, "get_config", return_value={}),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)) as srun,
        ):
            result = CliRunner().invoke(mod.cli, ["-d", value, "-f", "/dev/null"])
        assert result.exit_code == 0, result.output
        assert srun.call_args[0][0][1:] == ["-d", value, "-f", "/dev/null"]

    def test_with_a_tunnel_the_dump_is_refused_in_one_line(self):
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner
        import pgcli.dump as mod

        manager = MagicMock()
        manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        with (
            patch.object(mod, "get_config", return_value={}),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)) as srun,
        ):
            result = CliRunner().invoke(mod.cli, ["-d", "host=a,b dbname=db", "-f", "/dev/null"])
        assert result.exit_code == 1
        assert "cannot route a multi-host connection string through an SSH tunnel" in result.output
        srun.assert_not_called()
        manager.stop_tunnel.assert_called()


class TestTheDatabaseOperand:
    """pg_dump(1): the first non-option argument is the dbname, "equivalent to
    specifying" -d, connection strings included. It was never read, so
    `pgcli_dump postgresql://db.internal/mydb` took its tunnel decision for
    localhost and pg_dump dialed db.internal directly."""

    def test_an_operand_uri_names_the_server(self, clean_pg_env):
        from pgcli.dump_args import parse_connection_args

        host, port, _, has_host, has_port = parse_connection_args(["postgresql://db.example.com:5433/mydb", "-Fc"])
        assert (host, port, has_host, has_port) == ("db.example.com", 5433, True, True)

    def test_the_operand_is_found_past_options_with_values(self, clean_pg_env):
        from pgcli.dump_args import parse_connection_args

        host, *_ = parse_connection_args(["-t", "public.orders", "-f", "out.sql", "host=db.example.com dbname=mydb"])
        assert host == "db.example.com"
        # The value of -t is not the operand, whatever it looks like.
        host, *_ = parse_connection_args(["-t", "postgresql://not.a.host/x", "mydb"])
        assert host == "localhost"

    def test_a_bare_name_operand_changes_nothing(self, clean_pg_env):
        from pgcli.dump_args import parse_connection_args

        host, port, remaining, has_host, has_port = parse_connection_args(["mydb", "-Fc"])
        assert (host, port, has_host, has_port) == ("localhost", 5432, False, False)
        assert remaining == ["mydb", "-Fc"]

    def test_the_operand_is_rewritten_for_the_tunnel(self):
        from pgcli.dump_args import build_tunneled_args

        assert build_tunneled_args(["postgresql://db.example.com:5433/mydb", "-Fc"], "127.0.0.1", 40000, None, None, True, True) == [
            "postgresql://127.0.0.1:40000/mydb", "-Fc",
        ]  # fmt: skip
        assert build_tunneled_args(["-Fc", "host=db.example.com dbname=mydb"], "127.0.0.1", 40000, None, None, True, False) == [
            "-Fc", "host=127.0.0.1 dbname=mydb", "-p", "40000",
        ]  # fmt: skip

    def test_the_wrapper_tunnels_an_operand_uri(self, clean_pg_env):
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner
        import pgcli.dump as mod

        manager = MagicMock()
        manager.start_tunnel.return_value = ("127.0.0.1", 54321)
        with (
            patch.object(mod, "get_config", return_value={}),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)) as srun,
        ):
            result = CliRunner().invoke(mod.cli, ["postgresql://db.internal/mydb", "-f", "/dev/null"])
        assert result.exit_code == 0, result.output
        manager.start_tunnel.assert_called_once_with(host="db.internal", port=5432, dsn_alias=None)
        assert srun.call_args[0][0][1] == "postgresql://127.0.0.1:54321/mydb"


class TestSmallGetoptGaps:
    def test_timeout_takes_a_value(self):
        # pg_isready's --timeout SECS: without it, under --dsn the seconds became the database.
        from pgcli.dump_args import getopt_items

        assert getopt_items(["--timeout", "5"]) == [("--timeout", "5")]

    def test_a_keyword_string_without_host_is_a_connection_string(self):
        # libpq: any value with "=" is a connection string; read as a bare
        # name it sent .pgpass after a database called "dbname=mydb user=bob".
        from pgcli.dump_args import dbname_identity, is_connstring

        assert dbname_identity("dbname=mydb user=bob") == ("bob", "mydb")
        assert is_connstring("dbname=mydb") is True
        assert is_connstring("mydb") is False

    @pytest.mark.parametrize("option", ["--host", "--port"])
    def test_a_dangling_long_option_is_not_rewritten(self, option):
        # It got the tunnel's value and then the tunnel again: pg_dump should
        # see the option it was given and refuse it.
        from pgcli.dump_args import build_tunneled_args

        assert build_tunneled_args([option], "127.0.0.1", 40000, None, None, False, False) == [option, "-h", "127.0.0.1", "-p", "40000"]


class TestTheWrappersAndPs:
    def test_the_attached_dbname_is_redacted_without_a_tunnel(self, caplog):
        # getopt's -dpassword=... kept the "d" glued to the keyword, and the
        # no-tunnel path logs the arguments as given.
        import logging
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner
        import pgcli.dump as mod

        manager = MagicMock()
        manager.start_tunnel.side_effect = lambda host, port, dsn_alias=None: (host, port)
        with (
            patch.object(mod, "get_config", return_value={}),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)),
            caplog.at_level(logging.DEBUG),
        ):
            result = CliRunner().invoke(mod.cli, ["--tunnel-verbose", "-dpassword=s3cret host=db.internal dbname=x", "-f", "/dev/null"])
        assert result.exit_code == 0, result.output
        assert "Executing:" in caplog.text and "s3cret" not in caplog.text

    @pytest.mark.parametrize("modname", ["pgcli.dump", "pgcli.dumpall", "pgcli.isready"])
    def test_the_wrappers_hide_the_password_from_ps(self, modname):
        # Only pgcli rewrote its process title; a wrapper given
        # postgresql://u:pass@h/db showed it in ps for the whole dump.
        import importlib
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner

        mod = importlib.import_module(modname)
        fake = MagicMock()
        fake.getproctitle.return_value = f"{modname} postgresql://u:s3cret@h/db -f /dev/null"
        manager = MagicMock()
        manager.start_tunnel.side_effect = lambda host, port, dsn_alias=None: (host, port)
        with (
            patch.object(mod, "setproctitle", fake),
            patch.object(mod, "get_config", return_value={}),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)),
        ):
            CliRunner().invoke(mod.cli, ["postgresql://u:s3cret@h/db", "-f", "/dev/null"])
        fake.setproctitle.assert_called_once_with(f"{modname} postgresql://u:xxxx@h/db -f /dev/null")


class TestTunnelFailureInTheWrappers:
    @pytest.mark.parametrize("modname", ["pgcli.dump", "pgcli.dumpall", "pgcli.isready"])
    def test_a_tunnel_that_cannot_be_opened_is_one_line(self, modname):
        import importlib
        from unittest.mock import MagicMock, patch
        from click.testing import CliRunner
        from pgcli.ssh_tunnel import SSHTunnelError

        mod = importlib.import_module(modname)
        manager = MagicMock()
        manager.start_tunnel.side_effect = SSHTunnelError("no route to bastion")
        with (
            patch.object(mod, "get_config", return_value={}),
            patch.object(mod, "get_tunnel_manager_from_config", return_value=manager),
            patch.object(mod.subprocess, "run", return_value=MagicMock(returncode=0)) as srun,
        ):
            result = CliRunner().invoke(mod.cli, ["-h", "db.internal"])
        assert result.exit_code == 1
        assert "SSH tunnel error: no route to bastion" in result.output
        assert "Traceback" not in result.output
        srun.assert_not_called()

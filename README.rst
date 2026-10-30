pgcli.daf
=========

::

     .--.   .--.                         _  _         _          __
    /    \_/    \    _ __    __ _   ___ | |(_)     __| |  __ _  / _|
    |  o     o  |   | '_ \  / _` | / __|| || |    / _` | / _` || |_
     \   .-.   /    | |_) || (_| || (__ | || | _ | (_| || (_| ||  _|
      '-'| |'-'     | .__/  \__, | \___||_||_|(_) \__,_| \__,_||_|
         | |        |_|     |___/
         '-'        the Postgres REPL, with a DBA's toolbox bolted on

A fork of `pgcli <https://github.com/dbcli/pgcli>`_: everything upstream
does (auto-completion, syntax highlighting, pretty tables), plus SSH tunnels,
psql-style scripting, dump wrappers, named-query libraries and a set of safety
rails, built by a DBA who lives in it all day.

Tracks upstream **4.7.1**. Latest release: **4.7.4**
(`releases <https://github.com/DiegoDAF/pgcli.daf/releases>`_). Fixes that
make sense for everyone are sent upstream as small PRs.

.. image:: screenshots/pgcli.gif
.. image:: screenshots/image01.png

Quick start
-----------

pgcli.daf installs with `uv <https://docs.astral.sh/uv/>`_, in an environment
of its own, so it never touches the system Python or other tools (pgAdmin
included). Get uv first if you do not have it
(`other ways <https://docs.astral.sh/uv/getting-started/installation/>`_):

::

    curl -LsSf https://astral.sh/uv/install.sh | sh

Then pgcli.daf, from the release wheel or straight from the tag:

::

    uv tool install "pgcli[sshtunnel,keyring] @ https://github.com/DiegoDAF/pgcli.daf/releases/download/v4.7.4/pgcli-4.7.4-py3-none-any.whl"

    uv tool install "pgcli[sshtunnel,keyring] @ git+https://github.com/DiegoDAF/pgcli.daf@v4.7.4"

To upgrade, run the same command for the new version with
``--force --reinstall`` (without ``--reinstall`` the dependencies stay old).

Both extras matter: ``sshtunnel`` brings paramiko for the tunnels, ``keyring``
lets pgcli save database and tunnel passwords in the OS keyring. That installs
four commands: ``pgcli``, ``pgcli_dump``, ``pgcli_dumpall`` and
``pgcli_isready``. The CI runs the tests on Linux and on Windows.

Usage
-----

::

    pgcli mydb
    pgcli postgresql://user@db.example.com:5432/app?sslmode=verify-full
    pgcli --dsn production                       # an alias from the config
    pgcli service=reporting                      # .pg_service.conf, every libpq key
    pgcli --ssh-tunnel user@bastion -h db.internal app

``pgcli --help`` lists every option. The usual libpq `environment variables
<https://www.postgresql.org/docs/current/libpq-envars.html>`_ (``PGHOST``,
``PGSSLMODE``, ``PGPASSFILE``...) work as in psql.

What pgcli.daf adds
-------------------

SSH tunnels
^^^^^^^^^^^

Native paramiko, no ``sshtunnel`` library.

* ``--ssh-tunnel user@bastion[:port]`` on the command line, or automatic:
  ``[ssh tunnels]`` matches hosts by regex, ``[dsn ssh tunnels]`` matches DSN
  aliases.
* ``~/.ssh/config`` is honoured the way ``ssh`` reads it: ``IdentityFile``,
  ``ProxyJump``, ``ProxyCommand`` (with ``%h %p %r``), ``Include``, and
  ``/etc/ssh/ssh_config`` after the user's file.
* ``host_key_policy`` (``auto-add``, ``warn``, ``reject``), ``allow_agent``,
  and ``ssh_tunnel_save_password`` to keep the key passphrase in the keyring.
* ``.pgpass`` works through the tunnel: the password is looked up for the
  real host and port, not for the tunnel's local one.
* ``\c db user otherhost`` opens that server's own tunnel; a reconnect after a
  dropped connection revives a dead tunnel. Tunnel failures are one clear line.

Dump wrappers
^^^^^^^^^^^^^

``pgcli_dump``, ``pgcli_dumpall`` and ``pgcli_isready`` wrap the PostgreSQL
tools with the same tunnels and aliases. Every other option goes through
untouched.

::

    pgcli_dump --dsn production -F c -f app.dump          # the alias is the connection
    pgcli_dump --ssh-tunnel user@bastion -h db.internal app > app.sql
    pgcli_dumpall --dsn production -g -f globals.sql
    pgcli_isready --dsn production

* ``--dsn ALIAS`` uses the alias as the connection, as ``pgcli --dsn`` does
  (``-h``/``-p``/``-U`` still override it, a bare database name replaces its
  database).
* ``-v`` reaches pg_dump and pg_dumpall; the tunnel's own debug output is
  ``--tunnel-verbose`` there (``-v`` on ``pgcli_isready``).
* The password file is read exactly as libpq reads it.

Scripting, the psql way
^^^^^^^^^^^^^^^^^^^^^^^

::

    pgcli -c "select count(*) from orders" -c "\dt" app        # several -c
    pgcli -f schema.sql -f data.sql app                        # several -f, and -c with -f
    pgcli -t -o out.csv -c "select id, name from users" app    # rows only, to a file
    pgcli -y -f cleanup.sql app                                # no destructive-query prompt
    pgcli --no-timings --no-status -c "select 1" app
    pgcli -l                                                   # databases, with their size

* Exit codes as in psql: 1 for a failed ``-c``, 3 for a failed script when
  ``--on-error stop`` (the default, psql's ``ON_ERROR_STOP``); ``resume`` runs
  everything.
* Statements run one at a time: ``\watch`` repeats only its own statement, a
  backslash command ends at its line, a multi-line ``\copy (query) to 'file'``
  stays whole.
* ``--timeout`` / ``connect_timeout`` so a dead host fails fast.
* SQL pasted straight from a markdown answer runs: the fences and the prose are
  dropped, and pgcli says what it removed.

Query tool comforts (pgAdmin-inspired)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **F5** cycles EXPLAIN mode: off, plan, plan with an analysis summary
  (diagnostics, depesz-style per-node stats). **F6** toggles paste mode (each
  statement echoed with its result). **F9** runs only the selected text.
* ``\autocommit [on|off]``, ``stream_results`` (each statement's result as soon
  as it finishes), NOTICE messages streamed live (``vacuum verbose``).
* ``\hist [N]``: the SQL run this session, with elapsed times.
* Completion for ``SET`` (from ``pg_settings``) and ``SET ROLE``.
* Prompt tokens ``\T`` (transaction status) and ``\service``.
* ``$PSQL_EDITOR`` honoured by ``\e``; ``\cd`` and ``\ls`` with path completion.

Named queries and aliases, organised
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``dsn.d/`` and ``namedqueries.d/``: one file per alias or query family.
  A ``-17`` suffix (``activity-17.conf``) offers those queries only on servers
  of that version or newer, as ``.psqlrc-17`` does.
* ``\ne name`` edits a named query in your editor, ``\nq`` runs one without
  echoing it, ``\nr`` reloads them all, ``\n`` lists them sorted.
* ``--init-command`` globally or per DSN, ``application_name``.

Output
^^^^^^

* Number columns right-aligned, as psql and pgAdmin show them.
* The ``float`` format of ``[data_formats]`` actually applies.
* Text from ``SQL_ASCII`` databases decoded (UTF-8, then
  ``sql_ascii_fallback_encoding``) instead of printed as bytes.
* ``#`` is PostgreSQL's XOR operator, not a comment.
* A second Ctrl-C while a query is being cancelled no longer leaves the
  connection stuck.
* The banner says whether SSL is on, and warns when ``sslmode=prefer`` fell
  back to plain text.

Logs and state
^^^^^^^^^^^^^^

* ``log_rotation_mode``: ``none``, ``day-of-week``, ``day-of-month`` or
  ``date``; ``log_destination``; ``log_truncate_on_rotation``.
* History and log live in ``$XDG_STATE_HOME`` (``~/.local/state/pgcli``).

Security
^^^^^^^^

* ``\restrict`` / ``\unrestrict`` (CVE-2025-8714), applied per statement.
* Passwords never reach the debug log, ``--list-dsn``, error lines or the
  process title, in any spelling libpq accepts.
* History, log and config files are created readable by their owner only.
* ``\i``, ``\o`` and ``\log-file`` resolve symlinks and refuse system paths.

Windows
^^^^^^^

* The password file is read the way libpq does there
  (``%APPDATA%\postgresql\pgpass.conf``, no POSIX mode check).
* ``Include`` paths in the ssh config keep their backslashes.

Config
------

The first run writes ``~/.config/pgcli/config`` with every option documented
in place. ``\?`` inside pgcli lists the commands.

Versions
--------

``<upstream major.minor>.<our patch>``: 4.7.4 is upstream 4.7 plus our patch
level 4. See the `changelog <changelog.rst>`_.

Thanks
------

To `Amjith Ramanujam <https://github.com/amjith>`_,
`Irina Truong <https://github.com/j-bennet>`_,
`Damien Baty <https://github.com/dbaty>`_ and everyone behind
`dbcli/pgcli <https://github.com/dbcli/pgcli>`_. Built on
`prompt_toolkit <https://github.com/prompt-toolkit/python-prompt-toolkit>`_,
`psycopg <https://www.psycopg.org/>`_, `paramiko <https://www.paramiko.org/>`_
and `Click <https://click.palletsprojects.com/>`_.

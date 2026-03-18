pgcli.daf — A REPL for Postgres
================================

Fork of `pgcli <https://github.com/dbcli/pgcli>`_ with additional features.
Compatible with upstream pgcli **4.4.0**.

This is a postgres client that does auto-completion and syntax highlighting.

.. image:: screenshots/pgcli.gif
.. image:: screenshots/image01.png

Quick Start
-----------

::

    $ pip install -U pgcli

    or

    $ sudo apt-get install pgcli # Only on Debian based Linux (e.g. Ubuntu, Mint, etc)
    $ brew install pgcli  # Only on macOS

Usage
-----

::

    $ pgcli [database_name]

    or

    $ pgcli postgresql://[user[:password]@][netloc][:port][/dbname][?extra=value[&other=other-value]]

Examples:

::

    $ pgcli local_database

    $ pgcli postgres://amjith:pa$$w0rd@example.com:5432/app_db?sslmode=verify-ca&sslrootcert=/myrootcert

For more details:

::

    $ pgcli --help

    Usage: pgcli [OPTIONS] [DBNAME] [USERNAME]

    Options:
      --application-name TEXT  Application name for the connection.
      --auto-vertical-output   Automatically switch to vertical output mode if
                               the result is wider than the terminal width.
      -c, --command TEXT       Run command (SQL or internal) and exit. Multiple
                               -c options are allowed.
      -d, --dbname TEXT        Database name to connect to.
      -D, --dsn TEXT           Use DSN configured into the [alias_dsn] section
                               of pgclirc file.
      -f, --file FILE          Execute commands from file, then exit. Multiple
                               -f options are allowed.
      -h, --host TEXT          Host address of the postgres database.
      --init-command TEXT      SQL statement to execute after connecting.
      --less-chatty            Skip intro on startup and goodbye on exit.
      -l, --list               List available databases, then exit.
      --list-dsn               List of DSN configured into the [alias_dsn]
                               section of pgclirc file.
      --log-file TEXT          Write all queries & output into a file, in
                               addition to the normal output destination.
      -w, --no-password        Never prompt for password.
      --no-status              Suppress query status line (e.g., SELECT 1).
      --no-timings             Suppress query execution time display.
      -o, --output TEXT        Send query results to file (or |pipe).
      -W, --password           Force password prompt.
      --pgclirc FILE           Location of pgclirc file.
      --ping                   Check database connectivity, then exit.
      -p, --port INTEGER       Port number at which the postgres instance is
                               listening.
      --prompt TEXT            Prompt format (Default: "\u@\h:\d> ").
      --prompt-dsn TEXT        Prompt format for connections using DSN aliases
                               (Default: "\u@\h:\d> ").
      --row-limit INTEGER      Set threshold for row limit prompt. Use 0 to
                               disable prompt.
      --single-connection      Do not use a separate connection for completions.
      --ssh-tunnel TEXT        Open an SSH tunnel to the given address and
                               connect to the database from it.
      -t, --tuples-only        Print rows only, suppress headers, status, and
                               timing.
      -U, --username TEXT      Username to connect to the postgres database.
      -u, --user TEXT          Username to connect to the postgres database.
      -v, --version            Version of pgcli.
      --warn TEXT              Warn before running a destructive query.
      -y, --yes                Force destructive commands without confirmation
                               prompt.
      --help                   Show this message and exit.

``pgcli`` also supports many of the same `environment variables`_ as ``psql`` for login options (e.g. ``PGHOST``, ``PGPORT``, ``PGUSER``, ``PGPASSWORD``, ``PGDATABASE``).

The SSL-related environment variables are also supported, so if you need to connect a postgres database via ssl connection, you can set set environment like this:

::

    export PGSSLMODE="verify-full"
    export PGSSLCERT="/your-path-to-certs/client.crt"
    export PGSSLKEY="/your-path-to-keys/client.key"
    export PGSSLROOTCERT="/your-path-to-ca/ca.crt"
    pgcli -h localhost -p 5432 -U username postgres

.. _environment variables: https://www.postgresql.org/docs/current/libpq-envars.html

Features
--------

The `pgcli` is written using prompt_toolkit_.

* Auto-completes as you type for SQL keywords as well as tables and
  columns in the database.
* Syntax highlighting using Pygments.
* Smart-completion (enabled by default) will suggest context-sensitive
  completion.

    - ``SELECT * FROM <tab>`` will only show table names.
    - ``SELECT * FROM users WHERE <tab>`` will only show column names.

* Primitive support for ``psql`` back-slash commands.
* Pretty prints tabular data.

.. _prompt_toolkit: https://github.com/jonathanslenders/python-prompt-toolkit

Config
------

A config file is automatically created at ``~/.config/pgcli/config`` at first launch.
See the file itself for a description of all available options.

pgcli.daf Features
------------------

SSH Tunnel Support
^^^^^^^^^^^^^^^^^^

Connect to databases through SSH tunnels using native Paramiko (no ``sshtunnel`` dependency):

::

    $ pgcli --ssh-tunnel user@bastion-host -h db.internal mydb

Configure per-DSN tunnels in ``~/.config/pgcli/config``:

::

    [ssh tunnels]
    # Match by hostname
    db.internal = ssh://user@bastion:22

    # Per-DSN tunnel
    [alias_dsn.ssh tunnels]
    production = ssh://user@bastion:22

SSH tunnel features:

* Reads ``IdentityFile`` from ``~/.ssh/config`` (host-specific and wildcard)
* Configurable host key verification: ``auto-add`` (default), ``warn``, ``reject``
* ``.pgpass`` support works correctly through tunnels
* ``allow_agent`` config option for passphrase-protected keys

Companion Commands
^^^^^^^^^^^^^^^^^^

Three additional commands are installed alongside ``pgcli``:

* ``pgcli_dump`` — ``pg_dump`` wrapper with SSH tunnel support
* ``pgcli_dumpall`` — ``pg_dumpall`` wrapper with SSH tunnel support
* ``pgcli_isready`` — ``pg_isready`` wrapper with SSH tunnel support

All three support ``--ssh-tunnel``, ``--dsn``, and ``-v/--verbose`` options,
and pass all other options through to the underlying PostgreSQL command.

::

    $ pgcli_dump --ssh-tunnel user@bastion -h db.internal mydb > backup.sql
    $ pgcli_dump --dsn production mydb -F c -f backup.dump
    $ pgcli_isready --dsn production

CLI Execution Options
^^^^^^^^^^^^^^^^^^^^^

Run commands non-interactively, useful for scripting:

::

    # Execute SQL and exit
    $ pgcli -c "SELECT count(*) FROM users" mydb

    # Multiple commands
    $ pgcli -c "SELECT 1" -c "SELECT 2" mydb

    # Execute from file
    $ pgcli -f setup.sql -f data.sql mydb

    # Tuples-only output (no headers, no status — like psql -t)
    $ pgcli -t -c "SELECT name FROM users" mydb

    # Suppress timings or status independently
    $ pgcli --no-timings -c "SELECT 1" mydb
    $ pgcli --no-status -c "SELECT 1" mydb

    # Skip destructive query confirmation
    $ pgcli -y -f cleanup.sql mydb

    # Output to file
    $ pgcli -o results.csv -t -c "SELECT * FROM users" mydb

Named Queries
^^^^^^^^^^^^^

* ``namedqueries.d/`` directory support for organizing queries in separate files
* ``dsn.d/`` directory support for organizing DSN aliases
* ``\nq`` — quiet mode: execute named query without printing the query text
* ``\nr`` — reload named queries without restarting pgcli
* Alphabetically sorted ``\n`` output

Prompt and Display
^^^^^^^^^^^^^^^^^^

* ``\T`` escape sequence — shows transaction status in prompt (idle, in transaction, failed)
* ``--init-command`` — execute SQL after connecting (also configurable per-DSN in config)
* Log rotation with multiple modes: ``day-of-week``, ``day-of-month``, ``date``

Security
^^^^^^^^

* ``\restrict`` / ``\unrestrict`` — meta-command blocking for safe dump restoration (CVE-2025-8714)
* SSH passwords sanitized from debug logs
* Passwords masked in ``--list-dsn`` output
* ``re.fullmatch()`` for SSH tunnel hostname matching (prevents partial matches)
* SQL passwords redacted from debug logs (``CREATE/ALTER USER/ROLE``)
* File path sanitization in ``\i``, ``\o``, ``\log-file`` (symlink resolution, system path blocking)

Thanks
------

Based on the work of `Amjith Ramanujam <https://github.com/amjith>`_ and
`Irina Truong <https://github.com/j-bennet>`_ on the original
`pgcli <https://github.com/dbcli/pgcli>`_.

Built with `Python Prompt Toolkit <https://github.com/jonathanslenders/python-prompt-toolkit>`_,
`Click <https://click.palletsprojects.com/>`_, and
`psycopg <https://www.psycopg.org/>`_.

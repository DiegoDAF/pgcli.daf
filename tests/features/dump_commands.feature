Feature: pgcli_dump and pgcli_dumpall commands
  Test the pg_dump and pg_dumpall wrappers with SSH tunnel support

  Scenario: pgcli_dump shows help
    When we run pgcli_dump with --help
    Then we see pgcli_dump help output
    And pgcli_dump exits successfully

  Scenario: pgcli_dumpall shows help
    When we run pgcli_dumpall with --help
    Then we see pgcli_dumpall help output
    And pgcli_dumpall exits successfully

  Scenario: pgcli_dump passes version option to pg_dump
    When we run pgcli_dump with --version
    Then we see pg_dump version output

  Scenario: pgcli_dumpall passes version option to pg_dumpall
    When we run pgcli_dumpall with --version
    Then we see pg_dumpall version output

  Scenario: pgcli_dump with schema-only option
    When we run pgcli_dump with -h localhost -d postgres --schema-only
    Then pgcli_dump attempts database connection

  Scenario: pgcli_dump with custom format
    When we run pgcli_dump with -h localhost -d postgres -F c --schema-only
    Then pgcli_dump attempts database connection

  Scenario: pgcli_dumpall with globals-only option
    When we run pgcli_dumpall with -h localhost -g
    Then pgcli_dumpall attempts database connection

  Scenario: pgcli_dumpall with roles-only option
    When we run pgcli_dumpall with -h localhost -r
    Then pgcli_dumpall attempts database connection

  Scenario: pgcli_dump verbose mode shows debug info
    When we run pgcli_dump with --tunnel-verbose --help
    Then we see pgcli_dump help output
    And pgcli_dump exits successfully

  Scenario: pgcli_dump with --ssh-tunnel option shows in help
    When we run pgcli_dump with --help
    Then we see ssh-tunnel option in help
    And pgcli_dump exits successfully

  Scenario: pgcli_dump with --dsn option shows in help
    When we run pgcli_dump with --help
    Then we see dsn option in help
    And pgcli_dump exits successfully

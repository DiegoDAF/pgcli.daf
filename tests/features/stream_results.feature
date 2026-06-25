Feature: stream_results config option,
  run multiple statements with results printed per statement,
  and exit

  Scenario: run pgcli with stream_results enabled and multiple statements
     When we run pgcli with stream_results enabled and "SELECT 1 as first_col; SELECT 2 as second_col"
      then we see both streamed results
      and pgcli exits successfully

  Scenario: run pgcli with stream_results disabled (default) and multiple statements
     When we run pgcli with stream_results disabled and "SELECT 1 as first_col; SELECT 2 as second_col"
      then we see both streamed results
      and pgcli exits successfully

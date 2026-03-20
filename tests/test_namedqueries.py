# -*- coding: utf-8 -*-
"""Tests for ExtendedNamedQueries with namedqueries.d support."""

import os
import tempfile
import shutil
from configobj import ConfigObj

from pgcli.namedqueries import ExtendedNamedQueries


class TestExtendedNamedQueries:
    """Tests for ExtendedNamedQueries class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "config")
        self.include_dir = os.path.join(self.temp_dir, "namedqueries.d")

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_config(self, queries=None):
        """Create a main config file with optional named queries."""
        config = ConfigObj(self.config_file, encoding="utf-8")
        if queries:
            config["named queries"] = queries
        config.write()
        return ConfigObj(self.config_file, encoding="utf-8")

    def _create_include_file(self, filename, queries):
        """Create an include file with named queries."""
        os.makedirs(self.include_dir, exist_ok=True)
        filepath = os.path.join(self.include_dir, filename)
        config = ConfigObj(filepath, encoding="utf-8")
        config["named queries"] = queries
        config.write()

    def test_no_include_dir(self):
        """Test behavior when namedqueries.d doesn't exist."""
        config = self._create_config({"ver": "SELECT version()"})
        nq = ExtendedNamedQueries.from_config(config)

        assert nq.list() == ["ver"]
        assert nq.get("ver") == "SELECT version()"

    def test_empty_include_dir(self):
        """Test behavior with empty namedqueries.d directory."""
        os.makedirs(self.include_dir)
        config = self._create_config({"ver": "SELECT version()"})
        nq = ExtendedNamedQueries.from_config(config)

        assert nq.list() == ["ver"]

    def test_load_from_include_dir(self):
        """Test loading queries from namedqueries.d."""
        config = self._create_config()
        self._create_include_file("activity.conf", {"ps": "SELECT * FROM pg_stat_activity", "locks": "SELECT * FROM pg_locks"})

        nq = ExtendedNamedQueries.from_config(config)

        assert set(nq.list()) == {"ps", "locks"}
        assert nq.get("ps") == "SELECT * FROM pg_stat_activity"
        assert nq.get("locks") == "SELECT * FROM pg_locks"

    def test_multiple_include_files(self):
        """Test loading from multiple .conf files."""
        config = self._create_config()
        self._create_include_file("activity.conf", {"ps": "SELECT * FROM pg_stat_activity"})
        self._create_include_file("vacuum.conf", {"vacuum_stats": "SELECT * FROM pg_stat_user_tables"})

        nq = ExtendedNamedQueries.from_config(config)

        assert set(nq.list()) == {"ps", "vacuum_stats"}

    def test_main_config_precedence(self):
        """Test that main config queries take precedence over includes."""
        config = self._create_config({
            "ps": "SELECT pid FROM pg_stat_activity"  # override
        })
        self._create_include_file(
            "activity.conf",
            {
                "ps": "SELECT * FROM pg_stat_activity",  # will be overridden
                "locks": "SELECT * FROM pg_locks",
            },
        )

        nq = ExtendedNamedQueries.from_config(config)

        # Main config version should win
        assert nq.get("ps") == "SELECT pid FROM pg_stat_activity"
        # Include version still accessible
        assert nq.get("locks") == "SELECT * FROM pg_locks"

    def test_get_source(self):
        """Test get_source method to identify query origin."""
        config = self._create_config({"main_query": "SELECT 1"})
        self._create_include_file("test.conf", {"include_query": "SELECT 2"})

        nq = ExtendedNamedQueries.from_config(config)

        assert nq.get_source("main_query") == "config"
        assert nq.get_source("include_query") == "include"
        assert nq.get_source("nonexistent") is None

    def test_get_all(self):
        """Test get_all method returns combined dictionary."""
        config = self._create_config({
            "q1": "SELECT 1",
            "q2": "SELECT 2",  # override included
        })
        self._create_include_file("test.conf", {"q2": "SELECT override", "q3": "SELECT 3"})

        nq = ExtendedNamedQueries.from_config(config)
        all_queries = nq.get_all()

        assert all_queries == {
            "q1": "SELECT 1",
            "q2": "SELECT 2",  # main config wins
            "q3": "SELECT 3",
        }

    def test_reload_includes(self):
        """Test reload_includes refreshes from directory."""
        config = self._create_config()
        self._create_include_file("test.conf", {"q1": "SELECT 1"})

        nq = ExtendedNamedQueries.from_config(config)
        assert nq.list() == ["q1"]

        # Add another file
        self._create_include_file("test2.conf", {"q2": "SELECT 2"})

        # Before reload, q2 shouldn't be visible
        assert "q2" not in nq.list()

        # After reload, q2 should be visible
        nq.reload_includes()
        assert set(nq.list()) == {"q1", "q2"}

    def test_only_conf_files_loaded(self):
        """Test that only .conf files are loaded from include dir."""
        config = self._create_config()
        self._create_include_file("valid.conf", {"valid_query": "SELECT 1"})
        # Create a non-.conf file (should be ignored)
        os.makedirs(self.include_dir, exist_ok=True)
        with open(os.path.join(self.include_dir, "readme.txt"), "w") as f:
            f.write("This is not a config file")
        with open(os.path.join(self.include_dir, "backup.conf.bak"), "w") as f:
            f.write("[named queries]\nbackup = SELECT 'backup'")

        nq = ExtendedNamedQueries.from_config(config)

        assert nq.list() == ["valid_query"]

    def test_invalid_include_file_handled(self):
        """Test that invalid config files don't crash loading."""
        config = self._create_config()
        self._create_include_file("valid.conf", {"valid_query": "SELECT 1"})
        # Create an invalid config file
        os.makedirs(self.include_dir, exist_ok=True)
        with open(os.path.join(self.include_dir, "invalid.conf"), "w") as f:
            f.write("this is not valid config syntax [[[")

        nq = ExtendedNamedQueries.from_config(config)

        # Should still load valid queries
        assert "valid_query" in nq.list()

    def test_save_query_to_main_config(self):
        """Test that save() writes to main config, not includes."""
        config = self._create_config()
        nq = ExtendedNamedQueries.from_config(config)

        nq.save("new_query", "SELECT 'new'")

        # Reload config and verify
        config = ConfigObj(self.config_file, encoding="utf-8")
        assert config["named queries"]["new_query"] == "SELECT 'new'"

    def test_delete_query_from_main_config(self):
        """Test that delete() removes from main config."""
        config = self._create_config({"to_delete": "SELECT 1"})
        nq = ExtendedNamedQueries.from_config(config)

        result = nq.delete("to_delete")
        assert "Deleted" in result

        # Reload config and verify
        config = ConfigObj(self.config_file, encoding="utf-8")
        assert "to_delete" not in config.get("named queries", {})

    def test_files_loaded_in_sorted_order(self):
        """Test that include files are loaded in sorted order."""
        config = self._create_config()
        # Create files in non-alphabetical order
        self._create_include_file("z_last.conf", {"query": "z_version"})
        self._create_include_file(
            "a_first.conf",
            {
                "query": "a_version"  # This should be overridden by z_last.conf
            },
        )

        nq = ExtendedNamedQueries.from_config(config)

        # Since files are loaded in sorted order (a_ then z_),
        # the z_last.conf version should win
        assert nq.get("query") == "z_version"

    def test_explicit_include_dir(self):
        """Test using an explicit include directory path."""
        config = self._create_config()
        custom_dir = os.path.join(self.temp_dir, "custom_queries")
        os.makedirs(custom_dir)

        # Create a file in the custom directory
        custom_file = os.path.join(custom_dir, "custom.conf")
        custom_config = ConfigObj(custom_file, encoding="utf-8")
        custom_config["named queries"] = {"custom_query": "SELECT 'custom'"}
        custom_config.write()

        nq = ExtendedNamedQueries.from_config(config, include_dir=custom_dir)

        assert nq.get("custom_query") == "SELECT 'custom'"

    def test_get_nonexistent_query(self):
        """Test getting a query that doesn't exist."""
        config = self._create_config()
        nq = ExtendedNamedQueries.from_config(config)

        assert nq.get("nonexistent") is None

    def test_load_without_section_header(self):
        """Test loading queries from file without [named queries] section."""
        config = self._create_config()
        os.makedirs(self.include_dir, exist_ok=True)

        # Create file without section header - just key=value pairs
        filepath = os.path.join(self.include_dir, "simple.conf")
        with open(filepath, "w") as f:
            f.write('# Simple queries without section\n')
            f.write('uptime = "select now() - pg_postmaster_start_time() as uptime;"\n')
            f.write('version = "select version();"\n')

        nq = ExtendedNamedQueries.from_config(config)

        assert set(nq.list()) == {"uptime", "version"}
        assert nq.get("uptime") == "select now() - pg_postmaster_start_time() as uptime;"
        assert nq.get("version") == "select version();"

    def test_mixed_formats_in_include_dir(self):
        """Test loading from files with and without section headers."""
        config = self._create_config()
        os.makedirs(self.include_dir, exist_ok=True)

        # File with section
        self._create_include_file("with_section.conf", {"query_a": "SELECT 'a'"})

        # File without section
        filepath = os.path.join(self.include_dir, "without_section.conf")
        with open(filepath, "w") as f:
            f.write('query_b = "SELECT \'b\'"\n')

        nq = ExtendedNamedQueries.from_config(config)

        assert set(nq.list()) == {"query_a", "query_b"}

    def test_includedir_directive(self):
        """Test includedir directive in config."""
        # Create a custom include directory
        custom_dir = os.path.join(self.temp_dir, "my_queries")
        os.makedirs(custom_dir)

        # Create config with includedir directive
        config = ConfigObj(self.config_file, encoding="utf-8")
        config["named queries"] = {"includedir": "./my_queries", "main_query": "SELECT 'main'"}
        config.write()
        config = ConfigObj(self.config_file, encoding="utf-8")

        # Create a file in the custom directory
        custom_file = os.path.join(custom_dir, "custom.conf")
        with open(custom_file, "w") as f:
            f.write('included_query = "SELECT \'included\'"\n')

        nq = ExtendedNamedQueries.from_config(config)

        # Should find both queries
        assert set(nq.list()) == {"main_query", "included_query"}
        # includedir should not appear as a query
        assert "includedir" not in nq.list()
        assert nq.get("includedir") is None

    def test_includedir_absolute_path(self):
        """Test includedir with absolute path."""
        custom_dir = os.path.join(self.temp_dir, "absolute_queries")
        os.makedirs(custom_dir)

        config = ConfigObj(self.config_file, encoding="utf-8")
        config["named queries"] = {
            "includedir": custom_dir  # absolute path
        }
        config.write()
        config = ConfigObj(self.config_file, encoding="utf-8")

        custom_file = os.path.join(custom_dir, "test.conf")
        with open(custom_file, "w") as f:
            f.write('abs_query = "SELECT 1"\n')

        nq = ExtendedNamedQueries.from_config(config)

        assert nq.get("abs_query") == "SELECT 1"

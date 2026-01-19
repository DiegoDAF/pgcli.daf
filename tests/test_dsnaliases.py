# -*- coding: utf-8 -*-
"""Tests for DsnAliases class with directory-based includes."""

import os
import tempfile
import shutil
import pytest
from configobj import ConfigObj

from pgcli.dsnaliases import DsnAliases


class TestDsnAliases:
    """Test cases for DsnAliases class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.temp_dir, "config")
        self.include_dir = os.path.join(self.temp_dir, "dsn.d")

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_config(self, aliases=None):
        """Create a test config file with optional DSN aliases."""
        config = ConfigObj(self.config_path, encoding="utf-8")
        config["alias_dsn"] = aliases or {}
        config.write()
        return ConfigObj(self.config_path, encoding="utf-8")

    def _create_include_file(self, filename, aliases, with_section=False):
        """Create a test include file with DSN aliases."""
        os.makedirs(self.include_dir, exist_ok=True)
        filepath = os.path.join(self.include_dir, filename)
        config = ConfigObj(filepath, encoding="utf-8")
        if with_section:
            config["alias_dsn"] = aliases
        else:
            for key, value in aliases.items():
                config[key] = value
        config.write()

    def test_no_include_dir(self):
        """Test behavior when no include directory exists."""
        config = self._create_config({"local": "postgresql://localhost/test"})
        dsn = DsnAliases.from_config(config)

        assert dsn.list() == ["local"]
        assert dsn.get("local") == "postgresql://localhost/test"

    def test_empty_include_dir(self):
        """Test behavior with empty dsn.d directory."""
        os.makedirs(self.include_dir)
        config = self._create_config({"local": "postgresql://localhost/test"})
        dsn = DsnAliases.from_config(config)

        assert dsn.list() == ["local"]

    def test_load_from_include_dir(self):
        """Test loading DSN aliases from dsn.d."""
        config = self._create_config()
        self._create_include_file("production.conf", {
            "prod-db": "postgresql://prod.example.com/app",
            "prod-ro": "postgresql://prod-ro.example.com/app"
        })

        dsn = DsnAliases.from_config(config)

        assert set(dsn.list()) == {"prod-db", "prod-ro"}
        assert dsn.get("prod-db") == "postgresql://prod.example.com/app"
        assert dsn.get("prod-ro") == "postgresql://prod-ro.example.com/app"

    def test_multiple_include_files(self):
        """Test loading from multiple .conf files."""
        config = self._create_config()
        self._create_include_file("production.conf", {
            "prod-db": "postgresql://prod.example.com/app"
        })
        self._create_include_file("staging.conf", {
            "staging-db": "postgresql://staging.example.com/app"
        })

        dsn = DsnAliases.from_config(config)

        assert set(dsn.list()) == {"prod-db", "staging-db"}

    def test_main_config_precedence(self):
        """Test that main config aliases take precedence over includes."""
        config = self._create_config({
            "mydb": "postgresql://main.example.com/app"
        })
        self._create_include_file("override.conf", {
            "mydb": "postgresql://include.example.com/app"
        })

        dsn = DsnAliases.from_config(config)

        assert dsn.get("mydb") == "postgresql://main.example.com/app"

    def test_get_source(self):
        """Test get_source returns correct location."""
        config = self._create_config({
            "main-db": "postgresql://main.example.com/app"
        })
        self._create_include_file("include.conf", {
            "include-db": "postgresql://include.example.com/app"
        })

        dsn = DsnAliases.from_config(config)

        assert dsn.get_source("main-db") == "config"
        assert dsn.get_source("include-db") == "include"
        assert dsn.get_source("nonexistent") is None

    def test_get_all(self):
        """Test get_all returns all aliases."""
        config = self._create_config({
            "main-db": "postgresql://main.example.com/app"
        })
        self._create_include_file("include.conf", {
            "include-db": "postgresql://include.example.com/app"
        })

        dsn = DsnAliases.from_config(config)
        all_aliases = dsn.get_all()

        assert "main-db" in all_aliases
        assert "include-db" in all_aliases
        assert all_aliases["main-db"] == "postgresql://main.example.com/app"

    def test_reload_includes(self):
        """Test reloading include files."""
        config = self._create_config()
        self._create_include_file("test1.conf", {
            "db1": "postgresql://db1.example.com/app"
        })

        dsn = DsnAliases.from_config(config)
        assert dsn.list() == ["db1"]

        # Add another file
        self._create_include_file("test2.conf", {
            "db2": "postgresql://db2.example.com/app"
        })

        # Before reload, db2 shouldn't be visible
        assert "db2" not in dsn.list()

        # After reload, db2 should be visible
        dsn.reload_includes()
        assert set(dsn.list()) == {"db1", "db2"}

    def test_only_conf_files_loaded(self):
        """Test that only .conf files are loaded from include dir."""
        config = self._create_config()
        self._create_include_file("valid.conf", {
            "valid-db": "postgresql://valid.example.com/app"
        })

        # Create non-.conf files
        os.makedirs(self.include_dir, exist_ok=True)
        with open(os.path.join(self.include_dir, "invalid.txt"), "w") as f:
            f.write('txt-db = "postgresql://txt.example.com/app"')
        with open(os.path.join(self.include_dir, "README.md"), "w") as f:
            f.write("# This is a readme")

        dsn = DsnAliases.from_config(config)

        assert dsn.list() == ["valid-db"]

    def test_invalid_include_file_handled(self):
        """Test that invalid config files don't crash loading."""
        config = self._create_config()
        self._create_include_file("valid.conf", {
            "valid-db": "postgresql://valid.example.com/app"
        })

        # Create an invalid config file
        os.makedirs(self.include_dir, exist_ok=True)
        with open(os.path.join(self.include_dir, "invalid.conf"), "w") as f:
            f.write("this is not valid = config [ syntax")

        dsn = DsnAliases.from_config(config)

        # Should still load valid aliases
        assert "valid-db" in dsn.list()

    def test_files_loaded_in_sorted_order(self):
        """Test that files are loaded in alphabetical order."""
        config = self._create_config()
        # Create files that override each other
        self._create_include_file("b_second.conf", {
            "mydb": "postgresql://second.example.com/app"
        })
        self._create_include_file("a_first.conf", {
            "mydb": "postgresql://first.example.com/app"
        })

        dsn = DsnAliases.from_config(config)

        # b_second.conf should override a_first.conf (loaded later alphabetically)
        assert dsn.get("mydb") == "postgresql://second.example.com/app"

    def test_explicit_include_dir(self):
        """Test using explicitly provided include directory."""
        custom_dir = os.path.join(self.temp_dir, "custom_dsn")
        os.makedirs(custom_dir)

        config = self._create_config()
        # Create file in custom directory (not the default dsn.d)
        custom_file = os.path.join(custom_dir, "custom.conf")
        custom_config = ConfigObj(custom_file, encoding="utf-8")
        custom_config["custom-db"] = "postgresql://custom.example.com/app"
        custom_config.write()

        dsn = DsnAliases.from_config(config, include_dir=custom_dir)

        assert dsn.list() == ["custom-db"]

    def test_get_nonexistent_alias(self):
        """Test getting a non-existent alias returns None."""
        config = self._create_config({"db": "postgresql://localhost/test"})
        dsn = DsnAliases.from_config(config)

        assert dsn.get("nonexistent") is None

    def test_load_without_section_header(self):
        """Test loading files without [alias_dsn] section header."""
        config = self._create_config()

        # Create a simple file without section header
        os.makedirs(self.include_dir, exist_ok=True)
        filepath = os.path.join(self.include_dir, "simple.conf")
        with open(filepath, "w") as f:
            f.write('# Simple DSN aliases\n')
            f.write('local-db = "postgresql://localhost/mydb"\n')
            f.write('dev-db = "postgresql://dev.example.com/app"\n')

        dsn = DsnAliases.from_config(config)

        assert set(dsn.list()) == {"local-db", "dev-db"}
        assert dsn.get("local-db") == "postgresql://localhost/mydb"
        assert dsn.get("dev-db") == "postgresql://dev.example.com/app"

    def test_load_with_section_header(self):
        """Test loading files with [alias_dsn] section header."""
        config = self._create_config()
        self._create_include_file("sectioned.conf", {
            "section-db": "postgresql://section.example.com/app"
        }, with_section=True)

        dsn = DsnAliases.from_config(config)

        assert dsn.list() == ["section-db"]
        assert dsn.get("section-db") == "postgresql://section.example.com/app"

    def test_mixed_formats_in_include_dir(self):
        """Test loading from files with and without section headers."""
        config = self._create_config()

        # File with section header
        self._create_include_file("with_section.conf", {
            "db-a": "postgresql://a.example.com/app"
        }, with_section=True)

        # File without section header (simple format)
        os.makedirs(self.include_dir, exist_ok=True)
        filepath = os.path.join(self.include_dir, "without_section.conf")
        with open(filepath, "w") as f:
            f.write('db-b = "postgresql://b.example.com/app"\n')

        dsn = DsnAliases.from_config(config)

        assert set(dsn.list()) == {"db-a", "db-b"}

    def test_includedir_directive(self):
        """Test includedir directive in config."""
        # Create a custom include directory
        custom_dir = os.path.join(self.temp_dir, "my_dsn")
        os.makedirs(custom_dir)

        # Create main config with includedir directive
        config = ConfigObj(self.config_path, encoding="utf-8")
        config["alias_dsn"] = {
            "main-db": "postgresql://main.example.com/app",
            "includedir": "./my_dsn"
        }
        config.write()
        config = ConfigObj(self.config_path, encoding="utf-8")

        # Create file in custom directory
        custom_file = os.path.join(custom_dir, "custom.conf")
        custom_config = ConfigObj(custom_file, encoding="utf-8")
        custom_config["included-db"] = "postgresql://included.example.com/app"
        custom_config.write()

        dsn = DsnAliases.from_config(config)

        # Should find both aliases
        assert set(dsn.list()) == {"main-db", "included-db"}
        # includedir should not appear as an alias
        assert "includedir" not in dsn.list()
        assert dsn.get("includedir") is None

    def test_includedir_absolute_path(self):
        """Test includedir with absolute path."""
        custom_dir = os.path.join(self.temp_dir, "absolute_dsn")
        os.makedirs(custom_dir)

        # Create main config with absolute includedir
        config = ConfigObj(self.config_path, encoding="utf-8")
        config["alias_dsn"] = {
            "includedir": custom_dir
        }
        config.write()
        config = ConfigObj(self.config_path, encoding="utf-8")

        # Create file in custom directory
        custom_file = os.path.join(custom_dir, "test.conf")
        custom_config = ConfigObj(custom_file, encoding="utf-8")
        custom_config["abs-db"] = "postgresql://abs.example.com/app"
        custom_config.write()

        dsn = DsnAliases.from_config(config)

        assert dsn.list() == ["abs-db"]

    def test_dict_like_access(self):
        """Test dict-like access with [] operator."""
        config = self._create_config({"mydb": "postgresql://localhost/test"})
        dsn = DsnAliases.from_config(config)

        assert dsn["mydb"] == "postgresql://localhost/test"

        with pytest.raises(KeyError):
            _ = dsn["nonexistent"]

    def test_contains_operator(self):
        """Test 'in' operator for checking alias existence."""
        config = self._create_config({"mydb": "postgresql://localhost/test"})
        dsn = DsnAliases.from_config(config)

        assert "mydb" in dsn
        assert "nonexistent" not in dsn

    def test_iteration(self):
        """Test iterating over aliases."""
        config = self._create_config({
            "db1": "postgresql://db1.example.com/app",
            "db2": "postgresql://db2.example.com/app"
        })
        dsn = DsnAliases.from_config(config)

        aliases = list(dsn)
        assert set(aliases) == {"db1", "db2"}

    def test_sorted_output(self):
        """Test that list() returns sorted aliases."""
        config = self._create_config({
            "zebra-db": "postgresql://zebra.example.com/app",
            "alpha-db": "postgresql://alpha.example.com/app",
            "mike-db": "postgresql://mike.example.com/app"
        })
        dsn = DsnAliases.from_config(config)

        assert dsn.list() == ["alpha-db", "mike-db", "zebra-db"]

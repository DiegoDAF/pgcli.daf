# -*- coding: utf-8 -*-
"""Extended DSN Aliases support with directory-based includes.

This module provides support for loading DSN aliases from files in a
`dsn.d` directory, similar to how namedqueries.d works.
"""

import os
import logging
from configobj import ConfigObj

logger = logging.getLogger(__name__)


class DsnAliases:
    """DSN Aliases manager with support for loading from a directory.

    Loads DSN aliases from the main config file's [alias_dsn] section
    and from individual files in a `dsn.d` directory located in the
    same directory as the main config file.

    Each file in dsn.d should be a valid config file with DSN aliases.
    Files can optionally have an [alias_dsn] section header.

    Example structure:
        ~/.config/pgcli/
            config                  # main config with [alias_dsn]
            dsn.d/
                production.conf     # production DSN aliases
                staging.conf        # staging DSN aliases
                local.conf          # local development DSNs
    """

    INCLUDE_DIR_NAME = "dsn.d"
    SECTION_NAME = "alias_dsn"
    DIRECTIVES = {"includedir"}

    def __init__(self, config, include_dir=None):
        """Initialize DsnAliases.

        Args:
            config: The main ConfigObj configuration object
            include_dir: Optional path to the include directory. If None,
                        will be determined from config.filename
        """
        self.config = config
        self._include_dir = include_dir
        self._included_aliases = {}
        self._load_included_aliases()

    @classmethod
    def from_config(cls, config, include_dir=None):
        """Create a DsnAliases instance from a config object.

        Args:
            config: The main ConfigObj configuration object
            include_dir: Optional path to the include directory

        Returns:
            DsnAliases instance
        """
        return cls(config, include_dir)

    def _get_include_dir(self):
        """Get the path to the dsn.d directory.

        Checks in order:
        1. Explicit include_dir passed to constructor
        2. includedir directive in [alias_dsn] section
        3. Default dsn.d in config directory

        Returns:
            Path to the include directory, or None if it cannot be determined
        """
        if self._include_dir:
            return self._include_dir

        config_dir = None
        if hasattr(self.config, "filename") and self.config.filename:
            config_dir = os.path.dirname(self.config.filename)

        # Check for includedir directive in alias_dsn section
        alias_dsn = self.config.get(self.SECTION_NAME, {})
        includedir = alias_dsn.get("includedir")
        if includedir:
            # Resolve relative paths from config directory
            if config_dir and not os.path.isabs(includedir):
                return os.path.join(config_dir, includedir)
            return includedir

        # Default to dsn.d in config directory
        if config_dir:
            return os.path.join(config_dir, self.INCLUDE_DIR_NAME)

        return None

    def _load_included_aliases(self):
        """Load DSN aliases from all files in the include directory."""
        include_dir = self._get_include_dir()

        if not include_dir:
            logger.debug("No include directory configured for DSN aliases")
            return

        if not os.path.isdir(include_dir):
            logger.debug(f"DSN aliases include directory does not exist: {include_dir}")
            return

        logger.debug(f"Loading DSN aliases from include directory: {include_dir}")

        # Get all .conf files in the directory, sorted for consistent ordering
        try:
            files = sorted(
                f
                for f in os.listdir(include_dir)
                if f.endswith(".conf") and os.path.isfile(os.path.join(include_dir, f))
            )
        except OSError as e:
            logger.warning(f"Error reading DSN aliases include directory: {e}")
            return

        for filename in files:
            filepath = os.path.join(include_dir, filename)
            self._load_aliases_from_file(filepath)

    def _load_aliases_from_file(self, filepath):
        """Load DSN aliases from a single config file.

        Files in dsn.d can use two formats:
        1. With section: [alias_dsn] followed by key=value pairs
        2. Without section: just key=value pairs (entire file is aliases)

        Args:
            filepath: Path to the config file to load
        """
        try:
            file_config = ConfigObj(filepath, encoding="utf-8")

            # First try to get from [alias_dsn] section
            aliases = file_config.get(self.SECTION_NAME, {})

            # If no section found, treat entire file as aliases
            # (excluding any sections that might exist)
            if not aliases:
                aliases = {k: v for k, v in file_config.items()
                          if not isinstance(v, dict)}

            if aliases:
                logger.debug(
                    f"Loaded {len(aliases)} DSN aliases from {os.path.basename(filepath)}"
                )
                # Merge aliases, later files override earlier ones
                self._included_aliases.update(aliases)
            else:
                logger.debug(f"No DSN aliases found in {os.path.basename(filepath)}")

        except Exception as e:
            logger.warning(f"Error loading DSN aliases from {filepath}: {e}")

    def list(self):
        """List all DSN aliases from config and include directory.

        Returns:
            List of alias names (combined from main config and includes), sorted
        """
        # Get aliases from main config (excluding directives)
        main_aliases = {k: v for k, v in self.config.get(self.SECTION_NAME, {}).items()
                        if k not in self.DIRECTIVES}

        # Combine with included aliases (main config takes precedence)
        all_aliases = dict(self._included_aliases)
        all_aliases.update(main_aliases)

        return sorted(all_aliases.keys())

    def get(self, name):
        """Get a DSN alias by name.

        Aliases from the main config take precedence over included aliases.

        Args:
            name: The name of the alias to retrieve

        Returns:
            The DSN connection string, or None if not found
        """
        # Don't return directives as aliases
        if name in self.DIRECTIVES:
            return None

        # First check main config (takes precedence)
        main_aliases = self.config.get(self.SECTION_NAME, {})
        if name in main_aliases:
            return main_aliases[name]

        # Then check included aliases
        return self._included_aliases.get(name, None)

    def get_all(self):
        """Get all DSN aliases as a dictionary.

        Returns:
            Dictionary of alias_name -> connection_string
        """
        # Combine included aliases with main config (main takes precedence)
        # Exclude directives
        all_aliases = dict(self._included_aliases)
        main_aliases = {k: v for k, v in self.config.get(self.SECTION_NAME, {}).items()
                        if k not in self.DIRECTIVES}
        all_aliases.update(main_aliases)
        return all_aliases

    def get_source(self, name):
        """Get the source of a DSN alias (main config or include file).

        Args:
            name: The name of the alias

        Returns:
            'config' if from main config, 'include' if from include directory,
            or None if not found
        """
        main_aliases = self.config.get(self.SECTION_NAME, {})
        if name in main_aliases:
            return "config"
        if name in self._included_aliases:
            return "include"
        return None

    def reload_includes(self):
        """Reload DSN aliases from the include directory.

        This can be called to refresh the included aliases without
        restarting pgcli.
        """
        self._included_aliases = {}
        self._load_included_aliases()

    def __iter__(self):
        """Iterate over all DSN alias names."""
        return iter(self.list())

    def __contains__(self, name):
        """Check if an alias exists."""
        return self.get(name) is not None

    def __getitem__(self, name):
        """Get a DSN alias by name (dict-like access)."""
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

# -*- coding: utf-8 -*-
"""Extended Named Queries support with directory-based includes.

This module extends pgspecial's NamedQueries to support loading additional
named queries from files in a `namedqueries.d` directory.
"""

import os
import logging
from configobj import ConfigObj
from pgspecial.namedqueries import NamedQueries

logger = logging.getLogger(__name__)


class ExtendedNamedQueries(NamedQueries):
    """Extended NamedQueries with support for loading from a directory.

    In addition to loading named queries from the main config file's
    [named queries] section, this class also loads queries from individual
    files in a `namedqueries.d` directory located in the same directory
    as the main config file.

    Each file in namedqueries.d should be a valid config file with a
    [named queries] section. The filename (without extension) can be used
    as a logical grouping but doesn't affect the query names.

    Example structure:
        ~/.config/pgcli/
            config                  # main config with [named queries]
            namedqueries.d/
                activity.conf       # [named queries] section with activity queries
                vacuum.conf         # [named queries] section with vacuum queries
                custom.conf         # [named queries] section with custom queries
    """

    INCLUDE_DIR_NAME = "namedqueries.d"

    def __init__(self, config, include_dir=None):
        """Initialize ExtendedNamedQueries.

        Args:
            config: The main ConfigObj configuration object
            include_dir: Optional path to the include directory. If None,
                        will be determined from config.filename
        """
        super().__init__(config)
        self._include_dir = include_dir
        self._included_queries = {}
        self._load_included_queries()

    @classmethod
    def from_config(cls, config, include_dir=None):
        """Create an ExtendedNamedQueries instance from a config object.

        Args:
            config: The main ConfigObj configuration object
            include_dir: Optional path to the include directory

        Returns:
            ExtendedNamedQueries instance
        """
        return cls(config, include_dir)

    def _get_include_dir(self):
        """Get the path to the namedqueries.d directory.

        Checks in order:
        1. Explicit include_dir passed to constructor
        2. @includedir directive in [named queries] section
        3. Default namedqueries.d in config directory

        Returns:
            Path to the include directory, or None if it cannot be determined
        """
        if self._include_dir:
            return self._include_dir

        config_dir = None
        if hasattr(self.config, "filename") and self.config.filename:
            config_dir = os.path.dirname(self.config.filename)

        # Check for includedir directive in named queries section
        named_queries = self.config.get(self.section_name, {})
        includedir = named_queries.get("includedir")
        if includedir:
            # Resolve relative paths from config directory
            if config_dir and not os.path.isabs(includedir):
                return os.path.join(config_dir, includedir)
            return includedir

        # Default to namedqueries.d in config directory
        if config_dir:
            return os.path.join(config_dir, self.INCLUDE_DIR_NAME)

        return None

    def _load_included_queries(self):
        """Load named queries from all files in the include directory."""
        include_dir = self._get_include_dir()

        if not include_dir:
            logger.debug("No include directory configured for named queries")
            return

        if not os.path.isdir(include_dir):
            logger.debug(f"Named queries include directory does not exist: {include_dir}")
            return

        logger.debug(f"Loading named queries from include directory: {include_dir}")

        # Get all .conf files in the directory, sorted for consistent ordering
        try:
            files = sorted(
                f
                for f in os.listdir(include_dir)
                if f.endswith(".conf") and os.path.isfile(os.path.join(include_dir, f))
            )
        except OSError as e:
            logger.warning(f"Error reading named queries include directory: {e}")
            return

        for filename in files:
            filepath = os.path.join(include_dir, filename)
            self._load_queries_from_file(filepath)

    def _load_queries_from_file(self, filepath):
        """Load named queries from a single config file.

        Files in namedqueries.d can use two formats:
        1. With section: [named queries] followed by key=value pairs
        2. Without section: just key=value pairs (entire file is queries)

        Args:
            filepath: Path to the config file to load
        """
        try:
            file_config = ConfigObj(filepath, encoding="utf-8")

            # First try to get from [named queries] section
            queries = file_config.get(self.section_name, {})

            # If no section found, treat entire file as queries
            # (excluding any sections that might exist)
            if not queries:
                queries = {k: v for k, v in file_config.items()
                          if not isinstance(v, dict)}

            if queries:
                logger.debug(
                    f"Loaded {len(queries)} named queries from {os.path.basename(filepath)}"
                )
                # Merge queries, later files override earlier ones
                self._included_queries.update(queries)
            else:
                logger.debug(f"No named queries found in {os.path.basename(filepath)}")

        except Exception as e:
            logger.warning(f"Error loading named queries from {filepath}: {e}")

    # Directives that are not queries
    DIRECTIVES = {"includedir"}

    def list(self):
        """List all named queries from config and include directory.

        Returns:
            List of query names (combined from main config and includes)
        """
        # Get queries from main config (excluding directives)
        main_queries = {k: v for k, v in self.config.get(self.section_name, {}).items()
                        if k not in self.DIRECTIVES}

        # Combine with included queries (main config takes precedence)
        all_queries = dict(self._included_queries)
        all_queries.update(main_queries)

        return sorted(all_queries.keys())

    def get(self, name):
        """Get a named query by name.

        Queries from the main config take precedence over included queries.

        Args:
            name: The name of the query to retrieve

        Returns:
            The query string, or None if not found
        """
        # Don't return directives as queries
        if name in self.DIRECTIVES:
            return None

        # First check main config (takes precedence)
        main_queries = self.config.get(self.section_name, {})
        if name in main_queries:
            return main_queries[name]

        # Then check included queries
        return self._included_queries.get(name, None)

    def get_all(self):
        """Get all named queries as a dictionary.

        Returns:
            Dictionary of query_name -> query_string
        """
        # Combine included queries with main config (main takes precedence)
        # Exclude directives
        all_queries = dict(self._included_queries)
        main_queries = {k: v for k, v in self.config.get(self.section_name, {}).items()
                        if k not in self.DIRECTIVES}
        all_queries.update(main_queries)
        return all_queries

    def get_source(self, name):
        """Get the source of a named query (main config or include file).

        Args:
            name: The name of the query

        Returns:
            'config' if from main config, 'include' if from include directory,
            or None if not found
        """
        main_queries = self.config.get(self.section_name, {})
        if name in main_queries:
            return "config"
        if name in self._included_queries:
            return "include"
        return None

    def reload_includes(self):
        """Reload named queries from the include directory.

        This can be called to refresh the included queries without
        restarting pgcli.
        """
        self._included_queries = {}
        self._load_included_queries()

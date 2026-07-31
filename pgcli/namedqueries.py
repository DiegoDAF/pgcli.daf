# -*- coding: utf-8 -*-
"""Extended Named Queries support with directory-based includes.

This module extends pgspecial's NamedQueries to support loading additional
named queries from files in a `namedqueries.d` directory.
"""

import os
import re
import logging
from configobj import ConfigObj
from pgspecial.namedqueries import NamedQueries

logger = logging.getLogger(__name__)

# Trailing "-<version>" in a filename stem marks a server-version variant,
# psql-style (.psqlrc-17, .psqlrc-9.6): "activity-17.conf", "activity-9.6.conf".
_VERSION_SUFFIX_RE = re.compile(r"^(?P<base>.+)-(?P<ver>\d{1,2}(?:\.\d)?)$")


def server_major_version(server_version_num):
    """Comparable major version from libpq's integer server_version.

    170004 -> 17; 90624 -> 9.6 (pre-10 servers keep their minor, matching
    the .psqlrc-9.6 naming convention).
    """
    major = server_version_num // 10000
    if major >= 10:
        return major
    return major + (server_version_num % 10000 // 100) / 10


class ExtendedNamedQueries(NamedQueries):
    """Extended NamedQueries with support for loading from a directory.

    In addition to loading named queries from the main config file's
    [named queries] section, this class also loads queries from individual
    files in a `namedqueries.d` directory located in the same directory
    as the main config file.

    Each file in namedqueries.d should be a valid config file with a
    [named queries] section. The filename (without extension) can be used
    as a logical grouping but doesn't affect the query names.

    Files may carry a server-version suffix in the filename, psql-style
    (like ~/.psqlrc-17 / ~/.psqlrc-9.6): the queries in "activity-17.conf"
    are only offered when the connected server is version 17 or newer, and
    among several variants of the same query name the one with the HIGHEST
    version <= the server's version wins (best-fit). Files without a suffix
    are version-agnostic and act as the fallback.

    Example structure:
        ~/.config/pgcli/
            config                  # main config with [named queries]
            namedqueries.d/
                activity.conf       # fallback variant, any server version
                activity-10.conf    # used when server >= 10 (and < any higher variant)
                activity-17.conf    # used when server >= 17
                vacuum.conf         # [named queries] section with vacuum queries
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
        # {query_name: {version_float: sql}} - version 0.0 = no suffix
        self._versioned_queries = {}
        # Effective view for the current server version (None = not connected
        # yet: the highest variant of each name is offered).
        self._server_version = None
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
            files = sorted(f for f in os.listdir(include_dir) if f.endswith(".conf") and os.path.isfile(os.path.join(include_dir, f)))
        except OSError as e:
            logger.warning(f"Error reading named queries include directory: {e}")
            return

        self._versioned_queries = {}
        for filename in files:
            filepath = os.path.join(include_dir, filename)
            self._load_queries_from_file(filepath)
        self._recompute_effective()

    @staticmethod
    def _version_from_filename(filename):
        """Version encoded in a filename, psql-style: "activity-17.conf" -> 17.0.

        Returns 0.0 for files without a version suffix (version-agnostic).
        """
        stem = os.path.splitext(filename)[0]
        m = _VERSION_SUFFIX_RE.match(stem)
        return float(m.group("ver")) if m else 0.0

    def _load_queries_from_file(self, filepath):
        """Load named queries from a single config file.

        Files in namedqueries.d can use two formats:
        1. With section: [named queries] followed by key=value pairs
        2. Without section: just key=value pairs (entire file is queries)

        The filename's version suffix (if any) applies to every query in the
        file; recommended layout is one query per file, named after it.

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
                queries = {k: v for k, v in file_config.items() if not isinstance(v, dict)}

            if queries:
                version = self._version_from_filename(os.path.basename(filepath))
                logger.debug(
                    f"Loaded {len(queries)} named queries from {os.path.basename(filepath)}"
                    + (f" (server version >= {version:g})" if version else "")
                )
                for name, sql in queries.items():
                    self._versioned_queries.setdefault(name, {})[version] = sql
            else:
                logger.debug(f"No named queries found in {os.path.basename(filepath)}")

        except Exception as e:
            logger.warning(f"Error loading named queries from {filepath}: {e}")

    def _recompute_effective(self):
        """Pick, per query name, the variant that fits the server version.

        Best-fit: the highest version <= the connected server's version wins;
        a version-less file (0.0) is the always-available fallback. Names whose
        variants ALL require a newer server are hidden entirely. While the
        server version is unknown (before connecting), the highest variant of
        each name is offered.
        """
        effective = {}
        sv = self._server_version
        for name, versions in self._versioned_queries.items():
            if sv is None:
                best = max(versions)
            else:
                candidates = [v for v in versions if v <= sv]
                if not candidates:
                    continue
                best = max(candidates)
            effective[name] = versions[best]
        self._included_queries = effective

    def set_server_version(self, server_version):
        """Filter the included queries for the connected server's version.

        Args:
            server_version: numeric major version (17, or 9.6 for pre-10
                servers); None reverts to the not-connected view.
        """
        self._server_version = server_version
        self._recompute_effective()
        logger.debug(f"Named queries filtered for server version {server_version}: {len(self._included_queries)} available")

    # Directives that are not queries
    DIRECTIVES = {"includedir"}

    def list(self):
        """List all named queries from config and include directory.

        Returns:
            List of query names (combined from main config and includes)
        """
        # Get queries from main config (excluding directives)
        main_queries = {k: v for k, v in self.config.get(self.section_name, {}).items() if k not in self.DIRECTIVES}

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
        main_queries = {k: v for k, v in self.config.get(self.section_name, {}).items() if k not in self.DIRECTIVES}
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
        restarting pgcli. The current server-version filter is kept.
        """
        self._included_queries = {}
        self._load_included_queries()

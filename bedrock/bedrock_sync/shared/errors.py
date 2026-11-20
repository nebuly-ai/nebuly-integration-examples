"""Errors the command line may show as they are.

Their messages never hold secrets, request bodies or conversation text. Any
other exception is reported by type only.
"""

from __future__ import annotations


class ConnectorError(RuntimeError):
    """A problem the operator can act on, with a message safe to show."""


class ConfigError(ConnectorError, ValueError):
    """Invalid or missing settings."""


class StateError(ConnectorError):
    """The state file is missing, foreign, busy or inconsistent."""

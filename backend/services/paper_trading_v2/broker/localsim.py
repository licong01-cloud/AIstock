"""Compatibility import for the retired Paper-owned LocalSIM broker path."""

from backend.services.simulation_execution.localsim.runtime import LocalSimBackend

__all__ = ["LocalSimBackend"]

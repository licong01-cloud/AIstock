"""LocalSIM execution ownership boundary with cycle-safe lazy exports."""

from typing import Any

__all__ = ["LocalSimBackend"]


def __getattr__(name: str) -> Any:
    if name == "LocalSimBackend":
        from backend.services.simulation_execution.localsim.runtime import LocalSimBackend

        return LocalSimBackend
    raise AttributeError(name)

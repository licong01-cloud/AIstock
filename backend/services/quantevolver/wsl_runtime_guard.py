"""WSL runtime guard for official offline factor compute paths."""
from __future__ import annotations

import os
import platform
from dataclasses import dataclass


ERROR_CODE = "wsl_runtime_required"


@dataclass(frozen=True)
class WslRuntimeInfo:
    is_wsl: bool
    os_name: str
    system: str
    release: str
    version: str


class WslRuntimeRequiredError(RuntimeError):
    """Raised when a compute-only path is invoked outside WSL/Linux."""

    def __init__(self, operation: str, info: WslRuntimeInfo | None = None) -> None:
        self.operation = operation
        self.info = info or runtime_info()
        super().__init__(
            f"{ERROR_CODE}: {operation} must run in WSL/compute-node Linux; "
            f"current os={self.info.os_name}, system={self.info.system}, "
            f"release={self.info.release}"
        )

    def to_dict(self) -> dict[str, str | bool]:
        return {
            "success": False,
            "error_code": ERROR_CODE,
            "error": str(self),
            "operation": self.operation,
            "os_name": self.info.os_name,
            "system": self.info.system,
            "release": self.info.release,
            "version": self.info.version,
        }


def runtime_info() -> WslRuntimeInfo:
    return WslRuntimeInfo(
        is_wsl=is_wsl_runtime(),
        os_name=os.name,
        system=platform.system(),
        release=platform.release(),
        version=platform.version(),
    )


def is_wsl_runtime() -> bool:
    if os.name == "nt":
        return False
    release = platform.release().lower()
    version = platform.version().lower()
    if "microsoft" in release or "wsl" in release:
        return True
    if "microsoft" in version or "wsl" in version:
        return True
    try:
        with open("/proc/version", "r", encoding="utf-8", errors="ignore") as fh:
            text = fh.read().lower()
        return "microsoft" in text or "wsl" in text
    except OSError:
        return False


def assert_wsl_runtime(operation: str) -> None:
    info = runtime_info()
    if not info.is_wsl:
        raise WslRuntimeRequiredError(operation, info)

"""Deprecated disposable database bootstrap.

CI no longer creates PostgreSQL services.  Database validation is routed to the
existing DEV database lane, so this historical entrypoint always fails closed.
"""

from __future__ import annotations

def _require_disposable_ci_target() -> None:
    raise RuntimeError(
        "Disposable PostgreSQL CI targets are removed; use the existing DEV database validation lane"
    )


def bootstrap_disposable_schema() -> None:
    _require_disposable_ci_target()


if __name__ == "__main__":
    bootstrap_disposable_schema()

from __future__ import annotations

"""SmartMonitorDB facade for backend package.

This simply re-exports the existing Postgres-backed SmartMonitorDB
implementation so that FastAPI routers can depend on it without
importing from the repository root.
"""

from typing import Any, Dict, List

import pg_smart_monitor_repo as _repo  # type: ignore


class SmartMonitorDB(_repo.SmartMonitorDB):  # type: ignore[misc]
    """Thin wrapper around the shared SmartMonitorDB implementation.

    The base class already uses `get_conn()` from app_pg, which is
    imported from the backend package here to keep imports consistent.
    """

    # we don't need to override anything for now
    pass


# Provide a singleton instance for routers
smart_monitor_db = SmartMonitorDB()

"""Source attribution for AIstock vn.py-style execution assets.

The files in this package are derived from selected vn.py/vnpy_algotrading
algorithm sources at commit 4133987530eb28f3538d1983545d81c4f83d7d59.
Original license: MIT License, Copyright (c) 2015-present, Xiaoyou Chen.
AIstock changes: remove vn.py runtime dependencies, adapt DTO/action boundary,
and keep broker, audit, risk, and persistence in AIstock adapters.
"""

from __future__ import annotations

UPSTREAM_REPO = "https://github.com/vnpy/vnpy_algotrading"
UPSTREAM_COMMIT = "4133987530eb28f3538d1983545d81c4f83d7d59"
UPSTREAM_LICENSE = "MIT License"
UPSTREAM_COPYRIGHT = "Copyright (c) 2015-present, Xiaoyou Chen"
AISTOCK_ASSET_VERSION = "2026.05.29-vnpy-style-v1"

SOURCE_FILE_MAP: dict[str, str] = {
    "SNIPER_MINIQMT": "vnpy_algotrading/algos/sniper_algo.py",
    "BEST_LIMIT_MINIQMT": "vnpy_algotrading/algos/best_limit_algo.py",
    "TWAP_LITE_MINIQMT": "vnpy_algotrading/algos/twap_algo.py",
    "TEMPLATE": "vnpy_algotrading/template.py",
    "BASE": "vnpy_algotrading/base.py",
    "ENGINE_SEMANTICS": "vnpy_algotrading/engine.py",
}


def source_attribution(algo_code: str) -> dict[str, str]:
    """Return machine-readable derived-source metadata for audit logs."""

    normalized = str(algo_code or "").strip().upper()
    return {
        "asset_version": AISTOCK_ASSET_VERSION,
        "upstream_repo": UPSTREAM_REPO,
        "upstream_commit": UPSTREAM_COMMIT,
        "upstream_source_file": SOURCE_FILE_MAP.get(normalized, SOURCE_FILE_MAP["TEMPLATE"]),
        "upstream_license": UPSTREAM_LICENSE,
        "upstream_copyright": UPSTREAM_COPYRIGHT,
        "aistock_changes": "removed vn.py runtime dependencies; adapted DTO/action boundary for Paper v2 MiniQMT",
    }

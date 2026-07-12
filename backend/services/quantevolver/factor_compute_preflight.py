"""Compact read-only readiness checks for official factor compute workflows."""
from __future__ import annotations

from typing import Any

from ...data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from .correlation_compute_service import get_correlation_factor_cache_status


def build_official_cache_preflight(
    *,
    target_end: str,
    eligible_factor_count: int,
) -> dict[str, Any]:
    """Describe whether the existing official cache can feed correlation safely."""
    try:
        status = get_correlation_factor_cache_status()
    except Exception as exc:
        return {
            "ok": False,
            "target_end": target_end,
            "eligible_factor_count": eligible_factor_count,
            "blockers": ["official_cache_status_unavailable"],
            "error": f"{type(exc).__name__}: {exc}",
        }

    blockers: list[str] = []
    if not status.get("integrity_ok"):
        blockers.append("official_cache_integrity_failed")
    cache_end = status.get("window_backtest_end") or status.get("as_of_date")
    if str(cache_end or "") != str(target_end):
        blockers.append("official_cache_snapshot_mismatch")
    if status.get("moneyflow_unit_contract_version") != MONEYFLOW_UNIT_CONTRACT_VERSION:
        blockers.append("official_cache_moneyflow_contract_mismatch")
    cached_count = int(status.get("cached_count") or 0)
    if cached_count < eligible_factor_count:
        blockers.append("official_cache_factor_coverage_incomplete")

    integrity = status.get("integrity") or {}
    return {
        "ok": not blockers,
        "target_end": target_end,
        "eligible_factor_count": eligible_factor_count,
        "cached_factor_count": cached_count,
        "cache_as_of_date": status.get("as_of_date"),
        "cache_window_backtest_end": status.get("window_backtest_end"),
        "cache_generated_at": status.get("generated_at"),
        "cache_source": status.get("cache_source"),
        "cache_root": status.get("cache_root"),
        "integrity_ok": bool(status.get("integrity_ok")),
        "as_of_date_distribution": integrity.get("as_of_date_distribution") or {},
        "moneyflow_unit_contract_version": status.get("moneyflow_unit_contract_version"),
        "expected_moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
        "blockers": blockers,
    }

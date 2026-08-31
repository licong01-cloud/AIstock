from __future__ import annotations

import ast
from datetime import date
from pathlib import Path

import pytest

from backend.services.simulation_signal.contracts import DailySelectionEvidence, canonical_json_sha256
from backend.services.simulation_signal.rebalance import RebalanceIntentService
from backend.services.simulation_signal.target_portfolio import TargetPortfolioService


def _evidence() -> DailySelectionEvidence:
    payload = {"symbols": ["000001.SZ", "600000.SH"], "source": "frozen_selection_input"}
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=date(2026, 8, 28),
        package_id="pkg_unit",
        manifest_sha256="manifest",
        runtime_profile_version_id="profile_v1",
        runtime_profile_hash="profile_hash",
        source_type="StrategyPackage",
        data_source="DB_HISTORICAL",
        candidate_count=2,
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
    )


def test_target_and_rebalance_are_pure_weight_evidence() -> None:
    target = TargetPortfolioService().build_equal_weight(
        evidence=_evidence(),
        symbols=("000001.SZ", "600000.SH"),
    )
    rebalance = RebalanceIntentService().compare_frozen_allocations(
        target=target,
        previous_target_weights={"000001.SZ": 1.0},
    )

    assert target.weights == {"000001.SZ": 0.5, "600000.SH": 0.5}
    assert rebalance.desired_weight_delta == {"000001.SZ": -0.5, "600000.SH": 0.5}
    assert "order" not in rebalance.model_dump_json().lower()


def test_signal_package_has_no_runtime_paper_or_broker_import() -> None:
    root = Path(__file__).resolve().parents[2] / "services" / "simulation_signal"
    forbidden = (
        "backend.services.paper_trading_v2",
        "backend.services.simulation_runtime",
        "backend.services.simulation_execution",
    )
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        assert not any(module.startswith(forbidden) for module in imports), path


def test_signal_evidence_and_weight_mappings_are_deeply_immutable() -> None:
    evidence = _evidence()
    target = TargetPortfolioService().build_equal_weight(
        evidence=evidence,
        symbols=("000001.SZ", "600000.SH"),
    )
    rebalance = RebalanceIntentService().compare_frozen_allocations(
        target=target,
        previous_target_weights={},
    )

    with pytest.raises(TypeError, match="cannot be mutated"):
        evidence.evidence_payload_json["symbols"] = []
    with pytest.raises(TypeError, match="cannot be mutated"):
        target.weights["000001.SZ"] = 0.25
    with pytest.raises(TypeError, match="cannot be mutated"):
        rebalance.desired_weight_delta["000001.SZ"] = 0.25

    assert evidence.model_dump(mode="json")["evidence_payload_json"]["symbols"] == [
        "000001.SZ",
        "600000.SH",
    ]

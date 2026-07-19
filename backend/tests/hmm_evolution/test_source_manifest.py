from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from backend.services.hmm_evolution.evaluator import (
    CandidateCoefficients,
    resolve_batch_common_dates,
)
from backend.services.hmm_evolution.models import (
    CandidateCoverage,
    CandidateLifecycle,
    CandidateManifest,
    CandidateRecord,
    CandidateSourceType,
    CoefficientStats,
)
from backend.services.hmm_evolution.source_manifest import build_source_manifest


def _candidate() -> CandidateRecord:
    manifest = CandidateManifest(
        source_type=CandidateSourceType.CONFIGURED_LOCAL,
        source_ref={"root_alias": "research", "relative_path": "candidate.json"},
        artifact_uri="configured-local://research/candidate.json",
        artifact_sha256="a" * 64,
        size_bytes=100,
        detected_format="hmm_sector_coefficients_legacy_v1",
        coverage=CandidateCoverage(
            start_date=date(2026, 1, 5),
            end_date=date(2026, 1, 5),
            date_count=1,
            sector_count_min=1,
            sector_count_max=1,
            stock_sector_map_count=2,
        ),
        coefficient_stats=CoefficientStats(min=1.0, max=1.0),
    )
    now = datetime.now(timezone.utc)
    return CandidateRecord(
        candidate_id=manifest.candidate_id,
        manifest_hash=manifest.manifest_hash,
        display_name="candidate",
        source_type=manifest.source_type,
        source_ref=manifest.source_ref,
        artifact_manifest=manifest,
        algorithm_version=manifest.algorithm_version,
        lifecycle_status=CandidateLifecycle.RESEARCH_ONLY,
        created_by="tester",
        row_version=1,
        created_at=now,
        updated_at=now,
    )


def test_source_manifest_keeps_phase0_receipts_without_absolute_paths() -> None:
    trade_date = date(2026, 1, 5)
    predictions = pd.DataFrame(
        [(trade_date, "B", 0.2), (trade_date, "A", 0.1)],
        columns=["trade_date", "symbol", "score"],
    )
    labels = pd.DataFrame(
        [(trade_date, "A", 20, 0.1), (trade_date, "B", 20, 0.2)],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    coefficients = CandidateCoefficients.from_payload(
        {
            "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
            "stock_sector_map": {"A": "S", "B": "S"},
        }
    )
    plan = resolve_batch_common_dates(
        predictions=predictions,
        labels=labels,
        candidates={"candidate": coefficients},
        window_start=trade_date,
        window_end=trade_date,
    )
    info = {
        name: {
            "source": "prediction_store",
            "uri": f"cas://qe/{name}",
            "sha256": sha,
            "size_bytes": 10,
            "row_count": artifact_rows,
            "zero_copy": True,
        }
        for name, sha, artifact_rows in (
            ("pred.pkl", "b" * 64, 20),
            ("label.pkl", "c" * 64, 18),
        )
    }

    manifest = build_source_manifest(
        base_loop_ref="qe_task/Loop8",
        predictions=predictions,
        labels=labels,
        artifact_source_info=info,
        candidate=_candidate(),
        date_plan=plan,
        label_horizon_days=20,
        market_forward_return={"mode": "disabled"},
        universe_evidence={
            "type": "source_loop_stock_pool_st_pit",
            "universe_id": "filtered_pool_fixture:qe_st_pit_fixture",
            "universe_hash": "d" * 64,
            "symbol_count": 2,
            "eligible_pair_count": 2,
        },
    )

    assert manifest["universe"]["type"] == "source_loop_stock_pool_st_pit"
    assert manifest["universe"]["universe_hash"] == "d" * 64
    assert manifest["universe"]["symbol_count"] == 2
    assert [item["artifact_name"] for item in manifest["artifacts"]] == [
        "pred.pkl",
        "label.pkl",
    ]
    assert [item["row_count"] for item in manifest["artifacts"]] == [20, 18]
    assert [item["selected_row_count"] for item in manifest["artifacts"]] == [2, 2]
    assert "F:/" not in str(manifest)
    assert all(item["zero_copy"] for item in manifest["artifacts"])


def test_source_manifest_rejects_incomplete_artifact_receipt() -> None:
    trade_date = date(2026, 1, 5)
    predictions = pd.DataFrame(
        [(trade_date, "A", 0.1)], columns=["trade_date", "symbol", "score"]
    )
    labels = pd.DataFrame(
        [(trade_date, "A", 10, 0.1)],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    coefficients = CandidateCoefficients.from_payload(
        {
            "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
            "stock_sector_map": {"A": "S"},
        }
    )
    plan = resolve_batch_common_dates(
        predictions=predictions,
        labels=labels,
        candidates={"candidate": coefficients},
        window_start=trade_date,
        window_end=trade_date,
    )

    with pytest.raises(ValueError, match="incomplete"):
        build_source_manifest(
            base_loop_ref="qe_task/Loop8",
            predictions=predictions,
            labels=labels,
            artifact_source_info={"pred.pkl": {}, "label.pkl": {}},
            candidate=_candidate(),
            date_plan=plan,
            label_horizon_days=10,
            market_forward_return={"mode": "disabled"},
            universe_evidence={
                "type": "source_loop_stock_pool_st_pit",
                "universe_id": "filtered_pool_fixture:qe_st_pit_fixture",
                "universe_hash": "d" * 64,
            },
        )

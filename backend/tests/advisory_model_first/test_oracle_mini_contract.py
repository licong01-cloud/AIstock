from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.feature_schema_v2 import feature_schema_hash
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    AdvisoryPolicySplitV1,
)
from backend.services.advisory_model_first.policy_utility_contracts import (
    FrozenDataIdentityV1,
)
from backend.services.advisory_model_first.research_control_contracts import (
    EvidenceReferenceV1,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    N1_DATASET_IDENTITY,
    Tier1PitSnapshotIdentityV1,
    build_n1_tier1_request,
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    filter_prediction_frame_to_pit,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _ref(role: str, digest: str = HASH_A) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=f"/evidence/{role}.json",
        sha256=digest,
        size_bytes=10,
    )


def _request(**overrides):
    calendar = FrozenDataIdentityV1(
        identity_kind="MARKET_CALENDAR",
        sha256=HASH_A,
        cutoff_trade_date="2026-06-30",
        row_count=606,
    )
    suspend = FrozenDataIdentityV1(
        identity_kind="SUSPEND_SIDECAR",
        sha256=HASH_B,
        cutoff_trade_date="2026-06-30",
        row_count=29_804,
    )
    cost = AdvisoryPolicyCostV1(buy_cost_bps=0.95, sell_cost_bps=5.95)
    split = AdvisoryPolicySplitV1()
    values = {
        "n0_completion_ref": _ref("n0", HASH_A),
        "n0_completion_receipt_sha256": HASH_B,
        "research_window_contract_ref": _ref("window", HASH_B),
        "research_window_contract_sha256": HASH_C,
        "research_window_contract_path": "/evidence/window.json",
        "registry_path": "/evidence/registry.jsonl",
        "route_path": "/evidence/route.md",
        "policy_dataset_bundle_root": "/data/p0c",
        "policy_dataset_bundle_id": N1_DATASET_IDENTITY,
        "policy_dataset_manifest_file_sha256": HASH_A,
        "policy_dataset_request_sha256": HASH_B,
        "program_id": "program",
        "binding_version_id": "binding",
        "package_id": "package",
        "manifest_sha256": HASH_C,
        "selection_runtime_semantics_hash": HASH_A,
        "style_profile_id": "short_rebound_v1",
        "style_profile_hash": HASH_B,
        "baseline_policy_sha256": HASH_A,
        "shadow_policy_sha256": HASH_B,
        "cost_policy": cost,
        "cost_policy_sha256": cost.policy_sha256,
        "split_policy": split,
        "split_policy_sha256": split.policy_sha256,
        "representative_seed_run_ids": {"leg_a": "run_a", "leg_b": "run_b"},
        "prediction_artifacts": {
            run_id: PredictionArtifactDescriptor(
                run_id=run_id,
                run_key=run_id,
                artifact_uri=f"/predictions/{run_id}.pkl",
                artifact_sha256=digest,
                size_bytes=1,
                row_count=100,
                date_start="2024-07-04",
                date_end="2026-03-10",
            )
            for run_id, digest in (("run_a", HASH_A), ("run_b", HASH_B))
        },
        "terminal_weights": {"leg_a": 0.6, "leg_b": 0.4},
        "pit_snapshot": Tier1PitSnapshotIdentityV1(
            artifact_ref=_ref("pit", HASH_C),
            spans_sha256=HASH_A,
            source_fingerprint_sha256=HASH_B,
            parameter_hash=HASH_C,
            scope_start="2024-07-04",
            cutoff="2026-03-10",
            span_count=10,
            instrument_count=10,
        ),
        "qlib_daily_root": "/data/qlib",
        "factor_data_root": "/data/factors",
        "factor_data_cutoff": date(2026, 6, 30),
        "suspend_data_root": "/data/suspend",
        "prediction_store_root": "/data/predictions",
        "market_calendar_identity": calendar,
        "suspend_sidecar_identity": suspend,
        "feature_schema_hash": feature_schema_hash(
            market_calendar_identity=calendar.model_dump(mode="json"),
            suspend_sidecar_identity=suspend.model_dump(mode="json"),
        ),
        "repository_root": "/repo",
        "repository_commit": "7" * 40,
        "output_root": "/artifacts/n1",
        "created_at": "2026-08-31T00:00:00Z",
    }
    values.update(overrides)
    return build_n1_tier1_request(**values)


def test_request_hash_is_stable_and_binds_file_and_semantic_hashes_separately() -> None:
    first = _request(created_at="2026-08-31T00:00:00Z")
    second = _request(created_at="2026-08-31T01:00:00Z")

    assert first.request_sha256 == second.request_sha256
    assert first.n0_completion_ref.sha256 == HASH_A
    assert first.n0_completion_receipt_sha256 == HASH_B
    assert first.research_window_contract_ref.sha256 == HASH_B
    assert first.research_window_contract_sha256 == HASH_C


def test_request_rejects_feature_schema_identity_drift() -> None:
    with pytest.raises(ValidationError, match="feature schema hash"):
        _request(feature_schema_hash="f" * 64)


def test_prediction_pit_filter_uses_membership_as_of_each_decision() -> None:
    snapshot = freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": date(2024, 7, 5),
                "eligible_end": date(2026, 3, 10),
                "entry_reason": "252_sessions",
                "exit_reason": None,
            }
        ],
        universe_key="aistock_equity_pit_canonical_v2",
        rule_version="shsz_a_252td_st_delist_asof_v2",
        scope_start=date(2024, 7, 4),
        cutoff=date(2026, 3, 10),
        state_identity="ready-v1",
        source_fingerprint_sha256=HASH_A,
        parameter_hash=HASH_B,
    )
    predictions = pd.DataFrame(
        {
            "trade_date": ["2024-07-04", "2024-07-05"],
            "instrument": ["000001.SZ", "000001.SZ"],
            "score": [99.0, 1.0],
        }
    )

    result = filter_prediction_frame_to_pit(predictions, snapshot)

    assert result[["trade_date", "instrument"]].to_dict("records") == [
        {"trade_date": pd.Timestamp("2024-07-05"), "instrument": "000001.SZ"}
    ]

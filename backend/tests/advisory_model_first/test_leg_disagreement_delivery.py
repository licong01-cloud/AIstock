from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import leg_disagreement_pipeline as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.leg_disagreement_contracts import (
    FrozenLegDisagreementRequestV1,
    build_leg_disagreement_request,
)
from backend.services.advisory_model_first.leg_disagreement_pipeline import (
    _deliver_bundle,
    _publish_bundle,
    _read_leg_bundle,
    _validate_bound_sources,
    _validate_parent_daily_parity,
    _validate_parent_source_parity,
    inspect_leg_disagreement_bundle,
    prepare_leg_disagreement_request,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _refs(tmp_path: Path) -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_leg_parent_overlay_manifest",
        "n3_leg_parent_overlay_receipt",
        "n3_leg_parent_qe_score_panel",
        "n3_leg_n2a_manifest",
        "n3_leg_n2a_request",
        "n3_leg_n2a_full_universe",
        "n3_leg_n1_manifest",
        "n3_leg_n1_cpcv",
        "n3_leg_n1_regime_daily",
    )
    refs: list[EvidenceReferenceV1] = []
    for index, role in enumerate(roles):
        path = tmp_path / f"evidence-{index}.bin"
        payload = f"evidence-{index}".encode()
        path.write_bytes(payload)
        import hashlib

        refs.append(
            EvidenceReferenceV1(
                role=role,
                artifact_uri=path.as_posix(),
                sha256=hashlib.sha256(payload).hexdigest(),
                size_bytes=len(payload),
            )
        )
    return tuple(refs)


def _request(tmp_path: Path):
    refs = _refs(tmp_path)
    source_dataset = "a" * 64
    parent_dataset = "b" * 64
    split = "c" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "parent_dataset_identity": parent_dataset,
            "n1_split_policy_sha256": split,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    return build_leg_disagreement_request(
        created_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
        evidence_refs=refs,
        parent_overlay_bundle_path=(tmp_path / ("1" * 64)).as_posix(),
        parent_overlay_bundle_id="1" * 64,
        parent_overlay_request_sha256="2" * 64,
        parent_overlay_receipt_sha256="3" * 64,
        n2a_bundle_path=(tmp_path / ("4" * 64)).as_posix(),
        n2a_bundle_id="4" * 64,
        n2a_request_sha256="5" * 64,
        n2a_receipt_sha256="6" * 64,
        n1_bundle_path=(tmp_path / ("7" * 64)).as_posix(),
        n1_bundle_id="7" * 64,
        n1_request_sha256="8" * 64,
        n1_split_policy_sha256=split,
        source_dataset_identity=source_dataset,
        parent_dataset_identity=parent_dataset,
        dataset_identity=dataset,
        policy_identity="d" * 64,
        registry_path=(tmp_path / "registry.jsonl").as_posix(),
        route_path=(tmp_path / "route.md").as_posix(),
        repository_root=tmp_path.as_posix(),
        repository_commit="e" * 40,
        output_root=(tmp_path / "output").as_posix(),
    )


def _publish(tmp_path: Path) -> tuple[object, Path]:
    request = _request(tmp_path)
    dates = pd.bdate_range("2025-01-02", periods=2)
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates.repeat(5),
            "instrument": [f"{index:06d}.SZ" for _ in dates for index in range(5)],
            "outcome_known": True,
            "economic_net_excess_bps": 1.0,
        }
    )
    oof = features[["decision_as_of_trade_date", "instrument"]].assign(
        parent_rank_pct=0.5,
        economic_net_excess_bps=1.0,
        outcome_known=True,
        linear_oof_score=0.5,
        linear_oof_score_count=7,
        expanded_oof_score=0.5,
        expanded_oof_score_count=7,
    )
    folds = pd.DataFrame(
        {
            "trial_id": ["N3_LEG_LINEAR_COMPARATOR_V1", "N3_LEG_DISAGREEMENT_EXPANDED_V1"],
            "path_id": ["path-0", "path-0"],
            "train_row_count": [5, 5],
            "validation_row_count": [5, 5],
            "coefficient_json": ["[0]", "[0]"],
            "intercept": [0.0, 0.0],
        }
    )
    daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "parent_rank_ic": [0.0, 0.0],
            "parent_top5_net_excess_bps": [1.0, 1.0],
            "parent_top5_churn": [None, 0.0],
        }
    )
    summary = {
        "schema_version": "advisory_n3_leg_disagreement_model_summary_v1",
        "request_sha256": request.request_sha256,
        "selected_trial_id": None,
        "eligible": False,
        "reason_codes": ["TEST_NEGATIVE"],
    }
    frontier = {
        "schema_version": "advisory_n3_leg_disagreement_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": [],
        "selected_trial_id": None,
        "selected_trial_count": 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "ALL_FOUR_FAMILYWISE_LOWERS_AND_SUPPORT__SELECT_ONCE",
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    bundle = _publish_bundle(
        request=request,
        features=features,
        oof_scores=oof,
        fold_diagnostics=folds,
        daily_metrics=daily,
        model_summary=summary,
        frontier=frontier,
        elapsed_seconds=1.0,
    )
    return request, bundle


def test_bundle_publish_inspect_and_delivery_are_restart_safe(tmp_path: Path) -> None:
    request, bundle = _publish(tmp_path)
    inspected = inspect_leg_disagreement_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["selected_trial_count"] == 0
    assert inspected["next_task"] == "N3_MINUTE_INFORMATION_SET_MVE"
    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)
    assert first["registry"]["appended_count"] == 1
    assert second["registry"]["duplicate_noop_count"] == 1
    assert second["route"]["status"] == "EXACT_NOOP"


def test_prepare_normalizes_inherited_cross_os_control_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle_ids = [str(index) * 64 for index in (1, 2, 3)]
    parent_path, n2a_path, n1_path = (tmp_path / bundle_id for bundle_id in bundle_ids)
    foreign_dev = (Path("/") / "mnt" / "f" / "Dev").as_posix()
    foreign_control = f"{foreign_dev}/AIstock_model_artifacts/control"
    parent = {
        "request": SimpleNamespace(
            request_sha256="4" * 64,
            parent_bundle_path=f"{foreign_dev}/parent-qe",
            registry_path=f"{foreign_control}/trial_registry.jsonl",
            route_path=f"{foreign_control}/current_route.md",
        ),
        "receipt": SimpleNamespace(receipt_sha256="5" * 64),
        "record": SimpleNamespace(dataset_identity="6" * 64, policy_identity="7" * 64),
    }
    n2a = {
        "request": SimpleNamespace(request_sha256="8" * 64),
        "receipt": SimpleNamespace(receipt_sha256="9" * 64),
        "record": SimpleNamespace(dataset_identity="a" * 64),
    }
    n1 = {"request": SimpleNamespace(request_sha256="b" * 64, split_policy_sha256="c" * 64)}
    monkeypatch.setattr(pipeline, "_read_overlay_bundle", lambda _: parent)
    monkeypatch.setattr(pipeline, "_read_n2a_bundle", lambda _: n2a)
    monkeypatch.setattr(pipeline, "_read_n1_bundle", lambda _: n1)
    monkeypatch.setattr(pipeline, "_validate_bound_sources", lambda **_: None)
    monkeypatch.setattr(pipeline, "_cross_os_git_dirty_paths", lambda _: [])
    monkeypatch.setattr(pipeline, "_cross_os_git_commit", lambda _: "d" * 40)
    monkeypatch.setattr(pipeline, "_git_origin_main_commit", lambda _: "d" * 40)
    monkeypatch.setattr(pipeline, "_write_immutable_request", lambda *_: None)

    def fake_reference(path: Path, *, role: str) -> EvidenceReferenceV1:
        return EvidenceReferenceV1(role=role, artifact_uri=path.as_posix(), sha256="e" * 64, size_bytes=1)

    monkeypatch.setattr(pipeline, "evidence_reference_for_file", fake_reference)
    request = prepare_leg_disagreement_request(
        parent_overlay_bundle_path=parent_path,
        n2a_bundle_path=n2a_path,
        n1_bundle_path=n1_path,
        repository_root=tmp_path,
        output_root=tmp_path / "output",
        output_path=tmp_path / "request.json",
    )
    assert request.registry_path == pipeline._resolve_bound_path(parent["request"].registry_path).as_posix()
    assert request.route_path == pipeline._resolve_bound_path(parent["request"].route_path).as_posix()


def test_bundle_mutation_is_rejected(tmp_path: Path) -> None:
    _, bundle = _publish(tmp_path)
    summary_path = bundle / "model_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["reason_codes"] = []
    summary_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _read_leg_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID"


def test_parent_selected_one_cannot_seed_information_set_request() -> None:
    parent = {
        "receipt": SimpleNamespace(
            selected_trial_count=1,
            selected_trial_id="x",
            next_task="N3_PARENT_OVERLAY_CONFIRMATION_DESIGN",
            decision_use=DecisionUse.NAVIGATION_ONLY,
            sealed_holdout_accessed=False,
            deployable=False,
        ),
        "record": SimpleNamespace(
            experiment_id="ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1",
            policy_identity="p",
        ),
    }
    n2a = {
        "record": SimpleNamespace(
            experiment_id="ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",
            evaluated_trial_count=0,
            decision_use=DecisionUse.NAVIGATION_ONLY,
            policy_identity="p",
        )
    }
    n1 = {
        "request": SimpleNamespace(decision_date_start=date(2024, 7, 4), decision_date_end=date(2026, 2, 2)),
        "learnability": SimpleNamespace(sealed_holdout_accessed=False),
    }
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_bound_sources(
            parent_path=Path("parent"),
            parent=parent,
            n2a_path=Path("n2a"),
            n2a=n2a,
            n1_path=Path("n1"),
            n1=n1,
        )
    assert caught.value.reason_code == "ADVISORY_N3_LEG_MVE_REQUEST_INVALID"


def test_parent_source_and_daily_parity_are_exact_gates() -> None:
    request = FrozenLegDisagreementRequestV1.model_construct(expected_known_row_count=2)
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2025-01-02", "2025-01-02", "2025-01-03"]),
            "instrument": ["000001.SZ", "000002.SZ", "000001.SZ"],
            "score__IC_WEIGHTED_PARENT": [0.1, 0.2, 0.3],
            "economic_net_excess_bps": [1.0, 2.0, None],
            "outcome_known": [True, True, False],
        }
    )
    parent = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2025-01-02", "2025-01-02"]),
            "instrument": ["000001.SZ", "000002.SZ"],
            "score": [0.1, 0.2],
            "economic_net_excess_bps": [1.0, 2.0],
            "outcome_known": [True, True],
        }
    )
    _validate_parent_source_parity(features=features, parent_panel=parent, request=request)
    poisoned = parent.copy()
    poisoned.loc[0, "score"] = 0.1000000001
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_parent_source_parity(features=features, parent_panel=poisoned, request=request)
    assert caught.value.reason_code == "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED"

    daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "parent_rank_ic": [0.1, 0.2],
            "parent_top5_net_excess_bps": [10.0, 20.0],
            "parent_top5_evaluable": [True, True],
            "parent_top5_churn": [None, 0.4],
        }
    )
    _validate_parent_daily_parity(daily=daily, parent_daily=daily.copy())
    drifted = daily.copy()
    drifted.loc[1, "parent_top5_net_excess_bps"] += 1e-6
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_parent_daily_parity(daily=daily, parent_daily=drifted)
    assert caught.value.reason_code == "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED"

    typed_missing = daily.copy()
    typed_missing.loc[1, "parent_top5_net_excess_bps"] = float("nan")
    typed_missing.loc[1, "parent_top5_evaluable"] = False
    _validate_parent_daily_parity(daily=typed_missing, parent_daily=daily.copy())
    invalid_partial = typed_missing.copy()
    invalid_partial.loc[1, "parent_top5_net_excess_bps"] = 12.0
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_parent_daily_parity(daily=invalid_partial, parent_daily=daily.copy())
    assert caught.value.reason_code == "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED"

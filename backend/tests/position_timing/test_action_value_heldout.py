from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing import action_value_heldout as heldout
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256


def _write_prior_request(
    root: Path, folder: str, schema: str, symbols: list[str], suffix: str
) -> Path:
    path = root / folder / "requests" / f"{suffix}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": schema, "selected_symbols": symbols}
    payload["request_sha256"] = canonical_sha256(payload)
    path.write_bytes(canonical_json_bytes(payload))
    return path


def _prior_root(tmp_path: Path) -> Path:
    root = tmp_path / "research"
    _write_prior_request(
        root,
        "action_value_v2",
        "position_timing_action_value_request_v4",
        ["000001.SZ", "000002.SZ"],
        "v4",
    )
    _write_prior_request(
        root,
        "action_value_incremental_v1",
        "position_timing_action_value_increment_request_v3",
        ["000002.SZ", "000003.SZ"],
        "increment",
    )
    _write_prior_request(
        root,
        "action_value_entry_only_v1",
        "position_timing_entry_only_policy_request_v1",
        ["000001.SZ", "000004.SZ"],
        "entry",
    )
    return root


def test_prior_request_identity_uses_union_and_reads_no_outcomes(tmp_path: Path) -> None:
    identity = heldout.prior_request_identity(_prior_root(tmp_path))

    assert identity["request_count"] == 3
    assert identity["forbidden_symbols"] == (
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
        "000004.SZ",
    )
    assert identity["forbidden_symbol_count"] == 4
    assert identity["outcomes_read"] is False
    assert identity["aggregate_sha256"] == canonical_sha256(
        {key: value for key, value in identity.items() if key != "aggregate_sha256"}
    )
    serialized = json.loads(canonical_json_bytes(identity))
    assert canonical_sha256(serialized) == canonical_sha256(identity)


def test_prior_request_identity_rejects_unbound_request(tmp_path: Path) -> None:
    root = _prior_root(tmp_path)
    path = root / "action_value_v2" / "requests" / "v4.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["selected_symbols"].append("000005.SZ")
    path.write_bytes(canonical_json_bytes(payload))

    with pytest.raises(ActionValueError, match="HELDOUT_PRIOR_REQUEST_IDENTITY_MISMATCH"):
        heldout.prior_request_identity(root)


def test_heldout_selection_is_deterministic_and_disjoint() -> None:
    universe = [f"{index:06d}.SZ" for index in range(1, 150)]
    forbidden = universe[:30]

    first = heldout.select_heldout_symbols(
        universe, forbidden_symbols=forbidden, seed=20260907, limit=64
    )
    second = heldout.select_heldout_symbols(
        tuple(reversed(universe)),
        forbidden_symbols=forbidden,
        seed=20260907,
        limit=64,
    )

    assert first == second
    assert len(first) == 64
    assert not set(first).intersection(forbidden)


def test_heldout_selection_fails_when_population_is_too_small() -> None:
    with pytest.raises(ActionValueError, match="HELDOUT_POPULATION_UNAVAILABLE"):
        heldout.select_heldout_symbols(
            ["000001.SZ", "000002.SZ"],
            forbidden_symbols=["000001.SZ"],
            seed=20260907,
            limit=2,
        )


def _prior_identity(tmp_path: Path) -> dict:
    payload = {
        "schema_version": "position_timing_prior_action_value_requests_v1",
        "research_root": (tmp_path / "timing" / "research").as_posix(),
        "request_folders": heldout.PRIOR_REQUEST_FOLDERS,
        "request_count": 1,
        "folder_counts": {"x": 1},
        "requests": [{"request_sha256": "1" * 64}],
        "forbidden_symbols": ("000001.SZ",),
        "forbidden_symbol_count": 1,
        "outcomes_read": False,
    }
    return {**payload, "aggregate_sha256": canonical_sha256(payload)}


def _request(tmp_path: Path) -> dict:
    evaluation = tuple(f"{index:06d}.SZ" for index in range(100, 164))
    prior = _prior_identity(tmp_path)
    payload = {
        "schema_version": heldout.REQUEST_SCHEMA,
        "pipeline_id": heldout.PIPELINE_ID,
        "created_at": "2026-09-10T10:00:00+08:00",
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "a" * 40,
        "timing_root": (tmp_path / "timing").as_posix(),
        "parent_entry_only": {
            "bundle_path": (tmp_path / "entry").as_posix(),
            "manifest_file": {"path": "x", "sha256": "1" * 64, "size_bytes": 1},
            "manifest_sha256": "2" * 64,
            "request_sha256": "3" * 64,
            "receipt_sha256": "4" * 64,
        },
        "parent_v4": {
            "bundle_path": (tmp_path / "v4").as_posix(),
            "manifest_file": {"path": "x", "sha256": "5" * 64, "size_bytes": 1},
            "manifest_sha256": "6" * 64,
            "request_sha256": "7" * 64,
            "receipt_sha256": "8" * 64,
            "training_rows_file": {"path": "x", "sha256": "9" * 64, "size_bytes": 1},
            "oof_predictions_file": {"path": "x", "sha256": "a" * 64, "size_bytes": 1},
        },
        "candidate_root": (tmp_path / "candidate").as_posix(),
        "training_symbols": ("000010.SZ", "000011.SZ"),
        "evaluation_symbols": evaluation,
        "population_spec": {
            "start": "2018-08-01",
            "end": "2026-08-31",
            "seed": 20260907,
            "symbol_limit": 64,
            "selected_symbols": evaluation,
            "selection": "SHA256_SEED_AFTER_ALL_PRIOR_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
            "forbidden_symbol_count": 1,
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "daily_replay_source_identity": {
            "schema_version": "position_timing_daily_replay_source_identity_v1",
            "aggregate_sha256": "b" * 64,
        },
        "corporate_action_snapshot": {
            "path": (tmp_path / "corporate-actions.json").as_posix(),
            "sha256": "c" * 64,
            "size_bytes": 1,
        },
        "parent_source_sha256": "d" * 64,
        "parent_feature_spec_sha256": "e" * 64,
        "parent_policy_sha256": "f" * 64,
        "candidate_policy_sha256": canonical_sha256(
            heldout.STUDY_CONTRACT["candidate_policy"]
        ),
        "study_contract": heldout.STUDY_CONTRACT,
        "study_contract_sha256": heldout.STUDY_CONTRACT_SHA256,
        "result_class": heldout.RESULT_CLASS,
        "registry_write": False,
        "current_write": False,
        "model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
    }
    # The policy hash is not merely the serialized policy-contract hash.
    from backend.services.position_timing.action_value import CORE_INFORMATION_BLOCK
    from backend.services.position_timing.action_value_advice import (
        ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
        action_authority_policy_sha256,
    )

    payload["candidate_policy_sha256"] = action_authority_policy_sha256(
        CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY
    )
    payload["request_sha256"] = canonical_sha256(payload)
    return payload


def _receipt(request: dict, evidence: str = "INCONCLUSIVE") -> dict:
    comparisons = {
        "BUY_AND_HOLD": {"effect_evidence": evidence},
        "FROZEN_L1_V1": {"effect_evidence": evidence},
    }
    joint = heldout._joint_evidence(comparisons)
    payload = {
        "schema_version": heldout.RECEIPT_SCHEMA,
        "pipeline_id": heldout.PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "result_class": heldout.RESULT_CLASS,
        "provenance_reason": heldout.PROVENANCE_REASON,
        "trial_count": 2,
        "planned_candidate_policy_count": 1,
        "familywise_hypothesis_count": 2,
        "selected_trial_count": 1 if joint == "SUPPORTED" else 0,
        "joint_effect_evidence": joint,
        "candidate_policy_sha256": request["candidate_policy_sha256"],
        "heldout_policy": {"comparisons": comparisons},
        "serving_status": "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED",
        "registry_written": False,
        "current_written": False,
        "model_artifact_written": False,
        "card_written": False,
        "alert_written": False,
        "order_written": False,
        "database_written": False,
        "runtime_written": False,
    }
    payload["receipt_sha256"] = canonical_sha256(payload)
    return payload


def test_request_rejects_overlap_and_write_flags(tmp_path: Path) -> None:
    request = _request(tmp_path)
    path = tmp_path / "request.json"
    path.write_bytes(canonical_json_bytes(request))
    loaded = heldout._load_request(path)
    assert loaded["request_sha256"] == request["request_sha256"]
    assert tuple(loaded["evaluation_symbols"]) == request["evaluation_symbols"]

    request["evaluation_symbols"] = (
        request["training_symbols"][0],
        *request["evaluation_symbols"][1:],
    )
    request["population_spec"]["selected_symbols"] = request["evaluation_symbols"]
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="HELDOUT_REQUEST_IDENTITY_MISMATCH"):
        heldout._load_request(path)

    request = _request(tmp_path)
    request["runtime_write"] = True
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="HELDOUT_REQUEST_IDENTITY_MISMATCH"):
        heldout._load_request(path)


def test_joint_evidence_preserves_negative_and_multiplicity() -> None:
    assert (
        heldout._joint_evidence(
            {
                "BUY_AND_HOLD": {"effect_evidence": "SUPPORTED"},
                "FROZEN_L1_V1": {"effect_evidence": "SUPPORTED"},
            }
        )
        == "SUPPORTED"
    )
    assert (
        heldout._joint_evidence(
            {
                "BUY_AND_HOLD": {"effect_evidence": "NEGATIVE"},
                "FROZEN_L1_V1": {"effect_evidence": "INCONCLUSIVE"},
            }
        )
        == "NEGATIVE"
    )
    assert (
        heldout._joint_evidence(
            {
                "BUY_AND_HOLD": {"effect_evidence": "SUPPORTED"},
                "FROZEN_L1_V1": {"effect_evidence": "INCONCLUSIVE"},
            }
        )
        == "INCONCLUSIVE"
    )


def test_bundle_is_immutable_inspectable_and_retry_is_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(tmp_path)
    receipt = _receipt(request)
    bundle = (
        Path(request["timing_root"])
        / "research"
        / heldout.ARTIFACT_FOLDER
        / "bundles"
        / request["request_sha256"]
    )
    frame = pd.DataFrame({"x": [1]})
    heldout._publish_bundle(
        bundle,
        request=request,
        receipt=receipt,
        oof=frame,
        sleeves=frame,
        daily=frame,
    )
    manifest_before = (bundle / "manifest.json").read_bytes()

    assert heldout.inspect_heldout_bundle(bundle)["receipt"] == receipt
    monkeypatch.setattr(heldout, "_clean_repository_commit", lambda _: "a" * 40)
    request_path = tmp_path / "request.json"
    request_path.write_bytes(canonical_json_bytes(request))
    result = heldout.run_heldout_request(request_path)

    assert result["status"] == "ALREADY_MATERIALIZED"
    assert (bundle / "manifest.json").read_bytes() == manifest_before


def test_bundle_corruption_fails_closed(tmp_path: Path) -> None:
    request = _request(tmp_path)
    receipt = _receipt(request)
    bundle = tmp_path / "bundle"
    frame = pd.DataFrame({"x": [1]})
    heldout._publish_bundle(
        bundle,
        request=request,
        receipt=receipt,
        oof=frame,
        sleeves=frame,
        daily=frame,
    )
    (bundle / "heldout_sleeve_days.parquet").write_bytes(b"corrupt")

    with pytest.raises(ActionValueError, match="HELDOUT_BUNDLE_FILE_IDENTITY_MISMATCH"):
        heldout.inspect_heldout_bundle(bundle)

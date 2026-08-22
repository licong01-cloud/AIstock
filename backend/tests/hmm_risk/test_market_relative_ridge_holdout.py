from __future__ import annotations

import copy
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from psycopg2 import OperationalError

from backend.services.hmm_risk import market_relative_ridge_holdout as subject
from backend.services.hmm_risk.market_relative_jump_spike import MARKET_FEATURES, RELATIVE_FEATURES, Preprocessor
from backend.services.hmm_risk.state_model_set import StateModelSetError
from scripts.hmm_risk import run_market_relative_ridge_holdout as cli


def _receipt(body: dict[str, object]) -> dict[str, object]:
    return {**body, "receipt_sha256": subject.canonical_sha256(body)}


def _development_request_identity() -> dict[str, object]:
    return {
        "candidate_attempt_index": 3,
        "contract_version": "C-011-P2-3C-D1-D6",
        "expected_producer_commit": subject.EXPECTED_CANDIDATE_PRODUCER,
        "fixed_market_parameters": {"jump_penalty": 4.0, "seed": 42},
        "forbidden_holdout_date_set_sha256": "2783d628df3160f0786b0d6d1f8c4f89a46952ca7903559fa33e616d05e0232b",
        "holdout_end": subject.HOLDOUT_END.isoformat(),
        "holdout_start": subject.HOLDOUT_START.isoformat(),
        "holdout_trading_day_count": subject.HOLDOUT_TRADING_DAYS,
        "prior_not_available_report_sha256s": {
            "P2-3A": "034fdf3c7a2354bad62bdea0a55b675f2552c42a65d1ccacbc454561e75f12ec",
            "P2-3B": "d3298654ed9f2080f4623c2c50721ebf9951d2034d42cfdfe225f36e4ee0fc45",
        },
        "schema_version": "hmm_risk_market_conditioned_ridge_candidate_request_v1",
        "source_sha256": "2806f0bd63869f7eb11d1f00d4682332339ce96838c29d720cff492efdb61518",
    }


def _candidate_report() -> dict[str, object]:
    components: list[dict[str, object]] = []
    for component in ("market", "L1", "L2"):
        components.append(_receipt({"component": component, "phase": "development"}))
    components.extend(
        [
            _receipt(
                {
                    "schema_version": subject.P2_3C_MARKET_COMPONENT_SCHEMA_VERSION,
                    "component": "market",
                    "phase": "final-development",
                    "fixed_jump_penalty": 4.0,
                    "fixed_seed": 42,
                    "preprocess": {"kind": "market"},
                    "preprocess_sha256": "1" * 64,
                    "centers": [[0.0] * 5, [1.0] * 5],
                    "centers_sha256": "2" * 64,
                    "semantic_mapping": {"0": "risk_off", "1": "risk_on"},
                    "semantic_mapping_sha256": "3" * 64,
                }
            ),
            _receipt(
                {
                    "schema_version": subject.P2_3C_COMPONENT_SCHEMA_VERSION,
                    "component": "L1",
                    "level": "L1",
                    "phase": "final-development",
                    "selected_alpha": 100.0,
                    "canonical_sector_count": 31,
                    "canonical_sector_sha256": "4" * 64,
                    "preprocess": {"kind": "L1"},
                    "preprocess_sha256": "5" * 64,
                    "fit": {"kind": "L1"},
                }
            ),
            _receipt(
                {
                    "schema_version": subject.P2_3C_COMPONENT_SCHEMA_VERSION,
                    "component": "L2",
                    "level": "L2",
                    "phase": "final-development",
                    "selected_alpha": 100.0,
                    "canonical_sector_count": 131,
                    "canonical_sector_sha256": "6" * 64,
                    "preprocess": {"kind": "L2"},
                    "preprocess_sha256": "7" * 64,
                    "fit": {"kind": "L2"},
                }
            ),
        ]
    )
    request_identity = _development_request_identity()
    body: dict[str, object] = {
        "schema_version": subject.P2_3C_REPORT_SCHEMA_VERSION,
        "status": "P2_3C_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE",
        "producer_commit": subject.EXPECTED_CANDIDATE_PRODUCER,
        "completed_fit_count": 36,
        "planned_fit_count": 36,
        "candidate_attempt_index": 3,
        "request_identity": request_identity,
        "request_identity_sha256": subject.canonical_sha256(request_identity),
        "feature_formula_sha256": "4" * 64,
        "components": components,
        "component_receipt_sha256s": [item["receipt_sha256"] for item in components],
        "holdout_accessed": False,
        "product_acceptance_performed": False,
        "model_write": False,
        "ready_write": False,
        "database_write": False,
        "runtime_action": False,
    }
    return {**body, "report_sha256": subject.canonical_sha256(body)}


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.write_bytes(subject.canonical_json_bytes(value) + b"\n")


def _artifact_outputs(root: Path) -> dict[str, str]:
    acceptance = (root / "acceptance.json").resolve()
    child_1 = (root / "children" / "p2_4_holdout_child_1.json").resolve()
    child_2 = (root / "children" / "p2_4_holdout_child_2.json").resolve()
    return {
        "acceptance_output": str(acceptance),
        "acceptance_failure_output": str(acceptance.with_name("acceptance.failure.json")),
        "model_output": str((root / "model.json").resolve()),
        "ready_output": str((root / "ready.json").resolve()),
        "child_1_output": str(child_1),
        "child_1_failure_output": str(child_1.with_name("p2_4_holdout_child_1.failure.json")),
        "child_2_output": str(child_2),
        "child_2_failure_output": str(child_2.with_name("p2_4_holdout_child_2.failure.json")),
    }


def _request(candidate_hash: str, *, artifact_root: Path | None = None) -> dict[str, object]:
    holdout = {
        "source": {
            "source_start": subject.EXPECTED_SOURCE_START,
            "source_end": "2026-04-30",
            "circ_mv_history_start": subject.EXPECTED_CIRC_MV_HISTORY_START,
            "universe_key": subject.EXPECTED_UNIVERSE_KEY,
            "universe_rule_version": subject.EXPECTED_UNIVERSE_RULE_VERSION,
            "benchmark_ts_code": subject.EXPECTED_BENCHMARK_TS_CODE,
            "security_identity_manifest_path": subject.EXPECTED_SECURITY_IDENTITY_MANIFEST_PATH,
            "security_identity_manifest_sha256": subject.EXPECTED_SECURITY_IDENTITY_MANIFEST_SHA256,
            "provider_absence_manifest_path": subject.EXPECTED_PROVIDER_ABSENCE_MANIFEST_PATH,
            "provider_absence_manifest_sha256": subject.EXPECTED_PROVIDER_ABSENCE_MANIFEST_SHA256,
        },
        "state_start": subject.HOLDOUT_START.isoformat(),
        "state_end": subject.HOLDOUT_END.isoformat(),
        "state_trading_day_count": subject.HOLDOUT_TRADING_DAYS,
        "outcome_tail_end": "2026-04-30",
        "outcome_tail_trading_day_count": subject.OUTCOME_TAIL_TRADING_DAYS,
        "dataset_manifest_sha256": "1" * 64,
        "mapping_manifest_sha256": "2" * 64,
        "calendar_manifest_sha256": "3" * 64,
        "feature_formula_sha256": "4" * 64,
        "security_identity_manifest_sha256": "5" * 64,
        "provider_absence_manifest_sha256": "6" * 64,
        "constituents_sha256": "7" * 64,
        "state_date_set_sha256": "8" * 64,
        "outcome_tail_date_set_sha256": "9" * 64,
    }
    evaluation = subject.canonical_sha256(
        {
            "contract_version": subject.CONTRACT_VERSION,
            "candidate_report_sha256": candidate_hash,
            "holdout_state_date_set_sha256": holdout["state_date_set_sha256"],
        }
    )
    body = {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "candidate_report_sha256": candidate_hash,
        "candidate_producer_commit": subject.EXPECTED_CANDIDATE_PRODUCER,
        "development_request_sha256": subject.EXPECTED_DEVELOPMENT_REQUEST_SHA256,
        "holdout_evaluation_id": evaluation,
        "holdout_source": holdout,
        "artifact_outputs": _artifact_outputs(artifact_root or Path("F:/AIstock_artifacts/p2_4_test")),
    }
    return {**body, "request_sha256": subject.canonical_sha256(body)}


def _frozen(report: dict[str, object]) -> subject.FrozenCandidate:
    components = report["components"]
    assert isinstance(components, list)
    return subject.FrozenCandidate(
        report=report,
        market=copy.deepcopy(components[3]),
        levels={"L1": copy.deepcopy(components[4]), "L2": copy.deepcopy(components[5])},
    )


def _closure_request(*, candidate_hash: str = "a" * 64) -> dict[str, object]:
    return {
        "candidate_report_sha256": candidate_hash,
        "holdout_evaluation_id": "b" * 64,
        "holdout_source": {
            "state_date_set_sha256": "d" * 64,
            "outcome_tail_date_set_sha256": "e" * 64,
            "feature_formula_sha256": "4" * 64,
        },
    }


def _child(
    index: int,
    *,
    status: str = "FULL_READY",
    payload_marker: str = "same",
    request: dict[str, object] | None = None,
) -> dict[str, object]:
    authority = request or _closure_request()
    holdout_source = authority["holdout_source"]
    assert isinstance(holdout_source, dict)
    product_passed = status != "NOT_AVAILABLE"
    level_payloads = {
        level: {
            "state": _receipt({"level": level, "kind": "state"}),
            "metrics": _receipt({"level": level, "product_metrics_passed": product_passed}),
        }
        for level in ("L1", "L2")
    }
    payload = {
        "candidate_report_sha256": authority["candidate_report_sha256"],
        "holdout_evaluation_id": authority["holdout_evaluation_id"],
        "holdout_source_sha256": subject.canonical_sha256(holdout_source),
        "state_date_set_sha256": holdout_source["state_date_set_sha256"],
        "outcome_tail_date_set_sha256": holdout_source["outcome_tail_date_set_sha256"],
        "market_receipt": _receipt({"marker": payload_marker}),
        "levels": level_payloads,
        "quintiles": _receipt({"kind": "quintiles"}),
        "hierarchy_sha256": "f" * 64,
        "coverage": _receipt({"status": status, "product_metrics_passed": product_passed}),
        "runtime_versions": {},
        "fit_count": 0,
        "selection_performed": False,
        "database_write": False,
        "runtime_action": False,
    }
    body = {
        "schema_version": subject.CHILD_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "algorithm_version": subject.ALGORITHM_VERSION,
        "status": "child_complete",
        "process_index": index,
        "producer_commit": "a" * 40,
        "holdout_accessed": True,
        "product_acceptance_performed": True,
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": subject.canonical_sha256(payload),
        "model_write": False,
        "ready_write": False,
    }
    return {**body, "report_sha256": subject.canonical_sha256(body)}


def _segment(start: date, end: date, count: int) -> tuple[date, ...]:
    span = (end - start).days
    positions = [round(index * span / (count - 1)) for index in range(count)]
    assert len(set(positions)) == count
    return tuple(start + timedelta(days=value) for value in positions)


def _preprocessor(features: tuple[str, ...]) -> Preprocessor:
    return Preprocessor(
        feature_names=features,
        lower=tuple(-100.0 for _ in features),
        upper=tuple(100.0 for _ in features),
        mean=tuple(0.0 for _ in features),
        std=tuple(1.0 for _ in features),
        valid_row_count=1000,
        valid_identity_sha256="a" * 64,
    )


def _holdout_panel(codes: list[str], dates: tuple[date, ...]) -> pd.DataFrame:
    rows: list[dict[str, float | str | pd.Timestamp]] = []
    midpoint = (len(codes) - 1) / 2.0
    for date_index, day in enumerate(dates):
        market = -1.0 if date_index % 17 < 5 else 1.0
        for code_index, code in enumerate(codes):
            relative = (code_index - midpoint) / max(1.0, midpoint)
            row: dict[str, float | str | pd.Timestamp] = {
                "trade_date": pd.Timestamp(day),
                "l1_code": code,
                "daily_return": 0.0005 * relative + 0.0001 * ((date_index % 5) - 2),
            }
            for feature in MARKET_FEATURES:
                row[feature] = market
            row["daily_return"] = 0.0005 * relative + 0.0001 * ((date_index % 5) - 2)
            for feature_index, feature in enumerate(RELATIVE_FEATURES):
                row[feature] = relative * (1.0 + feature_index / 10.0)
            rows.append(row)
    return pd.DataFrame(rows).set_index(["trade_date", "l1_code"]).sort_index()


def _evaluation_fixture() -> tuple[dict[str, object], subject.FrozenCandidate, dict[str, object]]:
    development = _segment(subject.DEVELOPMENT_START, subject.DEVELOPMENT_END, subject.DEVELOPMENT_TRADING_DAYS)
    state = _segment(subject.HOLDOUT_START, subject.HOLDOUT_END, subject.HOLDOUT_TRADING_DAYS)
    tail = tuple(
        subject.HOLDOUT_END + timedelta(days=index)
        for index in range(1, 41)
        if (subject.HOLDOUT_END + timedelta(days=index)).weekday() < 5
    )[:20]
    calendar = (*development, *state, *tail)
    l1_codes = [f"L1-{index:03d}" for index in range(31)]
    l2_codes = [f"L2-{index:03d}" for index in range(131)]
    benchmark_rows = [[day.isoformat(), 0.0] for day in calendar]
    inputs: dict[str, object] = {
        "trading_dates": calendar,
        "panel": _holdout_panel(l1_codes, (*state, *tail)),
        "l2_panel": _holdout_panel(l2_codes, (*state, *tail)),
        "dataset_manifest": {"calendar_benchmark": {"rows": benchmark_rows}},
        "mapping_manifest": {"mapping": "v1"},
        "feature_definition": {"formula": "v1", "level": "L1"},
        "l2_feature_definition": {"formula": "v1", "level": "L2"},
        "security_identity_manifest": {"security": "v1"},
        "provider_absence_manifest": {"absence": "v1"},
        "constituents": {
            parent: {"l2_codes": [code for position, code in enumerate(l2_codes) if position % len(l1_codes) == index]}
            for index, parent in enumerate(l1_codes)
        },
        "c010_diagnostic": {"aggregate_evidence": {"l2_domain_receipts": []}},
    }
    market_preprocess = _preprocessor(tuple(MARKET_FEATURES))
    level_preprocess = _preprocessor(tuple(RELATIVE_FEATURES))
    market = {
        "schema_version": subject.P2_3C_MARKET_COMPONENT_SCHEMA_VERSION,
        "preprocess": market_preprocess.payload(),
        "preprocess_sha256": subject.canonical_sha256(market_preprocess.payload()),
        "centers": [[-1.0] * len(MARKET_FEATURES), [1.0] * len(MARKET_FEATURES)],
        "semantic_mapping": {"0": "risk_off", "1": "risk_on"},
    }
    market["centers_sha256"] = subject.sha256_bytes(subject.np.asarray(market["centers"], dtype="<f8").tobytes())
    market["semantic_mapping_sha256"] = subject.canonical_sha256(market["semantic_mapping"])
    levels: dict[str, dict[str, object]] = {}
    for level, count in (("L1", 31), ("L2", 131)):
        coefficient = subject.np.asarray([1.0, 0.0, 0.0, 0.0, 0.0] + [0.0] * 5, dtype="<f8")
        fit_body = {
            "alpha": 100.0,
            "coefficient": coefficient.tolist(),
            "coefficient_sha256": subject.sha256_bytes(coefficient.tobytes()),
            "intercept": 0.0,
            "row_count": 1000,
            "feature_count": 10,
            "training_identity_sha256": "b" * 64,
        }
        levels[level] = {
            "schema_version": subject.P2_3C_COMPONENT_SCHEMA_VERSION,
            "level": level,
            "selected_alpha": 100.0,
            "canonical_sector_count": count,
            "canonical_sector_sha256": subject.canonical_sha256(l1_codes if level == "L1" else l2_codes),
            "preprocess": level_preprocess.payload(),
            "preprocess_sha256": subject.canonical_sha256(level_preprocess.payload()),
            "fit": _receipt(fit_body),
        }
    candidate_report = {
        "report_sha256": "c" * 64,
        "feature_formula_sha256": subject.canonical_sha256(
            {"L1": inputs["feature_definition"], "L2": inputs["l2_feature_definition"]}
        ),
    }
    candidate = subject.FrozenCandidate(report=candidate_report, market=market, levels=levels)
    holdout_source = {
        "source": {
            "source_start": subject.EXPECTED_SOURCE_START,
            "source_end": tail[-1].isoformat(),
            "circ_mv_history_start": subject.EXPECTED_CIRC_MV_HISTORY_START,
            "universe_key": subject.EXPECTED_UNIVERSE_KEY,
            "universe_rule_version": subject.EXPECTED_UNIVERSE_RULE_VERSION,
            "benchmark_ts_code": subject.EXPECTED_BENCHMARK_TS_CODE,
            "security_identity_manifest_path": subject.EXPECTED_SECURITY_IDENTITY_MANIFEST_PATH,
            "security_identity_manifest_sha256": subject.EXPECTED_SECURITY_IDENTITY_MANIFEST_SHA256,
            "provider_absence_manifest_path": subject.EXPECTED_PROVIDER_ABSENCE_MANIFEST_PATH,
            "provider_absence_manifest_sha256": subject.EXPECTED_PROVIDER_ABSENCE_MANIFEST_SHA256,
        },
        "state_start": subject.HOLDOUT_START.isoformat(),
        "state_end": subject.HOLDOUT_END.isoformat(),
        "state_trading_day_count": subject.HOLDOUT_TRADING_DAYS,
        "outcome_tail_end": tail[-1].isoformat(),
        "outcome_tail_trading_day_count": 20,
        "dataset_manifest_sha256": subject.canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_sha256": subject.canonical_sha256(inputs["mapping_manifest"]),
        "calendar_manifest_sha256": subject.canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "feature_formula_sha256": subject.canonical_sha256(
            {"L1": inputs["feature_definition"], "L2": inputs["l2_feature_definition"]}
        ),
        "security_identity_manifest_sha256": subject.canonical_sha256(inputs["security_identity_manifest"]),
        "provider_absence_manifest_sha256": subject.canonical_sha256(inputs["provider_absence_manifest"]),
        "constituents_sha256": subject.canonical_sha256(inputs["constituents"]),
        "state_date_set_sha256": subject.canonical_sha256([day.isoformat() for day in state]),
        "outcome_tail_date_set_sha256": subject.canonical_sha256([day.isoformat() for day in tail]),
    }
    evaluation = subject.canonical_sha256(
        {
            "contract_version": subject.CONTRACT_VERSION,
            "candidate_report_sha256": candidate_report["report_sha256"],
            "holdout_state_date_set_sha256": holdout_source["state_date_set_sha256"],
        }
    )
    request_body = {
        "schema_version": subject.REQUEST_SCHEMA_VERSION,
        "contract_version": subject.CONTRACT_VERSION,
        "candidate_report_sha256": candidate_report["report_sha256"],
        "candidate_producer_commit": subject.EXPECTED_CANDIDATE_PRODUCER,
        "development_request_sha256": subject.EXPECTED_DEVELOPMENT_REQUEST_SHA256,
        "holdout_evaluation_id": evaluation,
        "holdout_source": holdout_source,
        "artifact_outputs": _artifact_outputs(Path("F:/AIstock_artifacts/p2_4_evaluation_test")),
    }
    request = {**request_body, "request_sha256": subject.canonical_sha256(request_body)}
    return inputs, candidate, request


def test_request_builder_freezes_loaded_source_and_exact_output_authority() -> None:
    inputs, candidate, expected = _evaluation_fixture()
    holdout_source = expected["holdout_source"]
    assert isinstance(holdout_source, dict)
    source = holdout_source["source"]
    outputs = expected["artifact_outputs"]
    assert isinstance(source, dict) and isinstance(outputs, dict)

    request = subject.build_holdout_request(
        inputs,
        candidate,
        source=source,
        artifact_outputs=outputs,
    )

    assert request == expected
    receipt = subject.validate_static_request(request, candidate)
    assert receipt["fit_count"] == 0
    assert receipt["selection_performed"] is False
    assert receipt["holdout_accessed"] is False


@pytest.mark.parametrize(
    ("field", "invalid_value", "invalid_components"),
    [
        ("mapping_manifest", None, ["mapping_manifest"]),
        ("dataset_manifest", {"calendar_benchmark": {}}, []),
    ],
)
def test_request_builder_rejects_missing_source_component_instead_of_hashing_null(
    field: str, invalid_value: object, invalid_components: list[str]
) -> None:
    inputs, candidate, expected = _evaluation_fixture()
    inputs[field] = invalid_value
    holdout_source = expected["holdout_source"]
    assert isinstance(holdout_source, dict)
    source = holdout_source["source"]
    outputs = expected["artifact_outputs"]
    assert isinstance(source, dict) and isinstance(outputs, dict)

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.build_holdout_request(inputs, candidate, source=source, artifact_outputs=outputs)

    assert captured.value.reason_code == subject.REASON_SOURCE
    assert captured.value.evidence == {"invalid_components": invalid_components}


def test_candidate_and_request_preflight_close_exact_authority(tmp_path: Path) -> None:
    report = _candidate_report()
    path = tmp_path / "candidate.json"
    _write_json(path, report)
    candidate = subject.load_frozen_candidate(path, expected_sha256=str(report["report_sha256"]))
    request = _request(str(report["report_sha256"]))

    receipt = subject.validate_static_request(request, candidate)

    assert receipt["fit_count"] == 0
    assert receipt["selection_performed"] is False
    assert receipt["holdout_accessed"] is False


def test_candidate_component_drift_is_rejected_even_when_outer_hash_is_recomputed(tmp_path: Path) -> None:
    report = _candidate_report()
    components = report["components"]
    assert isinstance(components, list) and isinstance(components[4], dict)
    components[4]["selected_alpha"] = 10.0
    body = {key: value for key, value in report.items() if key != "report_sha256"}
    report["report_sha256"] = subject.canonical_sha256(body)
    path = tmp_path / "candidate.json"
    _write_json(path, report)

    with pytest.raises(subject.HoldoutAcceptanceError, match="receipt hash") as captured:
        subject.load_frozen_candidate(path, expected_sha256=str(report["report_sha256"]))

    assert captured.value.reason_code == subject.REASON_CANDIDATE


@pytest.mark.parametrize("payload", ['{"schema_version":"a","schema_version":"b"}', '{"value":NaN}'])
def test_json_reader_rejects_duplicate_keys_and_non_finite_constants(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "request.json"
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.load_request(path)

    assert captured.value.reason_code == subject.REASON_REQUEST
    assert captured.value.stage == "preflight"


def test_child_readback_rejects_duplicate_keys_canonically(tmp_path: Path) -> None:
    path = tmp_path / "child.json"
    path.write_text('{"schema_version":"a","schema_version":"b"}', encoding="utf-8")

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._read_child(path)

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY


def test_request_cannot_change_candidate_or_reuse_development_source() -> None:
    report = _candidate_report()
    candidate = _frozen(report)
    request = _request(str(report["report_sha256"]))
    request["candidate_report_sha256"] = "0" * 64

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.validate_static_request(request, candidate)

    assert captured.value.reason_code == subject.REASON_REQUEST
    assert captured.value.stage == "preflight"


def test_request_binds_frozen_source_policy_and_every_artifact_path(tmp_path: Path) -> None:
    report = _candidate_report()
    candidate = _frozen(report)
    request = _request(str(report["report_sha256"]), artifact_root=tmp_path / "artifacts")

    subject.validate_static_request(request, candidate)
    outputs = request["artifact_outputs"]
    assert isinstance(outputs, dict)
    subject.validate_output_identity(
        request,
        acceptance_output=Path(outputs["acceptance_output"]),
        model_output=Path(outputs["model_output"]),
        ready_output=Path(outputs["ready_output"]),
        child_1_output=Path(outputs["child_1_output"]),
        child_2_output=Path(outputs["child_2_output"]),
        repository_root=tmp_path / "repository",
    )

    drifted = copy.deepcopy(request)
    drifted["holdout_source"]["source"]["universe_rule_version"] = "drift"
    drifted_body = {key: value for key, value in drifted.items() if key != "request_sha256"}
    drifted["request_sha256"] = subject.canonical_sha256(drifted_body)
    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.validate_static_request(drifted, candidate)
    assert captured.value.reason_code == subject.REASON_REQUEST

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.validate_output_identity(
            request,
            acceptance_output=Path(outputs["acceptance_output"]).with_name("other.json"),
            model_output=Path(outputs["model_output"]),
            ready_output=Path(outputs["ready_output"]),
            child_1_output=Path(outputs["child_1_output"]),
            child_2_output=Path(outputs["child_2_output"]),
            repository_root=tmp_path / "repository",
        )
    assert captured.value.reason_code == subject.REASON_REQUEST


def test_child_evaluator_runs_zero_fit_full_l1_l2_path_with_frozen_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs, candidate, request = _evaluation_fixture()
    l2_codes = sorted(inputs["l2_panel"].index.get_level_values("l1_code").unique())
    groups = {code: min(4, index * 5 // len(l2_codes)) for index, code in enumerate(l2_codes)}
    monkeypatch.setattr(
        subject,
        "freeze_quintiles",
        lambda *args, **kwargs: {"groups": {"size": groups, "liquidity": groups}, "receipt_sha256": "f" * 64},
    )
    monkeypatch.setattr(subject, "runtime_versions", lambda: {"fixed_single_thread": True})

    report = subject.evaluate_child(
        inputs,
        request,
        candidate,
        process_index=1,
        producer_commit="a" * 40,
    )

    payload = report["reproducibility_payload"]
    assert report["status"] == "child_complete"
    assert payload["fit_count"] == 0
    assert payload["selection_performed"] is False
    assert payload["levels"]["L1"]["state"]["state_receipt"]["state_row_count"] == 31 * 242
    assert payload["levels"]["L2"]["state"]["state_receipt"]["state_row_count"] == 131 * 242
    coverage = payload["coverage"]
    assert coverage["receipt_sha256"] == subject.canonical_sha256(
        {key: value for key, value in coverage.items() if key != "receipt_sha256"}
    )
    evidence = coverage["evidence"]
    assert len(evidence["daily"]) == 242
    assert len(evidence["l1_sector"]) == 31
    assert len(evidence["l2_sector"]) == 131
    assert len(evidence["size_quintiles"]) == 5
    assert len(evidence["liquidity_quintiles"]) == 5
    assert len(evidence["parents"]) == 31
    assert evidence["l2_sector_sha256"] == subject.canonical_sha256(evidence["l2_sector"])
    assert report["model_write"] is False
    assert report["ready_write"] is False


def test_product_gate_applies_exact_l1_l2_and_risk_boundaries() -> None:
    primary = {
        "metric_coverage_passed": True,
        "rank_ic_newey_west": {"metric_valid": True, "mean": 0.02, "t_stat": 1.96},
        "spread_newey_west": {"metric_valid": True, "mean": 0.005, "t_stat": 1.96},
    }
    risk = {"metric_valid": True, "precision_lift": 0.10, "recall": 0.25}
    quarters = [{"coverage_passed": True, "mean_rank_ic": value} for value in (0.01, 0.02, 0.03, -0.02)]

    assert subject.product_gate(level="L2", primary=primary, risk=risk, quarters=quarters) == {
        "directional_metrics_passed": True,
        "risk_metrics_passed": True,
        "product_metrics_passed": True,
    }
    below = copy.deepcopy(primary)
    below["spread_newey_west"]["mean"] = 0.004999
    assert (
        subject.product_gate(level="L2", primary=below, risk=risk, quarters=quarters)["product_metrics_passed"] is False
    )
    l1 = copy.deepcopy(primary)
    l1["rank_ic_newey_west"]["mean"] = 1e-12
    l1["spread_newey_west"]["mean"] = 1e-12
    assert subject.product_gate(level="L1", primary=l1, risk=risk, quarters=[])["product_metrics_passed"] is True


def test_product_gate_does_not_fill_unavailable_or_borrow_other_level() -> None:
    primary = {
        "metric_coverage_passed": False,
        "rank_ic_newey_west": {"metric_valid": False, "mean": None, "t_stat": None},
        "spread_newey_west": {"metric_valid": True, "mean": 1.0, "t_stat": 100.0},
    }
    risk = {"metric_valid": True, "precision_lift": 1.0, "recall": 1.0}
    quarters = [{"coverage_passed": True, "mean_rank_ic": 1.0}] * 4

    assert (
        subject.product_gate(level="L2", primary=primary, risk=risk, quarters=quarters)["product_metrics_passed"]
        is False
    )


def test_product_metrics_rejects_identity_intersection_shrinkage() -> None:
    day = subject.HOLDOUT_START
    codes = [f"L2-{index}" for index in range(5)]
    scores = {(code, day): float(index) for index, code in enumerate(codes)}
    states = {(code, day): ("trending" if index >= 3 else "fading") for index, code in enumerate(codes)}
    outcomes = {
        horizon: {(code, day): float(index) / 100.0 for index, code in enumerate(codes[:-1])}
        for horizon in subject.HORIZONS
    }

    metrics = subject.product_metrics(
        scores,
        states,
        {day: "risk_on"},
        outcomes,
        {(code, day): False for code in codes[:-1]},
        (day,),
        level="L2",
    )

    ten_day = metrics["daily_metrics"]["10"]
    assert ten_day["daily_rank_ic"] == []
    assert ten_day["daily_spread"] == []
    assert {row["metric"] for row in ten_day["unavailable"]} == {"rank_ic", "spread"}
    assert all("identity_mismatch" in row for row in ten_day["unavailable"])


def test_risk_path_uses_minimum_forward_excess_not_terminal_return() -> None:
    start = date(2025, 4, 1)
    calendar = tuple(start + timedelta(days=index) for index in range(11))
    index = pd.MultiIndex.from_product(
        [[pd.Timestamp(day) for day in calendar], ["L1-A"]], names=["trade_date", "l1_code"]
    )
    returns = [0.0, -0.03, -0.03, 0.07] + [0.0] * 7
    panel = pd.DataFrame({"daily_return": returns}, index=index)
    benchmark = {day: 0.0 for day in calendar}

    outcome = subject._risk_path_outcomes(panel, benchmark, calendar, (start,))

    assert outcome[("L1-A", start)] is True


def test_warning_does_not_invent_first_day_entry_but_allows_risk_off_context() -> None:
    first = date(2025, 4, 1)
    second = date(2025, 4, 2)
    states = {("A", first): "fading", ("A", second): "fading"}

    no_market_risk = subject._warning_rows(states, {first: "risk_on", second: "risk_on"})
    market_risk = subject._warning_rows(states, {first: "risk_off", second: "risk_on"})

    assert no_market_risk[("A", first)] is False
    assert no_market_risk[("A", second)] is False
    assert market_risk[("A", first)] is True


@pytest.mark.parametrize(
    ("status", "model_write", "ready_write"),
    [
        ("FULL_READY", True, True),
        ("COVERAGE_AVAILABLE", True, False),
        ("NOT_AVAILABLE", False, False),
    ],
)
def test_parent_closure_preserves_three_mutually_exclusive_states(
    status: str, model_write: bool, ready_write: bool
) -> None:
    request = _closure_request()
    draft = subject.close_children(
        _child(1, status=status), _child(2, status=status), request=request, producer_commit="a" * 40
    )
    acceptance = subject.finalize_acceptance(
        draft,
        model_sha256="1" * 64 if model_write else None,
        ready_sha256="2" * 64 if ready_write else None,
    )

    assert acceptance["status"] == status
    assert acceptance["model_write"] is model_write
    assert acceptance["ready_write"] is ready_write
    assert acceptance["fit_count"] == 0
    assert acceptance["selection_performed"] is False


def test_parent_rejects_fresh_process_payload_drift_before_any_writer() -> None:
    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.close_children(
            _child(1),
            _child(2, payload_marker="drift"),
            request=_closure_request(),
            producer_commit="a" * 40,
        )

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY


def test_parent_rejects_two_self_hashed_children_that_drift_from_request_authority() -> None:
    request = _closure_request()
    first = _child(1, request=request)
    second = _child(2, request=request)
    for child in (first, second):
        payload = child["reproducibility_payload"]
        assert isinstance(payload, dict)
        payload["candidate_report_sha256"] = "9" * 64
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(payload)
        child_body = {key: value for key, value in child.items() if key != "report_sha256"}
        child["report_sha256"] = subject.canonical_sha256(child_body)

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.close_children(first, second, request=request, producer_commit="a" * 40)

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert "parent authority" in str(captured.value)


def test_model_and_ready_writer_cannot_promote_coverage_or_not_available() -> None:
    report = _candidate_report()
    candidate = _frozen(report)
    coverage = subject.close_children(
        _child(1, status="COVERAGE_AVAILABLE"),
        _child(2, status="COVERAGE_AVAILABLE"),
        request=_closure_request(),
        producer_commit="a" * 40,
    )
    model = subject.model_artifact(coverage, candidate)

    assert model["ready"] is False
    assert model["activation_requires_matching_final_acceptance"] is True
    assert set(model["market"]) == {
        "schema_version",
        "fixed_jump_penalty",
        "fixed_seed",
        "preprocess",
        "preprocess_sha256",
        "centers",
        "centers_sha256",
        "semantic_mapping",
        "semantic_mapping_sha256",
    }
    assert "unavailable_items" not in model["levels"]["L1"]
    assert "target" not in model["levels"]["L2"]
    assert model["source_identity"]["hierarchy_sha256"] == "f" * 64
    assert model["state_projection"] == {
        "method": "daily_cross_section_top_bottom_fraction",
        "state_fraction": subject.STATE_FRACTION,
        "minimum_extreme_count": subject.MINIMUM_EXTREME_COUNT,
        "tie_tolerance": subject.STATE_TIE_TOLERANCE,
        "semantic_order": ["fading", "neutral", "trending"],
        "missing_policy": "typed_unavailable_no_neutral_fill",
    }
    with pytest.raises(subject.HoldoutAcceptanceError):
        subject.ready_artifact(coverage, model)

    not_available = subject.close_children(
        _child(1, status="NOT_AVAILABLE"),
        _child(2, status="NOT_AVAILABLE"),
        request=_closure_request(),
        producer_commit="a" * 40,
    )
    with pytest.raises(subject.HoldoutAcceptanceError):
        subject.model_artifact(not_available, candidate)


def test_acceptance_cannot_claim_required_artifacts_before_durable_writes() -> None:
    full = subject.close_children(
        _child(1, status="FULL_READY"),
        _child(2, status="FULL_READY"),
        request=_closure_request(),
        producer_commit="a" * 40,
    )

    assert full["model_write_required"] is True
    assert full["ready_write_required"] is True
    assert "model_write" not in full
    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.finalize_acceptance(full, model_sha256=None, ready_sha256=None)
    assert captured.value.reason_code == subject.REASON_READBACK


def test_final_bundle_readback_closes_acceptance_model_and_ready() -> None:
    report = _candidate_report()
    candidate = _frozen(report)
    request = _closure_request(candidate_hash=str(report["report_sha256"]))
    draft = subject.close_children(
        _child(1, request=request),
        _child(2, request=request),
        request=request,
        producer_commit="a" * 40,
    )
    model = subject.model_artifact(draft, candidate)
    ready = subject.ready_artifact(draft, model)
    acceptance = subject.finalize_acceptance(
        draft,
        model_sha256=str(model["model_sha256"]),
        ready_sha256=str(ready["ready_sha256"]),
    )

    receipt = subject.validate_artifact_bundle(acceptance, model=model, ready=ready)
    assert receipt["bundle_valid"] is True

    drifted = copy.deepcopy(ready)
    drifted["model_sha256"] = "9" * 64
    drifted_body = {key: value for key, value in drifted.items() if key != "ready_sha256"}
    drifted["ready_sha256"] = subject.canonical_sha256(drifted_body)
    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.validate_artifact_bundle(acceptance, model=model, ready=drifted)
    assert captured.value.reason_code == subject.REASON_READBACK


def test_failure_after_model_write_reports_actual_partial_side_effect() -> None:
    receipt = subject.failure_receipt(
        request={"holdout_evaluation_id": "b" * 64},
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_READBACK, "ready failed", stage="writer"),
        holdout_accessed=True,
        product_acceptance_performed=False,
        model_sha256="1" * 64,
    )

    assert receipt["status"] == "NOT_AVAILABLE"
    assert receipt["model_write"] is True
    assert receipt["model_sha256"] == "1" * 64
    assert receipt["ready_write"] is False
    assert receipt["product_acceptance_performed"] is False


def test_benchmark_returns_reads_authoritative_calendar_manifest_rows() -> None:
    rows = [["2026-01-05", 0.0125], ["2026-01-06", -0.004]]

    assert subject._benchmark_returns({"calendar_benchmark": {"rows": rows}}) == {
        date(2026, 1, 5): 0.0125,
        date(2026, 1, 6): -0.004,
    }


def test_typed_failure_receipt_preserves_message_when_evidence_is_empty() -> None:
    receipt = subject.failure_receipt(
        request={"holdout_evaluation_id": "b" * 64},
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(
            subject.REASON_METRIC,
            "holdout benchmark returns are missing",
            stage="metric",
        ),
        holdout_accessed=True,
    )

    assert receipt["failure_reason_code"] == subject.REASON_METRIC
    assert receipt["failure_stage"] == "metric"
    assert receipt["failure_evidence"] == {
        "exception_type": "HoldoutAcceptanceError",
        "error_message": "holdout benchmark returns are missing",
    }


def test_parent_propagates_valid_child_business_failure_without_reclassifying_it(tmp_path: Path) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request=request,
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(
            subject.REASON_METRIC,
            "holdout benchmark returns are missing",
            stage="metric",
        ),
        holdout_accessed=True,
    )

    error = cli._child_failure_error(
        child_failure,
        request=request,
        producer_commit="a" * 40,
        process_index=1,
        returncode=1,
        failure_path=tmp_path / "child.failure.json",
    )

    assert error.reason_code == subject.REASON_METRIC
    assert error.stage == "metric"
    assert error.evidence["child_failure_reason_code"] == subject.REASON_METRIC
    assert error.evidence["child_failure_evidence"]["error_message"] == "holdout benchmark returns are missing"


def test_parent_rejects_child_failure_receipt_with_mismatched_evaluation_identity(tmp_path: Path) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request={"holdout_evaluation_id": "c" * 64},
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=True,
    )

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._child_failure_error(
            child_failure,
            request=request,
            producer_commit="a" * 40,
            process_index=1,
            returncode=1,
            failure_path=tmp_path / "child.failure.json",
        )

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert captured.value.stage == "fresh_process"


def test_parent_rejects_child_failure_receipt_with_mismatched_producer_commit(tmp_path: Path) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request=request,
        producer_commit="c" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=True,
    )

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._child_failure_error(
            child_failure,
            request=request,
            producer_commit="a" * 40,
            process_index=1,
            returncode=1,
            failure_path=tmp_path / "child.failure.json",
        )

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert captured.value.evidence["invalid_fields"] == ["producer_commit"]


def test_parent_rejects_child_failure_receipt_with_untrusted_observation_flag(tmp_path: Path) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request=request,
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=True,
    )
    child_failure["holdout_accessed"] = "yes"

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._child_failure_error(
            child_failure,
            request=request,
            producer_commit="a" * 40,
            process_index=1,
            returncode=1,
            failure_path=tmp_path / "child.failure.json",
        )

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert captured.value.evidence["invalid_fields"] == ["holdout_accessed"]


def test_parent_rejects_child_failure_receipt_with_missing_observation_field(tmp_path: Path) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request=request,
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=None,
    )
    del child_failure["holdout_accessed"]

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._child_failure_error(
            child_failure,
            request=request,
            producer_commit="a" * 40,
            process_index=1,
            returncode=1,
            failure_path=tmp_path / "child.failure.json",
        )

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert captured.value.evidence["invalid_fields"] == ["holdout_accessed"]


def test_parent_rejects_child_failure_receipt_with_impossible_side_effect_order(tmp_path: Path) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request=request,
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=False,
        product_acceptance_performed=True,
    )

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._child_failure_error(
            child_failure,
            request=request,
            producer_commit="a" * 40,
            process_index=1,
            returncode=1,
            failure_path=tmp_path / "child.failure.json",
        )

    assert captured.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert captured.value.evidence["invalid_fields"] == ["product_acceptance_performed"]


def test_parent_failure_receipt_propagates_child_business_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    child_failure = subject.failure_receipt(
        request=request,
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=True,
    )
    output = tmp_path / "acceptance.json"
    args = cli.argparse.Namespace(
        request=tmp_path / "request.json",
        candidate=tmp_path / "candidate.json",
        output=output,
        model_output=tmp_path / "model.json",
        ready_output=tmp_path / "ready.json",
        child_dir=tmp_path / "children",
        db_env_prefix="P2_4_TEST",
    )
    monkeypatch.setattr(cli, "load_request", lambda *args, **kwargs: request)
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "validate_static_request", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "_validate_cli_outputs", lambda *args, **kwargs: {})
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: cli.subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr=""),
    )
    monkeypatch.setattr(cli, "_read_child_failure", lambda *args, **kwargs: child_failure)

    assert cli._parent(args) == 1

    failure = json.loads((tmp_path / "acceptance.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_METRIC
    assert failure["failure_stage"] == "metric"
    assert failure["holdout_accessed"] is True
    assert failure["product_acceptance_performed"] is False
    assert failure["failure_evidence"]["child_failure_reason_code"] == subject.REASON_METRIC


def test_parent_marks_side_effects_unknown_when_first_child_failure_receipt_is_unreadable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    output = tmp_path / "acceptance.json"
    args = cli.argparse.Namespace(
        request=tmp_path / "request.json",
        candidate=tmp_path / "candidate.json",
        output=output,
        model_output=tmp_path / "model.json",
        ready_output=tmp_path / "ready.json",
        child_dir=tmp_path / "children",
        db_env_prefix="P2_4_TEST",
    )
    monkeypatch.setattr(cli, "load_request", lambda *args, **kwargs: request)
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "validate_static_request", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "_validate_cli_outputs", lambda *args, **kwargs: {})
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: cli.subprocess.CompletedProcess(args=[], returncode=2, stdout="", stderr=""),
    )

    def fail_readback(*args: object, **kwargs: object) -> None:
        raise subject.HoldoutAcceptanceError(subject.REASON_READBACK, "child failure missing", stage="fresh_process")

    monkeypatch.setattr(cli, "_read_child_failure", fail_readback)

    assert cli._parent(args) == 1

    failure = json.loads((tmp_path / "acceptance.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_READBACK
    assert failure["holdout_accessed"] is None
    assert failure["product_acceptance_performed"] is None


def test_parent_preserves_known_first_child_side_effects_when_second_failure_receipt_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = {"holdout_evaluation_id": "b" * 64}
    invalid_child_failure = subject.failure_receipt(
        request={"holdout_evaluation_id": "c" * 64},
        producer_commit="a" * 40,
        error=subject.HoldoutAcceptanceError(subject.REASON_METRIC, "metric unavailable", stage="metric"),
        holdout_accessed=True,
    )
    output = tmp_path / "acceptance.json"
    args = cli.argparse.Namespace(
        request=tmp_path / "request.json",
        candidate=tmp_path / "candidate.json",
        output=output,
        model_output=tmp_path / "model.json",
        ready_output=tmp_path / "ready.json",
        child_dir=tmp_path / "children",
        db_env_prefix="P2_4_TEST",
    )
    completed = iter(
        (
            cli.subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            cli.subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr=""),
        )
    )
    monkeypatch.setattr(cli, "load_request", lambda *args, **kwargs: request)
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "validate_static_request", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "_validate_cli_outputs", lambda *args, **kwargs: {})
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: next(completed))
    monkeypatch.setattr(cli, "_read_child_failure", lambda *args, **kwargs: invalid_child_failure)

    assert cli._parent(args) == 1

    failure = json.loads((tmp_path / "acceptance.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_REPRODUCIBILITY
    assert failure["holdout_accessed"] is True
    assert failure["product_acceptance_performed"] is True


def test_write_once_is_external_collision_safe_and_canonical(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    output = tmp_path / "artifacts" / "receipt.json"
    value = {"status": "NOT_AVAILABLE", "ready": False}

    path = subject.write_once(output, value, repository_root=repository)

    assert json.loads(path.read_text(encoding="utf-8")) == value
    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        subject.write_once(output, value, repository_root=repository)
    assert captured.value.reason_code == subject.REASON_COLLISION
    with pytest.raises(subject.HoldoutAcceptanceError):
        subject.preflight_output(repository / "inside.json", repository_root=repository)


def test_unknown_failure_receipt_never_claims_model_ready_or_runtime() -> None:
    receipt = subject.failure_receipt(
        request={"holdout_evaluation_id": "b" * 64},
        producer_commit="a" * 40,
        error=RuntimeError("boom"),
        holdout_accessed=True,
    )

    assert receipt["failure_reason_code"] == subject.REASON_UNEXPECTED
    assert receipt["failure_evidence"] == {"exception_type": "RuntimeError", "error_message": "boom"}
    assert receipt["model_write"] is False
    assert receipt["ready_write"] is False
    assert receipt["database_write"] is False
    assert receipt["runtime_action"] is False


def test_cli_invalid_candidate_stops_before_database_loader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request_path = tmp_path / "request.json"
    candidate_path = tmp_path / "candidate.json"
    _write_json(request_path, {"schema_version": subject.REQUEST_SCHEMA_VERSION})
    _write_json(candidate_path, {"schema_version": "bad"})
    monkeypatch.setattr(cli, "_load_l1_source_inputs", lambda *args, **kwargs: pytest.fail("DB loader called"))

    result = cli.main(
        [
            "--request",
            str(request_path),
            "--candidate",
            str(candidate_path),
            "--output",
            str(tmp_path / "artifacts" / "acceptance.json"),
            "--model-output",
            str(tmp_path / "artifacts" / "model.json"),
            "--ready-output",
            str(tmp_path / "artifacts" / "ready.json"),
            "--child-dir",
            str(tmp_path / "artifacts" / "children"),
            "--db-env-prefix",
            "P2_4_TEST",
        ]
    )

    assert result == 1
    failure = json.loads((tmp_path / "artifacts" / "acceptance.failure.json").read_text(encoding="utf-8"))
    assert failure["holdout_accessed"] is False
    assert failure["fit_count"] == 0
    assert failure["model_write"] is False


def test_cli_prepares_canonical_request_before_parent_without_product_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs, candidate, expected = _evaluation_fixture()
    holdout_source = expected["holdout_source"]
    assert isinstance(holdout_source, dict)
    tail_end = date.fromisoformat(str(holdout_source["outcome_tail_end"]))
    artifact_root = tmp_path / "artifacts"
    request_path = artifact_root / "request.json"
    candidate_path = tmp_path / "candidate.json"
    _write_json(candidate_path, {"candidate": "patched fixture"})
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: candidate)
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(cli, "_resolve_outcome_tail_end", lambda *args, **kwargs: tail_end)
    monkeypatch.setattr(cli, "_load_l1_source_inputs", lambda *args, **kwargs: inputs)

    result = cli.main(
        [
            "--prepare-request",
            "--request",
            str(request_path),
            "--candidate",
            str(candidate_path),
            "--output",
            str(artifact_root / "acceptance.json"),
            "--model-output",
            str(artifact_root / "model.json"),
            "--ready-output",
            str(artifact_root / "ready.json"),
            "--child-dir",
            str(artifact_root / "children"),
            "--db-env-prefix",
            "P2_4_TEST",
        ]
    )

    assert result == 0
    request = json.loads(request_path.read_text(encoding="utf-8"))
    subject.validate_static_request(request, candidate)
    subject.validate_loaded_source(inputs, request)
    assert request["holdout_source"]["outcome_tail_end"] == tail_end.isoformat()
    assert not (artifact_root / "acceptance.json").exists()
    assert not (artifact_root / "model.json").exists()
    assert not (artifact_root / "ready.json").exists()


def test_request_preparation_resolves_tail_from_calendar_only_and_closes_readonly_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [(subject.HOLDOUT_END + timedelta(days=index),) for index in range(1, 21)]
    observed: dict[str, object] = {}

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

        def fetchall(self) -> list[tuple[date]]:
            return rows

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def rollback(self) -> None:
            observed["rollback"] = True

        def close(self) -> None:
            observed["close"] = True

    monkeypatch.setattr(cli, "_connect_readonly", lambda prefix: (Connection(), {"prefix": prefix}))

    assert cli._resolve_outcome_tail_end("P2_4_TEST") == rows[-1][0]
    assert "SELECT cal_date::date" in str(observed["query"])
    assert "pct_chg" not in str(observed["query"])
    assert observed["params"] == (subject.HOLDOUT_END, subject.OUTCOME_TAIL_TRADING_DAYS)
    assert observed["rollback"] is True
    assert observed["close"] is True


def test_outcome_tail_query_failure_is_typed_and_closes_readonly_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, bool] = {}

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            raise OperationalError("database system is in recovery mode")

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def rollback(self) -> None:
            observed["rollback"] = True

        def close(self) -> None:
            observed["close"] = True

    monkeypatch.setattr(cli, "_connect_readonly", lambda prefix: (Connection(), {"prefix": prefix}))

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._resolve_outcome_tail_end("P2_4_TEST")

    assert captured.value.reason_code == subject.REASON_SOURCE
    assert captured.value.stage == "source_preflight"
    assert captured.value.evidence == {
        "exception_type": "OperationalError",
        "source_reason_code": cli.SOURCE_LOADER_FAILURE,
        "error_message": "database system is in recovery mode",
    }
    assert observed == {"rollback": True, "close": True}


def test_request_preparation_classifies_database_recovery_before_holdout_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, candidate, _ = _evaluation_fixture()
    artifact_root = tmp_path / "artifacts"
    request_path = artifact_root / "request.json"
    candidate_path = tmp_path / "candidate.json"
    _write_json(candidate_path, {"candidate": "patched fixture"})
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: candidate)
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    calls = 0

    def fail_connect(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise OperationalError("database system is in recovery mode")

    monkeypatch.setattr(cli, "_connect_readonly", fail_connect)
    argv = [
        "--prepare-request",
        "--request",
        str(request_path),
        "--candidate",
        str(candidate_path),
        "--output",
        str(artifact_root / "acceptance.json"),
        "--model-output",
        str(artifact_root / "model.json"),
        "--ready-output",
        str(artifact_root / "ready.json"),
        "--child-dir",
        str(artifact_root / "children"),
        "--db-env-prefix",
        "P2_4_TEST",
    ]

    assert cli.main(argv) == 1
    failure = json.loads((artifact_root / "acceptance.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_SOURCE
    assert failure["failure_stage"] == "source_preflight"
    assert failure["failure_evidence"] == {
        "exception_type": "OperationalError",
        "source_reason_code": cli.SOURCE_LOADER_FAILURE,
        "error_message": "database system is in recovery mode",
    }
    assert failure["holdout_accessed"] is False
    assert failure["product_acceptance_performed"] is False
    assert failure["model_write"] is False
    assert failure["ready_write"] is False
    assert not request_path.exists()
    assert calls == 1

    assert cli.main(argv) == 2
    assert calls == 1


def test_cli_request_preparation_failure_records_access_and_blocks_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, candidate, expected = _evaluation_fixture()
    holdout_source = expected["holdout_source"]
    assert isinstance(holdout_source, dict)
    tail_end = date.fromisoformat(str(holdout_source["outcome_tail_end"]))
    artifact_root = tmp_path / "artifacts"
    request_path = artifact_root / "request.json"
    candidate_path = tmp_path / "candidate.json"
    _write_json(candidate_path, {"candidate": "patched fixture"})
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: candidate)
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)
    monkeypatch.setattr(cli, "_resolve_outcome_tail_end", lambda *args, **kwargs: tail_end)
    calls = 0

    def fail_loader(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise StateModelSetError(
            "hmm_risk_stock_fact_provider_absence_unverified: 601969.SH/2026-01-30/market.moneyflow_ts"
        )

    monkeypatch.setattr(cli, "_load_l1_source_inputs", fail_loader)
    argv = [
        "--prepare-request",
        "--request",
        str(request_path),
        "--candidate",
        str(candidate_path),
        "--output",
        str(artifact_root / "acceptance.json"),
        "--model-output",
        str(artifact_root / "model.json"),
        "--ready-output",
        str(artifact_root / "ready.json"),
        "--child-dir",
        str(artifact_root / "children"),
        "--db-env-prefix",
        "P2_4_TEST",
    ]

    assert cli.main(argv) == 1
    failure_path = artifact_root / "acceptance.failure.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert failure["holdout_accessed"] is True
    assert failure["product_acceptance_performed"] is False
    assert failure["fit_count"] == 0
    assert failure["failure_reason_code"] == subject.REASON_SOURCE
    assert failure["failure_stage"] == "source_preflight"
    assert failure["failure_evidence"] == {
        "exception_type": "StateModelSetError",
        "source_reason_code": "hmm_risk_stock_fact_provider_absence_unverified",
        "error_message": ("hmm_risk_stock_fact_provider_absence_unverified: 601969.SH/2026-01-30/market.moneyflow_ts"),
    }
    assert not request_path.exists()
    assert calls == 1

    assert cli.main(argv) == 2
    assert calls == 1


def test_holdout_source_loader_uses_stable_reason_for_untyped_state_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_loader(*args: object, **kwargs: object) -> None:
        raise StateModelSetError("source failed without typed reason")

    monkeypatch.setattr(cli, "_load_l1_source_inputs", fail_loader)

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._load_holdout_inputs({"holdout_source": {"source": {}}}, db_prefix="P2_4_TEST")

    assert captured.value.reason_code == subject.REASON_SOURCE
    assert captured.value.stage == "source_preflight"
    assert captured.value.evidence == {
        "exception_type": "StateModelSetError",
        "source_reason_code": cli.SOURCE_LOADER_FAILURE,
        "error_message": "source failed without typed reason",
    }


def test_holdout_source_loader_classifies_database_recovery_as_source_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_loader(*args: object, **kwargs: object) -> None:
        raise OperationalError("database system is in recovery mode")

    monkeypatch.setattr(cli, "_load_l1_source_inputs", fail_loader)

    with pytest.raises(subject.HoldoutAcceptanceError) as captured:
        cli._load_holdout_inputs({"holdout_source": {"source": {}}}, db_prefix="P2_4_TEST")

    assert captured.value.reason_code == subject.REASON_SOURCE
    assert captured.value.stage == "source_preflight"
    assert captured.value.evidence == {
        "exception_type": "OperationalError",
        "source_reason_code": cli.SOURCE_LOADER_FAILURE,
        "error_message": "database system is in recovery mode",
    }


def test_cli_output_drift_stops_before_holdout_loader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact_root = tmp_path / "artifacts"
    report = _candidate_report()
    request = _request(str(report["report_sha256"]), artifact_root=artifact_root)
    request_path = tmp_path / "request.json"
    candidate_path = tmp_path / "candidate.json"
    _write_json(request_path, request)
    _write_json(candidate_path, report)
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: _frozen(report))
    monkeypatch.setattr(cli, "_load_l1_source_inputs", lambda *args, **kwargs: pytest.fail("DB loader called"))
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)

    result = cli.main(
        [
            "--request",
            str(request_path),
            "--candidate",
            str(candidate_path),
            "--output",
            str(artifact_root / "drifted_acceptance.json"),
            "--model-output",
            str(artifact_root / "model.json"),
            "--ready-output",
            str(artifact_root / "ready.json"),
            "--child-dir",
            str(artifact_root / "children"),
            "--db-env-prefix",
            "P2_4_TEST",
        ]
    )

    assert result == 1
    failure = json.loads((artifact_root / "drifted_acceptance.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_reason_code"] == subject.REASON_REQUEST
    assert failure["holdout_accessed"] is False
    assert failure["product_acceptance_performed"] is False


def test_child_loader_failure_records_holdout_access_without_claiming_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact_root = tmp_path / "artifacts"
    report = _candidate_report()
    request = _request(str(report["report_sha256"]), artifact_root=artifact_root)
    request_path = tmp_path / "request.json"
    candidate_path = tmp_path / "candidate.json"
    _write_json(request_path, request)
    _write_json(candidate_path, report)
    monkeypatch.setattr(cli, "load_frozen_candidate", lambda *args, **kwargs: _frozen(report))
    monkeypatch.setattr(cli, "_producer_commit", lambda: "a" * 40)

    def fail_loader(*args: object, **kwargs: object) -> None:
        raise RuntimeError("load")

    monkeypatch.setattr(cli, "_load_l1_source_inputs", fail_loader)

    result = cli.main(
        [
            "--request",
            str(request_path),
            "--candidate",
            str(candidate_path),
            "--output",
            str(artifact_root / "acceptance.json"),
            "--model-output",
            str(artifact_root / "model.json"),
            "--ready-output",
            str(artifact_root / "ready.json"),
            "--child-dir",
            str(artifact_root / "children"),
            "--db-env-prefix",
            "P2_4_TEST",
            "--child-index",
            "1",
        ]
    )

    assert result == 1
    failure = json.loads((artifact_root / "children" / "p2_4_holdout_child_1.failure.json").read_text(encoding="utf-8"))
    assert failure["holdout_accessed"] is True
    assert failure["product_acceptance_performed"] is False

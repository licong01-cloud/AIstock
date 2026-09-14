from __future__ import annotations

from datetime import date, timedelta
import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk.risk_l1_g2b import (
    CONTRACT_VERSION,
    INPUT_SCHEMA_VERSION,
    REASON_COVERAGE,
    REASON_REPRODUCIBILITY,
    RISK_FEATURES,
    RiskL1G2BError,
    close_processes,
    lightgbm_profile,
    load_model,
    predict_single_date,
    project_risk_levels,
    read_input_bundle,
    run_process,
    validate_input_bundle,
    write_input_bundle,
)
from backend.services.hmm_risk.rotation_l1_gbdt import RotationL1G2AError, build_materialised_panel
from backend.services.hmm_risk.risk_l1_prediction import build_oof_prediction_rows
from backend.services.hmm_risk.state_model_set import canonical_sha256
from scripts.hmm_risk import run_risk_l1_g2b as cli


def _calendar() -> tuple[date, ...]:
    start, end = date(2021, 1, 1), date(2026, 3, 31)
    return tuple(start + timedelta(days=offset) for offset in range((end - start).days + 1) if (start + timedelta(days=offset)).weekday() < 5)


def _bundle() -> dict[str, object]:
    dates = _calendar()
    sectors = tuple(f"80{index:04d}.SI" for index in range(31))
    rows = []
    for day_index, day in enumerate(dates):
        for sector_index, sector in enumerate(sectors):
            event = sector_index >= 25
            row: dict[str, object] = {
                "trade_date": day,
                "sector_code": sector,
                "risk_event_10d": float(event),
                "relative_adverse_excursion_10d": -0.06 if event else -0.01,
                "risk_event_10d_mature": True,
                "reason__risk_event_10d": None,
            }
            for feature_index, feature in enumerate(RISK_FEATURES):
                row[feature] = float(sector_index + feature_index * 0.01 + day_index * 1e-7)
                row[f"reason__{feature}"] = None
            rows.append(row)
    panel = pd.DataFrame.from_records(rows).set_index(["trade_date", "sector_code"]).sort_index()
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "identity": {
            "contract_version": CONTRACT_VERSION,
            "source_sha256": "1" * 64,
            "mapping_sha256": "2" * 64,
            "feature_contract_sha256": "3" * 64,
            "target_contract_sha256": "4" * 64,
        },
        "panel": panel,
    }


class _Booster:
    def model_to_string(self) -> str:
        return "fixed-risk-model"

    def num_trees(self) -> int:
        return 240

    def predict(self, features, raw_score=False, pred_contrib=False):
        first = np.asarray(features.iloc[:, 0], dtype=np.float64)
        probability = np.clip(0.5 + first, 0.01, 0.99)
        raw = np.log(probability / (1.0 - probability))
        if pred_contrib:
            output = np.zeros((len(features), len(RISK_FEATURES) + 1), dtype=np.float64)
            output[:, -1] = raw
            return output
        return raw if raw_score else probability


class _Estimator:
    profiles: list[dict[str, object]] = []

    def __init__(self, **profile):
        self.profiles.append(profile)
        self.booster_ = _Booster()

    def fit(self, features, target):
        assert len(features) == len(target)
        return self

    def predict(self, features, pred_leaf=False):
        if pred_leaf:
            return np.zeros((len(features), 240), dtype=np.int64)
        return np.zeros(len(features), dtype=np.int64)

    def predict_proba(self, features):
        positive = self.booster_.predict(features)
        return np.column_stack((1.0 - positive, positive))


def test_risk_path_label_uses_all_ten_future_days_and_can_mature_beyond_decision_end() -> None:
    days = tuple(date(2026, 1, 1) + timedelta(days=offset) for offset in range(12))
    decisions = days[:2]
    sectors = tuple(f"80{index:04d}.SI" for index in range(31))
    sector_close = {}
    benchmark_close = {day: 100.0 for day in days}
    for sector_index, sector in enumerate(sectors):
        for index, day in enumerate(days):
            value = 100.0
            if sector_index == 0 and index == 3:
                value = 94.0
            sector_close[(day, sector)] = value
    panel = build_materialised_panel(
        calendar=decisions,
        outcome_calendar=days,
        sector_close=sector_close,
        benchmark_close=benchmark_close,
        stock_daily_inputs=[],
        include_targets=False,
        include_risk_target=True,
    )
    assert panel.loc[(decisions[0], sectors[0]), "relative_adverse_excursion_10d"] == pytest.approx(-0.06)
    assert panel.loc[(decisions[0], sectors[0]), "risk_event_10d"] == 1.0
    assert panel.loc[(decisions[0], sectors[1]), "risk_event_10d"] == 0.0
    assert bool(panel.loc[(decisions[0], sectors[0]), "risk_event_10d_mature"]) is True


def test_risk_path_label_fails_closed_on_missing_future_close() -> None:
    days = tuple(date(2026, 1, 1) + timedelta(days=offset) for offset in range(12))
    sectors = tuple(f"80{index:04d}.SI" for index in range(31))
    sector_close = {(day, sector): 100.0 for day in days for sector in sectors}
    del sector_close[(days[5], sectors[0])]
    panel = build_materialised_panel(
        calendar=days[:1],
        outcome_calendar=days,
        sector_close=sector_close,
        benchmark_close={day: 100.0 for day in days},
        stock_daily_inputs=[],
        include_targets=False,
        include_risk_target=True,
    )
    row = panel.loc[(days[0], sectors[0])]
    assert math.isnan(float(row["risk_event_10d"]))
    assert bool(row["risk_event_10d_mature"]) is False
    assert row["reason__risk_event_10d"] == "hmm_risk_risk_l1_target_unavailable"


def test_risk_target_rejects_an_explicit_empty_outcome_calendar() -> None:
    sectors = tuple(f"80{index:04d}.SI" for index in range(31))
    with pytest.raises(RotationL1G2AError, match="outcome calendar"):
        build_materialised_panel(
            calendar=(date(2026, 1, 1),),
            outcome_calendar=(),
            sector_close={(date(2026, 1, 1), sector): 100.0 for sector in sectors},
            benchmark_close={date(2026, 1, 1): 100.0},
            stock_daily_inputs=[],
            include_targets=False,
            include_risk_target=True,
        )


def test_input_bundle_is_immutable_and_order_invariant(tmp_path) -> None:
    bundle = _bundle()
    validate_input_bundle(bundle)
    output = tmp_path / "outside" / "risk-input"
    output.parent.mkdir()
    manifest = write_input_bundle(bundle, output, forbidden_roots=())
    readback = read_input_bundle(output, forbidden_roots=())
    assert readback["manifest"] == manifest
    assert readback["manifest"]["sector_count"] == 31
    shuffled = dict(bundle)
    shuffled["panel"] = bundle["panel"].sample(frac=1.0, random_state=7)
    second = tmp_path / "outside" / "risk-input-shuffled"
    second_manifest = write_input_bundle(shuffled, second, forbidden_roots=())
    assert second_manifest["logical_input_sha256"] == manifest["logical_input_sha256"]


def test_projection_preserves_ties_and_fails_whole_day_below_28() -> None:
    day = date(2026, 1, 2)
    sectors = tuple(f"80{index:04d}.SI" for index in range(31))
    index = pd.MultiIndex.from_product([[day], sectors], names=["trade_date", "sector_code"])
    scores = pd.Series(np.linspace(0.0, 1.0, 31), index=index)
    scores.iloc[-7:] = 0.9
    rows = project_risk_levels(scores, {})
    tied = [row for row in rows if row["risk_score"] == 0.9]
    assert len({row["risk_percentile"] for row in tied}) == 1
    broken = scores.copy()
    broken.iloc[:4] = np.nan
    unavailable = project_risk_levels(broken, {})
    assert all(row["availability"] == "unavailable" for row in unavailable)
    assert {row["reason_code"] for row in unavailable} == {REASON_COVERAGE}


def test_process_runs_exact_six_fits_and_closes_two_fresh_processes() -> None:
    _Estimator.profiles.clear()
    bundle = _bundle()
    def runtime() -> dict[str, str]:
        return {"runtime": "test"}
    first = run_process(
        bundle,
        producer_commit="a" * 40,
        process_index=1,
        estimator_factory=_Estimator,
        runtime_validator=runtime,
    )
    second = run_process(
        bundle,
        producer_commit="a" * 40,
        process_index=2,
        estimator_factory=_Estimator,
        runtime_validator=runtime,
    )
    assert len(_Estimator.profiles) == 12
    assert all(profile == lightgbm_profile() for profile in _Estimator.profiles)
    assert first["reproducibility_payload"]["fit_progress"]["completed"] == 6
    acceptance, model = close_processes(first, second)
    assert acceptance["fit_count"] == 12
    assert acceptance["status"] == "development_complete"
    assert acceptance["research_product_gate"]["effect_threshold_applied"] is False
    assert acceptance["tail_accessed"] is False
    assert acceptance["risk_l1_research_surface_status"] == "NOT_AVAILABLE"
    assert model["model_hash"] == canonical_sha256("fixed-risk-model")
    rows = build_oof_prediction_rows(
        acceptance=acceptance,
        process_reports=(first, second),
        sector_names={code: code for code in _bundle()["panel"].index.get_level_values("sector_code").unique()},
    )
    assert len(rows) == len(first["reproducibility_payload"]["oof_prediction_rows"])
    assert {row["risk_l1_research_surface_status"] for row in rows} == {"NOT_AVAILABLE"}


def test_closure_rejects_different_payloads() -> None:
    def runtime() -> dict[str, str]:
        return {"runtime": "test"}
    first = run_process(
        _bundle(), producer_commit="a" * 40, process_index=1, estimator_factory=_Estimator, runtime_validator=runtime
    )
    second = run_process(
        _bundle(), producer_commit="a" * 40, process_index=2, estimator_factory=_Estimator, runtime_validator=runtime
    )
    second["reproducibility_payload"]["metrics"]["precision"] = 0.0
    with pytest.raises(RiskL1G2BError) as caught:
        close_processes(first, second)
    assert caught.value.reason_code == REASON_REPRODUCIBILITY


def test_runtime_failure_is_mapped_to_the_risk_environment_contract() -> None:
    def broken_runtime():
        raise RuntimeError("wrong threads")

    with pytest.raises(RiskL1G2BError) as caught:
        run_process(
            _bundle(),
            producer_commit="a" * 40,
            process_index=1,
            estimator_factory=_Estimator,
            runtime_validator=broken_runtime,
        )

    assert caught.value.reason_code == "hmm_risk_risk_l1_fit_failed"
    assert caught.value.stage == "environment"


def test_build_input_cli_requests_only_the_explicit_risk_contract(tmp_path, monkeypatch) -> None:
    authority = tmp_path / "authority.json"
    authority.write_text(json.dumps({"authority": "fixed"}), encoding="utf-8")
    captured = {}

    def build(**kwargs):
        captured.update(kwargs)
        return ({"risk_l1_bundle": _bundle()}, {}, {})

    monkeypatch.setattr(cli, "build_rotation_l1_inputs_from_assets", build)
    monkeypatch.setattr(
        cli,
        "write_input_bundle",
        lambda bundle, output_root, forbidden_roots: {"manifest_sha256": "a" * 64},
    )
    result = cli._build_input(
        argparse.Namespace(
            industry_pit_authority=authority,
            candidate_root=tmp_path / "candidate",
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "provider.json",
            output_root=tmp_path / "outside" / "risk-input",
        )
    )

    assert result == 0
    assert captured["risk_l1_contract"] is True
    assert "g2a_contract" not in captured


def _minimal_child(process_index: int) -> dict[str, object]:
    payload = {
        "profile": lightgbm_profile(),
        "input_identity": {
            "source_sha256": "1" * 64,
            "mapping_sha256": "2" * 64,
            "feature_contract_sha256": "3" * 64,
            "target_contract_sha256": "4" * 64,
        },
        "metrics": {"effect_accepted": False},
        "final_model": {"model_sha256": canonical_sha256("model")},
        "research_product_gate": {
            "passed": True,
            "effect_threshold_applied": False,
            "product_readback_pending": True,
        },
        "risk_l1_research_surface_status": "NOT_AVAILABLE",
        "risk_l1_capability_status": "NOT_AVAILABLE",
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
    }
    body = {
        "schema_version": "hmm_risk_risk_l1_g2b_process_v1",
        "process_index": process_index,
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": canonical_sha256(payload),
        "final_model_text": "model",
    }
    return {**body, "report_sha256": canonical_sha256(body)}


def test_parent_cli_persists_two_child_closure_and_typed_failure(tmp_path, monkeypatch) -> None:
    def write_child(command, _failure_path):
        output = Path(command[command.index("--output-file") + 1])
        process_index = int(command[command.index("--process-index") + 1])
        cli._write_once(output, _minimal_child(process_index))

    monkeypatch.setattr(cli, "_run_child", write_child)
    output = tmp_path / "formal-output"
    result = cli._run_parent(
        argparse.Namespace(input_root=tmp_path / "input", output_root=output, producer_commit="a" * 40)
    )

    assert result == 0
    assert (output / "acceptance.json").exists()
    assert (output / "model.json").exists()
    assert json.loads((output / "acceptance.json").read_text(encoding="utf-8"))["fit_count"] == 12

    def fail_child(_command, _failure_path):
        raise RiskL1G2BError("typed_child_failure", "child failed", stage="fit")

    monkeypatch.setattr(cli, "_run_child", fail_child)
    failed_output = tmp_path / "failed-output"
    failed = cli._run_parent(
        argparse.Namespace(input_root=tmp_path / "input", output_root=failed_output, producer_commit="a" * 40)
    )
    assert failed == 2
    failure = json.loads((failed_output / "parent.failure.json").read_text(encoding="utf-8"))
    assert failure["reason_code"] == "typed_child_failure"
    assert failure["tail_accessed"] is False


def test_parent_cli_rejects_an_existing_output_root_without_overwrite(tmp_path) -> None:
    output = tmp_path / "already-exists"
    output.mkdir()

    with pytest.raises(RiskL1G2BError) as caught:
        cli._run_parent(
            argparse.Namespace(input_root=tmp_path / "input", output_root=output, producer_commit="a" * 40)
        )

    assert caught.value.reason_code == "hmm_risk_risk_l1_output_collision"
    assert list(output.iterdir()) == []


def test_single_date_inference_is_zero_fit_and_target_free() -> None:
    bundle = _bundle()
    process = run_process(
        bundle,
        producer_commit="a" * 40,
        process_index=1,
        estimator_factory=_Estimator,
        runtime_validator=lambda: {"runtime": "test"},
    )
    # Rebuild the second report hash after changing only its process identity.
    second_body = {key: value for key, value in process.items() if key != "report_sha256"}
    second_body["process_index"] = 2
    second = {**second_body, "report_sha256": canonical_sha256(second_body)}
    _, artifact = close_processes(process, second)
    loaded = load_model(artifact, booster_factory=lambda model_str: _Booster())
    day = date(2026, 4, 1)
    source = bundle["panel"].xs(_calendar()[-1], level="trade_date").loc[:, list(RISK_FEATURES)]
    rows = predict_single_date(source, trade_date=day, as_of_date=date(2026, 3, 31), model=loaded)
    assert len(rows) == 31
    assert all(row["model_fit_count"] == 0 for row in rows)
    assert all(row["target_columns_read"] is False and row["tail_accessed"] is False for row in rows)

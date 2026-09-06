from __future__ import annotations

from datetime import date
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import rotation_l1_gbdt as subject
from scripts.hmm_risk import run_rotation_l1_g2a as cli


def _test_runtime() -> dict[str, object]:
    return {"test_runtime": True}


def _calendar() -> tuple[date, ...]:
    return tuple(pd.bdate_range("2021-01-01", "2026-03-31").date)


def _bundle(*, missing: bool = False) -> dict[str, object]:
    calendar = _calendar()
    sectors = tuple(f"80{index:04d}" for index in range(31))
    index = pd.MultiIndex.from_product([calendar, sectors], names=["trade_date", "sector_code"])
    day = np.repeat(np.arange(len(calendar), dtype=np.float64), len(sectors))
    rank = np.tile(np.linspace(-1.0, 1.0, len(sectors)), len(calendar))
    data = {}
    for feature_index, feature in enumerate(subject.CONTINUOUS_FEATURES, start=1):
        data[feature] = rank * feature_index + np.sin(day / (9.0 + feature_index))
    data["target_5d"] = rank + 0.8 * np.sin(day / 7.0 + rank * 6.0)
    data["target_10d"] = rank + 0.8 * np.cos(day / 11.0 + rank * 5.0)
    frame = pd.DataFrame(data, index=index)
    for column in subject.VALUE_COLUMNS:
        frame[f"reason__{column}"] = None
    for horizon in subject.HORIZONS:
        frame[f"target_{horizon}d_mature"] = True
    if missing:
        frame.loc[(calendar[-20], sectors[0]), subject.CONTINUOUS_FEATURES[:2]] = np.nan
        for column in subject.CONTINUOUS_FEATURES[:2]:
            frame.loc[(calendar[-20], sectors[0]), f"reason__{column}"] = "test_missing"
    return {
        "schema_version": subject.INPUT_SCHEMA_VERSION,
        "panel": frame,
        "benchmark_close": {item: 100.0 + index + 2.0 * np.sin(index / 5.0) for index, item in enumerate(calendar)},
        "identity": {
            "source_sha256": "a" * 64,
            "mapping_sha256": "b" * 64,
            "feature_contract_sha256": "c" * 64,
            "development_end": "2026-03-31",
            "source_cutoff": "2026-08-31",
            "tail_mature_decision_counts": {"5": 99, "10": 94},
            "tail_mature_date_sha256": "d" * 64,
        },
    }


def _battery_report(*, selected_horizon: int = 10, power_status: str = "INSUFFICIENT") -> dict[str, object]:
    bundle = _bundle()
    market_receipts = []
    for index in range(5):
        receipt_body = {
            "schema_version": "hmm_risk_rotation_l1_market_context_a_v1",
            "fold_index": index,
            "target_accessed": False,
        }
        market_receipts.append({**receipt_body, "receipt_sha256": subject.canonical_sha256(receipt_body)})
    horizons = {}
    for horizon in subject.HORIZONS:
        horizons[str(horizon)] = {
            "forward_power": {
                "status": power_status,
                "tail_mature_decision_count": bundle["identity"]["tail_mature_decision_counts"][str(horizon)],
                "tail_outcome_accessed": False,
            }
        }
    body = {
        "schema_version": "hmm_risk_rotation_l1_g2a_battery_v1",
        "contract_version": subject.CONTRACT_VERSION,
        "runtime_identity": {"test_runtime": True},
        "producer_commit": "e" * 40,
        "input_identity": bundle["identity"],
        "fit_count": 15,
        "ridge_fit_count": 10,
        "market_fit_count": 5,
        "market_context_receipts": market_receipts,
        "horizons": horizons,
        "selection": {
            "selected_horizon": selected_horizon,
            "model_class": "RIDGE_COMPARATOR",
            "gbdt_horizon_optimality_not_claimed": True,
        },
        "tail_accessed": False,
        "model_write_performed": False,
        "database_write_performed": False,
    }
    return {**body, "receipt_sha256": subject.canonical_sha256(body)}


def test_materialised_panel_uses_t_minus_one_features_and_future_only_for_target() -> None:
    calendar = tuple(pd.bdate_range("2025-01-02", periods=90).date)
    sectors = tuple(f"80{index:04d}" for index in range(31))
    sector_close = {
        (day, sector): 100.0 + day_index + sector_index
        for day_index, day in enumerate(calendar)
        for sector_index, sector in enumerate(sectors)
    }
    benchmark = {day: 200.0 + index for index, day in enumerate(calendar)}
    stock = [
        {
            "source_date": day,
            "sector_code": sector,
            "pit_breadth_above_ma20": 0.75,
            "moneyflow_net_amount_cny": 10.0,
            "moneyflow_traded_amount_cny": 100.0,
        }
        for day in calendar
        for sector in sectors
    ]
    panel = subject.build_materialised_panel(
        calendar=calendar,
        sector_close=sector_close,
        benchmark_close=benchmark,
        stock_daily_inputs=stock,
    )
    decision = calendar[70]
    row = panel.loc[(decision, sectors[0])]
    assert row["pit_breadth_above_ma20"] == 0.75
    assert row["moneyflow_intensity_20d"] == pytest.approx(0.1)
    assert np.isfinite(row[list(subject.CONTINUOUS_FEATURES)].to_numpy(dtype=np.float64)).all()
    assert all(row[f"reason__{feature}"] is None for feature in subject.CONTINUOUS_FEATURES)
    assert bool(row["target_5d_mature"])
    original_feature = row["relative_momentum_5d"]
    changed_close = dict(sector_close)
    changed_close[(decision, sectors[0])] *= 2.0
    changed = subject.build_materialised_panel(
        calendar=calendar,
        sector_close=changed_close,
        benchmark_close=benchmark,
        stock_daily_inputs=stock,
    )
    assert changed.loc[(decision, sectors[0]), "relative_momentum_5d"] == original_feature
    assert changed.loc[(decision, sectors[0]), "target_5d"] != row["target_5d"]


def test_materialised_panel_preserves_missing_moneyflow_and_market_context() -> None:
    calendar = tuple(pd.bdate_range("2025-01-02", periods=90).date)
    sectors = tuple(f"80{index:04d}" for index in range(31))
    sector_close = {(day, sector): 100.0 + index for index, day in enumerate(calendar) for sector in sectors}
    benchmark = {day: 200.0 + index for index, day in enumerate(calendar)}
    stock = [
        {
            "source_date": day,
            "sector_code": sector,
            "pit_breadth_above_ma20": 0.5,
            "moneyflow_net_amount_cny": (None if day == calendar[55] and sector == sectors[0] else 1.0),
            "moneyflow_traded_amount_cny": 10.0,
        }
        for day in calendar
        for sector in sectors
    ]
    panel = subject.build_materialised_panel(
        calendar=calendar,
        sector_close=sector_close,
        benchmark_close=benchmark,
        stock_daily_inputs=stock,
    )
    assert np.isnan(panel.loc[(calendar[60], sectors[0]), "moneyflow_intensity_20d"])
    assert panel.loc[(calendar[60], sectors[0]), "reason__moneyflow_intensity_20d"]


def test_validate_input_bundle_rejects_unknown_schema_and_partial_denominator() -> None:
    bundle = _bundle()
    bad = dict(bundle)
    bad["schema_version"] = "future"
    with pytest.raises(subject.RotationL1G2AError, match="envelope"):
        subject.validate_input_bundle(bad)
    partial = dict(bundle)
    partial["panel"] = bundle["panel"].iloc[1:]
    with pytest.raises(subject.RotationL1G2AError, match="denominator"):
        subject.validate_input_bundle(partial)


def test_validate_input_bundle_rejects_nan_without_reason_and_maturity_drift() -> None:
    bundle = _bundle(missing=True)
    missing_reason = dict(bundle)
    missing_reason["panel"] = bundle["panel"].copy()
    identity = missing_reason["panel"].index[-20 * 31]
    feature = subject.CONTINUOUS_FEATURES[0]
    missing_reason["panel"].loc[identity, f"reason__{feature}"] = None
    with pytest.raises(subject.RotationL1G2AError, match="validity/reason"):
        subject.validate_input_bundle(missing_reason)

    maturity_drift = dict(bundle)
    maturity_drift["panel"] = bundle["panel"].copy()
    maturity_drift["panel"].loc[identity, "target_5d_mature"] = False
    with pytest.raises(subject.RotationL1G2AError, match="maturity/value"):
        subject.validate_input_bundle(maturity_drift)


def test_input_bundle_write_readback_is_immutable_and_hash_guarded(tmp_path) -> None:
    output = tmp_path / "g2a-bundle"
    manifest = subject.write_input_bundle(_bundle(), output, forbidden_roots=())
    readback = subject.read_input_bundle(output, forbidden_roots=())
    assert manifest["manifest_sha256"] == readback["manifest"]["manifest_sha256"]
    with pytest.raises(subject.RotationL1G2AError, match="already exists"):
        subject.write_input_bundle(_bundle(), output, forbidden_roots=())
    panel_path = output / "panel.h5"
    panel_path.write_bytes(panel_path.read_bytes() + b"drift")
    with pytest.raises(subject.RotationL1G2AError, match="identity differs"):
        subject.read_input_bundle(output, forbidden_roots=())


def test_fold_slices_are_504_days_and_horizon_purged() -> None:
    _frame, calendar, _sectors, _benchmark = subject.validate_input_bundle(_bundle())
    for horizon in subject.HORIZONS:
        folds = subject.fold_slices(calendar, horizon=horizon)
        assert len(folds) == 5
        assert all(len(item.train_dates) == 504 for item in folds)
        assert all(len(item.purge_dates) == horizon for item in folds)
        assert all(max(item.train_dates) < min(item.purge_dates) < min(item.validation_dates) for item in folds)


def test_market_context_features_use_only_t_minus_one_and_three_prior_returns() -> None:
    calendar = tuple(pd.bdate_range("2025-01-02", periods=20).date)
    close = {day: 100.0 + index + np.sin(index) for index, day in enumerate(calendar)}
    raw = subject._market_raw_features(close, calendar)
    decision = calendar[10]
    before = raw.loc[decision].copy()
    changed = dict(close)
    changed[decision] *= 10.0
    after = subject._market_raw_features(changed, calendar).loc[decision]
    assert after.to_dict() == before.to_dict()
    expected_return = close[calendar[9]] / close[calendar[8]] - 1.0
    expected_volatility = np.std(
        [close[calendar[item]] / close[calendar[item - 1]] - 1.0 for item in (7, 8, 9)], ddof=0
    )
    assert before["daily_return"] == pytest.approx(expected_return)
    assert before["volatility_3d"] == pytest.approx(expected_volatility)


def test_cross_section_rank_preserves_nan_and_market_sign() -> None:
    frame, calendar, _sectors, _benchmark = subject.validate_input_bundle(_bundle(missing=True))
    ranked = subject.cross_section_rank_features(frame)
    assert int(ranked.loc[:, list(subject.CONTINUOUS_FEATURES)].isna().sum().sum()) == 2
    finite = ranked[subject.CONTINUOUS_FEATURES[2]].dropna()
    assert finite.between(-0.5, 0.5).all()
    first_day = calendar[0]
    first_cross_section = ranked.loc[(first_day, slice(None)), subject.CONTINUOUS_FEATURES[2]]
    assert float(first_cross_section.min()) == pytest.approx(-0.5)
    assert float(first_cross_section.max()) == pytest.approx(0.5)


def test_ridge_battery_uses_both_horizons_without_tail_or_model_write() -> None:
    report = subject.run_ridge_battery(_bundle(), producer_commit="e" * 40, runtime_validator=_test_runtime)
    assert report["fit_count"] == 15
    assert report["market_fit_count"] == 5
    assert set(report["horizons"]) == {"5", "10"}
    assert report["selection"]["selected_horizon"] in {5, 10}
    assert report["selection"]["model_class"] == "RIDGE_COMPARATOR"
    assert report["tail_accessed"] is False
    assert report["model_write_performed"] is False
    assert report["producer_commit"] == "e" * 40
    assert all(
        report["horizons"][str(horizon)]["forward_power"]["status"] in {"INSUFFICIENT", "SUFFICIENT", "UNAVAILABLE"}
        for horizon in subject.HORIZONS
    )


def test_battery_readback_rejects_power_or_producer_identity_drift() -> None:
    report = _battery_report()
    report["horizons"]["10"]["forward_power"]["status"] = "PENDING_INSUFFICIENT_POWER"
    body = {key: value for key, value in report.items() if key != "receipt_sha256"}
    report["receipt_sha256"] = subject.canonical_sha256(body)
    with pytest.raises(subject.RotationL1G2AError, match="power receipt"):
        subject.validate_battery_report(report, expected_identity=_bundle()["identity"])


def test_formal_runtime_fails_closed_before_fit_when_thread_environment_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(subject.SINGLE_THREAD_ENVIRONMENT[0], raising=False)
    with pytest.raises(subject.RotationL1G2AError, match="one thread") as caught:
        subject.require_formal_runtime()
    assert caught.value.reason_code == subject.REASON_FIT


def test_cli_child_failure_persists_typed_receipt(tmp_path) -> None:
    output = tmp_path / "battery.json"
    assert (
        cli.main(
            [
                "battery-child",
                "--input-root",
                str(tmp_path / "missing-input"),
                "--output-file",
                str(output),
                "--producer-commit",
                "e" * 40,
            ]
        )
        == 2
    )
    failure = json.loads((tmp_path / "battery.failure.json").read_text(encoding="utf-8"))
    assert failure["reason_code"] == subject.REASON_INPUT
    assert failure["fit_success_claimed"] is False
    assert failure["tail_accessed"] is False


class _FakeBooster:
    def model_to_string(self) -> str:
        return "fixed-model"


class _FakeEstimator:
    def __init__(self, **kwargs: object):
        self.kwargs = kwargs
        self.booster_ = _FakeBooster()

    def fit(self, features: pd.DataFrame, target: pd.Series) -> "_FakeEstimator":
        self._mean = float(target.mean())
        return self

    def predict(self, features: pd.DataFrame, pred_leaf: bool = False, pred_contrib: bool = False) -> np.ndarray:
        score = features[subject.CONTINUOUS_FEATURES[0]].fillna(0.0).to_numpy(dtype=np.float64) + self._mean
        if pred_leaf:
            # Seven leaves, each spanning all dates in this synthetic rank panel.
            leaf = np.floor((features[subject.CONTINUOUS_FEATURES[0]].to_numpy() + 0.5) * 7).clip(0, 6)
            return np.tile(leaf.reshape(-1, 1), (1, 240))
        if pred_contrib:
            result = np.zeros((len(features), len(subject.FEATURES) + 1), dtype=np.float64)
            result[:, 0] = features[subject.CONTINUOUS_FEATURES[0]].fillna(0.0)
            result[:, -1] = self._mean
            return result
        return score


def test_gbdt_process_enforces_profile_and_closes_two_identical_processes() -> None:
    first = subject.run_gbdt_process(
        _bundle(),
        battery_report=_battery_report(),
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        battery_report=_battery_report(),
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    assert first["reproducibility_payload"]["fit_count"] == 12
    assert first["reproducibility_payload"]["profile"]["n_estimators"] == 240
    assert first["reproducibility_payload_sha256"] == second["reproducibility_payload_sha256"]
    acceptance = subject.close_processes(first, second)
    assert acceptance["research_surface_status"] == "AVAILABLE_EXPERIMENTAL"
    assert acceptance["forward_power_status"] == "INSUFFICIENT"
    assert acceptance["tail_accessed"] is False


def test_gbdt_process_fails_closed_on_leaf_date_collapse() -> None:
    class Collapsed(_FakeEstimator):
        def predict(self, features: pd.DataFrame, pred_leaf: bool = False, pred_contrib: bool = False) -> np.ndarray:
            if pred_leaf:
                days = pd.Index(features.index.get_level_values("trade_date")).factorize()[0]
                return np.tile(days.reshape(-1, 1), (1, 240))
            return super().predict(features, pred_leaf=pred_leaf, pred_contrib=pred_contrib)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.run_gbdt_process(
            _bundle(),
            battery_report=_battery_report(),
            process_index=1,
            estimator_factory=Collapsed,
            runtime_validator=_test_runtime,
        )
    assert caught.value.reason_code == subject.REASON_LEAF


def test_state_projection_keeps_boundary_tie_neutral_without_index_fallback() -> None:
    day = date(2026, 1, 5)
    sectors = [f"80{index:04d}" for index in range(31)]
    values = np.arange(31, dtype=np.float64)
    values[5:8] = values[5]
    scores = pd.Series(
        values,
        index=pd.MultiIndex.from_product([[day], sectors], names=["trade_date", "sector_code"]),
    )
    states, receipt = subject.project_states(scores)
    assert all(states[(day, sectors[index])] == "neutral" for index in range(5, 8))
    assert receipt["daily"][0]["state_counts"]["fading"] == 5
    assert receipt["daily"][0]["spread_available"] is True


def test_close_processes_rejects_different_payload_hashes() -> None:
    first = subject.run_gbdt_process(
        _bundle(),
        battery_report=_battery_report(),
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        battery_report=_battery_report(),
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second["reproducibility_payload_sha256"] = "f" * 64
    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(first, second)
    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY

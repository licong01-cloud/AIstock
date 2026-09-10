from __future__ import annotations

import copy
from datetime import date
import json
from pathlib import Path
import sys
from types import SimpleNamespace

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
    for feature_index, feature in enumerate(subject.V14_CONTINUOUS_FEATURES, start=1):
        data[feature] = rank * feature_index + np.sin(day / (9.0 + feature_index))
    data["target_5d"] = rank + 0.8 * np.sin(day / 7.0 + rank * 6.0)
    data["target_10d"] = rank + 0.8 * np.cos(day / 11.0 + rank * 5.0)
    frame = pd.DataFrame(data, index=index)
    for column in subject.VALUE_COLUMNS:
        frame[f"reason__{column}"] = None
    for horizon in subject.HORIZONS:
        frame[f"target_{horizon}d_mature"] = True
    if missing:
        frame.loc[(calendar[-20], sectors[0]), subject.V14_CONTINUOUS_FEATURES[:2]] = np.nan
        for column in subject.V14_CONTINUOUS_FEATURES[:2]:
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
    assert row["moneyflow_intensity_delta_5d"] == pytest.approx(0.0)
    assert np.isfinite(row[list(subject.V14_CONTINUOUS_FEATURES)].to_numpy(dtype=np.float64)).all()
    assert all(row[f"reason__{feature}"] is None for feature in subject.V14_CONTINUOUS_FEATURES)
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
            "moneyflow_reason_code": (
                "hmm_risk_rotation_provider_absence" if day == calendar[55] and sector == sectors[0] else None
            ),
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
    assert np.isnan(panel.loc[(calendar[60], sectors[0]), "moneyflow_intensity_delta_5d"])
    assert (
        panel.loc[(calendar[60], sectors[0]), "reason__moneyflow_intensity_delta_5d"]
        == "hmm_risk_rotation_provider_absence"
    )


def test_moneyflow_delta_uses_canonical_t_minus_one_and_t_minus_six_endpoints() -> None:
    calendar = tuple(pd.bdate_range("2025-01-02", periods=50).date)
    sectors = tuple(f"80{index:04d}" for index in range(31))
    sector_close = {(day, sector): 100.0 + index for index, day in enumerate(calendar) for sector in sectors}
    benchmark = {day: 200.0 + index for index, day in enumerate(calendar)}
    stock = [
        {
            "source_date": day,
            "sector_code": sector,
            "pit_breadth_above_ma20": 0.5,
            "moneyflow_net_amount_cny": float(day_index + 1),
            "moneyflow_traded_amount_cny": 100.0,
        }
        for day_index, day in enumerate(calendar)
        for sector in sectors
    ]
    panel = subject.build_materialised_panel(
        calendar=calendar,
        sector_close=sector_close,
        benchmark_close=benchmark,
        stock_daily_inputs=stock,
    )
    decision_index = 35
    expected_current = sum(range(16, 36)) / 2000.0
    expected_lagged = sum(range(11, 31)) / 2000.0
    assert panel.loc[(calendar[decision_index], sectors[0]), "moneyflow_intensity_delta_5d"] == pytest.approx(
        expected_current - expected_lagged
    )

    mutated = [dict(item) for item in stock]
    for item in mutated:
        if item["source_date"] == calendar[decision_index]:
            item["moneyflow_net_amount_cny"] = 1e9
    changed = subject.build_materialised_panel(
        calendar=calendar,
        sector_close=sector_close,
        benchmark_close=benchmark,
        stock_daily_inputs=mutated,
    )
    assert (
        changed.loc[(calendar[decision_index], sectors[0]), "moneyflow_intensity_delta_5d"]
        == panel.loc[(calendar[decision_index], sectors[0]), "moneyflow_intensity_delta_5d"]
    )


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


def test_validate_input_bundle_rejects_v13_identity_and_low_delta_coverage() -> None:
    stale = dict(_bundle())
    stale["schema_version"] = "hmm_risk_rotation_l1_g2a_input_bundle_v1"
    with pytest.raises(subject.RotationL1G2AError, match="envelope"):
        subject.validate_input_bundle(stale)

    insufficient = dict(_bundle())
    insufficient["panel"] = insufficient["panel"].copy()
    dates = _calendar()[:160]
    insufficient["panel"].loc[(list(dates), slice(None)), "moneyflow_intensity_delta_5d"] = np.nan
    insufficient["panel"].loc[(list(dates), slice(None)), "reason__moneyflow_intensity_delta_5d"] = (
        "hmm_risk_rotation_moneyflow_history_incomplete"
    )
    with pytest.raises(subject.RotationL1G2AError, match="moneyflow delta coverage") as caught:
        subject.validate_input_bundle(insufficient)
    assert caught.value.reason_code == subject.REASON_FEATURE


def test_v14_gbdt_row_requires_market_and_eight_of_nine_continuous_features() -> None:
    frame, _calendar_value, _sectors, _benchmark = subject.validate_input_bundle(_bundle())
    ranked = subject.cross_section_rank_features(frame, continuous_features=subject.V14_CONTINUOUS_FEATURES)
    ranked["market_regime_sign"] = 1.0
    identity = ranked.index[0]
    ranked.loc[identity, subject.V14_CONTINUOUS_FEATURES[:1]] = np.nan
    assert bool(
        subject._eligible_rows(
            ranked,
            ridge=False,
            continuous_features=subject.V14_CONTINUOUS_FEATURES,
            minimum_valid_continuous_features=8,
        ).loc[identity]
    )
    ranked.loc[identity, subject.V14_CONTINUOUS_FEATURES[1:2]] = np.nan
    assert not bool(
        subject._eligible_rows(
            ranked,
            ridge=False,
            continuous_features=subject.V14_CONTINUOUS_FEATURES,
            minimum_valid_continuous_features=8,
        ).loc[identity]
    )


def test_validate_input_bundle_rejects_nan_without_reason_and_maturity_drift() -> None:
    bundle = _bundle(missing=True)
    missing_reason = dict(bundle)
    missing_reason["panel"] = bundle["panel"].copy()
    identity = missing_reason["panel"].index[-20 * 31]
    feature = subject.V14_CONTINUOUS_FEATURES[0]
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
    first_day = calendar[0]
    frame.loc[(first_day, slice(None)), "moneyflow_intensity_delta_5d"] = np.arange(
        subject.CANONICAL_SECTOR_COUNT, dtype=np.float64
    )
    ranked = subject.cross_section_rank_features(frame, continuous_features=subject.V14_CONTINUOUS_FEATURES)
    assert int(ranked.loc[:, list(subject.V14_CONTINUOUS_FEATURES)].isna().sum().sum()) == 2
    finite = ranked[subject.V14_CONTINUOUS_FEATURES[2]].dropna()
    assert finite.between(-0.5, 0.5).all()
    first_cross_section = ranked.loc[(first_day, slice(None)), subject.V14_CONTINUOUS_FEATURES[2]]
    assert float(first_cross_section.min()) == pytest.approx(-0.5)
    assert float(first_cross_section.max()) == pytest.approx(0.5)
    delta_cross_section = ranked.loc[(first_day, slice(None)), "moneyflow_intensity_delta_5d"]
    assert float(delta_cross_section.min()) == pytest.approx(-0.5)
    assert float(delta_cross_section.max()) == pytest.approx(0.5)


def test_v14_forbids_a_new_ridge_battery() -> None:
    with pytest.raises(subject.RotationL1G2AError, match="forbids a new battery") as caught:
        subject.run_ridge_battery(_bundle(), producer_commit="e" * 40, runtime_validator=_test_runtime)
    assert caught.value.reason_code == subject.REASON_HORIZON


def test_v14_cli_has_no_battery_subcommand_and_plans_exactly_24_fits() -> None:
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["battery-child"])
    assert cli._parent_fit_progress(Path("missing"))["planned"] == 24


def test_v14_parent_validates_frozen_v13_reference_before_creating_output(tmp_path, monkeypatch) -> None:
    invalid_reference = tmp_path / "invalid-v13-process.json"
    invalid_reference.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "_ensure_external_new_directory",
        lambda _path: pytest.fail("output must not be created before v1.3 reference validation"),
    )
    args = SimpleNamespace(
        v13_process_file=invalid_reference,
        output_root=tmp_path / "output",
        input_root=tmp_path / "input",
        producer_commit="e" * 40,
    )

    with pytest.raises(subject.RotationL1G2AError) as caught:
        cli._run_parent(args)

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY


def test_formal_runtime_fails_closed_before_fit_when_thread_environment_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(subject.SINGLE_THREAD_ENVIRONMENT[0], raising=False)
    with pytest.raises(subject.RotationL1G2AError, match="one thread") as caught:
        subject.require_formal_runtime()
    assert caught.value.reason_code == subject.REASON_FIT


def test_cli_child_failure_persists_typed_receipt(tmp_path) -> None:
    output = tmp_path / "fresh_process_1.json"
    assert (
        cli.main(
            [
                "model-child",
                "--input-root",
                str(tmp_path / "missing-input"),
                "--output-file",
                str(output),
                "--process-index",
                "1",
                "--producer-commit",
                "e" * 40,
            ]
        )
        == 2
    )
    failure = json.loads((tmp_path / "fresh_process_1.failure.json").read_text(encoding="utf-8"))
    assert failure["reason_code"] == subject.REASON_INPUT
    assert failure["contract_version"] == subject.CONTRACT_VERSION
    assert failure["fit_success_claimed"] is False
    assert failure["tail_accessed"] is False
    body = {key: value for key, value in failure.items() if key != "failure_sha256"}
    assert failure["failure_sha256"] == subject.canonical_sha256(body)


def test_parent_fit_progress_aggregates_success_failure_and_not_started(tmp_path) -> None:
    child_error = subject.RotationL1G2AError(
        subject.REASON_LEAF,
        "leaf failed",
        stage="leaf_date_coverage",
        evidence={
            "fit_progress": {
                "planned": 12,
                "started": 2,
                "completed": 2,
                "failed": 0,
                "active_fit": None,
            }
        },
    )
    cli._write_once(
        tmp_path / "fresh_process_1.failure.json",
        cli._failure(child_error, stage="model-child"),
    )

    progress = cli._parent_fit_progress(tmp_path)

    assert progress["planned"] == 24
    assert progress["started"] == 2
    assert progress["completed"] == 2
    assert progress["failed"] == 0
    assert [item["status"] for item in progress["components"]] == ["failed", "not_started"]
    assert [item["readback_valid"] for item in progress["components"]] == [True, True]


def test_parent_rejects_tampered_child_failure_receipt(tmp_path) -> None:
    path = tmp_path / "fresh_process_1.failure.json"
    failure = cli._failure(RuntimeError("original"), stage="fresh_process")
    failure["message"] = "tampered"
    cli._write_once(path, failure)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        cli._run_child([sys.executable, "-c", "raise SystemExit(2)"], path)

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert caught.value.stage == "fresh_process_readback"


def test_parent_rejects_rehashed_failure_from_stale_contract(tmp_path) -> None:
    path = tmp_path / "fresh_process_1.failure.json"
    failure = cli._failure(RuntimeError("original"), stage="fresh_process")
    failure["contract_version"] = "hmm_risk_rotation_l1_g2a_v1_2"
    body = {key: value for key, value in failure.items() if key != "failure_sha256"}
    failure["failure_sha256"] = subject.canonical_sha256(body)
    cli._write_once(path, failure)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        cli._run_child([sys.executable, "-c", "raise SystemExit(2)"], path)

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert caught.value.stage == "fresh_process_readback"


class _FakeBooster:
    def model_to_string(self) -> str:
        return "fixed-model"


class _FakeEstimator:
    def __init__(self, **kwargs: object):
        self.kwargs = kwargs
        self.booster_ = _FakeBooster()

    def fit(self, features: pd.DataFrame, target: pd.Series) -> "_FakeEstimator":
        assert tuple(features.columns) == subject.V14_FEATURES
        ranked_values = features.loc[:, list(subject.V14_CONTINUOUS_FEATURES)].to_numpy(dtype=np.float64)
        assert np.nanmin(ranked_values) >= -0.5
        assert np.nanmax(ranked_values) <= 0.5
        self._mean = float(target.mean())
        return self

    def predict(self, features: pd.DataFrame, pred_leaf: bool = False, pred_contrib: bool = False) -> np.ndarray:
        score = features[subject.V14_CONTINUOUS_FEATURES[0]].fillna(0.0).to_numpy(dtype=np.float64) + self._mean
        if pred_leaf:
            # Seven leaves, each spanning all dates in this synthetic rank panel.
            leaf = np.floor((features[subject.V14_CONTINUOUS_FEATURES[0]].to_numpy() + 0.5) * 7).clip(0, 6)
            return np.tile(leaf.reshape(-1, 1), (1, 240))
        if pred_contrib:
            result = np.zeros((len(features), len(subject.V14_FEATURES) + 1), dtype=np.float64)
            result[:, 0] = features[subject.V14_CONTINUOUS_FEATURES[0]].fillna(0.0)
            result[:, -1] = self._mean
            return result
        return score


class _CapturingEstimator(_FakeEstimator):
    fitted_targets: list[np.ndarray] = []

    def fit(self, features: pd.DataFrame, target: pd.Series) -> "_CapturingEstimator":
        type(self).fitted_targets.append(target.to_numpy(dtype=np.float64, copy=True))
        super().fit(features, target)
        return self


def _as_v13_reference(child: dict[str, object]) -> dict[str, object]:
    legacy = copy.deepcopy(child)
    payload = legacy["reproducibility_payload"]
    payload["contract_version"] = subject.V13_CONTRACT_VERSION
    payload.pop("horizon_authority")
    payload.pop("horizon_authority_sha256")
    payload.pop("delta_feature_coverage")
    payload["battery_receipt_sha256"] = "f" * 64
    payload["input_identity"]["feature_contract_sha256"] = "9" * 64
    for fold in payload["folds"]:
        fold["feature_contributions"]["shape"][1] = len(subject.V13_FEATURES) + 1
    for row in payload["oof_prediction_rows"]:
        if row["feature_contributions"] is not None:
            row["feature_contributions"].pop(8)
    payload["oof_prediction_rows_sha256"] = subject.canonical_sha256(payload["oof_prediction_rows"])
    legacy["schema_version"] = subject.V13_PROCESS_SCHEMA_VERSION
    legacy["reproducibility_payload_sha256"] = subject.canonical_sha256(payload)
    legacy["report_sha256"] = subject.canonical_sha256(
        {key: value for key, value in legacy.items() if key != "report_sha256"}
    )
    return legacy


def test_v15_rank_target_uses_full_daily_cross_section_and_average_ties() -> None:
    day = date(2025, 1, 2)
    sectors = tuple(f"80{index:04d}" for index in range(subject.CANONICAL_SECTOR_COUNT))
    index = pd.MultiIndex.from_product([[day], sectors], names=["trade_date", "sector_code"])
    raw = pd.Series(np.arange(subject.CANONICAL_SECTOR_COUNT, dtype=np.float64), index=index)
    raw.iloc[10:12] = 10.0

    labels, receipt = subject.build_rank_training_target(raw)

    assert labels.iloc[0] == pytest.approx(-0.5)
    assert labels.iloc[-1] == pytest.approx(0.5)
    assert labels.iloc[10] == labels.iloc[11] == pytest.approx((10.5 / 30.0) - 0.5)
    assert receipt["transform"] == subject.V15_TARGET_TRANSFORM
    assert receipt["sector_count"] == 31
    assert receipt["date_count"] == 1


def test_v15_rank_target_rejects_partial_or_non_finite_daily_target() -> None:
    day = date(2025, 1, 2)
    sectors = tuple(f"80{index:04d}" for index in range(subject.CANONICAL_SECTOR_COUNT))
    index = pd.MultiIndex.from_product([[day], sectors], names=["trade_date", "sector_code"])
    raw = pd.Series(np.arange(subject.CANONICAL_SECTOR_COUNT, dtype=np.float64), index=index)

    with pytest.raises(subject.RotationL1G2AError) as partial:
        subject.build_rank_training_target(raw.iloc[:-1])
    assert partial.value.reason_code == subject.REASON_LABEL

    raw.iloc[3] = np.nan
    with pytest.raises(subject.RotationL1G2AError) as non_finite:
        subject.build_rank_training_target(raw)
    assert non_finite.value.reason_code == subject.REASON_LABEL


def test_v15_rank_target_all_equal_cross_section_is_neutral_without_fallback() -> None:
    day = date(2025, 1, 2)
    sectors = tuple(f"80{index:04d}" for index in range(subject.CANONICAL_SECTOR_COUNT))
    index = pd.MultiIndex.from_product([[day], sectors], names=["trade_date", "sector_code"])

    labels, receipt = subject.build_rank_training_target(pd.Series(3.0, index=index))

    assert labels.eq(0.0).all()
    assert receipt["minimum"] == receipt["maximum"] == 0.0


def test_v15_rank_target_rejects_cross_date_sector_identity_drift() -> None:
    days = (date(2025, 1, 2), date(2025, 1, 3))
    sectors = tuple(f"80{index:04d}" for index in range(subject.CANONICAL_SECTOR_COUNT))
    identities = [(days[0], code) for code in sectors]
    identities.extend((days[1], code) for code in (*sectors[:-1], "809999"))
    index = pd.MultiIndex.from_tuples(identities, names=["trade_date", "sector_code"])

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.build_rank_training_target(pd.Series(np.arange(len(index), dtype=np.float64), index=index))

    assert caught.value.reason_code == subject.REASON_LABEL


def test_v15_rank_target_canonical_receipt_is_invariant_to_input_row_order() -> None:
    days = (date(2025, 1, 2), date(2025, 1, 3))
    sectors = tuple(f"80{index:04d}" for index in range(subject.CANONICAL_SECTOR_COUNT))
    index = pd.MultiIndex.from_product([days, sectors], names=["trade_date", "sector_code"])
    raw = pd.Series(np.arange(len(index), dtype=np.float64), index=index)
    reordered = raw.sample(frac=1.0, random_state=42)

    expected_labels, expected_receipt = subject.build_rank_training_target(raw)
    actual_labels, actual_receipt = subject.build_rank_training_target(reordered)

    pd.testing.assert_series_equal(actual_labels, expected_labels)
    assert actual_receipt == expected_receipt


def test_v15_process_fits_rank_target_and_closes_against_v14_reference() -> None:
    _CapturingEstimator.fitted_targets = []
    bundle = _bundle()
    v14_reference = subject.run_gbdt_process(
        bundle,
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    children = [
        subject.run_gbdt_process(
            bundle,
            producer_commit="e" * 40,
            process_index=index,
            model_contract_version=subject.V15_CONTRACT_VERSION,
            estimator_factory=_CapturingEstimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]

    assert len(_CapturingEstimator.fitted_targets) == 12
    assert all(
        float(target.min()) >= -0.5 and float(target.max()) <= 0.5 for target in _CapturingEstimator.fitted_targets
    )
    payload = children[0]["reproducibility_payload"]
    assert payload["contract_version"] == subject.V15_CONTRACT_VERSION
    assert payload["input_feature_contract_version"] == subject.V14_CONTRACT_VERSION
    assert payload["target_transform"] == subject.V15_TARGET_TRANSFORM
    assert all(fold["training_target_receipt"]["sector_count"] == 31 for fold in payload["folds"])
    assert all(fold["training_target_receipt"]["fit_row_count"] == fold["fit_row_count"] for fold in payload["folds"])
    assert payload["final_model"]["training_target_receipt"]["sector_count"] == 31
    assert payload["final_model"]["training_target_receipt"]["fit_row_count"] == payload["final_model"]["fit_row_count"]

    acceptance = subject.close_processes(*children, v14_reference=v14_reference, input_bundle=bundle)
    assert acceptance["contract_version"] == subject.V15_CONTRACT_VERSION
    assert acceptance["paired_v14_diagnostic"]["baseline_contract_version"] == subject.V14_CONTRACT_VERSION
    assert acceptance["paired_v14_diagnostic"]["candidate_contract_version"] == subject.V15_CONTRACT_VERSION
    assert acceptance["tail_accessed"] is False

    with pytest.raises(subject.RotationL1G2AError) as missing_authority:
        subject.close_processes(*children, v14_reference=v14_reference)
    assert missing_authority.value.reason_code == subject.REASON_INPUT

    tampered_bundle = copy.deepcopy(bundle)
    first_train_date = subject.fold_slices(_calendar(), horizon=subject.FIXED_HORIZON)[0].train_dates[0]
    first_identity = (first_train_date, tampered_bundle["panel"].index.get_level_values("sector_code")[0])
    tampered_bundle["panel"].loc[first_identity, "target_10d"] += 1.0
    with pytest.raises(subject.RotationL1G2AError) as tampered_authority:
        subject.close_processes(*children, v14_reference=v14_reference, input_bundle=tampered_bundle)
    assert tampered_authority.value.reason_code == subject.REASON_REPRODUCIBILITY


def test_cli_defaults_to_v14_and_requires_explicit_v15_selection() -> None:
    parser = cli._parser()
    default_args = parser.parse_args(
        [
            "model-child",
            "--input-root",
            "input",
            "--output-file",
            "output.json",
            "--process-index",
            "1",
            "--producer-commit",
            "e" * 40,
        ]
    )
    explicit_args = parser.parse_args(
        [
            "model-child",
            "--input-root",
            "input",
            "--output-file",
            "output.json",
            "--process-index",
            "1",
            "--producer-commit",
            "e" * 40,
            "--model-contract-version",
            subject.V15_CONTRACT_VERSION,
        ]
    )

    assert default_args.model_contract_version == subject.V14_CONTRACT_VERSION
    assert explicit_args.model_contract_version == subject.V15_CONTRACT_VERSION


def test_v15_cli_failure_receipt_keeps_explicit_contract_identity(tmp_path) -> None:
    output = tmp_path / "fresh_process_1.json"
    assert (
        cli.main(
            [
                "model-child",
                "--input-root",
                str(tmp_path / "missing-input"),
                "--output-file",
                str(output),
                "--process-index",
                "1",
                "--producer-commit",
                "e" * 40,
                "--model-contract-version",
                subject.V15_CONTRACT_VERSION,
            ]
        )
        == 2
    )
    failure = json.loads((tmp_path / "fresh_process_1.failure.json").read_text(encoding="utf-8"))
    assert failure["contract_version"] == subject.V15_CONTRACT_VERSION


def test_v16_score_is_target_free_average_rank_and_row_order_invariant() -> None:
    bundle = _bundle()
    expected, folds = subject.build_v16_scores(bundle)
    changed = copy.deepcopy(bundle)
    changed["panel"]["target_10d"] = changed["panel"]["target_10d"] * -17.0
    actual, changed_folds = subject.build_v16_scores(changed)

    pd.testing.assert_series_equal(actual, expected)
    assert changed_folds == folds
    first_day = folds[0].validation_dates[0]
    day = expected.loc[(first_day, slice(None))]
    assert float(day.min()) == pytest.approx(-0.5)
    assert float(day.max()) == pytest.approx(0.5)

    reordered = copy.deepcopy(bundle)
    reordered["panel"] = reordered["panel"].sample(frac=1.0, random_state=42)
    reordered_scores, _ = subject.build_v16_scores(reordered)
    pd.testing.assert_series_equal(reordered_scores, expected)


def test_v16_does_not_gate_on_unused_historical_training_feature_coverage() -> None:
    bundle = _bundle()
    oof_dates = {
        day for fold in subject.fold_slices(_calendar(), horizon=subject.FIXED_HORIZON) for day in fold.validation_dates
    }
    historical_dates = [day for day in _calendar() if day not in oof_dates][:300]
    index = (historical_dates, slice(None))
    bundle["panel"].loc[index, "moneyflow_intensity_delta_5d"] = np.nan
    bundle["panel"].loc[index, "reason__moneyflow_intensity_delta_5d"] = "historical_not_used"

    with pytest.raises(subject.RotationL1G2AError) as legacy_gate:
        subject.validate_input_bundle(bundle)
    assert legacy_gate.value.reason_code == subject.REASON_FEATURE

    scores, _folds = subject.build_v16_scores(bundle)
    assert np.isfinite(scores.to_numpy(dtype=np.float64)).all()


def test_v16_zero_fit_process_closes_against_input_and_v14_authorities() -> None:
    def forbidden_estimator(**_kwargs):
        raise AssertionError("v1.6 must not construct or fit an estimator")

    bundle = _bundle()
    v14_reference = subject.run_gbdt_process(
        bundle,
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    children = [
        subject.run_gbdt_process(
            bundle,
            producer_commit="e" * 40,
            process_index=index,
            model_contract_version=subject.V16_CONTRACT_VERSION,
            estimator_factory=forbidden_estimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]
    payload = children[0]["reproducibility_payload"]
    assert payload["fit_count"] == 0
    assert payload["gbdt_fit_count"] == 0
    assert payload["market_fit_count"] == 0
    assert payload["fit_progress"] == {
        "planned": 0,
        "started": 0,
        "completed": 0,
        "failed": 0,
        "active_fit": None,
    }
    assert payload["market_context_receipts"] == []
    assert payload["scoring_contract"]["feature"] == "moneyflow_intensity_delta_5d"
    assert payload["scoring_contract"]["training_performed"] is False
    assert payload["scoring_contract"]["target_accessed_for_score"] is False
    assert payload["scoring_contract"]["market_context_accessed_for_score"] is False
    assert children[0]["reproducibility_payload_sha256"] == children[1]["reproducibility_payload_sha256"]

    acceptance = subject.close_processes(
        *children,
        v14_reference=v14_reference,
        input_bundle=bundle,
    )
    assert acceptance["schema_version"] == subject.V16_ACCEPTANCE_SCHEMA_VERSION
    assert acceptance["contract_version"] == subject.V16_CONTRACT_VERSION
    assert acceptance["fit_count"] == 0
    assert acceptance["score_transform"] == subject.V16_SCORE_TRANSFORM
    assert acceptance["paired_v14_diagnostic"]["candidate_contract_version"] == subject.V16_CONTRACT_VERSION
    assert acceptance["tail_accessed"] is False

    forged = copy.deepcopy(children)
    for child in forged:
        child["reproducibility_payload"]["development_summary"]["mean_rank_ic"] += 0.01
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
        child["report_sha256"] = subject.canonical_sha256(
            {key: value for key, value in child.items() if key != "report_sha256"}
        )
    with pytest.raises(subject.RotationL1G2AError) as forged_summary:
        subject.close_processes(*forged, v14_reference=v14_reference, input_bundle=bundle)
    assert forged_summary.value.reason_code == subject.REASON_REPRODUCIBILITY

    with pytest.raises(subject.RotationL1G2AError) as missing_input:
        subject.close_processes(*children, v14_reference=v14_reference)
    assert missing_input.value.reason_code == subject.REASON_INPUT

    changed = copy.deepcopy(bundle)
    first_day = subject.fold_slices(_calendar(), horizon=subject.FIXED_HORIZON)[0].validation_dates[0]
    first_sector = changed["panel"].index.get_level_values("sector_code")[0]
    changed["panel"].loc[(first_day, first_sector), "moneyflow_intensity_delta_5d"] = 1_000_000.0
    with pytest.raises(subject.RotationL1G2AError) as drift:
        subject.close_processes(*children, v14_reference=v14_reference, input_bundle=changed)
    assert drift.value.reason_code == subject.REASON_REPRODUCIBILITY

    changed_target = copy.deepcopy(bundle)
    changed_target["panel"]["target_10d"] *= -1.0
    with pytest.raises(subject.RotationL1G2AError) as metric_drift:
        subject.close_processes(*children, v14_reference=v14_reference, input_bundle=changed_target)
    assert metric_drift.value.reason_code == subject.REASON_REPRODUCIBILITY


def test_v16_missing_score_is_unavailable_without_neutral_fallback() -> None:
    bundle = _bundle()
    first_day = subject.fold_slices(_calendar(), horizon=subject.FIXED_HORIZON)[0].validation_dates[0]
    first_sector = bundle["panel"].index.get_level_values("sector_code")[0]
    bundle["panel"].loc[(first_day, first_sector), "moneyflow_intensity_delta_5d"] = np.nan
    bundle["panel"].loc[(first_day, first_sector), "reason__moneyflow_intensity_delta_5d"] = "source_missing"

    child = subject.run_gbdt_process(
        bundle,
        producer_commit="e" * 40,
        process_index=1,
        model_contract_version=subject.V16_CONTRACT_VERSION,
        runtime_validator=_test_runtime,
    )
    row = next(
        row
        for row in child["reproducibility_payload"]["oof_prediction_rows"]
        if row["trade_date"] == first_day.isoformat() and row["sector_code"] == first_sector
    )
    assert row["availability"] == "unavailable"
    assert row["reason_code"] == "source_missing"
    assert row["rotation_score"] is None
    assert row["forecast_state"] is None
    assert row["feature_contributions"] is None


def test_cli_requires_explicit_v16_and_keeps_zero_fit_parent_progress(tmp_path) -> None:
    args = cli._parser().parse_args(
        [
            "model-child",
            "--input-root",
            "input",
            "--output-file",
            "output.json",
            "--process-index",
            "1",
            "--producer-commit",
            "e" * 40,
            "--model-contract-version",
            subject.V16_CONTRACT_VERSION,
        ]
    )
    assert args.model_contract_version == subject.V16_CONTRACT_VERSION
    progress = cli._parent_fit_progress(tmp_path, contract_version=subject.V16_CONTRACT_VERSION)
    assert progress["planned"] == 0
    assert all(component["planned"] == 0 for component in progress["components"])


def _leaf_distribution(*, sparse_tree_count: int, sparse_date_count: int) -> tuple[object, pd.DataFrame, pd.Index]:
    dates = pd.Index(pd.bdate_range("2024-01-02", periods=504).date, name="trade_date")
    repeated_dates = dates.repeat(subject.CANONICAL_SECTOR_COUNT)
    features = pd.DataFrame({"x": np.zeros(len(repeated_dates))}, index=repeated_dates)
    sector_offsets = np.tile(np.arange(subject.CANONICAL_SECTOR_COUNT), len(dates))
    base = np.tile((sector_offsets % 7).reshape(-1, 1), (1, 240))
    day_offsets = np.repeat(np.arange(len(dates)), subject.CANONICAL_SECTOR_COUNT)
    for tree_index in range(sparse_tree_count):
        sparse_leaf = (day_offsets < sparse_date_count) & (sector_offsets < 18)
        base[:, tree_index] = sector_offsets % 6
        base[sparse_leaf, tree_index] = 6

    class LeafEstimator:
        def predict(self, _features: pd.DataFrame, pred_leaf: bool = False) -> np.ndarray:
            assert pred_leaf
            return base

    return LeafEstimator(), features, repeated_dates


def test_leaf_date_coverage_accepts_isolated_under_20_day_leaf_within_one_percent_budget() -> None:
    estimator, features, dates = _leaf_distribution(sparse_tree_count=1, sparse_date_count=18)
    receipt = subject._leaf_date_coverage(estimator, features, dates, fit_identity="test-fit")

    assert receipt["minimum"] == 18
    assert receipt["leaf_count"] == 1680
    assert receipt["below_target_leaf_count"] == 1
    assert receipt["below_target_leaf_fraction"] == pytest.approx(1 / 1680)
    assert receipt["hard_floor_violating_leaf_ids"] == []
    assert receipt["contract_passed"] is True


def test_leaf_date_coverage_rejects_more_than_one_percent_under_20_day_leaves() -> None:
    estimator, features, dates = _leaf_distribution(sparse_tree_count=20, sparse_date_count=18)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject._leaf_date_coverage(estimator, features, dates, fit_identity="test-fit")

    assert caught.value.reason_code == subject.REASON_LEAF
    assert caught.value.evidence["summary"]["below_target_leaf_count"] == 20
    assert caught.value.evidence["summary"]["contract_passed"] is False


def test_leaf_date_coverage_rejects_any_leaf_below_derived_ten_day_floor() -> None:
    estimator, features, dates = _leaf_distribution(sparse_tree_count=1, sparse_date_count=9)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject._leaf_date_coverage(estimator, features, dates, fit_identity="test-fit")

    assert caught.value.reason_code == subject.REASON_LEAF
    assert caught.value.evidence["summary"]["hard_floor_violating_leaf_ids"] == [[0, 6, 9]]
    assert caught.value.evidence["summary"]["contract_passed"] is False


def test_gbdt_process_enforces_profile_and_closes_two_identical_processes() -> None:
    first = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    assert first["reproducibility_payload"]["fit_count"] == 12
    assert first["reproducibility_payload"]["fit_progress"] == {
        "planned": 12,
        "started": 12,
        "completed": 12,
        "failed": 0,
        "active_fit": None,
    }
    assert first["reproducibility_payload"]["profile"]["n_estimators"] == 240
    assert first["reproducibility_payload"]["selected_horizon"] == 10
    assert first["reproducibility_payload"]["horizon_authority"] == subject.HORIZON_AUTHORITY
    assert first["reproducibility_payload"]["delta_feature_coverage"]["minimum_coverage"] == 0.90
    assert first["reproducibility_payload_sha256"] == second["reproducibility_payload_sha256"]
    fold_hashes = {fold["model_sha256"] for fold in first["reproducibility_payload"]["folds"]}
    assert fold_hashes
    assert {row["model_hash"] for row in first["reproducibility_payload"]["oof_prediction_rows"]} <= fold_hashes
    acceptance = subject.close_processes(first, second, v13_reference=_as_v13_reference(first))
    assert acceptance["research_surface_status"] == "NOT_AVAILABLE"
    assert acceptance["rotation_l1_capability_status"] == "NOT_AVAILABLE"
    assert acceptance["research_product_compute_conditions_satisfied"] is True
    assert acceptance["research_product_gate_passed"] is False
    assert acceptance["model_effect_tail_access_eligible"] is True
    assert acceptance["forward_power_status"] == "INSUFFICIENT"
    assert acceptance["tail_accessed"] is False
    assert acceptance["paired_v13_diagnostic"]["binding_gate_applied"] is False
    assert acceptance["paired_v13_diagnostic"]["common_date_count"] > 0
    assert "moneyflow_intensity_delta_5d" in acceptance["paired_v13_diagnostic"]["contributions"]["v1_4"]


def test_v14_closure_keeps_existing_v13_process_receipts_readable() -> None:
    children = [
        subject.run_gbdt_process(
            _bundle(),
            producer_commit="e" * 40,
            process_index=index,
            estimator_factory=_FakeEstimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]
    children = [_as_v13_reference(child) for child in children]

    acceptance = subject.close_processes(*children)

    assert acceptance["contract_version"] == subject.V13_CONTRACT_VERSION
    assert acceptance["schema_version"] == subject.V13_ACCEPTANCE_SCHEMA_VERSION
    assert acceptance["battery_receipt_sha256"] == "f" * 64
    assert "horizon_authority" not in acceptance


def test_close_processes_accepts_holiday_aligned_validation_window_start() -> None:
    bundle = _bundle()
    holiday_dates = set(pd.bdate_range("2025-10-01", "2025-10-08").date)
    panel = bundle["panel"]
    bundle["panel"] = panel.loc[~panel.index.get_level_values("trade_date").isin(holiday_dates)].copy()
    bundle["benchmark_close"] = {
        day: value for day, value in bundle["benchmark_close"].items() if day not in holiday_dates
    }
    children = [
        subject.run_gbdt_process(
            bundle,
            producer_commit="e" * 40,
            process_index=index,
            estimator_factory=_FakeEstimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]

    fold = children[0]["reproducibility_payload"]["folds"][4]
    assert fold["purge_dates"][-1] == "2025-09-30"
    assert fold["validation_start"] == "2025-10-09"
    acceptance = subject.close_processes(*children, v13_reference=_as_v13_reference(children[0]))

    assert acceptance["status"] == "development_complete"
    assert acceptance["tail_accessed"] is False


def test_close_processes_rejects_rehashed_stale_leaf_contract() -> None:
    first = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    for child in (first, second):
        leaf = child["reproducibility_payload"]["folds"][0]["leaf_date_coverage"]
        leaf.pop("hard_floor_distinct_dates")
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
        body = {key: value for key, value in child.items() if key != "report_sha256"}
        child["report_sha256"] = subject.canonical_sha256(body)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(first, second)

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY


def test_close_processes_rejects_rehashed_fold_authority_drift() -> None:
    children = [
        subject.run_gbdt_process(
            _bundle(),
            producer_commit="e" * 40,
            process_index=index,
            estimator_factory=_FakeEstimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]
    for child in children:
        fold = child["reproducibility_payload"]["folds"][0]
        fold["purge_dates"] = [*fold["purge_dates"][1:], "2023-09-04"]
        fold["validation_start"] = "2023-09-05"
        fold_body = {
            key: fold[key]
            for key in (
                "fold",
                "train_start",
                "train_end",
                "train_count",
                "train_date_sha256",
                "purge_dates",
                "validation_start",
                "validation_end",
                "validation_count",
                "validation_date_sha256",
            )
        }
        fold["receipt_sha256"] = subject.canonical_sha256(fold_body)
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
        child["report_sha256"] = subject.canonical_sha256(
            {key: value for key, value in child.items() if key != "report_sha256"}
        )

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(*children)

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert caught.value.stage == "closure"


def test_close_processes_rejects_empty_purge_dates_with_typed_failure() -> None:
    child = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    fold = child["reproducibility_payload"]["folds"][0]
    fold["purge_dates"] = []
    fold["receipt_sha256"] = subject.canonical_sha256(
        {
            key: fold[key]
            for key in (
                "fold",
                "train_start",
                "train_end",
                "train_count",
                "train_date_sha256",
                "purge_dates",
                "validation_start",
                "validation_end",
                "validation_count",
                "validation_date_sha256",
            )
        }
    )
    child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
    child["report_sha256"] = subject.canonical_sha256(
        {key: value for key, value in child.items() if key != "report_sha256"}
    )

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(child, {})

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert caught.value.stage == "closure"


def test_close_processes_rejects_rehashed_oof_fold_model_lineage_drift() -> None:
    first = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    first["reproducibility_payload"]["oof_prediction_rows"][0]["model_hash"] = "f" * 64
    first["reproducibility_payload"]["oof_prediction_rows_sha256"] = subject.canonical_sha256(
        first["reproducibility_payload"]["oof_prediction_rows"]
    )
    first["reproducibility_payload_sha256"] = subject.canonical_sha256(first["reproducibility_payload"])
    body = {key: value for key, value in first.items() if key != "report_sha256"}
    first["report_sha256"] = subject.canonical_sha256(body)

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(first, second)

    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY
    assert caught.value.stage == "closure"


def test_close_processes_rejects_rehashed_oof_as_of_calendar_drift() -> None:
    children = [
        subject.run_gbdt_process(
            _bundle(),
            producer_commit="e" * 40,
            process_index=index,
            estimator_factory=_FakeEstimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]
    for child in children:
        first_day = child["reproducibility_payload"]["oof_prediction_rows"][0]["trade_date"]
        for row in child["reproducibility_payload"]["oof_prediction_rows"]:
            if row["trade_date"] == first_day:
                row["as_of_date"] = "2020-01-01"
        child["reproducibility_payload"]["oof_prediction_rows_sha256"] = subject.canonical_sha256(
            child["reproducibility_payload"]["oof_prediction_rows"]
        )
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
        child["report_sha256"] = subject.canonical_sha256(
            {key: value for key, value in child.items() if key != "report_sha256"}
        )

    with pytest.raises(subject.RotationL1G2AError, match="as-of calendar"):
        subject.close_processes(*children)


def test_close_processes_rejects_existing_model_hash_from_the_wrong_fold() -> None:
    first = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    hashes = [fold["model_sha256"] for fold in first["reproducibility_payload"]["folds"]]
    if len(set(hashes)) == 1:
        first["reproducibility_payload"]["folds"][1]["model_sha256"] = "e" * 64
        second["reproducibility_payload"]["folds"][1]["model_sha256"] = "e" * 64
        hashes[1] = "e" * 64
    for child in (first, second):
        child["reproducibility_payload"]["oof_prediction_rows"][0]["model_hash"] = hashes[1]
        child["reproducibility_payload"]["oof_prediction_rows_sha256"] = subject.canonical_sha256(
            child["reproducibility_payload"]["oof_prediction_rows"]
        )
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
        child["report_sha256"] = subject.canonical_sha256(
            {key: value for key, value in child.items() if key != "report_sha256"}
        )

    with pytest.raises(subject.RotationL1G2AError, match="fold-model"):
        subject.close_processes(first, second)


def test_close_processes_rejects_rehashed_partial_oof_cross_section() -> None:
    children = [
        subject.run_gbdt_process(
            _bundle(),
            producer_commit="e" * 40,
            process_index=index,
            estimator_factory=_FakeEstimator,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]
    for child in children:
        child["reproducibility_payload"]["oof_prediction_rows"].pop()
        child["reproducibility_payload"]["oof_prediction_rows_sha256"] = subject.canonical_sha256(
            child["reproducibility_payload"]["oof_prediction_rows"]
        )
        child["reproducibility_payload_sha256"] = subject.canonical_sha256(child["reproducibility_payload"])
        child["report_sha256"] = subject.canonical_sha256(
            {key: value for key, value in child.items() if key != "report_sha256"}
        )

    with pytest.raises(subject.RotationL1G2AError, match="denominator"):
        subject.close_processes(*children)


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
            producer_commit="e" * 40,
            process_index=1,
            estimator_factory=Collapsed,
            runtime_validator=_test_runtime,
        )
    assert caught.value.reason_code == subject.REASON_LEAF
    assert caught.value.stage == "leaf_date_coverage"
    assert caught.value.evidence["fit_identity"] == "process-1:fold-1:gbdt"
    assert caught.value.evidence["fit_progress"] == {
        "planned": 12,
        "started": 2,
        "completed": 2,
        "failed": 0,
        "active_fit": None,
    }


def test_gbdt_fit_failure_records_active_fit_and_failed_count() -> None:
    class FitFailed(_FakeEstimator):
        def fit(self, features: pd.DataFrame, target: pd.Series) -> "_FakeEstimator":
            raise ValueError("fit failed")

    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.run_gbdt_process(
            _bundle(),
            producer_commit="e" * 40,
            process_index=1,
            estimator_factory=FitFailed,
            runtime_validator=_test_runtime,
        )
    assert caught.value.reason_code == subject.REASON_FIT
    assert caught.value.evidence["fit_progress"] == {
        "planned": 12,
        "started": 2,
        "completed": 1,
        "failed": 1,
        "active_fit": "process-1:fold-1:gbdt",
    }


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
        producer_commit="e" * 40,
        process_index=1,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=2,
        estimator_factory=_FakeEstimator,
        runtime_validator=_test_runtime,
    )
    second["reproducibility_payload_sha256"] = "f" * 64
    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(first, second)
    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY

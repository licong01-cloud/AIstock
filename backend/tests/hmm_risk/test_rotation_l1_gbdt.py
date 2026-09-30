from __future__ import annotations

import copy
from datetime import date
import json
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


def test_v16_single_date_feature_reuses_causal_moneyflow_window_and_reason() -> None:
    calendar = tuple(pd.bdate_range("2026-07-27", periods=26).date)
    sectors = tuple(f"801{index:03d}.SI" for index in range(31))
    stock = []
    for day_index, day in enumerate(calendar[:-1]):
        for sector_index, sector in enumerate(sectors):
            net = float(sector_index)
            if day_index >= 20:
                net = float(10 + 2 * sector_index)
            stock.append(
                {
                    "source_date": day,
                    "sector_code": sector,
                    "moneyflow_net_amount_cny": net,
                    "moneyflow_traded_amount_cny": 100.0,
                    "moneyflow_reason_code": None,
                }
            )

    frame = subject.build_v16_single_date_feature_frame(
        calendar=calendar,
        stock_daily_inputs=stock,
        trade_date=calendar[-1],
    )

    assert len(frame) == 31
    for sector_index, sector in enumerate(sectors):
        expected = (5.0 * sector_index + 50.0) / 2000.0
        assert frame.at[(calendar[-1], sector), subject.V16_SCORE_FEATURE] == pytest.approx(expected)
        assert frame.at[(calendar[-1], sector), f"reason__{subject.V16_SCORE_FEATURE}"] is None

    broken = copy.deepcopy(stock)
    broken[0]["moneyflow_reason_code"] = "provider_absent"
    missing = subject.build_v16_single_date_feature_frame(
        calendar=calendar,
        stock_daily_inputs=broken,
        trade_date=calendar[-1],
    )
    assert np.isnan(missing.at[(calendar[-1], sectors[0]), subject.V16_SCORE_FEATURE])
    assert missing.at[(calendar[-1], sectors[0]), f"reason__{subject.V16_SCORE_FEATURE}"] == "provider_absent"


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
                "planned": 0,
                "started": 0,
                "completed": 0,
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

    assert progress["planned"] == 0
    assert progress["started"] == 0
    assert progress["completed"] == 0
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


@pytest.fixture(scope="module")
def frozen_v14_reference() -> dict[str, object]:
    """Build a hash-valid read-only baseline without executing retired v1.4 training."""

    reference = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=1,
        model_contract_version=subject.V16_CONTRACT_VERSION,
        runtime_validator=_test_runtime,
    )
    payload = reference["reproducibility_payload"]
    payload["contract_version"] = subject.V14_CONTRACT_VERSION
    reference["schema_version"] = subject.PROCESS_SCHEMA_VERSION
    reference["reproducibility_payload_sha256"] = subject.canonical_sha256(payload)
    reference["report_sha256"] = subject.canonical_sha256(
        {key: value for key, value in reference.items() if key != "report_sha256"}
    )
    subject.validate_v14_process_reference(reference)
    return reference


def test_v16_cli_requires_and_prevalidates_explicit_v14_input_root(tmp_path, monkeypatch, frozen_v14_reference) -> None:
    old_bundle = _bundle()
    v14_reference = frozen_v14_reference
    new_bundle = copy.deepcopy(old_bundle)
    new_bundle["identity"] = {**new_bundle["identity"], "mapping_sha256": "2" * 64}
    old_root = tmp_path / "old-input"
    new_root = tmp_path / "new-input"
    subject.write_input_bundle(old_bundle, old_root, forbidden_roots=())
    subject.write_input_bundle(new_bundle, new_root, forbidden_roots=())
    reference_path = tmp_path / "v14-process.json"
    cli._write_once(reference_path, v14_reference)
    base = {
        "v13_process_file": None,
        "v14_process_file": reference_path,
        "input_root": new_root,
        "output_root": tmp_path / "output",
        "producer_commit": "f" * 40,
        "model_contract_version": subject.V16_CONTRACT_VERSION,
    }

    with pytest.raises(RuntimeError, match="requires --v14-input-root"):
        cli._run_parent(SimpleNamespace(**base, v14_input_root=None))

    reached_output_creation = False

    def stop_after_preflight(_path):
        nonlocal reached_output_creation
        reached_output_creation = True
        raise RuntimeError("preflight complete")

    monkeypatch.setattr(cli, "_ensure_external_new_directory", stop_after_preflight)
    with pytest.raises(RuntimeError, match="preflight complete"):
        cli._run_parent(SimpleNamespace(**base, v14_input_root=old_root))
    assert reached_output_creation is True


def test_v16_cli_rejects_unused_v14_input_root_before_output(tmp_path, monkeypatch, frozen_v14_reference) -> None:
    bundle = _bundle()
    v14_reference = frozen_v14_reference
    input_root = tmp_path / "input"
    subject.write_input_bundle(bundle, input_root, forbidden_roots=())
    reference_path = tmp_path / "v14-process.json"
    cli._write_once(reference_path, v14_reference)
    monkeypatch.setattr(
        cli,
        "_ensure_external_new_directory",
        lambda _path: pytest.fail("ambiguous authority must fail before output creation"),
    )

    with pytest.raises(RuntimeError, match="ambiguous"):
        cli._run_parent(
            SimpleNamespace(
                v13_process_file=None,
                v14_process_file=reference_path,
                v14_input_root=input_root,
                input_root=input_root,
                output_root=tmp_path / "output",
                producer_commit="f" * 40,
                model_contract_version=subject.V16_CONTRACT_VERSION,
            )
        )


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


def test_v16_zero_fit_process_closes_against_input_and_v14_authorities(frozen_v14_reference) -> None:
    bundle = _bundle()
    v14_reference = frozen_v14_reference
    children = [
        subject.run_gbdt_process(
            bundle,
            producer_commit="e" * 40,
            process_index=index,
            model_contract_version=subject.V16_CONTRACT_VERSION,
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


def test_v16_authority_only_rebind_closes_with_exact_logical_input(frozen_v14_reference) -> None:
    old_bundle = _bundle()
    v14_reference = frozen_v14_reference
    new_bundle = copy.deepcopy(old_bundle)
    new_bundle["identity"] = {
        **new_bundle["identity"],
        "source_sha256": "1" * 64,
        "mapping_sha256": "2" * 64,
    }
    new_bundle["panel"] = new_bundle["panel"].sample(frac=1.0, random_state=42)
    new_bundle["panel"] = new_bundle["panel"].loc[:, list(reversed(new_bundle["panel"].columns))]
    children = [
        subject.run_gbdt_process(
            new_bundle,
            producer_commit="f" * 40,
            process_index=index,
            model_contract_version=subject.V16_CONTRACT_VERSION,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]

    acceptance = subject.close_processes(
        *children,
        v14_reference=v14_reference,
        input_bundle=new_bundle,
        v14_input_bundle=old_bundle,
    )

    receipt = acceptance["paired_v14_diagnostic"]["input_authority_rebind"]
    assert receipt["schema_version"] == "hmm_risk_rotation_l1_g2a_input_authority_rebind_v1"
    assert receipt["changed_identity_fields"] == ["mapping_sha256", "source_sha256"]
    assert receipt["scope"] == "paired_v14_diagnostic_only"
    assert receipt["candidate_recomputed"] is True
    assert receipt["baseline_model_authority_reused"] is False
    assert receipt["tail_accessed"] is False
    assert receipt["row_count"] == len(new_bundle["panel"])
    assert receipt["receipt_sha256"] == subject.canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    )
    assert acceptance["producer_commit"] == "f" * 40
    assert acceptance["tail_accessed"] is False


def test_v16_authority_rebind_rejects_missing_ambiguous_and_non_authority_changes(frozen_v14_reference) -> None:
    old_bundle = _bundle()
    v14_reference = frozen_v14_reference
    new_bundle = copy.deepcopy(old_bundle)
    new_bundle["identity"] = {**new_bundle["identity"], "mapping_sha256": "2" * 64}
    children = [
        subject.run_gbdt_process(
            new_bundle,
            producer_commit="f" * 40,
            process_index=index,
            model_contract_version=subject.V16_CONTRACT_VERSION,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]

    with pytest.raises(subject.RotationL1G2AError, match="requires both") as missing:
        subject.close_processes(*children, v14_reference=v14_reference, input_bundle=new_bundle)
    assert missing.value.reason_code == subject.REASON_INPUT

    same_identity_children = [
        subject.run_gbdt_process(
            old_bundle,
            producer_commit="f" * 40,
            process_index=index,
            model_contract_version=subject.V16_CONTRACT_VERSION,
            runtime_validator=_test_runtime,
        )
        for index in (1, 2)
    ]
    with pytest.raises(subject.RotationL1G2AError, match="ambiguous") as ambiguous:
        subject.close_processes(
            *same_identity_children,
            v14_reference=v14_reference,
            input_bundle=old_bundle,
            v14_input_bundle=old_bundle,
        )
    assert ambiguous.value.reason_code == subject.REASON_INPUT

    disallowed = copy.deepcopy(new_bundle)
    disallowed["identity"] = {
        **disallowed["identity"],
        "feature_contract_sha256": "3" * 64,
    }
    with pytest.raises(subject.RotationL1G2AError, match="not an approved rebind") as identity_drift:
        subject.validate_v16_input_authority_rebind(v14_reference, old_bundle, disallowed)
    assert identity_drift.value.reason_code == subject.REASON_INPUT


@pytest.mark.parametrize("drift", ["feature", "target", "reason", "maturity", "benchmark", "calendar", "sector"])
def test_v16_authority_rebind_rejects_any_logical_data_drift(drift: str, frozen_v14_reference) -> None:
    old_bundle = _bundle()
    v14_reference = frozen_v14_reference
    new_bundle = copy.deepcopy(old_bundle)
    new_bundle["identity"] = {**new_bundle["identity"], "mapping_sha256": "2" * 64}
    if drift == "benchmark":
        first_day = next(iter(new_bundle["benchmark_close"]))
        new_bundle["benchmark_close"][first_day] += 1.0
    elif drift == "calendar":
        first_day = new_bundle["panel"].index.get_level_values("trade_date")[0]
        new_bundle["panel"] = new_bundle["panel"].drop(index=first_day, level="trade_date")
    elif drift == "sector":
        frame = new_bundle["panel"].reset_index()
        first_sector = frame["sector_code"].iloc[0]
        frame.loc[frame["sector_code"] == first_sector, "sector_code"] = "809999"
        new_bundle["panel"] = frame.set_index(["trade_date", "sector_code"])
    elif drift == "reason":
        column = "moneyflow_intensity_delta_5d"
        new_bundle["panel"].iloc[0, new_bundle["panel"].columns.get_loc(column)] = np.nan
        new_bundle["panel"].iloc[0, new_bundle["panel"].columns.get_loc(f"reason__{column}")] = "source_missing"
    elif drift == "maturity":
        new_bundle["panel"].iloc[0, new_bundle["panel"].columns.get_loc("target_10d")] = np.nan
        new_bundle["panel"].iloc[0, new_bundle["panel"].columns.get_loc("reason__target_10d")] = "not_mature"
        new_bundle["panel"].iloc[0, new_bundle["panel"].columns.get_loc("target_10d_mature")] = False
    else:
        column = "target_10d" if drift == "target" else "moneyflow_intensity_delta_5d"
        new_bundle["panel"].iloc[0, new_bundle["panel"].columns.get_loc(column)] += 1.0

    with pytest.raises(subject.RotationL1G2AError, match="logical data differs") as caught:
        subject.validate_v16_input_authority_rebind(v14_reference, old_bundle, new_bundle)

    assert caught.value.reason_code == subject.REASON_INPUT


def test_v16_authority_rebind_requires_v14_process_to_match_old_bundle(frozen_v14_reference) -> None:
    process_bundle = _bundle()
    v14_reference = frozen_v14_reference
    old_bundle = copy.deepcopy(process_bundle)
    old_bundle["identity"] = {**old_bundle["identity"], "source_sha256": "1" * 64}
    new_bundle = copy.deepcopy(old_bundle)
    new_bundle["identity"] = {**new_bundle["identity"], "mapping_sha256": "2" * 64}

    with pytest.raises(subject.RotationL1G2AError, match="process input authority differs") as caught:
        subject.validate_v16_input_authority_rebind(v14_reference, old_bundle, new_bundle)

    assert caught.value.reason_code == subject.REASON_INPUT


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


def test_cli_defaults_to_v16_rejects_retired_contracts_and_keeps_zero_fit_progress(tmp_path) -> None:
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
        ]
    )
    assert args.model_contract_version == subject.V16_CONTRACT_VERSION
    for retired in (subject.V14_CONTRACT_VERSION, "hmm_risk_rotation_l1_g2a_v1_5"):
        with pytest.raises(SystemExit):
            cli._parser().parse_args(
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
                    retired,
                ]
            )
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
        runtime_validator=_test_runtime,
    )
    second = subject.run_gbdt_process(
        _bundle(),
        producer_commit="e" * 40,
        process_index=2,
        runtime_validator=_test_runtime,
    )
    second["reproducibility_payload_sha256"] = "f" * 64
    with pytest.raises(subject.RotationL1G2AError) as caught:
        subject.close_processes(first, second)
    assert caught.value.reason_code == subject.REASON_REPRODUCIBILITY

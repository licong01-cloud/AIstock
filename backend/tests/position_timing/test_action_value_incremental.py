from __future__ import annotations

from datetime import date, datetime
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    ATR14_FEATURE_ORDER,
    ATR14_FEATURE_SPEC_SHA256,
    ATR14_INFORMATION_BLOCK,
    CORE_INFORMATION_BLOCK,
    FEATURE_ORDER,
    FEATURE_SPEC_SHA256,
    POLICY_SHA256,
    TZ,
    ActionValueError,
    feature_contract,
    market_features,
    policy_sha256_for,
)
from backend.services.position_timing.action_value_incremental import (
    PIPELINE_ID,
    RECEIPT_SCHEMA,
    REQUEST_SCHEMA,
    _deliver_registry,
    _load_request,
    _paired_policy_comparison,
    _publish_bundle,
    _require_matched_source_coverage,
    inspect_increment_bundle,
)
from backend.services.position_timing.action_value_model import fit_local_model
from backend.services.position_timing.action_value_research import (
    ActionValuePopulationSpec,
    build_action_value_rows,
)
from backend.services.position_timing.contracts import canonical_sha256
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1


class Candidate:
    def __init__(self, sessions: int = 180) -> None:
        self.calendar = pd.bdate_range("2022-01-03", periods=sessions)
        self.symbols = ("000001.SZ",)
        self._frames = {
            "000001.SZ": self._frame(0, stock=True),
            "000300.SH": self._frame(1, stock=False),
        }

    def _frame(self, offset: int, *, stock: bool) -> pd.DataFrame:
        trend = 10 + offset + np.arange(len(self.calendar)) * 0.01
        frame = pd.DataFrame(index=self.calendar)
        frame["open"] = trend * 0.998
        frame["high"] = trend * 1.012
        frame["low"] = trend * 0.988
        frame["close"] = trend
        frame["volume"] = 1_000_000 + np.arange(len(frame)) * 100
        frame["factor"] = 1.0
        frame["up_limit"] = trend * 1.1
        frame["down_limit"] = trend * 0.9
        frame["is_suspended"] = False
        frame["pit_active"] = stock
        return frame

    def bars(self, symbol: str) -> pd.DataFrame:
        return self._frames[symbol].copy()


def test_core_identity_is_unchanged_and_atr_contract_is_distinct() -> None:
    assert FEATURE_SPEC_SHA256 == "f3eabc9643b80db877508dddb4cd88918a7cdfc16c345acee34cbb437dac4617"
    assert POLICY_SHA256 == "99d29aef726937e43afb4dfa26cbc6cfcf7fc0ef04bc041d77ad106cbb8a518d"
    assert feature_contract(CORE_INFORMATION_BLOCK)[1] == FEATURE_ORDER
    assert feature_contract(ATR14_INFORMATION_BLOCK)[1] == ATR14_FEATURE_ORDER
    assert ATR14_FEATURE_ORDER[-10] == "atr14_sma_bps"
    assert ATR14_FEATURE_SPEC_SHA256 != FEATURE_SPEC_SHA256
    assert policy_sha256_for(ATR14_INFORMATION_BLOCK) != POLICY_SHA256
    with pytest.raises(ActionValueError, match="INFORMATION_BLOCK_UNSUPPORTED"):
        feature_contract("RSI_SEARCH_NOT_ALLOWED")


def test_atr14_uses_adjustment_consistent_gap_and_range_without_forward_fill() -> None:
    index = pd.bdate_range("2024-01-02", periods=35)
    factor = pd.Series(1.0, index=index)
    factor.iloc[10:] = 2.0
    adjusted_close = pd.Series(100.0, index=index)
    bars = pd.DataFrame(index=index)
    bars["factor"] = factor
    bars["close"] = adjusted_close / factor
    bars["open"] = adjusted_close / factor
    bars["high"] = 101.0 / factor
    bars["low"] = 99.0 / factor
    bars["volume"] = 1_000_000
    benchmark = pd.Series(np.linspace(100, 110, len(index)), index=index)

    features = market_features(bars, benchmark, information_block=ATR14_INFORMATION_BLOCK)

    assert features.loc[index[14], "atr14_sma_bps"] == pytest.approx(200.0)
    assert features.loc[index[10], "atr14_sma_bps"] != pytest.approx(5_200.0)
    missing = bars.copy()
    missing.loc[index[16], ["high", "low", "close"]] = np.nan
    missing_features = market_features(
        missing,
        benchmark,
        information_block=ATR14_INFORMATION_BLOCK,
    )
    assert missing_features.loc[index[16] : index[29], "atr14_sma_bps"].isna().all()
    assert pd.isna(missing_features.loc[index[30], "atr14_sma_bps"])
    assert pd.notna(missing_features.loc[index[31], "atr14_sma_bps"])


def test_optional_population_is_exactly_matched_to_core_labels() -> None:
    candidate = Candidate()
    spec = ActionValuePopulationSpec(
        start=candidate.calendar[35].date(),
        end=candidate.calendar[-25].date(),
        symbol_limit=1,
        review_stride=10,
    )
    core = build_action_value_rows(candidate, spec)
    augmented = build_action_value_rows(
        candidate,
        spec,
        information_block=ATR14_INFORMATION_BLOCK,
    )
    identity_columns = [
        "symbol",
        "decision_as_of",
        "objective",
        "planned_delta_qty",
        "net_action_value_bps",
    ]
    pd.testing.assert_frame_equal(core.rows[identity_columns], augmented.rows[identity_columns])
    assert augmented.rows["atr14_sma_bps"].notna().all()
    assert augmented.coverage["information_block"] == ATR14_INFORMATION_BLOCK
    assert augmented.coverage["feature_order"] == ATR14_FEATURE_ORDER


def test_optional_model_binds_feature_and_policy_identity() -> None:
    candidate = Candidate(820)
    population = build_action_value_rows(
        candidate,
        ActionValuePopulationSpec(
            start=candidate.calendar[35].date(),
            end=candidate.calendar[-25].date(),
            symbol_limit=1,
            review_stride=10,
        ),
        information_block=ATR14_INFORMATION_BLOCK,
    )
    cutoff = datetime.combine(candidate.calendar[-2].date(), datetime.min.time(), tzinfo=TZ)
    model = fit_local_model(
        population.rows,
        cutoff=cutoff,
        available_at=cutoff,
        source_sha256="a" * 64,
        request_sha256="b" * 64,
        source_commit="c" * 40,
        information_block=ATR14_INFORMATION_BLOCK,
    )
    assert model.metadata["information_block"] == ATR14_INFORMATION_BLOCK
    assert tuple(model.metadata["feature_order"]) == ATR14_FEATURE_ORDER
    assert model.metadata["feature_spec_sha256"] == ATR14_FEATURE_SPEC_SHA256
    assert model.metadata["policy_sha256"] == policy_sha256_for(ATR14_INFORMATION_BLOCK)
    with pytest.raises(ActionValueError, match="FEATURE_ORDER_MISMATCH"):
        model.predict(
            population.rows.loc[:0, FEATURE_ORDER],
            ["ENTRY_ACTION_VALUE_V2"],
            decision_as_of=cutoff,
        )


def test_paired_increment_uses_one_common_sleeve_day_series() -> None:
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    rows = []
    for sleeve in ("A", "B"):
        for ordinal, day in enumerate(days):
            rows.append(
                {
                    "sleeve_id": sleeve,
                    "valuation_date": day,
                    "baseline": "BUY_AND_HOLD",
                    "policy_wealth_cny": 100_000 + ordinal * 100,
                }
            )
            rows.append(
                {
                    "sleeve_id": sleeve,
                    "valuation_date": day,
                    "baseline": "FROZEN_L1_V1",
                    "policy_wealth_cny": 100_000 + ordinal * 100,
                }
            )
    core = pd.DataFrame(rows)
    atr = core.copy()
    atr.loc[atr["valuation_date"].eq(days[1]), "policy_wealth_cny"] += 10
    atr.loc[atr["valuation_date"].eq(days[2]), "policy_wealth_cny"] += 30

    daily, comparison = _paired_policy_comparison(core, atr)

    assert daily["sleeve_count"].eq(2).all()
    assert daily["incremental_net_value_cny"].tolist() == [0.0, 20.0, 40.0]
    assert comparison["planned_trial_count"] == 1
    assert comparison["paired_path_rows"] == 6
    assert comparison["comparison_sha256"] == canonical_sha256(
        {key: value for key, value in comparison.items() if key != "comparison_sha256"}
    )


def test_source_coverage_and_request_fail_closed(tmp_path) -> None:
    coverage = {
        "information_block": ATR14_INFORMATION_BLOCK,
        "outcomes_read": False,
        "coverage": {
            "000001.SZ": {
                "complete_core_sessions": 10,
                "complete_selected_feature_sessions": 9,
                "feature_nonmissing": {"atr14_sma_bps": 10},
            }
        },
    }
    with pytest.raises(ActionValueError, match="INCREMENT_OPTIONAL_COVERAGE_LOSS"):
        _require_matched_source_coverage(coverage)
    assert canonical_sha256({"feature_order": ("a", "b")}) == canonical_sha256(
        json.loads(json.dumps({"feature_order": ("a", "b")}))
    )

    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "information_block": ATR14_INFORMATION_BLOCK,
        "planned_trial_count": 1,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    assert _load_request(path) == request
    request["planned_trial_count"] = 2
    path.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(ActionValueError, match="INCREMENT_REQUEST_IDENTITY_MISMATCH"):
        _load_request(path)


def test_bundle_and_own_registry_are_immutable_and_exact_idempotent(tmp_path) -> None:
    timing_root = tmp_path / "timing"
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "information_block": ATR14_INFORMATION_BLOCK,
        "planned_trial_count": 1,
        "timing_root": timing_root.as_posix(),
        "population_spec": {"start": "2024-01-02", "end": "2024-12-31"},
    }
    request["request_sha256"] = canonical_sha256(request)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "request_sha256": request["request_sha256"],
        "source_sha256": "a" * 64,
        "effect_evidence": "INCONCLUSIVE",
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    bundle = (
        timing_root
        / "research"
        / "action_value_incremental_v1"
        / "bundles"
        / request["request_sha256"]
    )
    frame = pd.DataFrame({"value": [1.0]})
    kwargs = {
        "request": request,
        "coverage": {"outcomes_read": False},
        "core_oof": frame,
        "atr_oof": frame,
        "core_sleeves": frame,
        "atr_sleeves": frame,
        "paired_daily": frame,
        "receipt": receipt,
    }

    _publish_bundle(bundle, **kwargs)
    first = inspect_increment_bundle(bundle)
    _publish_bundle(bundle, **kwargs)
    second = inspect_increment_bundle(bundle)
    assert first["manifest"] == second["manifest"]

    first_registry = _deliver_registry(request, bundle, receipt)
    second_registry = _deliver_registry(request, bundle, receipt)
    records = AdvisoryResearchTrialRegistryV1(
        timing_root / "research_registry" / "timing_trial_registry_v1.jsonl"
    ).read()
    assert first_registry["appended_count"] == 1
    assert first_registry["duplicate_noop_count"] == 0
    assert second_registry["appended_count"] == 0
    assert second_registry["duplicate_noop_count"] == 1
    assert first_registry["registry_sha256"] == second_registry["registry_sha256"]
    assert len(records) == 1
    assert records[0].planned_trial_count == 1
    assert records[0].selected_trial_count == 0

    with (bundle / "paired_daily_increment.parquet").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ActionValueError, match="INCREMENT_BUNDLE_FILE_IDENTITY_MISMATCH"):
        inspect_increment_bundle(bundle)

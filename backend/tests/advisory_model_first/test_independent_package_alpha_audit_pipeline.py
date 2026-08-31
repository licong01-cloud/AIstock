from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.independent_package_alpha_audit_contracts import (
    ARM_IDS,
    CURRENT_PARENT_ARM_ID,
    PACKAGE_ARM_IDS,
    PACKAGE_IDS,
    PACKAGE_STATUSES,
    FACTOR_CLOSURE_50,
    FACTOR_CLOSURE_57,
    PKG_378_ARM_ID,
    PKG_5A5_ARM_ID,
    PKG_B668_ARM_ID,
    build_independent_package_alpha_audit_request,
)
from backend.services.advisory_model_first.independent_package_alpha_audit_pipeline import (
    IndependentPackageAuditMetricResult,
    _publish_bundle,
    _rank_one_arm,
    _read_bundle,
    _freeze_package_roster,
    build_independent_package_metrics,
)
from backend.services.advisory_model_first.strategy_package_batch_prediction import (
    PackagePredictionBatchResult,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.services.strategy_package.models import AlphaMode, PackageStatus
from backend.tests.advisory_model_first.test_alpha_signal_audit_pipeline import _n1_request
from backend.tests.advisory_model_first.test_independent_package_alpha_audit_contracts import _values


def _decision_dates() -> pd.DatetimeIndex:
    candidates = pd.bdate_range("2024-07-04", "2026-02-02")
    return pd.DatetimeIndex([*candidates[:385], pd.Timestamp("2026-02-02")])


def _synthetic_inputs():
    decisions = _decision_dates()
    symbols = [f"{index:06d}.SZ" for index in range(1, 61)]
    outcome_rows: list[dict] = []
    parent_rows: list[dict] = []
    package_rows = {arm_id: [] for arm_id in PACKAGE_ARM_IDS}
    coverage_rows: list[dict] = []
    ranking_rows: list[dict] = []
    limits = {PKG_378_ARM_ID: 60, PKG_5A5_ARM_ID: 58, PKG_B668_ARM_ID: 54}
    for day_index, decision in enumerate(decisions):
        day_outcomes: list[dict] = []
        for symbol_index, symbol in enumerate(symbols, start=1):
            slot_return = float(symbol_index * 5 + np.sin(day_index / 20.0) * 10)
            row = {
                "decision_as_of_trade_date": decision,
                "instrument": symbol,
                "target_trade_date": decision + pd.offsets.BDay(1),
                "planned_exit_trade_date": decision + pd.offsets.BDay(20),
                "effective_exit_trade_date": decision + pd.offsets.BDay(20),
                "outcome_status": "MATURED",
                "entry_price": 100.0,
                "exit_price": 101.0,
                "gross_excess_return_bps": slot_return + 5.0,
                "economic_net_excess_bps": slot_return,
                "outcome_known": True,
                "slot_return_bps": slot_return,
            }
            outcome_rows.append(row)
            day_outcomes.append(row)
            parent_rows.append({**row, "score__IC_WEIGHTED_PARENT": float(symbol_index)})
            package_scores = {
                PKG_378_ARM_ID: float(symbol_index),
                PKG_5A5_ARM_ID: float(np.sin(symbol_index * 0.3 + day_index * 0.01)),
                PKG_B668_ARM_ID: float(-symbol_index),
            }
            for arm_id, limit in limits.items():
                if symbol_index <= limit:
                    package_rows[arm_id].append(
                        {"datetime": decision, "instrument": symbol, "score": package_scores[arm_id]}
                    )
        for rank, row in enumerate(
            sorted(day_outcomes, key=lambda item: item["instrument"], reverse=True)[:50], start=1
        ):
            ranking_rows.append(
                {
                    "arm_id": "IC_WEIGHTED_PARENT",
                    "decision_as_of_trade_date": decision,
                    "instrument": row["instrument"],
                    "selection_effective_rank": rank,
                    "target_trade_date": row["target_trade_date"],
                    "combined_score": float(int(row["instrument"].split(".")[0])),
                }
            )
        for arm_id, limit in limits.items():
            coverage_rows.append(
                {
                    "arm_id": arm_id,
                    "decision_as_of_trade_date": decision,
                    "feature_input_count": limit,
                    "fully_scorable_feature_count": limit,
                    "finite_score_count": limit,
                    "missing_feature_row_count": 60 - limit,
                    "missing_feature_cell_count": 60 - limit,
                    "pit_or_market_absent_count": 60 - limit,
                }
            )
    outcomes = pd.DataFrame(outcome_rows)
    outcome_coverage = pd.DataFrame(
        {
            "decision_as_of_trade_date": decisions,
            "pit_member_count": 60,
            "known_outcome_count": 60,
            "matured_outcome_count": 60,
            "not_entered_count": 0,
            "unknown_outcome_count": 0,
            "known_outcome_fraction": 1.0,
            "status": "AVAILABLE",
        }
    )
    package_predictions = {
        arm_id: pd.DataFrame(rows).set_index(["datetime", "instrument"]).sort_index()
        for arm_id, rows in package_rows.items()
    }
    benchmark_dates = pd.bdate_range("2024-06-03", "2026-02-02")
    benchmark = pd.DataFrame(
        {
            "datetime": benchmark_dates,
            "instrument": "000300.SH",
            "open": np.linspace(100.0, 102.0, len(benchmark_dates)),
            "close": np.linspace(100.0, 104.0, len(benchmark_dates)),
        }
    ).set_index(["datetime", "instrument"])
    return {
        "parent_signal_outcomes": pd.DataFrame(parent_rows),
        "parent_rankings_top50": pd.DataFrame(ranking_rows),
        "package_predictions": package_predictions,
        "package_coverage": pd.DataFrame(coverage_rows),
        "outcomes": outcomes,
        "outcome_coverage": outcome_coverage,
        "benchmark_daily": benchmark,
        "decision_dates": decisions,
        "trading_calendar": pd.bdate_range("2024-06-03", "2026-03-10"),
        "n1_request": _n1_request(),
    }


def _fast_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.services.advisory_model_first import independent_package_alpha_audit_pipeline as pipeline
    from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
        _describe_daily_metric as actual_describe,
    )

    def describe(values, *, block_length, repetitions, seed):  # noqa: ANN001, ANN202
        return actual_describe(
            values,
            block_length=block_length,
            repetitions=20,
            seed=seed,
        )

    monkeypatch.setattr(pipeline, "_describe_daily_metric", describe)
    monkeypatch.setattr(pipeline, "_bucket_return_summary", lambda *_args, **_kwargs: [])


def test_four_arm_metrics_preserve_own_universe_and_fixed_pairwise_common_keys(monkeypatch) -> None:
    _fast_bootstrap(monkeypatch)
    inputs = _synthetic_inputs()
    result = build_independent_package_metrics(**inputs)

    coverage = result.coverage_daily.groupby("arm_id")["prediction_count"].min().to_dict()
    assert coverage == {
        CURRENT_PARENT_ARM_ID: 60,
        PKG_378_ARM_ID: 60,
        PKG_5A5_ARM_ID: 58,
        PKG_B668_ARM_ID: 54,
    }
    assert set(result.arm_summary["arms"]) == set(ARM_IDS)
    assert len(result.pairwise_summary["pairs"]) == 6
    parent_378 = result.pairwise_summary["pairs"][f"{CURRENT_PARENT_ARM_ID}_MINUS_{PKG_378_ARM_ID}"]
    parent_b668 = result.pairwise_summary["pairs"][f"{CURRENT_PARENT_ARM_ID}_MINUS_{PKG_B668_ARM_ID}"]
    assert parent_378["pairwise_common_row_count"] == len(inputs["decision_dates"]) * 60
    assert parent_b668["pairwise_common_row_count"] == len(inputs["decision_dates"]) * 54
    assert result.pairwise_summary["pairwise_universe_semantics"] == "PAIR_ONLY_COMMON_KEYS"
    assert result.arm_summary["universe_semantics"] == "ARM_OWN_UNIVERSE_NO_FOUR_ARM_INTERSECTION"


def test_future_outcome_poison_does_not_change_package_rankings() -> None:
    inputs = _synthetic_inputs()
    prediction = inputs["package_predictions"][PKG_378_ARM_ID].reset_index().rename(
        columns={"datetime": "decision_as_of_trade_date"}
    )
    merged = prediction.merge(
        inputs["outcomes"], on=["decision_as_of_trade_date", "instrument"], validate="one_to_one"
    )
    merged.insert(0, "arm_id", PKG_378_ARM_ID)
    targets = {
        pd.Timestamp(decision).normalize(): pd.Timestamp(frame["target_trade_date"].iloc[0]).normalize()
        for decision, frame in merged.groupby("decision_as_of_trade_date")
    }
    first = _rank_one_arm(merged, arm_id=PKG_378_ARM_ID, target_trade_dates=targets)
    poisoned = merged.copy()
    poisoned["economic_net_excess_bps"] *= -1000.0
    poisoned["slot_return_bps"] *= -1000.0
    second = _rank_one_arm(poisoned, arm_id=PKG_378_ARM_ID, target_trade_dates=targets)

    pd.testing.assert_frame_equal(first, second)


def _bundle_request(tmp_path: Path):
    values = _values()
    values["output_root"] = str(tmp_path)
    values["prediction_store_root"] = str(tmp_path / "prediction_store")
    values["registry_path"] = str(tmp_path / "registry.jsonl")
    return build_independent_package_alpha_audit_request(**values)


def test_bundle_is_zero_trial_immutable_and_exact_retry(tmp_path: Path) -> None:
    request = _bundle_request(tmp_path)
    descriptors = {
        arm_id: PredictionArtifactDescriptor(
            run_id=f"run_{arm_id}",
            run_key=f"run_{arm_id}",
            artifact_uri=f"aistock-prediction-store://runs/run_{arm_id}/prediction",
            artifact_sha256=(str(index + 1) * 64)[:64],
            size_bytes=1,
            row_count=1,
            date_start="2024-07-04",
            date_end="2026-02-02",
        )
        for index, arm_id in enumerate(PACKAGE_ARM_IDS)
    }
    prediction_identity = canonical_json_sha256(
        {arm_id: descriptor.model_dump(mode="json") for arm_id, descriptor in sorted(descriptors.items())}
    )
    causality = {"receipt_sha256": "9" * 64, "status": "PASS"}
    batch_receipt = {
        "prediction_identity_sha256": prediction_identity,
        "causality_parity_sha256": causality["receipt_sha256"],
        "temp_peak_bytes": 1,
    }
    batch = PackagePredictionBatchResult(
        predictions={},
        coverage_daily=pd.DataFrame(),
        prediction_descriptors=descriptors,
        prediction_store_run_ids={arm_id: f"run_{arm_id}" for arm_id in PACKAGE_ARM_IDS},
        batch_receipt=batch_receipt,
        causality_parity_receipt=causality,
    )
    metrics = IndependentPackageAuditMetricResult(
        coverage_daily=pd.DataFrame(
            {"arm_id": ARM_IDS, "decision_as_of_trade_date": pd.Timestamp("2024-07-04")}
        ),
        arm_signal_outcomes=pd.DataFrame({"arm_id": ARM_IDS, "instrument": "000001.SZ"}),
        rankings_top50=pd.DataFrame({"arm_id": ARM_IDS}),
        recall_daily=pd.DataFrame({"arm_id": ARM_IDS, "status": "AVAILABLE"}),
        top5_daily=pd.DataFrame({"arm_id": ARM_IDS, "status": "AVAILABLE"}),
        oracle_daily=pd.DataFrame({"arm_id": ARM_IDS}),
        signal_metrics_daily=pd.DataFrame({"arm_id": ARM_IDS}),
        arm_summary={"schema_version": "test"},
        pairwise_summary={"schema_version": "test"},
        regime_quarter_summary=pd.DataFrame({"period": ["2024Q3"]}),
    )
    source = {"sealed_holdout_accessed": False}
    source["source_identity_sha256"] = canonical_json_sha256(source)
    arguments = {
        "request": request,
        "environment": {"python": "test"},
        "source_receipt": source,
        "package_inventory": {"packages": []},
        "batch": batch,
        "metrics": metrics,
        "resource_report": {"peak_rss_bytes": 1, "temp_peak_bytes": 1, "total_wall_seconds": 1},
    }

    first = _publish_bundle(**arguments)
    second = _publish_bundle(**arguments)
    loaded = _read_bundle(first)

    assert first == second
    assert loaded["record"].planned_trial_count == 0
    assert loaded["record"].decision_use.value == "NAVIGATION_ONLY"
    assert loaded["manifest"]["sealed_holdout_accessed"] is False


def test_run_authorizes_development_window_before_batch_or_scientific_loaders(tmp_path, monkeypatch) -> None:
    from backend.services.advisory_model_first import independent_package_alpha_audit_pipeline as pipeline

    request = _bundle_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(pipeline, "_load_and_verify_bound_requests", lambda _request: (_n1_request(), object()))

    def deny(_request):  # noqa: ANN001, ANN202
        events.append("authorize")
        raise AdvisoryModelFirstError("denied", reason_code="ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED")

    monkeypatch.setattr(pipeline, "authorize_n1_development_access", deny)

    class ForbiddenBatch:
        def __init__(self):
            events.append("batch")
            raise AssertionError("batch must not be constructed")

    monkeypatch.setattr(pipeline, "StrategyPackageBatchPredictionRunner", ForbiddenBatch)
    with pytest.raises(AdvisoryModelFirstError) as captured:
        pipeline.run_independent_package_alpha_audit(request_path)

    assert captured.value.reason_code == "ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED"
    assert events == ["authorize"]


def test_package_freeze_uses_only_package_owned_source_and_self_contains_factor_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.services.advisory_model_first import independent_package_alpha_audit_pipeline as pipeline

    counts = dict(zip(PACKAGE_IDS, (57, 57, 50)))

    def manifest(package_id: str):  # noqa: ANN202
        count = counts[package_id]
        factors = [
            SimpleNamespace(factor_name=f"factor_{index:03d}", sha256=f"{index + 1:064x}")
            for index in range(count)
        ]
        model = SimpleNamespace(
            model_id=f"model_{package_id[-6:]}",
            sha256="a" * 64,
            model_code_assets=[],
        )
        value = SimpleNamespace(
            package_id=package_id,
            alpha_mode=AlphaMode.SINGLE_ALPHA,
            alpha_components=[SimpleNamespace(score_direction="higher_better")],
            factor_set=factors,
            model_asset=model,
        )
        value.model_dump = lambda **_kwargs: {
            "package_id": package_id,
            "alpha_mode": "single_alpha",
            "backtest_summary": {"rank_ic": 0.01},
        }
        return value

    records = {
        package_id: SimpleNamespace(
            package_id=package_id,
            package_status=PackageStatus(status),
            alpha_mode=AlphaMode.SINGLE_ALPHA,
            manifest_sha256=(str(index + 1) * 64)[:64],
            current_manifest=lambda package_id=package_id: manifest(package_id),
        )
        for index, (package_id, status) in enumerate(zip(PACKAGE_IDS, PACKAGE_STATUSES))
    }

    class Repository:
        def get(self, package_id: str):  # noqa: ANN202
            return records[package_id]

    class Resolver:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def load_frozen_source_for_strategy_package(self, *, manifest, package_id, cache_namespace):  # noqa: ANN001, ANN202
            self.calls.append(("load_frozen", package_id))
            source_root = tmp_path / "package_owned_sources" / package_id
            factors = source_root / "factors"
            factors.mkdir(parents=True)
            for factor in manifest.factor_set:
                (factors / f"{factor.factor_name}.py").write_text("# frozen factor\n", encoding="utf-8")
            return SimpleNamespace(package_id=package_id, factor_source_dir=factors)

        def prepare_workspace(self, *, package_id, source, **_kwargs):  # noqa: ANN001, ANN202
            self.calls.append(("prepare", package_id))
            workspace = tmp_path / "runtime" / package_id
            (workspace / "model").mkdir(parents=True)
            order = [path.stem for path in sorted(source.factor_source_dir.glob("*.py"))]
            (workspace / "factor_order.json").write_text(
                json.dumps({"factor_order": order, "alpha158_factors": [], "dynamic_factors": order}),
                encoding="utf-8",
            )
            (workspace / "manifest.json").write_text(
                json.dumps({"primary_assets": {"model_weight_relpath": "model/params.pkl"}}),
                encoding="utf-8",
            )
            (workspace / "model" / "params.pkl").write_bytes(b"model")
            convert = _kwargs["path_converter"]
            factor_files = {
                name: convert(str(source.factor_source_dir / f"{name}.py")) for name in order
            }
            entry = workspace / "strategy_package_factor_entry.py"
            entry.write_text(f"_FACTOR_FILES = {factor_files!r}\n", encoding="utf-8")
            return SimpleNamespace(
                workspace_path=workspace,
                factor_entry_path=entry,
                factor_source_dir=source.factor_source_dir,
                factor_order=order,
                dynamic_factors=order,
            )

    resolver = Resolver()
    monkeypatch.setattr(
        pipeline,
        "_factor_closure",
        lambda _manifest, order: FACTOR_CLOSURE_57 if len(order) == 57 else FACTOR_CLOSURE_50,
    )

    arms = _freeze_package_roster(
        repository=Repository(),
        resolver=resolver,
        output_root=tmp_path / "output",
    )

    assert resolver.calls == [
        action
        for package_id in PACKAGE_IDS
        for action in (("load_frozen", package_id), ("prepare", package_id))
    ]
    assert tuple(item.package_id for item in arms) == PACKAGE_IDS
    for arm in arms:
        described = {item.relative_path for item in arm.workspace_files}
        assert any(path.startswith("frozen_factor_sources/") for path in described)
        entry = (pipeline._local_path(arm.workspace_root) / "strategy_package_factor_entry.py").read_text(
            encoding="utf-8"
        )
        assert "package_owned_sources" not in entry

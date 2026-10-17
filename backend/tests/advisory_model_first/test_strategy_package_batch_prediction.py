from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.independent_package_alpha_audit_contracts import (
    FACTOR_CLOSURE_57,
    PACKAGE_378_ID,
    PACKAGE_5A5_ID,
    PACKAGE_B668_ID,
    PKG_378_ARM_ID,
    PKG_5A5_ARM_ID,
    RESOURCE_MAX_WALL_SECONDS,
    FrozenPackageAuditArmV1,
    WorkspaceFileDescriptorV1,
    build_independent_package_alpha_audit_request,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.advisory_model_first.strategy_package_batch_prediction import (
    BatchSourcePanels,
    FACTOR_INPUT_COPY_MODE_COW,
    FACTOR_INPUT_COPY_MODE_FILE,
    FACTOR_IO_MODE_FILE_BACKED,
    FACTOR_IO_MODE_IN_MEMORY,
    FACTOR_RESULT_PROJECTION_MODE_DECISION_DATES,
    FACTOR_RESULT_PROJECTION_MODE_FILE,
    StrategyPackageBatchPredictionRunner,
    _assert_file_backed_feature_parity,
    _prepare_static_panel,
    _publish_prediction_store,
    _slice_panel,
    _virtualized_factor_io,
    run_factor_group_batch,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.tests.advisory_model_first.test_oracle_mini_contract import HASH_A, HASH_B, HASH_C


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ref(role: str, digest: str, uri: str | None = None) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=uri or f"/evidence/{role}.json",
        sha256=digest,
        size_bytes=10,
    )


def _decisions() -> pd.DatetimeIndex:
    anchors = {pd.Timestamp("2024-07-04"), pd.Timestamp("2025-04-22"), pd.Timestamp("2026-02-02")}
    available = [item for item in pd.bdate_range("2024-07-04", "2026-02-02") if item not in anchors]
    return pd.DatetimeIndex(sorted([*anchors, *available[:383]]))


def _history_start(decisions: pd.DatetimeIndex) -> date:
    return pd.bdate_range(end=decisions[0], periods=66)[0].date()


def _pit_snapshot():
    spans = [
        {
            "ts_code": f"{index:06d}.SZ",
            "eligible_start": "2024-07-04",
            "eligible_end": "2026-03-10",
            "entry_reason": "ipo_252td",
            "exit_reason": "generation_end",
        }
        for index in range(1, 61)
    ]
    return freeze_pit_snapshot(
        spans,
        universe_key="aistock_equity_pit_canonical_v2",
        rule_version="test_rule",
        scope_start=date(2024, 7, 4),
        cutoff=date(2026, 3, 10),
        state_identity="test_state",
        source_fingerprint_sha256=HASH_A,
        parameter_hash=HASH_B,
    )


def _workspace(root: Path, *, arm_id: str, factor_count: int) -> tuple[Path, tuple[WorkspaceFileDescriptorV1, ...]]:
    workspace = root / arm_id
    (workspace / "model").mkdir(parents=True)
    order = [f"factor_{index:03d}" for index in range(factor_count)]
    (workspace / "factor_order.json").write_text(
        json.dumps({"factor_order": order, "alpha158_factors": [], "dynamic_factors": order}),
        encoding="utf-8",
    )
    (workspace / "manifest.json").write_text(
        json.dumps(
            {
                "primary_assets": {
                    "factor_entry_relpath": "strategy_package_factor_entry.py",
                    "model_weight_relpath": "model/params.pkl",
                }
            }
        ),
        encoding="utf-8",
    )
    (workspace / "strategy_package_factor_entry.py").write_text("# frozen test factor entry\n", encoding="utf-8")
    (workspace / "model" / "params.pkl").write_bytes(b"test-model")
    files = tuple(
        WorkspaceFileDescriptorV1(
            relative_path=path.relative_to(workspace).as_posix(),
            sha256=_sha(path),
            size_bytes=path.stat().st_size,
        )
        for path in sorted(item for item in workspace.rglob("*") if item.is_file())
    )
    return workspace, files


def _request(tmp_path: Path, snapshot) -> object:  # noqa: ANN001
    specs = (
        (PKG_378_ARM_ID, PACKAGE_378_ID, "BACKTEST_APPROVED", 57, FACTOR_CLOSURE_57, "a"),
        (PKG_5A5_ARM_ID, PACKAGE_5A5_ID, "PAPER_ENABLED", 57, FACTOR_CLOSURE_57, "b"),
    )
    packages = []
    for arm_id, package_id, status, factor_count, closure, seed in specs:
        workspace, files = _workspace(tmp_path / "workspaces", arm_id=arm_id, factor_count=factor_count)
        packages.append(
            FrozenPackageAuditArmV1(
                arm_id=arm_id,
                package_id=package_id,
                package_status=status,
                manifest_sha256=seed * 64,
                package_snapshot_ref=_ref(f"n2b_package_snapshot__{arm_id}", seed * 64),
                factor_count=factor_count,
                factor_closure_sha256=closure,
                model_closure_sha256=("f" if seed != "f" else "e") * 64,
                workspace_root=str(workspace),
                workspace_files=files,
            )
        )
    return build_independent_package_alpha_audit_request(
        n0_completion_ref=_ref("n0_completion", HASH_A),
        n0_completion_receipt_sha256=HASH_B,
        research_window_contract_ref=_ref("n0_window_contract", HASH_B),
        research_window_contract_sha256=HASH_C,
        n1_request_ref=_ref("n1_frozen_request", HASH_A),
        n1_request_sha256=HASH_B,
        n1_bundle_path="/artifacts/n1/bundle",
        n1_bundle_manifest_ref=_ref("n1_formal_bundle_manifest", HASH_C, "/artifacts/n1/bundle/manifest.json"),
        n1_bundle_id=HASH_C,
        n2a_request_ref=_ref("n2a_frozen_request", HASH_A),
        n2a_request_sha256=HASH_B,
        n2a_bundle_path="/artifacts/n2a/bundle",
        n2a_bundle_manifest_ref=_ref("n2a_formal_bundle_manifest", HASH_C, "/artifacts/n2a/bundle/manifest.json"),
        n2a_bundle_id=HASH_C,
        roster_exclusion_ref=_ref("n2b_roster_exclusion_receipt", HASH_A),
        roster_exclusion_bug_id="BUG-1302",
        excluded_package_id=PACKAGE_B668_ID,
        excluded_package_status="RETIRED",
        excluded_package_manifest_sha256="c" * 64,
        excluded_factor_name="neg_vol_adjusted_momentum",
        registry_path=str(tmp_path / "registry.jsonl"),
        program_id="program",
        binding_version_id="binding",
        current_parent_package_id="pkg_parent",
        current_parent_manifest_sha256=HASH_A,
        selection_runtime_semantics_hash=HASH_B,
        baseline_policy_sha256=HASH_A,
        shadow_policy_sha256=HASH_B,
        cost_policy_sha256=HASH_C,
        split_policy_sha256="d" * 64,
        pit_spans_sha256=snapshot.spans_sha256,
        feature_schema_hash="f" * 64,
        packages=tuple(packages),
        repository_root=str(tmp_path / "repo"),
        repository_commit="7" * 40,
        prediction_store_root=str(tmp_path / "prediction_store"),
        output_root=str(tmp_path / "output"),
        created_at="2026-08-31T00:00:00Z",
    )


def _source(decisions: pd.DatetimeIndex) -> BatchSourcePanels:
    lookback = pd.bdate_range(end=decisions[0], periods=66)
    source_dates = pd.DatetimeIndex(sorted(set(lookback).union(decisions)))
    instruments = [f"{index:06d}.SZ" for index in range(1, 61)]
    index = pd.MultiIndex.from_product([source_dates, instruments], names=["datetime", "instrument"])
    daily = pd.DataFrame(
        {
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.0,
            "volume": 100.0,
            "amount": 1000.0,
            "factor": 1.0,
        },
        index=index,
    )
    static = pd.DataFrame({"turnover_rate": 1.0}, index=index)
    return BatchSourcePanels(
        daily=daily,
        static_raw=static,
        history_start=_history_start(decisions),
        decision_end=date(2026, 2, 2),
        source_receipts=({"source_role": "fake"},),
        static_precomputed=True,
    )


def _fake_factor_runner(
    workspace: Path,
    _closure: str,
    source: BatchSourcePanels,
    decision_dates: list[date],
    _temp_root: Path,
) -> pd.DataFrame:
    order = json.loads((workspace / "factor_order.json").read_text(encoding="utf-8"))["factor_order"]
    wanted = pd.DatetimeIndex(pd.to_datetime(decision_dates)).normalize()
    dates = pd.to_datetime(source.daily.index.get_level_values("datetime")).normalize()
    index = source.daily.index[dates.isin(wanted)]
    instruments = index.get_level_values("instrument").str[:6].astype(int).to_numpy(dtype=float)
    day_values = pd.to_datetime(index.get_level_values("datetime")).map(pd.Timestamp.toordinal).to_numpy(dtype=float)
    base = instruments + day_values / 1_000_000.0
    return pd.DataFrame(
        {name: base + column * 1e-6 for column, name in enumerate(order)},
        index=index,
    )


def test_instrument_slice_is_independent_without_a_second_deep_copy() -> None:
    index = pd.MultiIndex.from_product(
        [pd.date_range("2026-01-02", periods=3), ["000001.SZ", "000002.SZ", "000003.SZ"]],
        names=["datetime", "instrument"],
    )
    source = pd.DataFrame({"close": np.arange(len(index), dtype="float64")}, index=index)

    sliced = _slice_panel(
        source,
        start=date(2026, 1, 2),
        cutoff=date(2026, 1, 4),
        instruments={"000001.SZ", "000003.SZ"},
    )

    assert set(sliced.index.get_level_values("instrument")) == {"000001.SZ", "000003.SZ"}
    assert not np.shares_memory(
        source["close"].to_numpy(copy=False),
        sliced["close"].to_numpy(copy=False),
    )
    sliced.iloc[0, 0] = -1.0
    assert source.iloc[0, 0] == 0.0


def test_static_precompute_uses_its_single_owned_copy(monkeypatch) -> None:
    index = pd.MultiIndex.from_product(
        [pd.date_range("2026-01-02", periods=2), ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    static_raw = pd.DataFrame({"pb": np.arange(1, len(index) + 1, dtype="float64")}, index=index)
    daily = pd.DataFrame({"close": np.arange(10, 10 + len(index), dtype="float64")}, index=index)
    observed: dict[str, bool] = {}

    def compute_once(static: pd.DataFrame, observed_daily: pd.DataFrame) -> pd.DataFrame:
        observed["shares_input"] = np.shares_memory(
            static["db_pb"].to_numpy(copy=False),
            static_raw["pb"].to_numpy(copy=False),
        )
        assert observed_daily is daily
        result = static.copy()
        result["derived"] = 1.0
        return result

    monkeypatch.setattr(
        "backend.services.advisory_model_first.strategy_package_batch_prediction.compute_precomputed_factors",
        compute_once,
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.strategy_package_batch_prediction.validate_precomputed_factors",
        lambda _frame: (True, []),
    )

    prepared = _prepare_static_panel(static_raw, daily, canonicalized=True)

    assert observed == {"shares_input": True}
    prepared.iloc[0, 0] = -1.0
    assert static_raw.iloc[0, 0] == 1.0


def test_batch_runner_reads_once_groups_once_and_loads_each_model_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.services.advisory_model_first.strategy_package_batch_prediction as batch_module

    decisions = _decisions()
    assert len(decisions) == 386
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)
    counts = {"source": 0, "factor": 0, "model": 0}
    exact_window_day_counts: list[int] = []
    original_dont_write_bytecode = sys.dont_write_bytecode

    def source_loader(_universe, _start, _end):  # noqa: ANN001, ANN202
        counts["source"] += 1
        return _source(decisions)

    def factor_runner(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        counts["factor"] += 1
        virtualize_io = kwargs.pop("virtualize_io", True)
        kwargs.pop("reusable_factor_values", None)
        if args[2].decision_end == args[3][0]:
            exact_window_day_counts.append(len(args[2].daily.index.get_level_values("datetime").unique()))
        frame = _fake_factor_runner(*args, **kwargs)
        frame.attrs["factor_resource_receipt"] = {
            "factor_io_mode": (FACTOR_IO_MODE_IN_MEMORY if virtualize_io else FACTOR_IO_MODE_FILE_BACKED),
            "factor_input_copy_mode": (FACTOR_INPUT_COPY_MODE_COW if virtualize_io else FACTOR_INPUT_COPY_MODE_FILE),
            "factor_result_projection_mode": (
                FACTOR_RESULT_PROJECTION_MODE_DECISION_DATES if virtualize_io else FACTOR_RESULT_PROJECTION_MODE_FILE
            ),
            "temp_peak_bytes": 0,
            "factor_calculation_count": len(frame.columns),
            "factor_reuse_count": 0,
            "result_write_count": len(frame.columns) if virtualize_io else 0,
            "projected_result_write_count": len(frame.columns) if virtualize_io else 0,
            "fallback_result_write_count": 0,
        }
        return frame

    def model_loader(path: Path):  # noqa: ANN202
        assert sys.dont_write_bytecode is True
        counts["model"] += 1
        order = json.loads((path.parents[1] / "factor_order.json").read_text(encoding="utf-8"))["factor_order"]
        return object(), "fake", None, len(order)

    monkeypatch.setattr(batch_module, "run_factor_group_batch", factor_runner)
    runner = StrategyPackageBatchPredictionRunner(
        source_loader=source_loader,
        model_loader=model_loader,
        model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
        history_start_resolver=lambda _start, _window: _history_start(decisions),
    )
    result = runner.run(
        request=request,
        pit_snapshot=snapshot,
        decision_dates=decisions,
        temp_root=tmp_path / "temp",
    )

    assert counts == {"source": 1, "factor": 390, "model": 2}
    assert sys.dont_write_bytecode is original_dont_write_bytecode
    assert result.batch_receipt["primary_factor_group_run_count"] == 386
    assert result.batch_receipt["primary_decision_batch_count"] == 386
    assert result.batch_receipt["primary_factor_group_run_count_per_decision"] == 1
    assert result.batch_receipt["diagnostic_factor_group_run_count"] == 3
    assert result.batch_receipt["daily_wsl_process_count"] == 0
    assert result.batch_receipt["daily_db_query_count"] == 0
    assert result.batch_receipt["factor_io_mode"] == FACTOR_IO_MODE_IN_MEMORY
    assert result.batch_receipt["factor_input_copy_mode"] == FACTOR_INPUT_COPY_MODE_COW
    assert result.batch_receipt["factor_result_projection_mode"] == FACTOR_RESULT_PROJECTION_MODE_DECISION_DATES
    assert result.batch_receipt["result_write_count"] == result.batch_receipt["factor_calculation_count"]
    assert result.batch_receipt["projected_result_write_count"] == result.batch_receipt["factor_calculation_count"]
    assert result.batch_receipt["fallback_result_write_count"] == 0
    assert result.batch_receipt["wall_limit_enabled"] is False
    assert result.batch_receipt["wall_limit_seconds"] is None
    assert result.batch_receipt["temp_storage_mode"] == "ENVIRONMENT_LOCAL_EPHEMERAL"
    assert len(result.batch_receipt["factor_resource_receipts"]) == 389
    assert result.batch_receipt["file_backed_parity_factor_group_run_count"] == 1
    assert result.batch_receipt["all_factor_group_run_count"] == 390
    assert len(result.batch_receipt["file_backed_parity_receipts"]) == 1
    assert {item["status"] for item in result.batch_receipt["file_backed_parity_receipts"]} == {"PASS"}
    assert set(result.batch_receipt["required_window_by_closure"].values()) == {61}
    assert result.batch_receipt["window_buffer_trading_days"] == 5
    assert result.batch_receipt["rolling_live_window_semantics"] is True
    assert set(exact_window_day_counts) == {66}
    assert set(result.predictions) == {PKG_378_ARM_ID, PKG_5A5_ARM_ID}
    assert all(len(frame) == 386 * 60 for frame in result.predictions.values())
    assert all(descriptor.row_count == 386 * 60 for descriptor in result.prediction_descriptors.values())
    assert result.causality_parity_receipt["status"] == "PASS"
    assert len(result.causality_parity_receipt["checks"]) == 6

    retry_descriptors, retry_run_ids = _publish_prediction_store(
        request=request,
        predictions=result.predictions,
    )
    assert {arm_id: descriptor.artifact_sha256 for arm_id, descriptor in retry_descriptors.items()} == {
        arm_id: descriptor.artifact_sha256 for arm_id, descriptor in result.prediction_descriptors.items()
    }
    assert retry_run_ids == result.prediction_store_run_ids


def test_batch_runner_rejects_workspace_file_roster_drift(tmp_path: Path) -> None:
    decisions = _decisions()
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)
    (Path(request.packages[0].workspace_root) / "unexpected.py").write_text("x = 1\n", encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as raised:
        StrategyPackageBatchPredictionRunner(
            source_loader=lambda _universe, _start, _end: _source(decisions),
            factor_runner=_fake_factor_runner,
            model_loader=lambda _path: (object(), "fake", None, 57),
            model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
            history_start_resolver=lambda _start, _window: _history_start(decisions),
        ).run(
            request=request,
            pit_snapshot=snapshot,
            decision_dates=decisions,
            temp_root=tmp_path / "temp",
        )

    assert raised.value.reason_code == "ADVISORY_PACKAGE_BATCH_ASSET_INVALID"


def test_batch_runner_rejects_model_loader_workspace_mutation(tmp_path: Path) -> None:
    decisions = _decisions()
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)
    original_dont_write_bytecode = sys.dont_write_bytecode

    def mutating_model_loader(path: Path):  # noqa: ANN202
        assert sys.dont_write_bytecode is True
        cache = path.parent / "__pycache__" / "model.cpython-310.pyc"
        cache.parent.mkdir()
        cache.write_bytes(b"unexpected bytecode")
        return object(), "fake", None, 57

    with pytest.raises(AdvisoryModelFirstError) as raised:
        StrategyPackageBatchPredictionRunner(
            source_loader=lambda _universe, _start, _end: _source(decisions),
            factor_runner=_fake_factor_runner,
            model_loader=mutating_model_loader,
            model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
            history_start_resolver=lambda _start, _window: _history_start(decisions),
        ).run(
            request=request,
            pit_snapshot=snapshot,
            decision_dates=decisions,
            temp_root=tmp_path / "temp",
        )

    assert raised.value.reason_code == "ADVISORY_PACKAGE_BATCH_ASSET_INVALID"
    assert sys.dont_write_bytecode is original_dont_write_bytecode


def test_batch_runner_rejects_future_sensitive_factor_output(tmp_path: Path) -> None:
    decisions = _decisions()
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)

    def malicious_factor_runner(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        frame = _fake_factor_runner(*args, **kwargs)
        decision_dates = args[3]
        if (
            len(decision_dates) == 1
            and decision_dates[0] == date(2024, 7, 4)
            and args[2].decision_end > decision_dates[0]
        ):
            reversed_values = -frame.iloc[:, 0].to_numpy()
            for column in frame.columns:
                frame[column] = reversed_values
        return frame

    runner = StrategyPackageBatchPredictionRunner(
        source_loader=lambda _universe, _start, _end: _source(decisions),
        factor_runner=malicious_factor_runner,
        model_loader=lambda path: (
            object(),
            "fake",
            None,
            len(json.loads((path.parents[1] / "factor_order.json").read_text(encoding="utf-8"))["factor_order"]),
        ),
        model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
        history_start_resolver=lambda _start, _window: _history_start(decisions),
    )
    with pytest.raises(AdvisoryModelFirstError) as raised:
        runner.run(
            request=request,
            pit_snapshot=snapshot,
            decision_dates=decisions,
            temp_root=tmp_path / "temp",
        )
    assert raised.value.reason_code == "ADVISORY_PACKAGE_BATCH_FUTURE_DEPENDENCY_DETECTED"


def test_batch_runner_rejects_factor_order_drift_inside_shared_closure(tmp_path: Path) -> None:
    decisions = _decisions()
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)
    second = Path(request.packages[1].workspace_root) / "factor_order.json"
    payload = json.loads(second.read_text(encoding="utf-8"))
    payload["factor_order"][-1] = "different_factor"
    second.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as raised:
        StrategyPackageBatchPredictionRunner(
            source_loader=lambda _universe, _start, _end: _source(decisions),
            factor_runner=_fake_factor_runner,
            model_loader=lambda _path: (object(), "fake", None, 57),
            model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
            history_start_resolver=lambda _start, _window: _history_start(decisions),
        ).run(
            request=request,
            pit_snapshot=snapshot,
            decision_dates=decisions,
            temp_root=tmp_path / "temp",
        )
    assert raised.value.reason_code == "ADVISORY_PACKAGE_BATCH_ASSET_INVALID"


def test_real_factor_batch_uses_one_physical_static_h5_with_hardlink_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.services.advisory_model_first.strategy_package_batch_prediction as batch_module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    factor_source = workspace / "factor_source.py"
    factor_source.write_text("# causal frozen factor source\n", encoding="utf-8")
    (workspace / "factor_order.json").write_text(
        json.dumps(
            {
                "factor_order": ["factor_000"],
                "alpha158_factors": [],
                "dynamic_factors": ["factor_000"],
            }
        ),
        encoding="utf-8",
    )
    (workspace / "strategy_package_factor_entry.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import pandas as pd",
                f"_FACTOR_FILES = {{'factor_000': {str(factor_source)!r}}}",
                "def calculate_001_factor_000():",
                "    frame = pd.read_hdf(Path.cwd() / 'daily_pv.h5')",
                "    static = pd.read_parquet(Path.cwd() / 'static_factors.parquet', columns=['db_turnover_rate'])",
                "    momentum = frame['close'].groupby(level='instrument').pct_change(fill_method=None)",
                "    combined = momentum.add(static['db_turnover_rate'] * 0.001)",
                "    result = combined.groupby(level='datetime').rank(pct=True).to_frame('factor_000')",
                "    path = Path.cwd() / 'result.h5'",
                "    result.to_hdf(path, key='data', mode='w')",
                "    return pd.read_hdf(path)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    days = pd.DatetimeIndex(["2025-04-21", "2025-04-22"])
    instruments = [f"{index:06d}.SZ" for index in range(1, 61)]
    index = pd.MultiIndex.from_product([days, instruments], names=["datetime", "instrument"])
    daily = pd.DataFrame(
        {
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": np.arange(len(index), dtype=float) + 1.0,
            "volume": 100.0,
            "amount": 1000.0,
            "factor": 1.0,
        },
        index=index,
    )
    source = BatchSourcePanels(
        daily=daily,
        static_raw=pd.DataFrame({"turnover_rate": 1.0}, index=index),
        history_start=date(2025, 4, 21),
        decision_end=date(2025, 4, 22),
        source_receipts=(),
    )
    monkeypatch.setattr(batch_module, "compute_precomputed_factors", lambda static, _daily: static)
    monkeypatch.setattr(batch_module, "validate_precomputed_factors", lambda _static: (True, []))

    file_backed = run_factor_group_batch(
        workspace,
        HASH_A,
        source,
        [date(2025, 4, 22)],
        tmp_path / "temp_file",
        virtualize_io=False,
    )
    reusable: dict[tuple[str, str], pd.Series] = {}
    features = run_factor_group_batch(
        workspace,
        HASH_A,
        source,
        [date(2025, 4, 22)],
        tmp_path / "temp_memory",
        virtualize_io=True,
        reusable_factor_values=reusable,
    )
    reused = run_factor_group_batch(
        workspace,
        HASH_A,
        source,
        [date(2025, 4, 22)],
        tmp_path / "temp_reused",
        virtualize_io=True,
        reusable_factor_values=reusable,
    )
    drift_workspace = tmp_path / "workspace_drift"
    drift_workspace.mkdir()
    drift_source = drift_workspace / "factor_source.py"
    drift_source.write_text("# causal frozen factor source with different identity\n", encoding="utf-8")
    (drift_workspace / "factor_order.json").write_text(
        (workspace / "factor_order.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (drift_workspace / "strategy_package_factor_entry.py").write_text(
        (workspace / "strategy_package_factor_entry.py")
        .read_text(encoding="utf-8")
        .replace(repr(str(factor_source)), repr(str(drift_source))),
        encoding="utf-8",
    )
    drifted = run_factor_group_batch(
        drift_workspace,
        HASH_B,
        source,
        [date(2025, 4, 22)],
        tmp_path / "temp_drifted",
        virtualize_io=True,
        reusable_factor_values=reusable,
    )

    pd.testing.assert_frame_equal(features, file_backed)
    pd.testing.assert_frame_equal(reused, file_backed)
    pd.testing.assert_frame_equal(drifted, file_backed)
    assert features.shape == (60, 1)
    assert features["factor_000"].notna().all()
    receipt = features.attrs["factor_resource_receipt"]
    assert receipt["static_h5_physical_file_count"] == 1
    assert receipt["static_h5_hardlink_alias_count"] == 6
    assert receipt["temp_peak_bytes"] > 0
    assert receipt["factor_io_mode"] == FACTOR_IO_MODE_IN_MEMORY
    assert receipt["factor_input_copy_mode"] == FACTOR_INPUT_COPY_MODE_COW
    assert receipt["factor_result_projection_mode"] == FACTOR_RESULT_PROJECTION_MODE_DECISION_DATES
    assert receipt["factor_calculation_count"] == 1
    assert receipt["factor_reuse_count"] == 0
    assert receipt["result_write_count"] == 1
    assert receipt["projected_result_write_count"] == 1
    assert receipt["fallback_result_write_count"] == 0
    assert reused.attrs["factor_resource_receipt"]["factor_calculation_count"] == 0
    assert reused.attrs["factor_resource_receipt"]["factor_reuse_count"] == 1
    assert drifted.attrs["factor_resource_receipt"]["factor_calculation_count"] == 1
    assert drifted.attrs["factor_resource_receipt"]["factor_reuse_count"] == 0
    assert file_backed.attrs["factor_resource_receipt"]["factor_io_mode"] == FACTOR_IO_MODE_FILE_BACKED
    assert file_backed.attrs["factor_resource_receipt"]["factor_input_copy_mode"] == FACTOR_INPUT_COPY_MODE_FILE
    assert (
        file_backed.attrs["factor_resource_receipt"]["factor_result_projection_mode"]
        == FACTOR_RESULT_PROJECTION_MODE_FILE
    )


def test_virtual_factor_io_projects_result_before_materialization_with_cow_snapshot(
    tmp_path: Path,
) -> None:
    days = pd.DatetimeIndex(["2025-04-21", "2025-04-22"])
    index = pd.MultiIndex.from_product(
        [days, ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame({"close": [1.0, 2.0]}, index=index)
    static = pd.DataFrame({"db_turnover_rate": [1.0, 1.0]}, index=index)
    result = pd.DataFrame({"factor": [10.0, 20.0]}, index=index)

    with _virtualized_factor_io(
        daily=daily,
        static=static,
        result_dates=[date(2025, 4, 22)],
    ):
        clean_path = tmp_path / "daily_pv_clean.h5"
        clean = pd.read_hdf("daily_pv.h5")
        clean.to_hdf(clean_path, key="data", mode="w")
        clean.iloc[:, 0] = 88.0
        captured_clean = pd.read_hdf(clean_path)

        result_path = tmp_path / "result.h5"
        result.to_hdf(result_path, key="data", mode="w")
        result.iloc[:, 0] = 99.0
        captured = pd.read_hdf(result_path)

        series_path = tmp_path / "series" / "result.h5"
        result_series = pd.Series([30.0, 40.0], index=index, name="factor")
        result_series.to_hdf(series_path, key="data", mode="w")
        result_series.iloc[:] = 77.0
        captured_series = pd.read_hdf(series_path)

    pd.testing.assert_frame_equal(captured_clean, daily)
    assert list(captured.index.get_level_values("datetime")) == [pd.Timestamp("2025-04-22")]
    assert captured.iloc[0, 0] == 20.0
    assert list(captured_series.index.get_level_values("datetime")) == [pd.Timestamp("2025-04-22")]
    assert captured_series.iloc[0] == 40.0


def test_virtual_factor_io_falls_back_to_full_result_when_index_needs_normalization(
    tmp_path: Path,
) -> None:
    days = pd.DatetimeIndex(["2025-04-21", "2025-04-22"])
    index = pd.MultiIndex.from_product(
        [days, ["SZ000001"]],
        names=["datetime", "instrument"],
    )
    result = pd.DataFrame({"factor": [10.0, 20.0]}, index=index)
    canonical_index = pd.MultiIndex.from_tuples(
        [(days[-1], "000001.SZ")],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame({"close": [1.0]}, index=canonical_index)

    with _virtualized_factor_io(
        daily=daily,
        static=daily,
        result_dates=[date(2025, 4, 22)],
    ) as projection_stats:
        result_path = tmp_path / "result.h5"
        result.to_hdf(result_path, key="data", mode="w")
        captured = pd.read_hdf(result_path)

    pd.testing.assert_frame_equal(captured, result)
    assert projection_stats == {
        "result_write_count": 1,
        "projected_result_write_count": 0,
        "fallback_result_write_count": 1,
    }


def test_virtual_factor_io_restores_pandas_after_exception() -> None:
    original_read_hdf = pd.read_hdf
    original_read_parquet = pd.read_parquet
    original_frame_to_hdf = pd.DataFrame.to_hdf
    original_series_to_hdf = pd.Series.to_hdf
    original_copy_on_write = bool(pd.options.mode.copy_on_write)
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2025-04-22"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame({"close": [1.0]}, index=index)
    static = pd.DataFrame({"db_turnover_rate": [1.0]}, index=index)

    with pytest.raises(RuntimeError, match="forced"):
        with _virtualized_factor_io(daily=daily, static=static):
            assert pd.read_hdf is not original_read_hdf
            assert pd.read_parquet is not original_read_parquet
            assert pd.options.mode.copy_on_write is True
            mutated = pd.read_hdf("daily_pv.h5")
            mutated.iloc[0, 0] = 9.0
            assert pd.read_hdf("daily_pv.h5").iloc[0, 0] == 1.0
            raise RuntimeError("forced")

    assert pd.read_hdf is original_read_hdf
    assert pd.read_parquet is original_read_parquet
    assert pd.DataFrame.to_hdf is original_frame_to_hdf
    assert pd.Series.to_hdf is original_series_to_hdf
    assert pd.options.mode.copy_on_write is original_copy_on_write


def test_real_closure_parity_rejects_value_drift() -> None:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2025-04-22"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    in_memory = pd.DataFrame({"factor": [1.0]}, index=index)
    file_backed = pd.DataFrame({"factor": [1.000001]}, index=index)

    with pytest.raises(AdvisoryModelFirstError) as raised:
        _assert_file_backed_feature_parity(
            in_memory=in_memory,
            file_backed=file_backed,
            closure_sha256=HASH_A,
            decision_date=date(2025, 4, 22),
        )

    assert raised.value.reason_code == "ADVISORY_PACKAGE_BATCH_LIVE_PARITY_FAILED"


def test_real_factor_batch_rejects_unsupported_virtual_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.services.advisory_model_first.strategy_package_batch_prediction as batch_module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    factor_source = workspace / "factor_source.py"
    factor_source.write_text("import h5py\n", encoding="utf-8")
    (workspace / "factor_order.json").write_text(
        json.dumps(
            {
                "factor_order": ["factor_000"],
                "alpha158_factors": [],
                "dynamic_factors": ["factor_000"],
            }
        ),
        encoding="utf-8",
    )
    (workspace / "strategy_package_factor_entry.py").write_text(
        "\n".join(
            [
                f"_FACTOR_FILES = {{'factor_000': {str(factor_source)!r}}}",
                "def calculate_001_factor_000():",
                "    raise AssertionError('must not run')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    days = pd.DatetimeIndex(["2025-04-22"])
    index = pd.MultiIndex.from_product([days, ["000001.SZ"]], names=["datetime", "instrument"])
    source = BatchSourcePanels(
        daily=pd.DataFrame(
            {
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.0,
                "volume": 100.0,
                "amount": 1000.0,
                "factor": 1.0,
            },
            index=index,
        ),
        static_raw=pd.DataFrame({"turnover_rate": 1.0}, index=index),
        history_start=date(2025, 4, 22),
        decision_end=date(2025, 4, 22),
        source_receipts=(),
    )
    monkeypatch.setattr(batch_module, "compute_precomputed_factors", lambda static, _daily: static)
    monkeypatch.setattr(batch_module, "validate_precomputed_factors", lambda _static: (True, []))

    with pytest.raises(AdvisoryModelFirstError) as raised:
        run_factor_group_batch(
            workspace,
            HASH_A,
            source,
            [date(2025, 4, 22)],
            tmp_path / "temp",
        )

    assert raised.value.reason_code == "ADVISORY_PACKAGE_BATCH_ASSET_INVALID"


def test_batch_runner_reports_unbounded_wall_telemetry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.services.advisory_model_first.strategy_package_batch_prediction as batch_module

    decisions = _decisions()
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)
    assert request.resource_max_wall_seconds is None
    clock = iter([0.0])
    monkeypatch.setattr(batch_module.time, "monotonic", lambda: next(clock, 10**9))

    result = StrategyPackageBatchPredictionRunner(
        source_loader=lambda _universe, _start, _end: _source(decisions),
        factor_runner=_fake_factor_runner,
        model_loader=lambda path: (
            object(),
            "fake",
            None,
            len(json.loads((path.parents[1] / "factor_order.json").read_text(encoding="utf-8"))["factor_order"]),
        ),
        model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
        history_start_resolver=lambda _start, _window: _history_start(decisions),
    ).run(
        request=request,
        pit_snapshot=snapshot,
        decision_dates=decisions,
        temp_root=tmp_path / "temp",
    )

    assert result.batch_receipt["primary_decision_batch_count"] == 386
    assert result.batch_receipt["wall_limit_enabled"] is False
    assert result.batch_receipt["wall_limit_seconds"] is None
    assert result.batch_receipt["wall_seconds"] > RESOURCE_MAX_WALL_SECONDS

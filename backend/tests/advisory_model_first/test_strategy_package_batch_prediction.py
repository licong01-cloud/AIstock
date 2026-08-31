from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.independent_package_alpha_audit_contracts import (
    FACTOR_CLOSURE_50,
    FACTOR_CLOSURE_57,
    PACKAGE_378_ID,
    PACKAGE_5A5_ID,
    PACKAGE_B668_ID,
    PKG_378_ARM_ID,
    PKG_5A5_ARM_ID,
    PKG_B668_ARM_ID,
    FrozenPackageAuditArmV1,
    WorkspaceFileDescriptorV1,
    build_independent_package_alpha_audit_request,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.advisory_model_first.strategy_package_batch_prediction import (
    BatchSourcePanels,
    FACTOR_IO_MODE_FILE_BACKED,
    FACTOR_IO_MODE_IN_MEMORY,
    StrategyPackageBatchPredictionRunner,
    _assert_file_backed_feature_parity,
    _publish_prediction_store,
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
        (PKG_B668_ARM_ID, PACKAGE_B668_ID, "SELECTION_ENABLED", 50, FACTOR_CLOSURE_50, "c"),
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
        n1_bundle_manifest_ref=_ref(
            "n1_formal_bundle_manifest", HASH_C, "/artifacts/n1/bundle/manifest.json"
        ),
        n1_bundle_id=HASH_C,
        n2a_request_ref=_ref("n2a_frozen_request", HASH_A),
        n2a_request_sha256=HASH_B,
        n2a_bundle_path="/artifacts/n2a/bundle",
        n2a_bundle_manifest_ref=_ref(
            "n2a_formal_bundle_manifest", HASH_C, "/artifacts/n2a/bundle/manifest.json"
        ),
        n2a_bundle_id=HASH_C,
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
    index = pd.MultiIndex.from_product(
        [source_dates, instruments], names=["datetime", "instrument"]
    )
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


def test_batch_runner_reads_once_groups_twice_and_loads_each_model_once(
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

    def source_loader(_universe, _start, _end):  # noqa: ANN001, ANN202
        counts["source"] += 1
        return _source(decisions)

    def factor_runner(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        counts["factor"] += 1
        virtualize_io = kwargs.pop("virtualize_io", True)
        if args[2].decision_end == args[3][0]:
            exact_window_day_counts.append(
                len(args[2].daily.index.get_level_values("datetime").unique())
            )
        frame = _fake_factor_runner(*args, **kwargs)
        frame.attrs["factor_resource_receipt"] = {
            "factor_io_mode": (
                FACTOR_IO_MODE_IN_MEMORY
                if virtualize_io
                else FACTOR_IO_MODE_FILE_BACKED
            ),
            "temp_peak_bytes": 0,
        }
        return frame

    def model_loader(path: Path):  # noqa: ANN202
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

    assert counts == {"source": 1, "factor": 780, "model": 3}
    assert result.batch_receipt["primary_factor_group_run_count"] == 772
    assert result.batch_receipt["primary_decision_batch_count"] == 386
    assert result.batch_receipt["primary_factor_group_run_count_per_decision"] == 2
    assert result.batch_receipt["diagnostic_factor_group_run_count"] == 6
    assert result.batch_receipt["daily_wsl_process_count"] == 0
    assert result.batch_receipt["daily_db_query_count"] == 0
    assert result.batch_receipt["factor_io_mode"] == FACTOR_IO_MODE_IN_MEMORY
    assert len(result.batch_receipt["factor_resource_receipts"]) == 778
    assert result.batch_receipt["file_backed_parity_factor_group_run_count"] == 2
    assert result.batch_receipt["all_factor_group_run_count"] == 780
    assert len(result.batch_receipt["file_backed_parity_receipts"]) == 2
    assert {
        item["status"] for item in result.batch_receipt["file_backed_parity_receipts"]
    } == {"PASS"}
    assert set(result.batch_receipt["required_window_by_closure"].values()) == {61}
    assert result.batch_receipt["window_buffer_trading_days"] == 5
    assert result.batch_receipt["rolling_live_window_semantics"] is True
    assert set(exact_window_day_counts) == {66}
    assert set(result.predictions) == {PKG_378_ARM_ID, PKG_5A5_ARM_ID, PKG_B668_ARM_ID}
    assert all(len(frame) == 386 * 60 for frame in result.predictions.values())
    assert all(descriptor.row_count == 386 * 60 for descriptor in result.prediction_descriptors.values())
    assert result.causality_parity_receipt["status"] == "PASS"
    assert len(result.causality_parity_receipt["checks"]) == 9

    retry_descriptors, retry_run_ids = _publish_prediction_store(
        request=request,
        predictions=result.predictions,
    )
    assert {
        arm_id: descriptor.artifact_sha256
        for arm_id, descriptor in retry_descriptors.items()
    } == {
        arm_id: descriptor.artifact_sha256
        for arm_id, descriptor in result.prediction_descriptors.items()
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
    features = run_factor_group_batch(
        workspace,
        HASH_A,
        source,
        [date(2025, 4, 22)],
        tmp_path / "temp_memory",
        virtualize_io=True,
    )

    pd.testing.assert_frame_equal(features, file_backed)
    assert features.shape == (60, 1)
    assert features["factor_000"].notna().all()
    receipt = features.attrs["factor_resource_receipt"]
    assert receipt["static_h5_physical_file_count"] == 1
    assert receipt["static_h5_hardlink_alias_count"] == 6
    assert receipt["temp_peak_bytes"] > 0
    assert receipt["factor_io_mode"] == FACTOR_IO_MODE_IN_MEMORY
    assert file_backed.attrs["factor_resource_receipt"]["factor_io_mode"] == FACTOR_IO_MODE_FILE_BACKED


def test_virtual_factor_io_restores_pandas_after_exception() -> None:
    original_read_hdf = pd.read_hdf
    original_read_parquet = pd.read_parquet
    original_frame_to_hdf = pd.DataFrame.to_hdf
    original_series_to_hdf = pd.Series.to_hdf
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
            raise RuntimeError("forced")

    assert pd.read_hdf is original_read_hdf
    assert pd.read_parquet is original_read_parquet
    assert pd.DataFrame.to_hdf is original_frame_to_hdf
    assert pd.Series.to_hdf is original_series_to_hdf


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
    index = pd.MultiIndex.from_product(
        [days, ["000001.SZ"]], names=["datetime", "instrument"]
    )
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


def test_batch_runner_checks_wall_limit_after_each_pit_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.services.advisory_model_first.strategy_package_batch_prediction as batch_module

    decisions = _decisions()
    snapshot = _pit_snapshot()
    request = _request(tmp_path, snapshot)
    clock = iter([0.0, float(request.resource_max_wall_seconds) + 1.0])
    monkeypatch.setattr(batch_module.time, "monotonic", lambda: next(clock))

    with pytest.raises(AdvisoryModelFirstError) as raised:
        StrategyPackageBatchPredictionRunner(
            source_loader=lambda _universe, _start, _end: _source(decisions),
            factor_runner=_fake_factor_runner,
            model_loader=lambda path: (
                object(),
                "fake",
                None,
                len(
                    json.loads(
                        (path.parents[1] / "factor_order.json").read_text(encoding="utf-8")
                    )["factor_order"]
                ),
            ),
            model_predictor=lambda _model, _inner, _kind, frame: frame.iloc[:, 0].to_numpy(),
            history_start_resolver=lambda _start, _window: _history_start(decisions),
        ).run(
            request=request,
            pit_snapshot=snapshot,
            decision_dates=decisions,
            temp_root=tmp_path / "temp",
        )

    assert raised.value.reason_code == "ADVISORY_PACKAGE_ALPHA_AUDIT_RESOURCE_LIMIT_EXCEEDED"
    assert raised.value.context["completed_decision_count"] == 1

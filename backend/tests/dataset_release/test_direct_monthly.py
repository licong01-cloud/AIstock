from __future__ import annotations

from datetime import date
import json

import numpy as np
import pandas as pd
import pytest
from scripts import update_backtest_dataset_monthly as cli
from scripts import qlib_authoritative_bin_export as authoritative_cli
from scripts.qlib_authoritative_smoke_backtest import minute_contract_failures

from backend.services.dataset_release.direct_monthly import (
    DIRECT_BENCHMARK_CODE,
    DIRECT_BENCHMARK_FIELDS,
    DIRECT_BENCHMARK_SCHEMA,
    DIRECT_COMPONENTS,
    DIRECT_FACTOR_SCHEMA,
    DIRECT_INDEX_CODES,
    DIRECT_MONTHLY_STATE_SCHEMA,
    DIRECT_SECTOR_AUTHORITY,
    DIRECT_SUSPEND_SCHEMA,
    DIRECT_TERMINAL_STATUS,
    LEGACY_DIRECT_MONTHLY_STATE_SCHEMA,
    DirectMonthlyError,
    DirectMonthlyLayout,
    DirectMonthlyRunner,
    cleanup_terminal_candidate,
    build_daily_benchmark_component,
    build_suspend_d_component,
    compact_status,
    component_plan,
    default_candidate_path,
    discover_latest_existing_direct_candidate,
    discover_latest_validated_baseline,
    initial_state,
    read_state,
    validate_direct_candidate_with_smoke,
    write_state,
    _run_qlib_component,
    _date_chunks,
    _build_sector_frame_from_classification,
    _ClassificationInterval,
    _filter_frame_to_pit,
    _read_classification_intervals,
)


def _layout(tmp_path) -> DirectMonthlyLayout:
    parent = tmp_path / "candidates"
    parent.mkdir()
    baseline = parent / "20260731-qe_hmm_full_v1-full-baseline-candidate"
    baseline.mkdir()
    return DirectMonthlyLayout.create(
        candidate_parent=parent,
        candidate_root=parent / "20260831-qe_hmm_full_v2-direct-20260902-candidate",
        baseline_root=baseline,
        cutoff=date(2026, 8, 31),
    )


def test_component_plan_includes_same_release_suspend_sidecar() -> None:
    plan = component_plan(july_minute_repaired=True)

    assert tuple(item.component for item in plan) == DIRECT_COMPONENTS
    assert {item.action for item in plan} == {"COMPONENT_REBUILD"}
    assert next(item for item in plan if item.component == "minute_bin").reason == (
        "july_repair_plus_august_tail"
    )
    assert next(item for item in plan if item.component == "daily_bin").reason == (
        "canonical_v2_pool_and_target_cutoff"
    )
    assert next(item for item in plan if item.component == "suspend_d").reason == (
        "same_release_canonical_v2_suspend_history"
    )


def test_authoritative_cli_isolated_dump_has_no_db_or_postprocess(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    captured: dict[str, object] = {}

    def fake_dump(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(authoritative_cli, "run_wsl_dump", fake_dump)
    monkeypatch.setattr(
        authoritative_cli,
        "require_readonly_pit_coverage",
        lambda *_args, **_kwargs: pytest.fail("isolated dump must not read PIT/DB state"),
    )
    result = authoritative_cli.main(
        [
            "--dataset",
            "stock_daily",
            "--stage",
            "dump",
            "--snapshot-id",
            "benchmark_daily",
            "--start",
            "2018-08-01",
            "--end",
            "2018-08-01",
            "--csv-root",
            str(tmp_path / "csv"),
            "--bin-root",
            str(tmp_path / "bin"),
            "--isolated-dump-only",
        ]
    )

    assert result == 0
    assert captured["dump_subcmd"] == "dump_all"
    assert captured["freq"] == "day"
    assert captured["max_workers"] is None
    assert json.loads(capsys.readouterr().out)["isolated_dump_only"] is True


def _prepare_benchmark_inputs(layout: DirectMonthlyLayout, *, dates: list[str]) -> pd.DataFrame:
    daily = layout.components_root / "daily_bin_candidate"
    (daily / "calendars").mkdir(parents=True)
    (daily / "calendars" / "day.txt").write_text("\n".join(dates) + "\n", encoding="utf-8")
    (daily / "instruments").mkdir()
    (daily / "instruments" / "all.txt").write_text(
        "000001.SZ\t2018-08-01\t2026-08-31\n"
        "000004.SZ\t2018-08-01\t2022-04-29\n"
        "000004.SZ\t2023-06-28\t2025-04-29\n",
        encoding="utf-8",
    )
    (daily / "features").mkdir()
    (daily / "meta_export.json").write_text(
        json.dumps(
            {
                "snapshot_id": "daily_bin_candidate",
                "end": layout.cutoff.isoformat(),
                "universe_key": "aistock_equity_pit_canonical_v2",
            }
        ),
        encoding="utf-8",
    )
    layout.reports_root.mkdir(parents=True)
    (layout.reports_root / "daily_bin_candidate_stock_daily_all.json").write_text(
        json.dumps({"dataset": "stock_daily", "stage": "all"}), encoding="utf-8"
    )
    index = layout.components_root / "index_context"
    index.mkdir()
    frame = pd.DataFrame(
        {
            "trade_date": dates,
            "ts_code": [DIRECT_BENCHMARK_CODE] * len(dates),
            "open": np.arange(len(dates), dtype=float) + 10.0,
            "high": np.arange(len(dates), dtype=float) + 11.0,
            "low": np.arange(len(dates), dtype=float) + 9.0,
            "close": np.arange(len(dates), dtype=float) + 10.5,
            "volume": np.arange(len(dates), dtype=float) + 100.0,
            "amount": np.arange(len(dates), dtype=float) + 1000.0,
        }
    )
    frame.to_hdf(index / "index_daily.h5", key="data", mode="w", format="fixed")
    (index / "meta.json").write_text(
        json.dumps({"end": layout.cutoff.isoformat()}), encoding="utf-8"
    )
    return frame


def _fake_benchmark_dump(*, frame, staging_root, project_root):
    del project_root
    output = staging_root / "qlib"
    (output / "calendars").mkdir(parents=True)
    (output / "calendars" / "day.txt").write_text(
        "\n".join(frame["date"].tolist()) + "\n", encoding="utf-8"
    )
    feature = output / "features" / DIRECT_BENCHMARK_CODE.lower()
    feature.mkdir(parents=True)
    for field in DIRECT_BENCHMARK_FIELDS:
        np.hstack(([0], frame[field].to_numpy(dtype="float32"))).astype("<f4").tofile(
            feature / f"{field}.day.bin"
        )
    return output


def test_daily_benchmark_completion_uses_index_h5_and_preserves_stock_spans(
    tmp_path,
    monkeypatch,
) -> None:
    layout = _layout(tmp_path)
    dates = ["2026-08-27", "2026-08-28", "2026-08-31"]
    _prepare_benchmark_inputs(layout, dates=dates)
    original = (layout.components_root / "daily_bin_candidate" / "instruments" / "all.txt").read_bytes()
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._run_daily_benchmark_dump",
        _fake_benchmark_dump,
    )

    result = build_daily_benchmark_component(layout, project_root=tmp_path)
    daily = layout.components_root / "daily_bin_candidate"
    receipt = json.loads(
        (layout.reports_root / "daily_benchmark_000300_completion.json").read_text(
            encoding="utf-8"
        )
    )
    benchmark_line = "000300.SH\t2018-08-01\t2026-08-31"

    assert result["status"] == "PASS"
    assert result["action"] == "BENCHMARK_ONLY_COMPLETION"
    assert (daily / "features" / "000300.sh").is_dir()
    assert {path.name for path in (daily / "features" / "000300.sh").iterdir()} == {
        f"{field}.day.bin" for field in DIRECT_BENCHMARK_FIELDS
    }
    assert (daily / "instruments" / "stock_universe.txt").read_bytes() == original
    assert (daily / "instruments" / "benchmark.txt").read_text(encoding="utf-8").splitlines() == [
        benchmark_line
    ]
    all_lines = (daily / "instruments" / "all.txt").read_text(encoding="utf-8").splitlines()
    assert all_lines[:-1] == original.decode("utf-8").splitlines()
    assert all_lines[-1] == benchmark_line
    assert "000300.SH" not in (daily / "instruments" / "stock_universe.txt").read_text(
        encoding="utf-8"
    )
    meta = json.loads((daily / "meta_export.json").read_text(encoding="utf-8"))
    assert meta["benchmark_only"]["selection_eligible"] is False
    assert meta["benchmark_only"]["selection_universe"] == "instruments/stock_universe.txt"
    assert receipt["schema_version"] == DIRECT_BENCHMARK_SCHEMA
    assert receipt["stock_universe_preserved"] is True
    assert receipt["calendar_offset"] == 0
    assert receipt["full_history_content_hash"] is False
    report = json.loads(
        (layout.reports_root / "daily_bin_candidate_stock_daily_all.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["benchmark_only_completion"]["rows"] == len(dates)

    replay = build_daily_benchmark_component(layout, project_root=tmp_path)
    assert replay["action"] == "REUSE_COMPLETED_DIRECT_OUTPUT"


def test_daily_benchmark_completion_rejects_calendar_gap(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    _prepare_benchmark_inputs(layout, dates=["2026-08-28", "2026-08-31"])
    frame = pd.read_hdf(
        layout.components_root / "index_context" / "index_daily.h5", key="data"
    )
    frame = frame.iloc[[0]]
    frame.to_hdf(
        layout.components_root / "index_context" / "index_daily.h5",
        key="data",
        mode="w",
        format="fixed",
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._run_daily_benchmark_dump",
        _fake_benchmark_dump,
    )

    with pytest.raises(DirectMonthlyError, match="exactly match"):
        build_daily_benchmark_component(layout, project_root=tmp_path)
    assert not (
        layout.components_root / "daily_bin_candidate" / "features" / "000300.sh"
    ).exists()


def test_terminal_candidate_reopens_only_missing_benchmark_stage(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    state = initial_state(layout)
    state["status"] = DIRECT_TERMINAL_STATUS
    for component in DIRECT_COMPONENTS:
        state["components"][component]["status"] = "PASS"
    write_state(layout, state)
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._terminal_candidate_requires_benchmark_repair",
        lambda _layout: True,
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._component_output_complete",
        lambda _layout, component: component != "index_context",
    )
    calls: list[str] = []

    def handler(component: str):
        def run(_layout):
            calls.append(component)
            return {"status": "PASS", "component": component}

        return run

    result = DirectMonthlyRunner(
        {component: handler(component) for component in DIRECT_COMPONENTS},
        validator=lambda _layout: {"status": "PASS"},
    ).run(layout)

    assert result["status"] == DIRECT_TERMINAL_STATUS
    assert calls == ["index_context"]


def test_legacy_four_component_state_resumes_only_missing_suspend_d(tmp_path) -> None:
    layout = _layout(tmp_path)
    legacy = initial_state(layout)
    legacy["schema_version"] = LEGACY_DIRECT_MONTHLY_STATE_SCHEMA
    legacy["status"] = DIRECT_TERMINAL_STATUS
    legacy["components"].pop("suspend_d")
    for component in legacy["components"]:
        legacy["components"][component]["status"] = "PASS"
        legacy["components"][component]["receipt"] = {
            "status": "PASS",
            "component": component,
        }
    layout.factor_root.mkdir(parents=True)
    (layout.factor_root / "meta.json").write_text(
        json.dumps(
            {
                "schema_version": DIRECT_FACTOR_SCHEMA,
                "sector_authority": DIRECT_SECTOR_AUTHORITY,
                "end": layout.cutoff.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    layout.candidate_root.mkdir(parents=True, exist_ok=True)
    layout.state_path.write_text(json.dumps(legacy), encoding="utf-8")
    observed = read_state(layout)
    assert observed is not None
    assert observed["schema_version"] == DIRECT_MONTHLY_STATE_SCHEMA
    assert observed["status"] == "PLANNING_DIRECT"
    assert observed["components"]["suspend_d"]["status"] == "PENDING"
    persisted_before_run = json.loads(layout.state_path.read_text(encoding="utf-8"))
    assert persisted_before_run["schema_version"] == LEGACY_DIRECT_MONTHLY_STATE_SCHEMA
    assert "suspend_d" not in persisted_before_run["components"]

    calls: list[str] = []

    def handler(component: str):
        def run(_layout: DirectMonthlyLayout):
            calls.append(component)
            return {"status": "PASS", "component": component}

        return run

    result = DirectMonthlyRunner(
        {component: handler(component) for component in DIRECT_COMPONENTS},
        validator=lambda _layout: {"status": "PASS"},
    ).run(layout)

    assert result["status"] == DIRECT_TERMINAL_STATUS
    assert result["schema_version"] == DIRECT_MONTHLY_STATE_SCHEMA
    assert calls == ["suspend_d"]


def test_suspend_component_uses_daily_calendar_and_canonical_pit_without_hashes(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    calendar = layout.components_root / "daily_bin_candidate" / "calendars" / "day.txt"
    calendar.parent.mkdir(parents=True)
    calendar.write_text("2026-08-28\n2026-08-31\n", encoding="utf-8")
    spans = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "eligible_start": [pd.Timestamp("2018-08-01"), pd.Timestamp("2018-08-01")],
            "eligible_end": [pd.Timestamp("2026-08-31"), pd.Timestamp("2026-08-28")],
        }
    )
    source = pd.DataFrame(
        {
            "trade_date": [
                "2026-08-31",
                "2026-08-31",
                "2026-08-31",
                "2026-08-31",
            ],
            "ts_code": ["000001.SZ", "600000.SH", "430001.BJ", "000002.SZ"],
            "suspend_type": ["S", "S", "S", "R"],
            "suspend_timing": [None, None, None, None],
        }
    )

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._load_pit_spans",
        lambda *_args: spans,
    )
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: Connection())
    monkeypatch.setattr(pd, "read_sql", lambda *_args, **_kwargs: source.copy())

    receipt = build_suspend_d_component(layout)

    written = pd.read_parquet(layout.suspend_root / "suspend_d.parquet")
    meta = json.loads((layout.suspend_root / "meta.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "PASS"
    assert written[["trade_date", "ts_code"]].to_dict("records") == [
        {"trade_date": pd.Timestamp("2026-08-31"), "ts_code": "000001.SZ"}
    ]
    assert meta["schema_version"] == DIRECT_SUSPEND_SCHEMA
    assert meta["end"] == "2026-08-31"
    assert meta["daily_row_counts"] == {"2026-08-28": 0, "2026-08-31": 1}
    assert meta["source_freeze"] is False
    assert meta["full_history_content_hash"] is False
    assert not any("sha256" in key for key in meta)


def test_suspend_component_rejects_empty_full_history_source(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    calendar = layout.components_root / "daily_bin_candidate" / "calendars" / "day.txt"
    calendar.parent.mkdir(parents=True)
    calendar.write_text("2026-08-31\n", encoding="utf-8")

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._load_pit_spans",
        lambda *_args: pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "eligible_start": [pd.Timestamp("2018-08-01")],
                "eligible_end": [pd.Timestamp("2026-08-31")],
            }
        ),
    )
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: Connection())
    monkeypatch.setattr(
        pd,
        "read_sql",
        lambda *_args, **_kwargs: pd.DataFrame(
            columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"]
        ),
    )

    with pytest.raises(DirectMonthlyError, match="source is empty"):
        build_suspend_d_component(layout)
    assert not layout.suspend_root.exists()


def test_layout_rejects_escape_existing_file_and_baseline_alias(tmp_path) -> None:
    parent = tmp_path / "candidates"
    parent.mkdir()
    baseline = parent / "20260831-qe_hmm_full_v2-direct-20260902-candidate"
    baseline.mkdir()

    with pytest.raises(DirectMonthlyError, match="direct child"):
        DirectMonthlyLayout.create(
            candidate_parent=parent,
            candidate_root=tmp_path / "20260831-qe_hmm_full_v2-direct-20260902-candidate",
            baseline_root=baseline,
            cutoff=date(2026, 8, 31),
        )
    with pytest.raises(DirectMonthlyError, match="different direct child"):
        DirectMonthlyLayout.create(
            candidate_parent=parent,
            candidate_root=baseline,
            baseline_root=baseline,
            cutoff=date(2026, 8, 31),
        )


def test_runner_executes_all_components_once_and_terminal_replay_is_noop(tmp_path) -> None:
    layout = _layout(tmp_path)
    calls: list[str] = []

    def handler(component: str):
        def run(_layout: DirectMonthlyLayout):
            calls.append(component)
            return {
                "status": "PASS",
                "component": component,
                "cutoff": _layout.cutoff.isoformat(),
            }

        return run

    runner = DirectMonthlyRunner(
        {component: handler(component) for component in DIRECT_COMPONENTS},
        validator=lambda _layout: {"status": "PASS"},
    )
    first = runner.run(layout)
    second = runner.run(layout)

    assert first["status"] == DIRECT_TERMINAL_STATUS
    assert second == first
    assert calls == list(DIRECT_COMPONENTS)
    assert all(first["components"][name]["status"] == "PASS" for name in DIRECT_COMPONENTS)
    assert compact_status(first)["source_freeze"] is False
    assert compact_status(first)["full_history_content_hash"] is False
    assert compact_status(first)["production_writes"] == 0


def test_runner_resumes_only_non_passed_component(tmp_path) -> None:
    layout = _layout(tmp_path)
    calls: list[str] = []

    def pass_handler(component: str):
        def run(_layout: DirectMonthlyLayout):
            calls.append(component)
            return {"status": "PASS", "component": component}

        return run

    failed_once = {"value": False}

    def minute(_layout: DirectMonthlyLayout):
        calls.append("minute_bin")
        if not failed_once["value"]:
            failed_once["value"] = True
            raise RuntimeError("injected interruption")
        return {"status": "PASS", "component": "minute_bin"}

    runner = DirectMonthlyRunner(
        {
            "daily_bin": pass_handler("daily_bin"),
            "minute_bin": minute,
            "factor_h5_static": pass_handler("factor_h5_static"),
            "index_context": pass_handler("index_context"),
            "suspend_d": pass_handler("suspend_d"),
        },
        validator=lambda _layout: {"status": "PASS"},
    )
    with pytest.raises(RuntimeError, match="injected interruption"):
        runner.run(layout)

    failed_state = read_state(layout)
    assert failed_state is not None
    assert failed_state["status"] == "FAILED"
    assert failed_state["components"]["daily_bin"]["status"] == "PASS"
    assert failed_state["components"]["minute_bin"]["status"] == "FAILED"

    resumed = runner.run(layout)

    assert resumed["status"] == DIRECT_TERMINAL_STATUS
    assert calls.count("daily_bin") == 1
    assert calls.count("minute_bin") == 2
    assert calls.count("factor_h5_static") == 1
    assert calls.count("index_context") == 1
    assert calls.count("suspend_d") == 1


def test_runner_rejects_partial_registry_and_invalid_receipt(tmp_path) -> None:
    layout = _layout(tmp_path)
    with pytest.raises(DirectMonthlyError, match="all components"):
        DirectMonthlyRunner({})

    def invalid(component: str):
        return lambda _layout: {"status": "PASS", "component": f"wrong-{component}"}

    runner = DirectMonthlyRunner(
        {component: invalid(component) for component in DIRECT_COMPONENTS},
        validator=lambda _layout: {"status": "PASS"},
    )
    with pytest.raises(DirectMonthlyError, match="receipt is invalid"):
        runner.run(layout)
    state = json.loads(layout.state_path.read_text(encoding="utf-8"))
    assert state["status"] == "FAILED"
    assert state["components"]["daily_bin"]["status"] == "FAILED"


def test_runner_persists_failed_top_level_when_validation_fails(tmp_path) -> None:
    layout = _layout(tmp_path)
    runner = DirectMonthlyRunner(
        {
            component: (lambda _layout, name=component: {"status": "PASS", "component": name})
            for component in DIRECT_COMPONENTS
        },
        validator=lambda _layout: {"status": "FAILED"},
    )

    with pytest.raises(DirectMonthlyError, match="validation receipt"):
        runner.run(layout)

    state = read_state(layout)
    assert state is not None
    assert state["status"] == "FAILED"
    assert all(state["components"][component]["status"] == "PASS" for component in DIRECT_COMPONENTS)


def test_daily_component_always_uses_structural_csv_resume(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    captured: dict[str, object] = {}

    class Completed:
        returncode = 0

    def fake_run(command, **_kwargs):
        captured["command"] = command
        calendar = layout.components_root / "daily_bin_candidate" / "calendars" / "day.txt"
        calendar.parent.mkdir(parents=True)
        calendar.write_text("2026-08-31\n", encoding="utf-8")
        return Completed()

    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly.subprocess.run",
        fake_run,
    )

    receipt = _run_qlib_component(
        layout,
        project_root=tmp_path,
        component="daily_bin",
        dataset="stock_daily",
        start=date(2018, 8, 1),
    )

    assert receipt["status"] == "PASS"
    assert "--resume-csv" in captured["command"]


def test_minute_component_uses_index_friendly_resume_without_full_history_validation(
    tmp_path, monkeypatch
) -> None:
    layout = _layout(tmp_path)
    captured: dict[str, object] = {}

    class Completed:
        returncode = 0

    def fake_run(command, **_kwargs):
        captured["command"] = command
        calendar = layout.components_root / "minute_bin_candidate" / "calendars" / "1min.txt"
        calendar.parent.mkdir(parents=True)
        calendar.write_text("2026-08-31 15:00:00\n", encoding="utf-8")
        return Completed()

    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly.subprocess.run",
        fake_run,
    )

    receipt = _run_qlib_component(
        layout,
        project_root=tmp_path,
        component="minute_bin",
        dataset="stock_minute",
        start=date(2024, 1, 2),
    )

    command = captured["command"]
    assert receipt["status"] == "PASS"
    assert "--resume-csv" in command
    assert "--skip-validation" in command
    assert "--no-validate-values" not in command
    assert "--minute-chunked-export" not in command
    assert "--minute-code-batch-size" not in command
    assert "--minute-chunk-months" not in command


def test_existing_nonempty_candidate_without_state_is_never_adopted(tmp_path) -> None:
    layout = _layout(tmp_path)
    layout.candidate_root.mkdir()
    (layout.candidate_root / "foreign.txt").write_text("do not adopt", encoding="utf-8")
    runner = DirectMonthlyRunner(
        {
            component: (lambda _layout, name=component: {"status": "PASS", "component": name})
            for component in DIRECT_COMPONENTS
        },
        validator=lambda _layout: {"status": "PASS"},
    )

    with pytest.raises(DirectMonthlyError, match="lacks its state"):
        runner.run(layout)
    assert (layout.candidate_root / "foreign.txt").read_text(encoding="utf-8") == "do not adopt"


def test_pit_filter_is_key_preserving_and_excludes_outside_spans() -> None:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-08-03"), "000001.SZ"),
            (pd.Timestamp("2026-08-04"), "000001.SZ"),
            (pd.Timestamp("2026-08-04"), "600000.SH"),
        ],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"close": [10.0, 11.0, 12.0]}, index=index)
    spans = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "eligible_start": [pd.Timestamp("2026-08-04"), pd.Timestamp("2026-08-01")],
            "eligible_end": [pd.Timestamp("2026-08-31"), pd.Timestamp("2026-08-03")],
        }
    )

    filtered = _filter_frame_to_pit(frame, spans, date(2026, 8, 1), date(2026, 8, 31))

    assert list(filtered.index) == [(pd.Timestamp("2026-08-04"), "000001.SZ")]
    assert filtered.iloc[0]["close"] == 11.0


def test_date_chunks_are_contiguous_and_close_at_cutoff() -> None:
    chunks = list(_date_chunks(date(2026, 1, 1), date(2026, 8, 31), months=3))

    assert chunks == [
        (date(2026, 1, 1), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 6, 30)),
        (date(2026, 7, 1), date(2026, 8, 31)),
    ]


def test_baseline_discovery_uses_latest_earlier_validated_metadata_only(tmp_path) -> None:
    parent = tmp_path / "candidates"
    parent.mkdir()
    for end, status in (
        ("2026-06-30", "final_validation_validated"),
        ("2026-07-31", "final_validation_validated"),
        ("2026-08-31", "final_validation_validated"),
        ("2026-07-31", "failed"),
    ):
        root = parent / f"candidate-{end}-{status}"
        factor = root / "components" / "factor_h5_static_candidate"
        factor.mkdir(parents=True)
        (factor / "meta.json").write_text(json.dumps({"end": end}), encoding="utf-8")
        (root / "run_state.json").write_text(json.dumps({"status": status}), encoding="utf-8")

    selected = discover_latest_validated_baseline(parent, cutoff=date(2026, 8, 31))

    assert selected.name == "candidate-2026-07-31-final_validation_validated"
    assert default_candidate_path(
        parent,
        cutoff=date(2026, 8, 31),
        observed_on=date(2026, 9, 2),
    ).name == "20260831-qe_hmm_full_v2-direct-20260902-candidate"


def test_baseline_is_optional_for_first_direct_candidate(tmp_path) -> None:
    parent = tmp_path / "candidates"
    parent.mkdir()

    assert discover_latest_validated_baseline(parent, cutoff=date(2026, 8, 31)) is None
    layout = DirectMonthlyLayout.create(
        candidate_parent=parent,
        candidate_root=parent / "20260831-qe_hmm_full_v2-direct-20260902-candidate",
        baseline_root=None,
        cutoff=date(2026, 8, 31),
    )
    assert initial_state(layout)["baseline_root"] is None


def test_missing_legacy_baseline_does_not_hide_terminal_candidate(tmp_path) -> None:
    layout = _layout(tmp_path)
    state = initial_state(layout)
    state["status"] = DIRECT_TERMINAL_STATUS
    for component in DIRECT_COMPONENTS:
        state["components"][component]["status"] = "PASS"
    write_state(layout, state)
    layout.baseline_root.rmdir()

    restored = DirectMonthlyLayout.create(
        candidate_parent=layout.candidate_parent,
        candidate_root=layout.candidate_root,
        baseline_root=layout.baseline_root,
        cutoff=layout.cutoff,
    )

    assert read_state(restored)["status"] == DIRECT_TERMINAL_STATUS
    assert discover_latest_existing_direct_candidate(
        layout.candidate_parent,
        cutoff=layout.cutoff,
    ) == layout.candidate_root


def test_cleanup_terminal_candidate_removes_only_disposable_paths_and_detaches_baseline(
    tmp_path,
) -> None:
    layout = _layout(tmp_path)
    layout.work_root.mkdir(parents=True)
    (layout.work_root / "source.csv").write_text("temporary", encoding="utf-8")
    legacy_factor = layout.components_root / "factor_h5_static_candidate"
    legacy_factor.mkdir(parents=True)
    (legacy_factor / "old.h5").write_text("old", encoding="utf-8")
    layout.factor_root.mkdir(parents=True)
    (layout.factor_root / "current.h5").write_text("current", encoding="utf-8")
    daily = layout.components_root / "daily_bin_candidate"
    daily.mkdir()
    (daily / "keep.bin").write_text("keep", encoding="utf-8")
    state = initial_state(layout)
    state["status"] = DIRECT_TERMINAL_STATUS
    for component in DIRECT_COMPONENTS:
        state["components"][component]["status"] = "PASS"
    state["components"]["factor_h5_static"]["receipt"] = {
        "path": str(layout.factor_root),
    }
    write_state(layout, state)

    plan = cleanup_terminal_candidate(layout, apply=False)
    assert plan["status"] == "PLAN_ONLY"
    assert plan["targets"] == [
        str(layout.work_root.relative_to(layout.candidate_root)),
        str(legacy_factor.relative_to(layout.candidate_root)),
    ]
    assert plan["baseline_detach"] is True
    assert layout.work_root.exists()

    applied = cleanup_terminal_candidate(layout, apply=True)
    assert applied["status"] == "APPLIED"
    assert not layout.work_root.exists()
    assert not legacy_factor.exists()
    assert layout.factor_root.is_dir()
    assert (daily / "keep.bin").is_file()

    layout.baseline_root.rmdir()
    detached = DirectMonthlyLayout.create(
        candidate_parent=layout.candidate_parent,
        candidate_root=layout.candidate_root,
        baseline_root=None,
        cutoff=layout.cutoff,
    )
    assert read_state(detached)["baseline_root"] is None


def test_cleanup_refuses_legacy_factor_without_active_v2_receipt(tmp_path) -> None:
    layout = _layout(tmp_path)
    legacy_factor = layout.components_root / "factor_h5_static_candidate"
    legacy_factor.mkdir(parents=True)
    layout.factor_root.mkdir(parents=True)
    state = initial_state(layout)
    state["status"] = DIRECT_TERMINAL_STATUS
    for component in DIRECT_COMPONENTS:
        state["components"][component]["status"] = "PASS"
    write_state(layout, state)

    with pytest.raises(DirectMonthlyError, match="cannot prove the active v2 factor"):
        cleanup_terminal_candidate(layout, apply=True)

    assert legacy_factor.is_dir()


def test_direct_cleanup_cli_plans_then_applies_without_touching_components(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    layout = _layout(tmp_path)
    layout.work_root.mkdir(parents=True)
    (layout.work_root / "source.csv").write_text("temporary", encoding="utf-8")
    state = initial_state(layout)
    state["status"] = DIRECT_TERMINAL_STATUS
    for component in DIRECT_COMPONENTS:
        state["components"][component]["status"] = "PASS"
    write_state(layout, state)
    monkeypatch.setattr(cli, "DIRECT_CANDIDATE_PARENT", layout.candidate_parent)

    assert cli.main(["--profile", "qe_hmm_full_v2", "cleanup", "--latest"]) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["status"] == "PLAN_ONLY"
    assert layout.work_root.is_dir()

    assert (
        cli.main(["--profile", "qe_hmm_full_v2", "cleanup", "--latest", "--apply"])
        == 0
    )
    applied = json.loads(capsys.readouterr().out)
    assert applied["status"] == "APPLIED"
    assert not layout.work_root.exists()
    assert layout.candidate_root.is_dir()


def test_monthly_candidate_discovery_resumes_same_cutoff_across_operator_dates(tmp_path) -> None:
    layout = _layout(tmp_path)
    write_state(layout, initial_state(layout))

    selected = discover_latest_existing_direct_candidate(
        layout.candidate_parent,
        cutoff=date(2026, 8, 31),
    )

    assert selected == layout.candidate_root
    assert selected != default_candidate_path(
        layout.candidate_parent,
        cutoff=date(2026, 8, 31),
        observed_on=date(2026, 9, 3),
    )


def test_sector_projection_uses_classification_without_index_membership(monkeypatch) -> None:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-08-03"), "000001.SZ"),
            (pd.Timestamp("2026-08-04"), "000001.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame({"close": [10.0, 10.1]}, index=index)
    moneyflow = pd.DataFrame({"mf_net_amt": [2.0, 3.0]}, index=index)
    published = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-08-03", "2026-08-04"]),
            "index_l2_code": ["801780.SI", "801780.SI"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "pct_change": [1.0, 0.99],
            "vol": [10.0, 11.0],
            "amount": [20.0, 21.0],
            "pe": [12.0, 12.1],
            "pb": [1.2, 1.3],
            "total_mv": [1000.0, 1010.0],
        }
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._load_sw_daily_for_projection",
        lambda _codes, _start, _end: published,
    )

    result = _build_sector_frame_from_classification(
        daily,
        moneyflow,
        intervals_by_symbol={
            "000001.SZ": (
                _ClassificationInterval(date(2021, 8, 2), date(2027, 1, 1), "480000"),
            )
        },
        l2_projection={"480000": "801780.SI"},
        l2_code_map={"801780.SI": 42},
        start=date(2026, 8, 1),
        end=date(2026, 8, 31),
    )

    assert list(result["l2_code_id"]) == [42, 42]
    assert list(result["sw2_pct_change"]) == pytest.approx([1.0, 0.99])
    assert list(result["sw2_mf_net_amt"]) == pytest.approx([2.0, 3.0])


def test_classification_reader_merges_exact_identity_overlap_and_rejects_conflict(tmp_path) -> None:
    layout = _layout(tmp_path)
    root = layout.industry_authority_root
    root.mkdir(parents=True)
    rows = [
        {
            "canonical_symbol": "000001.SZ",
            "causal_use_from": "2021-08-02",
            "causal_use_to_exclusive": None,
            "eligible_from": "2018-08-01",
            "eligible_to_exclusive": "2026-09-01",
            "identity": {"l2_code": "480300"},
            "unavailable_reason": None,
        },
        {
            "canonical_symbol": "000001.SZ",
            "causal_use_from": "2022-01-01",
            "causal_use_to_exclusive": None,
            "eligible_from": "2018-08-01",
            "eligible_to_exclusive": "2026-09-01",
            "identity": {"l2_code": "480300"},
            "unavailable_reason": None,
        },
    ]
    target = root / "classification_candidate.jsonl"
    target.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    result = _read_classification_intervals(layout)

    assert result["000001.SZ"] == (
        _ClassificationInterval(date(2021, 8, 2), date(2026, 9, 1), "480300"),
    )
    rows[1]["identity"] = {"l2_code": "480200"}
    target.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    with pytest.raises(DirectMonthlyError, match="intervals overlap"):
        _read_classification_intervals(layout)


def test_sector_published_fields_do_not_depend_on_moneyflow(monkeypatch) -> None:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-08-03"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame({"close": [10.0]}, index=index)
    published = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-08-03"]),
            "index_l2_code": ["801780.SI"],
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [101.0],
            "pct_change": [1.0],
            "vol": [10.0],
            "amount": [20.0],
            "pe": [12.0],
            "pb": [1.2],
            "total_mv": [1000.0],
        }
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._load_sw_daily_for_projection",
        lambda _codes, _start, _end: published,
    )

    result = _build_sector_frame_from_classification(
        daily,
        pd.DataFrame(),
        intervals_by_symbol={
            "000001.SZ": (
                _ClassificationInterval(date(2021, 8, 2), date(2027, 1, 1), "480000"),
            )
        },
        l2_projection={"480000": "801780.SI"},
        l2_code_map={"801780.SI": 42},
        start=date(2026, 8, 1),
        end=date(2026, 8, 31),
    )

    assert result.iloc[0]["sw2_pct_change"] == pytest.approx(1.0)
    assert pd.isna(result.iloc[0]["sw2_mf_net_amt"])


def test_sector_projection_preserves_classified_row_when_both_fact_sources_are_empty(
    monkeypatch,
) -> None:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-08-03"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame({"close": [10.0]}, index=index)
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._load_sw_daily_for_projection",
        lambda _codes, _start, _end: pd.DataFrame(),
    )

    result = _build_sector_frame_from_classification(
        daily,
        pd.DataFrame(),
        intervals_by_symbol={
            "000001.SZ": (
                _ClassificationInterval(date(2021, 8, 2), date(2027, 1, 1), "480300"),
            )
        },
        l2_projection={"480300": "801780.SI"},
        l2_code_map={"801780.SI": 42},
        start=date(2026, 8, 1),
        end=date(2026, 8, 31),
    )

    assert len(result) == 1
    assert result.iloc[0]["l2_code_id"] == 42
    assert pd.isna(result.iloc[0]["sw2_pct_change"])
    assert pd.isna(result.iloc[0]["sw2_mf_net_amt"])


def test_resume_rebuilds_only_factor_when_factor_contract_changed(tmp_path) -> None:
    layout = _layout(tmp_path)
    state = initial_state(layout)
    state["status"] = "FAILED"
    for component in DIRECT_COMPONENTS:
        state["components"][component]["status"] = "PASS"
        state["components"][component]["receipt"] = {"status": "PASS", "component": component}
    layout.suspend_root.mkdir(parents=True)
    (layout.suspend_root / "suspend_d.parquet").write_bytes(b"present")
    (layout.suspend_root / "meta.json").write_text(
        json.dumps(
            {
                "schema_version": DIRECT_SUSPEND_SCHEMA,
                "component": "suspend_d",
                "end": layout.cutoff.isoformat(),
                "universe_key": "aistock_equity_pit_canonical_v2",
                "source_freeze": False,
                "full_history_content_hash": False,
            }
        ),
        encoding="utf-8",
    )
    write_state(layout, state)
    calls: list[str] = []

    def handler(component: str):
        def run(_layout: DirectMonthlyLayout):
            calls.append(component)
            if component == "factor_h5_static":
                _layout.factor_root.mkdir(parents=True)
                (_layout.factor_root / "meta.json").write_text(
                    json.dumps(
                        {
                            "schema_version": DIRECT_FACTOR_SCHEMA,
                            "sector_authority": DIRECT_SECTOR_AUTHORITY,
                            "end": _layout.cutoff.isoformat(),
                        }
                    ),
                    encoding="utf-8",
                )
            return {"status": "PASS", "component": component}

        return run

    result = DirectMonthlyRunner(
        {component: handler(component) for component in DIRECT_COMPONENTS},
        validator=lambda _layout: {"status": "PASS"},
    ).run(layout)

    assert result["status"] == DIRECT_TERMINAL_STATUS
    assert calls == ["factor_h5_static"]


def test_direct_consumer_smoke_uses_contract_mode_without_strategy_thresholds(
    tmp_path, monkeypatch
) -> None:
    layout = _layout(tmp_path)
    commands: list[list[str]] = []

    class Completed:
        returncode = 0

    def fake_run(command, **_kwargs):
        commands.append(command)
        return Completed()

    index_frame = pd.DataFrame(
        {
            "trade_date": [date(2026, 8, 31)] * len(DIRECT_INDEX_CODES),
            "ts_code": list(DIRECT_INDEX_CODES),
            "close": [1.0] * len(DIRECT_INDEX_CODES),
        }
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly.validate_direct_candidate",
        lambda _layout: {"status": "PASS"},
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly.subprocess.run",
        fake_run,
    )
    monkeypatch.setattr(pd, "read_hdf", lambda *_args, **_kwargs: index_frame)

    result = validate_direct_candidate_with_smoke(layout, project_root=tmp_path)

    assert result["status"] == "PASS"
    assert len(commands) == 2
    qe_shell = commands[0][-1]
    minute_shell = commands[1][-1]
    assert "--contract-smoke-only" in qe_shell
    assert "--require-nonempty-source sector_data" in qe_shell
    assert "--min-feature-coverage" not in qe_shell
    assert "--contract-smoke-only" in minute_shell


def test_minute_contract_smoke_requires_field_presence_and_one_complete_stock_day() -> None:
    index = pd.MultiIndex.from_product(
        [pd.date_range("2026-08-31 09:31:00", periods=240, freq="min"), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    minute = pd.DataFrame({"close": 10.0, "factor": 1.0}, index=index)

    failures, non_null = minute_contract_failures(minute)

    assert failures == []
    assert non_null == {"close": 240, "factor": 240}
    minute["factor"] = float("nan")
    failures, _ = minute_contract_failures(minute)
    assert "minute field has no values: factor" in failures
    duplicate = pd.concat([minute.iloc[:1], minute])
    failures, _ = minute_contract_failures(duplicate)
    assert "minute provider contains duplicate datetime/instrument keys" in failures

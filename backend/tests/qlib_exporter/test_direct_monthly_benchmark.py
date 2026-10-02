from __future__ import annotations

from datetime import date
import json

import numpy as np
import pandas as pd
import pytest

from scripts import qlib_authoritative_bin_export as authoritative_cli
from backend.services.dataset_release.direct_monthly import (
    DIRECT_BENCHMARK_CODE,
    DIRECT_BENCHMARK_FIELDS,
    DIRECT_BENCHMARK_SCHEMA,
    DIRECT_COMPONENTS,
    DIRECT_TERMINAL_STATUS,
    DirectMonthlyError,
    DirectMonthlyLayout,
    DirectMonthlyRunner,
    build_daily_benchmark_component,
    initial_state,
    write_state,
)


def _layout(tmp_path) -> DirectMonthlyLayout:
    parent = tmp_path / "candidates"
    parent.mkdir()
    return DirectMonthlyLayout.create(
        candidate_parent=parent,
        candidate_root=parent / "20260831-qe_hmm_full_v2-direct-20260902-candidate",
        baseline_root=None,
        cutoff=date(2026, 8, 31),
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
            "--dataset", "stock_daily",
            "--stage", "dump",
            "--snapshot-id", "benchmark_daily",
            "--start", "2018-08-01",
            "--end", "2018-08-01",
            "--csv-root", str(tmp_path / "csv"),
            "--bin-root", str(tmp_path / "bin"),
            "--isolated-dump-only",
        ]
    )

    assert result == 0
    assert captured["dump_subcmd"] == "dump_all"
    assert captured["freq"] == "day"
    assert json.loads(capsys.readouterr().out)["isolated_dump_only"] is True


def _prepare_inputs(layout: DirectMonthlyLayout, dates: list[str]) -> None:
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
        json.dumps({"snapshot_id": "daily_bin_candidate", "end": "2026-08-31"}),
        encoding="utf-8",
    )
    layout.reports_root.mkdir(parents=True)
    (layout.reports_root / "daily_bin_candidate_stock_daily_all.json").write_text(
        json.dumps({"dataset": "stock_daily", "stage": "all"}), encoding="utf-8"
    )
    index = layout.components_root / "index_context"
    index.mkdir()
    values = np.arange(len(dates), dtype=float)
    pd.DataFrame(
        {
            "trade_date": dates,
            "ts_code": [DIRECT_BENCHMARK_CODE] * len(dates),
            "open": values + 10.0,
            "high": values + 11.0,
            "low": values + 9.0,
            "close": values + 10.5,
            "volume": values + 100.0,
            "amount": values + 1000.0,
        }
    ).to_hdf(index / "index_daily.h5", key="data", mode="w", format="fixed")
    (index / "meta.json").write_text(json.dumps({"end": "2026-08-31"}), encoding="utf-8")


def _fake_dump(*, frame, staging_root, project_root):
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


def test_completion_preserves_stock_spans_and_isolates_benchmark(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    dates = ["2026-08-27", "2026-08-28", "2026-08-31"]
    _prepare_inputs(layout, dates)
    all_path = layout.components_root / "daily_bin_candidate" / "instruments" / "all.txt"
    original = all_path.read_bytes()
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._run_daily_benchmark_dump",
        _fake_dump,
    )

    result = build_daily_benchmark_component(layout, project_root=tmp_path)
    daily = layout.components_root / "daily_bin_candidate"
    receipt = json.loads(
        (layout.reports_root / "daily_benchmark_000300_completion.json").read_text(encoding="utf-8")
    )
    benchmark_line = "000300.SH\t2018-08-01\t2026-08-31"

    assert result["action"] == "BENCHMARK_ONLY_COMPLETION"
    assert {path.name for path in (daily / "features" / "000300.sh").iterdir()} == {
        f"{field}.day.bin" for field in DIRECT_BENCHMARK_FIELDS
    }
    assert (daily / "instruments" / "stock_universe.txt").read_bytes() == original
    assert all_path.read_text(encoding="utf-8").splitlines()[:-1] == original.decode().splitlines()
    assert all_path.read_text(encoding="utf-8").splitlines()[-1] == benchmark_line
    assert "000300.SH" not in (daily / "instruments" / "stock_universe.txt").read_text()
    assert receipt["schema_version"] == DIRECT_BENCHMARK_SCHEMA
    assert receipt["stock_universe_preserved"] is True
    assert receipt["calendar_offset"] == 0
    assert build_daily_benchmark_component(layout, project_root=tmp_path)["action"] == (
        "REUSE_COMPLETED_DIRECT_OUTPUT"
    )


def test_completion_rejects_calendar_gap(tmp_path, monkeypatch) -> None:
    layout = _layout(tmp_path)
    _prepare_inputs(layout, ["2026-08-28", "2026-08-31"])
    path = layout.components_root / "index_context" / "index_daily.h5"
    pd.read_hdf(path, key="data").iloc[[0]].to_hdf(path, key="data", mode="w", format="fixed")
    monkeypatch.setattr(
        "backend.services.dataset_release.direct_monthly._run_daily_benchmark_dump",
        _fake_dump,
    )

    with pytest.raises(DirectMonthlyError, match="exactly match"):
        build_daily_benchmark_component(layout, project_root=tmp_path)


def test_terminal_candidate_reopens_only_index_stage(tmp_path, monkeypatch) -> None:
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

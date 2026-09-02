from __future__ import annotations

from datetime import date
import json

import pandas as pd
import pytest

from backend.services.dataset_release.direct_monthly import (
    DIRECT_COMPONENTS,
    DIRECT_TERMINAL_STATUS,
    DirectMonthlyError,
    DirectMonthlyLayout,
    DirectMonthlyRunner,
    compact_status,
    component_plan,
    default_candidate_path,
    discover_latest_validated_baseline,
    read_state,
    _date_chunks,
    _filter_frame_to_pit,
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


def test_component_plan_rebuilds_minute_for_july_repair_without_expanding_components() -> None:
    plan = component_plan(july_minute_repaired=True)

    assert tuple(item.component for item in plan) == DIRECT_COMPONENTS
    assert {item.action for item in plan} == {"COMPONENT_REBUILD"}
    assert next(item for item in plan if item.component == "minute_bin").reason == (
        "july_repair_plus_august_tail"
    )
    assert next(item for item in plan if item.component == "daily_bin").reason == (
        "canonical_v2_pool_and_target_cutoff"
    )


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
        },
        validator=lambda _layout: {"status": "PASS"},
    )
    with pytest.raises(RuntimeError, match="injected interruption"):
        runner.run(layout)

    failed_state = read_state(layout)
    assert failed_state is not None
    assert failed_state["components"]["daily_bin"]["status"] == "PASS"
    assert failed_state["components"]["minute_bin"]["status"] == "FAILED"

    resumed = runner.run(layout)

    assert resumed["status"] == DIRECT_TERMINAL_STATUS
    assert calls.count("daily_bin") == 1
    assert calls.count("minute_bin") == 2
    assert calls.count("factor_h5_static") == 1
    assert calls.count("index_context") == 1


def test_runner_rejects_partial_registry_and_invalid_receipt(tmp_path) -> None:
    layout = _layout(tmp_path)
    with pytest.raises(DirectMonthlyError, match="all four components"):
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
    assert state["components"]["daily_bin"]["status"] == "FAILED"


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

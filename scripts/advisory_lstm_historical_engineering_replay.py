"""File-only engineering replay of the existing 126 historical LSTM cohorts."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from scripts import advisory_lstm_rank_value_review as files  # noqa: E402
from backend.services.advisory_model_first import lstm_historical_engineering_replay as replay  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402

OUTPUT = Path("X:/AIstock_temp/ADVISORY/lstm-historical-engineering-replay-20261102")
SOURCE_FILES = (
    "scripts/advisory_lstm_historical_engineering_replay.py",
    "backend/services/advisory_model_first/lstm_historical_engineering_replay.py",
    *files.SOURCE_FILES,
    "backend/services/advisory_model_first/lstm_top10_business_review_v1.py",
    "backend/services/advisory_model_first/prequential_top10_5td_study_v1.py",
)


def refs():
    result = {name: dict(path=str(path), role=role, sha256=files.file_sha(path), size_bytes=path.stat().st_size)
              for name, (path, role) in files.source_spec().items()}
    original = files.load_json(files.ARCHIVE / "recovery-v2/protocol.json")["sources"]["finance"]
    path = Path(original["path"].replace("/mnt/x/", "X:/", 1))
    result["original_finance"] = dict(**{k: original[k] for k in ("sha256", "size_bytes")},
        path=str(path), role="EXISTING_FROZEN_5TD_VALUATION_SOURCE")
    check_refs(result)
    files.verify_archive(dict(sources=result))
    return result


def check_refs(sources, names=None):
    for name in names or sources:
        item = sources[name]
        path = Path(item["path"])
        if path.stat().st_size != item["size_bytes"] or files.file_sha(path) != item["sha256"]:
            raise ValueError(f"historical source changed: {name}")


def closure(committed=False):
    result = {}
    for name in dict.fromkeys(SOURCE_FILES):
        result[name] = files.code_sha((ROOT / name).read_bytes())
        if committed and result[name] != files.code_sha(subprocess.check_output(["git", "show", f"HEAD:{name}"], cwd=ROOT)):
            raise ValueError(f"commit replay source before execution: {name}")
    return result


def output_root(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path.absolute() or not path.is_relative_to(OUTPUT):
        raise ValueError("engineering output must stay in its isolated task evidence root")
    return path


class Archive:
    def __init__(self, sources):
        self.refs, self.read_names, self.payoff_reads = sources, set(), []
        self.info = {}
        for fold in (1, 2, 3):
            _cutoff, first, last = replay.specialist.fold_window(fold)
            meta = self.read(f"prediction{fold}", columns=[replay.DAY, "target_trade_date"])
            for day, group in meta.groupby(replay.DAY, sort=True):
                key = str(day.date())
                if (key in self.info or len(group) != 50 or group.target_trade_date.nunique() != 1
                        or not first <= key <= last):
                    raise ValueError("original heldout fold dates/population differ")
                self.info[key] = dict(fold=fold, fold_end=last, target_date=str(group.target_trade_date.iloc[0].date()))
        if len(self.info) != 126:
            raise ValueError("engineering replay must retain every original heldout day")
        meta = self.read("finance", columns=[replay.DAY, "label_information_end", "valuation_status"])
        for day, group in meta.groupby(replay.DAY):
            key = str(day.date())
            if key not in self.info or len(group) != 50 or group.label_information_end.nunique() != 1:
                raise ValueError("original finance metadata differs from the heldout population")
            self.info[key]["horizon_end"] = str(group.label_information_end.iloc[0].date())
        self.days = sorted(self.info)
        if any("horizon_end" not in self.info[day] for day in self.days):
            raise ValueError("historical maturity metadata is missing")

    def read(self, name, **kwargs):
        self.read_names.add(name)
        return pd.read_parquet(self.refs[name]["path"], **kwargs)

    def day(self, day):
        return self.info[day]

    def token(self):
        self.read_names.clear()
        return replay.shadow.spo.sha(self.refs)

    def verify(self, token):
        if token != replay.shadow.spo.sha(self.refs):
            raise replay.InputReadinessError("historical source version changed during computation")
        check_refs(self.refs, self.read_names)

    def inputs(self, day):
        return self.read("inputs", filters=[(replay.DAY, "==", pd.Timestamp(day))])

    def model(self, fold):
        name = f"model{fold}"
        self.read_names.add(name)
        return files.load_json(self.refs[name]["path"])

    def saved(self, day):
        return self.read(f"prediction{self.day(day)['fold']}", filters=[(replay.DAY, "==", pd.Timestamp(day))])

    def finance_at(self, day, at):
        info = self.day(day)
        if not replay.mature_at(info, at):
            return None
        cutoff = min(str(at.date()), info["fold_end"])
        self.read_names.add("original_finance")
        value = replay.specialist.finance_projection(self.refs["original_finance"]["path"], day, cutoff, cutoff)
        value = value.loc[value[replay.DAY].eq(pd.Timestamp(day))].copy()
        if len(value) != 50:
            raise ValueError("mature historical finance lost original fifty rows")
        self.payoff_reads.append(dict(decision_date=day, virtual_as_of=at.isoformat(), horizon_end=info["horizon_end"],
            cutoff=cutoff, read_after_horizon=True))
        return value


def freeze(path):
    root = output_root(path)
    destination = root / "engineering_manifest.json"
    if destination.exists():
        plan = replay.shadow.read_packet(destination)
        if plan["source_closure"] != closure() or plan["output_root"] != str(root):
            raise ValueError("engineering source or output identity changed")
        check_refs(plan["sources"])
        files.verify_archive(dict(sources=plan["sources"]))
        return plan
    plan = dict(schema=replay.SCHEMA, source_closure=closure(committed=True), sources=refs(),
        producer_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        output_root=str(root), expected_days=126, expected_rows=6300, max_new_fits=0,
        scope="EXISTING_DEVELOPMENT_ENGINEERING_REPLAY_NOT_NEW_ECONOMIC_EVIDENCE",
        registered_at=datetime.now(timezone.utc).isoformat())
    return replay.shadow.publish(destination, plan)


def audit(engine, source):
    """Second arithmetic path against the previously saved projected valuations."""
    old = source.read("finance")
    panel, daily = source.read("panel"), source.read("daily")
    complete, common, slot_counts = [], [], old.valuation_status.value_counts().to_dict()
    for day in source.days:
        prediction = engine.prediction(day)
        evaluation = engine.evaluation(day)
        rows = old.loc[old[replay.DAY].eq(pd.Timestamp(day))].copy()
        info = source.day(day)
        if evaluation is None:
            if not rows.valuation_status.eq("IMMATURE").all() or rows.hypothetical_liquidation_net_bps.notna().any():
                raise ValueError("unvalued cohort is not the original immature population")
            continue
        actual = replay.shadow.frame(evaluation["finance"]).sort_values(replay.KEY).reset_index(drop=True)
        actual["label_information_end"] = pd.to_datetime(actual.label_information_end)
        expected = rows.drop(columns="fold").sort_values(replay.KEY).reset_index(drop=True)
        pd.testing.assert_frame_equal(actual[expected.columns], expected, check_dtype=False, check_exact=False, rtol=1e-10, atol=1e-10)
        p = replay.shadow.frame(prediction["predictions"])
        joined = p.merge(expected, on=replay.KEY, validate="one_to_one")
        result = evaluation["result"]
        original = panel.loc[panel.decision_date.eq(pd.Timestamp(day))].iloc[0]
        for arm, rank, old_column in (("native", "selection_effective_rank", "baseline"), ("specialist", "advisory_rank", "candidate")):
            slots = joined.loc[joined[rank].le(10)]
            known = slots.valuation_status.isin(["AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"])
            independent = sum(slots.hypothetical_liquidation_net_bps) / 10 if known.all() else None
            actual_value, old_value = result[arm + "_net_bps"], original[old_column]
            if independent is None:
                if actual_value is not None or pd.notna(old_value):
                    raise ValueError("unknown arm acquired a value during replay")
            elif not np.isclose(actual_value, independent, rtol=1e-10, atol=1e-8) or not np.isclose(actual_value, old_value, rtol=1e-10, atol=1e-8):
                raise ValueError("current and original per-arm net valuations differ")
        if result["status"] == "COMPLETE":
            complete.append(day)
        prior = daily.loc[daily.decision_date.eq(pd.Timestamp(day))].iloc[0]
        if pd.notna(prior.delta):
            if result["status"] != "COMPLETE" or not np.isclose(result["delta_bps"], prior.delta, atol=1e-8):
                raise ValueError("original three-arm common-day delta differs")
            common.append(result["delta_bps"])
        if info["horizon_end"] > info["fold_end"]:
            raise ValueError("fold cutoff was bypassed")
    if len(common) != 102:
        raise ValueError("original three-arm common population changed")
    return dict(days=126, predicted_rows=6300, independent_arithmetic="PASS", per_day_original_parity="PASS",
        complete_pair_days=len(complete), original_three_arm_common_days=len(common),
        original_common_delta_bps=float(np.mean(common)), original_slot_status_counts=slot_counts,
        historical_pit_proven=False, independent_oos_evidence=False, economic_confirmation=False)


class InjectedArchive:
    """Controlled defects in copies of real history; never historical revision claims."""
    scenario_is_fault_injection = True

    def __init__(self, base):
        self.base, self.ready, self.revision, self.change_once = base, True, 0, False
        self.problem, self.on_inputs, self.input_reads = None, None, 0

    def __getattr__(self, name):
        return getattr(self.base, name)

    def token(self):
        if not self.ready:
            raise replay.InputReadinessError("injected consumed input is not ready")
        self.original_token = self.base.token()
        return replay.shadow.spo.sha(dict(source=self.original_token, simulated_revision=self.revision))

    def verify(self, value):
        if self.change_once:
            self.revision, self.change_once = self.revision + 1, False
        if value != replay.shadow.spo.sha(dict(source=self.original_token, simulated_revision=self.revision)):
            raise replay.InputReadinessError("injected post-compute source revision drift")
        self.base.verify(self.original_token)

    def inputs(self, day):
        self.input_reads += 1
        value = self.base.inputs(day).copy(deep=True)
        if self.on_inputs:
            self.on_inputs()
        if self.problem == "missing":
            return value.iloc[:-1]
        if self.problem == "duplicate":
            return pd.concat([value, value.iloc[:1]], ignore_index=True)
        if self.problem == "infinite":
            value.loc[value.index[0], "ret_1"] = np.inf
        return value

    def finance_at(self, day, at):
        value = self.base.finance_at(day, at)
        if value is not None and self.problem == "finance_nan":
            value = value.copy(deep=True)
            key = value.loc[value.valuation_status.eq("AVAILABLE")].index[0]
            value.loc[key, "hypothetical_liquidation_net_bps"] = np.nan
        if value is not None and self.problem == "finance_duplicate":
            value = pd.concat([value, value.iloc[:1]], ignore_index=True)
        return value


def fault_scenarios(root, plan, day):
    base, result = Archive(plan["sources"]), []
    info = base.day(day)
    target = info["target_date"]

    def setup(name):
        source, now = InjectedArchive(base), [replay.shadow.clock(day, 20, 55)]
        engine = replay.HistoricalReplay(root=root / "fault_scenarios" / name, source=source,
            clock=lambda: now[0], plan_sha256=plan["sha256"])
        return source, now, engine

    def passed(name):
        result.append(dict(scenario=name, status="PASS", reference_day=day,
            provenance="REAL_ARCHIVED_INPUT_WITH_EXPLICIT_FAULT_INJECTION_NOT_PERFORMANCE_EVIDENCE"))

    for name, at in (("late_2105", replay.shadow.clock(day, 21, 5)),
                     ("midnight", replay.shadow.clock(target, 0, 15)),
                     ("last_minute", replay.shadow.clock(target, 9, 29))):
        source, now, engine = setup(name)
        source.ready = False
        assert engine.capture(day)["status"] == "WAITING_DATA" and not engine.path(day).exists()
        source.ready, now[0] = True, at
        assert engine.capture(day)["status"] == "PREDICTED"
        raw, reads = engine.path(day).read_bytes(), source.input_reads
        assert engine.capture(day)["status"] == "PREDICTION_REUSED"
        assert engine.path(day).read_bytes() == raw and source.input_reads == reads
        passed(name)
    for name in ("open_deadline", "crossing_open"):
        source, now, engine = setup(name)
        now[0] = replay.shadow.clock(target, 9, 30 if name == "open_deadline" else 29)
        if name == "crossing_open":
            source.on_inputs = lambda: now.__setitem__(0, replay.shadow.clock(target, 9, 30))
            try:
                engine.capture(day)
            except ValueError:
                pass
            else:
                raise AssertionError("crossing-open prediction was accepted")
        assert engine.capture(day)["status"] == "MISSED_CAPTURE_WINDOW"
        assert not engine.path(day).exists()
        passed(name)
    source, _now, engine = setup("revision")
    source.change_once = True
    assert engine.capture(day)["status"] == "WAITING_DATA" and not engine.path(day).exists()
    assert engine.capture(day)["status"] == "PREDICTED"
    passed("revision")
    for name in ("missing", "duplicate", "infinite", "finance_nan", "finance_duplicate"):
        source, now, engine = setup(name)
        source.problem = name
        try:
            if name.startswith("finance_"):
                assert engine.capture(day)["status"] == "PREDICTED"
                now[0] = replay.shadow.clock(info["horizon_end"], 15)
                engine.evaluate(day)
            else:
                engine.capture(day)
        except ValueError:
            pass
        else:
            raise AssertionError(f"injected invalid history was accepted: {name}")
        passed(name)
    source, now, engine = setup("first_complete")
    assert engine.capture(day)["status"] == "PREDICTED"
    now[0] = replay.shadow.clock(info["horizon_end"], 15)
    assert engine.evaluate(day)["status"] == "COMPLETE"
    reads, first = len(base.payoff_reads), engine.evaluation(day)["sha256"]
    assert engine.evaluate(day)["status"] == "EVALUATION_REUSED"
    assert first == engine.evaluation(day)["sha256"] and len(base.payoff_reads) == reads
    passed("first_complete")
    return result


def run(path):
    root, plan = output_root(path), freeze(path)
    if (root / "evaluated").exists():
        stage = read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan["sha256"], parent_sha256=None)
        return dict(status="ENGINEERING_REPLAY_REUSED", stage_sha256=stage["stage_sha256"], new_fits=0)
    source, now = Archive(plan["sources"]), [None]
    engine = replay.HistoricalReplay(root=root / "sandbox", source=source, clock=lambda: now[0], plan_sha256=plan["sha256"])
    events = []
    for day in source.days:
        h = source.day(day)["horizon_end"]
        events.extend([(replay.shadow.clock(day, 16, 31), "capture", day),
                       (replay.shadow.clock(h, 14, 59), "before_h", day),
                       (replay.shadow.clock(h, 15), "evaluate", day)])
    trace = []
    for at, kind, day in sorted(events):
        now[0] = at
        result = engine.capture(day) if kind == "capture" else engine.evaluate(day)
        if kind == "capture" and result["status"] not in {"PREDICTED", "PREDICTION_REUSED"}:
            raise ValueError("historical capture failed")
        if kind == "before_h" and result["status"] != "IMMATURE":
            raise ValueError("valuation became visible before H close")
        trace.append(dict(virtual_at=at.isoformat(), decision_date=day, stage=kind, **result))
    report = audit(engine, source)
    states = {day: ((engine.evaluation(day) or {}).get("result", {}).get("status", "IMMATURE_OR_PENDING")) for day in source.days}
    cases = {}
    for name, status in (("complete", "COMPLETE"), ("unknown", "UNKNOWN"), ("immature", "IMMATURE_OR_PENDING")):
        chosen = next((day for day in source.days if states[day] == status), None)
        if chosen is None:
            raise ValueError(f"required real historical display state missing: {status}")
        cases[name] = engine.status(chosen)
    faults = fault_scenarios(root, plan, cases["complete"]["selected"]["decision_date"])
    max_error = max(engine.prediction(day)["prediction_max_abs_error"] for day in source.days)
    report.update(prediction_max_abs_error=max_error, financial_payload_reads=len(source.payoff_reads),
        before_h_checks=sum(t["stage"] == "before_h" for t in trace), source_closure_unchanged=closure() == plan["source_closure"],
        real_history_fault_scenarios=len(faults),
        selected_case_rule="FIRST_CHRONOLOGICAL_DAY_PER_STATE_NOT_RETURN_SELECTION", new_fits=0, database_writes=0)
    check_refs(plan["sources"])
    if not report["source_closure_unchanged"]:
        raise ValueError("replay source changed during execution")
    fixture = dict(provenance=dict(schema=replay.SCHEMA, engineering_manifest_sha256=plan["sha256"],
        source_commit=plan["producer_commit"], selection=report["selected_case_rule"], real_historical_data=True), cases=cases)
    publish_stage(study_root=root, stage="evaluated", plan_sha256=plan["sha256"], parent_sha256=None,
        artifacts={"report.json": files.json_bytes(report), "trace.json": files.json_bytes(trace),
                   "payoff_reads.json": files.json_bytes(source.payoff_reads), "fault_scenarios.json": files.json_bytes(faults),
                   "ui_fixture.json": files.json_bytes(fixture)})
    stage = read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan["sha256"], parent_sha256=None)
    return dict(status="ENGINEERING_REPLAY_COMPLETE", report=report, stage_sha256=stage["stage_sha256"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["freeze", "run"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = freeze(args.output) if args.command == "freeze" else run(args.output)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

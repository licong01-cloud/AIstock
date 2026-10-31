"""One fixed prospective look on saved P20 packets, never a return-data source.

Contract: advisory_lstm_prospective_confirmation_v1_f1_design_20261101.md.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from statistics import NormalDist
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from scripts import advisory_lstm_rank_value_review as files  # noqa: E402
from backend.services.advisory_model_first import lstm_spo_shadow_v1 as shadow  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402

SCHEMA = "advisory_lstm_prospective_confirmation_v1"
LIVE = files.MODELS
REPORT = files.ARCHIVE / "recovery-v2/study/evaluated/report.json"
OUTPUT = Path("X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101")
RELEASE_SHA = "877bfb0f0826487504ffbd5fcc92947497b226e864e10297ed1718392eb0b74e"
RULE = dict(first_d="2026-11-02", cohorts=150, last_d="2027-07-31", grace_sessions=5,
            absolute_close="2027-08-31T15:00:00+08:00", alpha=.05, power=.8, null_bps=0.,
            hac_lag=10, sensitivity_lags=[4, 20], max_new_fits=0,
            primary="SPECIALIST_MINUS_NATIVE_TOP10_FIXED5_NET_BPS", complete_population_required=True)
SOURCE_FILES = (*files.SOURCE_FILES, "scripts/advisory_lstm_prospective_confirmation.py",
                "backend/services/advisory_model_first/lstm_spo_shadow_source_v1.py",
                "backend/services/advisory_model_first/entry_price_daily_service.py")


def now_utc():
    return datetime.now(timezone.utc)


def instant(value):
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("an aware observed clock is required")
    return result


def source_closure(require_committed=False):
    result = {}
    for relative in SOURCE_FILES:
        raw = (ROOT / relative).read_bytes()
        result[relative] = files.code_sha(raw)
        if require_committed:
            committed = subprocess.check_output(["git", "show", f"HEAD:{relative}"], cwd=ROOT)
            if result[relative] != files.code_sha(committed):
                raise ValueError(f"uncommitted confirmation source: {relative}")
    return result


def references():
    return dict(model=LIVE / "model.json", release=LIVE / "release.json", aggregate=REPORT)


def ref(path):
    path = Path(path)
    return dict(path=str(path.resolve()), sha256=files.file_sha(path), size_bytes=path.stat().st_size)


def check_ref(value, path):
    if value != ref(path):
        raise ValueError(f"immutable source changed: {Path(path).name}")


def destination(value):
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("output must be absolute")
    path = files.output_root(path)
    if not path.is_relative_to(OUTPUT.resolve()):
        raise ValueError("output must stay in the exact task evidence root")
    return path


def budget(aggregate):
    stats = aggregate["statistics"]
    n, se = stats["paired_days"], stats["se_bps"]
    if n != 102 or not np.isfinite(se) or se <= 0:
        raise ValueError("expected the already-consumed P19 aggregate noise")
    z = NormalDist().inv_cdf(1 - RULE["alpha"]) + NormalDist().inv_cdf(RULE["power"])
    sigma = se * np.sqrt(n)
    scenarios = []
    for factor in (1., 1.5, 2.):
        for effect in (25., 50., float(stats["mean_bps"]), 100.):
            scenarios.append(dict(noise_multiplier=factor, effect_scenario_bps=effect,
                required_complete_days=int(np.ceil((z * sigma * factor / effect)**2))))
    return dict(status="CONDITIONAL_PLANNING_ONLY", historical_models="THREE_DIFFERENT_FOLDS",
        historical_complete_days=n, long_run_sd_proxy_bps=float(sigma), scenarios=scenarios,
        planned_days=RULE["cohorts"], mde80_bps_by_noise={str(k): float(z * sigma * k / np.sqrt(RULE["cohorts"]))
                                                    for k in (1., 1.5, 2.)},
        guaranteed_power=False, business_minimum_effect_bps=None, new_fits=0)


def freeze(output):
    output = destination(output)
    path = output / "study/preregistered"
    if path.exists():
        stored = json.loads((path / "protocol.json").read_bytes())
        verify(stored)
        read_stage(path, stage="preregistered", plan_sha256=shadow.spo.sha(stored), parent_sha256=None)
        return dict(status="PROTOCOL_REUSED", protocol_sha256=shadow.spo.sha(stored))
    observed = now_utc()
    if observed >= shadow.clock(RULE["first_d"], 0):
        raise ValueError("prospective protocol cannot be registered after first D begins")
    closure = source_closure(require_committed=True)
    model = shadow.checked_model((LIVE / "model.json").read_bytes())
    release = shadow.read_packet(LIVE / "release.json")
    if release["sha256"] != RELEASE_SHA or model["payload_sha256"] != shadow.PAYLOAD_SHA:
        raise ValueError("exact P20 release/model required")
    protocol = dict(schema=SCHEMA, rule=RULE, target=shadow.TARGET, program_id=shadow.PROGRAM,
        binding_version_id=shadow.BINDING, release_sha256=RELEASE_SHA,
        study_root=str(output / "study"),
        sources={name: ref(path) for name, path in references().items()}, source_closure=closure,
        producer_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        registered_at=observed.isoformat(), prior_pilot_excluded="2026-10-30")
    sha = shadow.spo.sha(protocol)
    plan_budget = budget(json.loads(REPORT.read_bytes()))
    verify(protocol)
    publish_stage(study_root=output / "study", stage="preregistered", plan_sha256=sha,
                  parent_sha256=None, artifacts={"protocol.json": files.json_bytes(protocol),
                                               "budget.json": files.json_bytes(plan_budget)})
    return dict(status="PREREGISTERED", protocol_sha256=sha, budget=plan_budget)


def verify(protocol):
    if (protocol.get("schema") != SCHEMA or protocol.get("rule") != RULE
            or protocol.get("target") != shadow.TARGET or protocol.get("program_id") != shadow.PROGRAM
            or protocol.get("binding_version_id") != shadow.BINDING
            or protocol.get("release_sha256") != RELEASE_SHA
            or protocol.get("source_closure") != source_closure()
            or instant(protocol["registered_at"]) >= shadow.clock(RULE["first_d"], 0)
            or set(protocol.get("sources", {})) != set(references())):
        raise ValueError("confirmation protocol identity differs")
    destination(protocol["study_root"])
    for name, path in references().items():
        check_ref(protocol["sources"][name], path)


def load_protocol(path):
    path = Path(path)
    protocol = json.loads(path.read_bytes())
    verify(protocol)
    if path.resolve() != Path(protocol["study_root"]) / "preregistered/protocol.json":
        raise ValueError("protocol moved outside its unique registered study")
    read_stage(path.parent, stage="preregistered", plan_sha256=shadow.spo.sha(protocol), parent_sha256=None)
    return protocol


def calendar_snapshot(output):
    """Only the calendar table; existing bounded connection is read-only."""
    from backend.services.advisory_model_first.lstm_spo_shadow_source_v1 import LstmSpoShadowSource
    output = destination(output)
    with LstmSpoShadowSource().connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT cal_date,is_trading FROM market.trading_calendar
                WHERE cal_date >= %s AND cal_date <= %s ORDER BY cal_date LIMIT 1000""",
                (RULE["first_d"], "2027-09-30"))
            rows = cursor.fetchall()
    body = dict(schema="confirmation_calendar_v1", source="market.trading_calendar_READ_ONLY",
                observed_at=now_utc().isoformat(), rows=[[str(day), value] for day, value in rows],
                database_write=False)
    validate_calendar(body)
    return shadow.publish(output, body)


def validate_calendar(packet):
    if (packet.get("schema") != "confirmation_calendar_v1"
            or packet.get("source") != "market.trading_calendar_READ_ONLY"
            or packet.get("database_write") is not False):
        raise ValueError("untrusted calendar snapshot role")
    instant(packet["observed_at"])
    rows = packet["rows"]
    if not rows or len(rows) > 1000:
        raise ValueError("calendar snapshot is empty or unbounded")
    expected = date.fromisoformat(RULE["first_d"])
    for day, trading in rows:
        if date.fromisoformat(day) != expected or type(trading) is not bool:
            raise ValueError("calendar has duplicate/missing dates or non-boolean state")
        expected += timedelta(days=1)
    return [day for day, trading in rows if trading]


def scheduled(packet):
    dates = validate_calendar(packet)
    n = RULE["cohorts"]
    if len(dates) < n + 5 + RULE["grace_sessions"] or dates[n - 1] > RULE["last_d"]:
        return None
    return dict(decision_dates=dates[:n], windows={d: dates[i:i + 6] for i, d in enumerate(dates[:n])},
                look_at=shadow.clock(dates[n + 4 + RULE["grace_sessions"]], 15))


def inspect(protocol):
    verify(protocol)
    captured, missed = [], []
    # No directory traversal or file open under evaluations in this command.
    for directory in sorted((LIVE / "days").iterdir()):
        if directory.name < RULE["first_d"] or directory.name > RULE["last_d"]:
            continue
        date.fromisoformat(directory.name)
        if (directory / "prediction.json").exists():
            captured.append(directory.name)
        if (directory / "missed.json").exists():
            missed.append(directory.name)
    return dict(status="REGISTERED_AWAITING_FIXED_LOOK", prediction_files_present=captured, missed_files_present=missed,
                file_presence_is_capture_acceptance=False,
                excluded_pilot=protocol["prior_pilot_excluded"], evaluation_files_read=0,
                new_fits=0, database_writes=0, economic_confirmation=False, production_ranking_replaced=False)


def check_prediction(path, protocol, window, model):
    packet = shadow.read_packet(path)
    pub = packet["publication"]
    if (packet.get("schema") != shadow.SCHEMA or packet.get("release_sha256") != RELEASE_SHA
            or packet.get("model_sha256") != shadow.MODEL_SHA
            or pub["program_id"] != protocol["program_id"]
            or pub["binding_version_id"] != protocol["binding_version_id"]
            or pub["decision_date"] != window[0] or pub["target_date"] != window[1]
            or packet.get("physical_fit_count") != 0 or packet.get("native_policy_changed") is not False):
        raise ValueError("prediction exact identity differs")
    observed = instant(packet["observed_at"])
    if not shadow.clock(window[0], 15) <= instant(pub["created_at"]) <= observed < shadow.clock(window[1], 9, 30):
        raise ValueError("prediction not captured after publication and before target open")
    expected = shadow.predict_current(shadow.frame(packet["inputs"]), model)
    saved = shadow.frame(packet["predictions"])
    pd.testing.assert_frame_equal(expected.sort_index(axis=1).reset_index(drop=True),
                                  saved.sort_index(axis=1).reset_index(drop=True),
                                  check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-12)
    return packet


def check_evaluation(packet, prediction, window):
    if (packet["prediction_sha256"] != prediction["sha256"]
            or packet["source_receipt"].get("source") != "CURRENT_DATABASE_NON_VINTAGE"
            or packet["source_receipt"].get("database_write") is not False
            or packet["source_receipt"].get("actual_fill_proven") is not False
            or packet["source_receipt"]["calendar"] != window
            or packet["source_receipt"]["horizon_end"] != window[-1]
            or packet["source_receipt"].get("read_after_maturity") is not True
            or instant(packet["observed_at"]) < shadow.clock(window[-1], 15)):
        raise ValueError("evaluation parent/calendar/maturity differs")
    calculated = shadow.paired_result(shadow.frame(prediction["predictions"]), shadow.frame(packet["finance"]))
    if shadow.spo.sha(calculated) != shadow.spo.sha(packet["result"]):
        raise ValueError("saved result differs from frozen fixed-slot arithmetic")
    return calculated


def hac(values, lag):
    """Intercept-only Newey-West, Bartlett weights, n/(n-1) correction."""
    values = np.asarray(values, float)
    n = len(values)
    if values.ndim != 1 or n <= lag + 1 or not np.isfinite(values).all():
        raise ValueError("HAC requires a complete equally spaced finite series")
    mean = float(values.mean())
    residual = values - mean
    meat = float(residual @ residual)
    for k in range(1, lag + 1):
        meat += 2 * (1 - k / (lag + 1)) * float(residual[k:] @ residual[:-k])
    variance = meat / (n * (n - 1))
    if variance < -1e-10:
        raise ValueError("negative HAC variance")
    se = float(np.sqrt(max(variance, 0.)))
    z = NormalDist().inv_cdf(1 - RULE["alpha"])
    return dict(days=n, lag=lag, mean_bps=mean, se_bps=se, lower_one_sided95_bps=mean - z * se,
                degenerate=se <= 1e-12)


def decision(values):
    if len(values) != RULE["cohorts"]:
        raise ValueError("cannot replace or drop a scheduled date")
    primary = hac(values, RULE["hac_lag"])
    supported = not primary["degenerate"] and primary["lower_one_sided95_bps"] > RULE["null_bps"]
    return dict(status="PROSPECTIVE_STATISTICAL_SUPPORT" if supported else "INCONCLUSIVE_OR_NOT_SUPPORTED",
        statistical_support=supported, primary=primary,
        sensitivity=[hac(values, lag) for lag in RULE["sensitivity_lags"]],
        economic_confirmation=False, actual_fill_proven=False, historical_pit_proven=False,
        production_ranking_replaced=False, automatic_activation=False)


def descriptive_economics(rows):
    """Cohort risk and magnitude; never a NAV or an additional selected endpoint."""
    result = {}
    for arm in ("native", "specialist"):
        values = np.asarray([r["result"][arm + "_net_bps"] for r in rows], float)
        result[arm] = dict(mean_net_bps=float(values.mean()), p10_net_bps=float(np.quantile(values, .1)),
                           worst_cohort_bps=float(values.min()), positive_cohort_fraction=float((values > 0).mean()))
    result["mean_entries_per_day"] = float(np.mean([r["result"]["intervention_count"] for r in rows]))
    result["actual_execution_or_capacity_verified"] = False
    return result


def collect(protocol, schedule):
    model = shadow.checked_model((LIVE / "model.json").read_bytes())
    rows, consumed, reads = [], {}, 0
    for day in schedule["decision_dates"]:
        directory, window = LIVE / "days" / day, schedule["windows"][day]
        path = directory / "prediction.json"
        row = dict(decision_date=day, status="MISSING_PREDICTION", delta_bps=None)
        if not path.exists():
            if (directory / "missed.json").exists():
                consumed[str(directory / "missed.json")] = ref(directory / "missed.json")
                miss = shadow.read_packet(directory / "missed.json")
                if miss["publication"]["decision_date"] != day or miss["release_sha256"] != RELEASE_SHA:
                    raise ValueError("missed capture identity differs")
                row["status"] = "MISSED_CAPTURE_WINDOW"
            rows.append(row)
            continue
        consumed[str(path)] = ref(path)
        prediction = check_prediction(path, protocol, window, model)
        paths = sorted((directory / "evaluations").glob("*.json"))
        if len(paths) > 32:
            raise ValueError("evaluation version count exceeds bounded contract")
        versions = []
        for item in paths:
            before = ref(item)
            packet = shadow.read_packet(item)
            reads += 1
            consumed[str(item)] = before
            if instant(packet["observed_at"]) > schedule["look_at"]:
                continue
            result = check_evaluation(packet, prediction, window)
            versions.append((instant(packet["observed_at"]), item.name, result))
        versions.sort(key=lambda value: value[:2])
        complete = [v for v in versions if v[2]["status"] == "COMPLETE"]
        if len(complete) > 1 and complete[0][0] == complete[1][0]:
            raise ValueError("ambiguous first complete evaluation clock")
        if complete:
            result = complete[0][2]
            row.update(status="COMPLETE", delta_bps=result["delta_bps"], result=result,
                       evaluation_file=complete[0][1])
        else:
            row["status"] = "UNKNOWN" if versions else "NO_EVALUATION_AT_CUTOFF"
        rows.append(row)
    for path, value in consumed.items():
        check_ref(value, path)
    return rows, consumed, reads


def finalize(protocol, calendar_path, output):
    output = destination(output)
    verify(protocol)
    if output != Path(protocol["study_root"]).resolve():
        raise ValueError("formal result must use the unique registered study root")
    sha = shadow.spo.sha(protocol)
    target = output / "evaluated"
    if target.exists():
        manifest = read_stage(target, stage="evaluated", plan_sha256=sha, parent_sha256=None)
        return dict(status="FINAL_RESULT_REUSED", stage_sha256=manifest["stage_sha256"], evaluation_files_read=0)
    calendar_ref = ref(calendar_path)
    calendar = shadow.read_packet(Path(calendar_path))
    schedule = scheduled(calendar)
    now = now_utc()
    absolute = instant(RULE["absolute_close"])
    if now < absolute and (schedule is None or now < schedule["look_at"]):
        return dict(status="WAITING_CALENDAR" if schedule is None else "AWAITING_FIXED_LOOK",
                    look_at=schedule["look_at"].isoformat() if schedule else None, evaluation_files_read=0,
                    economic_confirmation=False, production_ranking_replaced=False)
    if schedule is None or schedule["look_at"] > absolute:
        rows, consumed, reads = [], {}, 0
        result = dict(status="NOT_EVALUABLE", reason="ABSOLUTE_BUDGET_EXPIRED_WITHOUT_COMPLETE_CALENDAR")
    else:
        rows, consumed, reads = collect(protocol, schedule)
        gaps = [row for row in rows if row["status"] != "COMPLETE"]
        result = (dict(status="NOT_EVALUABLE", reason="SCHEDULED_COHORTS_INCOMPLETE", unresolved_dates=len(gaps))
                  if gaps else decision([row["delta_bps"] for row in rows]))
        if not gaps:
            result["descriptive_economics"] = descriptive_economics(rows)
            result["prospective_test_performed"] = True
    result.update(economic_confirmation=False, production_ranking_replaced=False, new_fits=0,
                  database_writes=0, evaluation_files_read=reads, scheduled_days=RULE["cohorts"])
    receipt = dict(protocol_sha256=sha, calendar=calendar_ref, consumed=consumed,
                   source_closure=source_closure(), finalized_at=now.isoformat(),
                   primary_look_at=schedule["look_at"].isoformat() if schedule else None)
    verify(protocol)
    check_ref(calendar_ref, calendar_path)
    publish_stage(study_root=output, stage="evaluated", plan_sha256=sha, parent_sha256=None,
                  artifacts={"result.json": files.json_bytes(result), "cohorts.json": files.json_bytes(rows),
                             "receipt.json": files.json_bytes(receipt)})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["freeze", "calendar", "inspect", "finalize"])
    parser.add_argument("--output")
    parser.add_argument("--protocol")
    parser.add_argument("--calendar")
    args = parser.parse_args()
    if args.command in {"freeze", "calendar", "finalize"} and not args.output:
        parser.error("this command requires --output")
    if args.command in {"inspect", "finalize"} and not args.protocol:
        parser.error("this command requires --protocol")
    if args.command == "finalize" and not args.calendar:
        parser.error("finalize requires --calendar")
    if args.command == "freeze":
        result = freeze(args.output)
    elif args.command == "calendar":
        packet = calendar_snapshot(args.output)
        result = dict(status="CALENDAR_SNAPSHOT", rows=len(packet["rows"]), sha256=packet["sha256"])
    else:
        protocol = load_protocol(args.protocol)
        result = inspect(protocol) if args.command == "inspect" else finalize(protocol, args.calendar, args.output)
    print(files.json_bytes(result).decode(), end="")


if __name__ == "__main__":
    main()

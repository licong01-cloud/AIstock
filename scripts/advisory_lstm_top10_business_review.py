"""V2 prospective contract: user value target and return-blind capture viability.

The V1 protocol and operator remain archived; their economic algorithms are reused.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import advisory_lstm_prospective_confirmation as v1  # noqa: E402
from backend.services.advisory_model_first.lstm_top10_business_review_v1 import CONTRACT, target_status  # noqa: E402

SCHEMA = "lstm_top10_business_confirmation_v2"
OUTPUT = Path("X:/AIstock_temp/ADVISORY/lstm-business-review-20261101")
LEGACY = v1.OUTPUT / "study/preregistered/protocol.json"
LEGACY_SHA = "9b78a785c44158fb689089514756d0a590a28db5d34d80a531b53cd15cb094e2"
SOURCE_FILES = (*v1.SOURCE_FILES, "scripts/advisory_lstm_top10_business_review.py",
                "backend/services/advisory_model_first/lstm_top10_business_review_v1.py")


def closure(require_committed=False):
    values = {}
    for name in SOURCE_FILES:
        values[name] = v1.files.code_sha((ROOT / name).read_bytes())
        if require_committed:
            committed = subprocess.check_output(["git", "show", f"HEAD:{name}"], cwd=ROOT)
            if values[name] != v1.files.code_sha(committed):
                raise ValueError(f"uncommitted V2 source: {name}")
    return values


def legacy():
    old = json.loads(LEGACY.read_bytes())
    if v1.shadow.spo.sha(old) != LEGACY_SHA or old["rule"] != v1.RULE:
        raise ValueError("archived V1 protocol identity differs")
    v1.read_stage(LEGACY.parent, stage="preregistered", plan_sha256=LEGACY_SHA, parent_sha256=None)
    # Authenticate the original closure at its actual producer, not at changed main.
    for name, expected in old["source_closure"].items():
        committed = subprocess.check_output(["git", "show", f"{old['producer_commit']}:{name}"], cwd=ROOT)
        if v1.files.code_sha(committed) != expected:
            raise ValueError("archived V1 producer source differs")
    if (Path(old["study_root"]) / "evaluated").exists():
        raise ValueError("V1 already has a formal result; cannot silently start another test")
    return old


def refs():
    return {**v1.references(), "legacy_protocol": LEGACY, "legacy_manifest": LEGACY.parent / "manifest.json"}


def verify(protocol):
    if (protocol.get("schema") != SCHEMA or protocol.get("rule") != v1.RULE
            or protocol.get("business_contract") != CONTRACT or protocol.get("supersedes_v1") != LEGACY_SHA
            or protocol.get("program_id") != v1.shadow.PROGRAM or protocol.get("binding_version_id") != v1.shadow.BINDING
            or protocol.get("target") != v1.shadow.TARGET or protocol.get("release_sha256") != v1.RELEASE_SHA
            or protocol.get("source_closure") != closure() or set(protocol.get("sources", {})) != set(refs())
            or v1.instant(protocol["registered_at"]) >= v1.shadow.clock(v1.RULE["first_d"], 0)
            or Path(protocol["study_root"]).resolve() != OUTPUT.resolve() / "study"):
        raise ValueError("V2 protocol identity differs")
    for name, path in refs().items():
        v1.check_ref(protocol["sources"][name], path)
    legacy()


def load(path):
    path = Path(path)
    protocol = json.loads(path.read_bytes())
    verify(protocol)
    if path.resolve() != OUTPUT.resolve() / "study/preregistered/protocol.json":
        raise ValueError("V2 protocol moved outside its unique study")
    v1.read_stage(path.parent, stage="preregistered", plan_sha256=v1.shadow.spo.sha(protocol), parent_sha256=None)
    return protocol


def freeze():
    root = v1.files.output_root(OUTPUT)
    path = root / "study/preregistered/protocol.json"
    if path.exists():
        protocol = load(path)
        return dict(status="PROTOCOL_REUSED", protocol_sha256=v1.shadow.spo.sha(protocol))
    observed = v1.now_utc()
    if observed >= v1.shadow.clock(v1.RULE["first_d"], 0):
        raise ValueError("V2 must be registered before first D begins")
    old = legacy()
    v1.shadow.checked_model((v1.LIVE / "model.json").read_bytes())
    if v1.shadow.read_packet(v1.LIVE / "release.json")["sha256"] != v1.RELEASE_SHA:
        raise ValueError("exact frozen release required")
    protocol = dict(schema=SCHEMA, rule=v1.RULE, target=v1.shadow.TARGET, program_id=v1.shadow.PROGRAM,
        binding_version_id=v1.shadow.BINDING, release_sha256=v1.RELEASE_SHA, business_contract=CONTRACT,
        supersedes_v1=LEGACY_SHA, registered_at=observed.isoformat(), study_root=str(root / "study"),
        source_closure=closure(require_committed=True), sources={name: v1.ref(path) for name, path in refs().items()},
        producer_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip())
    verify(protocol)
    budget = v1.budget(json.loads(v1.REPORT.read_bytes()))
    supersession = dict(active_schema=SCHEMA, archived_protocol_sha256=LEGACY_SHA,
        archived_registered_at=old["registered_at"], reason="USER_100BPS_TARGET_AND_RETURN_BLIND_EARLY_DATA_STOP",
        economic_look_count=1, old_artifacts_unchanged=True, model_changed=False, new_fits=0)
    v1.publish_stage(study_root=root / "study", stage="preregistered", plan_sha256=v1.shadow.spo.sha(protocol),
        parent_sha256=None, artifacts={"protocol.json": v1.files.json_bytes(protocol),
            "budget.json": v1.files.json_bytes(budget), "supersession.json": v1.files.json_bytes(supersession)})
    return dict(status="PREREGISTERED_V2", protocol_sha256=v1.shadow.spo.sha(protocol), target_increment_bps=100.)


def calendar_snapshot(path):
    """Use the existing bounded read-only session; no price or label tables."""
    from backend.services.advisory_model_first.lstm_spo_shadow_source_v1 import LstmSpoShadowSource
    path = v1.files.output_root(Path(path))
    if not path.is_relative_to(OUTPUT.resolve()):
        raise ValueError("new calendar must stay in the V2 evidence root")
    with LstmSpoShadowSource().connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT cal_date,is_trading FROM market.trading_calendar
                WHERE cal_date >= %s AND cal_date <= %s ORDER BY cal_date LIMIT 1000""",
                (v1.RULE["first_d"], "2027-09-30"))
            rows = cursor.fetchall()
    packet = dict(schema="confirmation_calendar_v1", source="market.trading_calendar_READ_ONLY",
        observed_at=v1.now_utc().isoformat(), rows=[[str(d), trading] for d, trading in rows], database_write=False)
    v1.validate_calendar(packet)
    return v1.shadow.publish(path, packet)


def health(protocol, calendar_path):
    verify(protocol)
    calendar_ref = v1.ref(calendar_path)
    calendar = v1.shadow.read_packet(Path(calendar_path))
    dates = v1.validate_calendar(calendar)
    model = v1.shadow.checked_model((v1.LIVE / "model.json").read_bytes())
    now, rows, consumed = v1.now_utc(), [], {}
    for i, day in enumerate(dates[:v1.RULE["cohorts"]]):
        if day > v1.RULE["last_d"]:
            break
        path = v1.LIVE / "days" / day / "prediction.json"
        missed_path = path.parent / "missed.json"
        if path.exists() and missed_path.exists():
            raise ValueError("prediction and missed capture cannot coexist")
        target = dates[i + 1] if i + 1 < len(dates) else None
        state = "PLANNED"
        if path.exists():
            if target is None:
                state = "WAITING_CALENDAR"
            else:
                consumed[str(path)] = v1.ref(path)
                packet = v1.check_prediction(path, protocol, [day, target], model)
                if v1.instant(packet["observed_at"]) > now:
                    raise ValueError("prediction observed in the future")
                state = "CAPTURED"
        elif missed_path.exists():
            consumed[str(missed_path)] = v1.ref(missed_path)
            miss = v1.shadow.read_packet(missed_path)
            pub = miss["publication"]
            if (target is None or miss["release_sha256"] != v1.RELEASE_SHA
                    or pub["program_id"] != protocol["program_id"] or pub["binding_version_id"] != protocol["binding_version_id"]
                    or pub["decision_date"] != day or pub["target_date"] != target
                    or not v1.shadow.clock(target, 9, 30) <= v1.instant(miss["observed_at"]) <= now):
                raise ValueError("missed capture identity/calendar/clock differs")
            state = "IRREVERSIBLE_CAPTURE_GAP"
        elif target and now >= v1.shadow.clock(target, 9, 30):
            state = "IRREVERSIBLE_CAPTURE_GAP"
        elif now >= v1.shadow.clock(day, 15):
            state = "CAPTURE_WINDOW_OPEN" if target else "WAITING_CALENDAR"
        rows.append(dict(decision_date=day, target_date=target, capture_status=state))
    for path, value in consumed.items():
        v1.check_ref(value, path)
    v1.check_ref(calendar_ref, calendar_path)
    gaps = [r["decision_date"] for r in rows if r["capture_status"] == "IRREVERSIBLE_CAPTURE_GAP"]
    return dict(status="NOT_EVALUABLE_CAPTURE_GAP" if gaps else "CAPTURE_VIABLE_SO_FAR",
        observed_at=now.isoformat(), calendar_through=calendar["rows"][-1][0],
        scheduled_days=v1.RULE["cohorts"], known_calendar_decisions=len(rows),
        unmaterialized_decisions=v1.RULE["cohorts"] - len(rows), capture_gaps=gaps, rows=rows,
        captured_days=sum(r["capture_status"] == "CAPTURED" for r in rows),
        calendar=calendar_ref, consumed=consumed, evaluation_files_read=0, unknown_valuations_not_inspected=True,
        new_fits=0, economic_confirmation=False)


def finalize(protocol, calendar_path):
    verify(protocol)
    root, sha = Path(protocol["study_root"]), v1.shadow.spo.sha(protocol)
    if (root / "evaluated").exists():
        manifest = v1.read_stage(root / "evaluated", stage="evaluated", plan_sha256=sha, parent_sha256=None)
        return dict(status="FINAL_RESULT_REUSED", stage_sha256=manifest["stage_sha256"], evaluation_files_read=0)
    state = health(protocol, calendar_path)
    calendar = v1.shadow.read_packet(Path(calendar_path))
    schedule = v1.scheduled(calendar)
    now, absolute = v1.now_utc(), v1.instant(v1.RULE["absolute_close"])
    rows, consumed, reads = state["rows"], state["consumed"], 0
    if state["capture_gaps"]:
        result = dict(status="NOT_EVALUABLE_CAPTURE_GAP", reason="RETURN_BLIND_IRREVERSIBLE_CAPTURE_GAP",
                      capture_gaps=state["capture_gaps"], alpha_absent_proven=False)
    elif now < absolute and (schedule is None or now < schedule["look_at"]):
        return dict(status="WAITING_CALENDAR" if schedule is None else "AWAITING_FIXED_LOOK",
                    capture_health=state["status"], target_increment_bps=100., evaluation_files_read=0)
    elif schedule is None or schedule["look_at"] > absolute:
        result = dict(status="NOT_EVALUABLE", reason="ABSOLUTE_BUDGET_EXPIRED_WITHOUT_COMPLETE_CALENDAR")
    else:
        rows, consumed, reads = v1.collect(protocol, schedule)
        gaps = [r for r in rows if r["status"] != "COMPLETE"]
        if gaps:
            result = dict(status="NOT_EVALUABLE", reason="SCHEDULED_COHORTS_INCOMPLETE", unresolved_dates=len(gaps))
        else:
            result = v1.decision([r["delta_bps"] for r in rows])
            mean = result["primary"]["mean_bps"]
            result.update(target_status=target_status(mean),
                business_review_status="WORTH_BUSINESS_REVIEW" if result["statistical_support"] and mean >= 100.
                else "TARGET_OR_STATISTICAL_SUPPORT_NOT_ESTABLISHED", true_mean_above_target_proven=False,
                descriptive_economics=v1.descriptive_economics(rows))
    result.update(target_increment_bps=100., scheduled_days=150, evaluation_files_read=reads,
                  economic_confirmation=False, production_ranking_replaced=False, new_fits=0, database_writes=0)
    receipt = dict(protocol_sha256=sha, finalized_at=now.isoformat(), calendar=state["calendar"], consumed=consumed,
                   capture_health=state, source_closure=closure())
    verify(protocol)
    v1.check_ref(state["calendar"], calendar_path)
    v1.publish_stage(study_root=root, stage="evaluated", plan_sha256=sha, parent_sha256=None,
        artifacts={"result.json": v1.files.json_bytes(result), "cohorts.json": v1.files.json_bytes(rows),
                   "receipt.json": v1.files.json_bytes(receipt)})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["freeze", "calendar", "health", "finalize"])
    parser.add_argument("--protocol")
    parser.add_argument("--calendar")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze()
    elif args.command == "calendar":
        if not args.output:
            parser.error("calendar requires an explicit new --output path")
        packet = calendar_snapshot(args.output)
        result = dict(status="CALENDAR_SNAPSHOT", rows=len(packet["rows"]), sha256=packet["sha256"])
    else:
        if not args.protocol or not args.calendar:
            parser.error("health/finalize require --protocol and --calendar")
        protocol = load(args.protocol)
        result = health(protocol, args.calendar) if args.command == "health" else finalize(protocol, args.calendar)
    print(v1.files.json_bytes(result).decode(), end="")


if __name__ == "__main__":
    main()

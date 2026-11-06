"""Zero-fit prospective observation of one immutable P19 model; no native writes."""
from datetime import date, datetime, time, timezone
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from . import lstm_specialist_spo_v1 as specialist
from . import spo_top10_5td_v1 as spo
from .entry_price_confirmation import _immutable_json
from .entry_price_daily_service import EntryWorkBudget, _daily_lock
from .generic_daily_price_input_v1 import KEY, _records

TARGET = specialist.LSTM
PROGRAM = "advp_ed918504a02f4b15892f0dba65da139b"
BINDING = "advb_9673d612b6d14668b4d7545fcee8d277"
MODEL_SHA = "8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813"
PAYLOAD_SHA = "50af0b697ec12e69474c4faa74561c2627df642c62c3ddf7c766ce3001dab12f"
SCHEMA = "lstm_spo_shadow_v1"
TZ = ZoneInfo("Asia/Shanghai")
RELEASE = dict(schema=SCHEMA, target=TARGET, program_id=PROGRAM, binding_version_id=BINDING,
               model_sha256=MODEL_SHA, model_payload_sha256=PAYLOAD_SHA, fold=3,
               training_cutoff="2025-07-31", first_decision_date="2026-10-27",
               mode="SHADOW_ONLY", physical_fit_count=0,
               valuation_policy_sha256=spo.CONTRACT["valuation_policy_sha256"])


def clock(day, hour, minute=0):
    return datetime.combine(date.fromisoformat(str(day)), time(hour, minute), TZ)


def enabled():
    value = os.getenv("AISTOCK_ADVISORY_LSTM_SPO_SHADOW_ENABLED", "true").lower().strip()
    if value not in {"true", "false"}:
        raise ValueError("LSTM shadow enabled setting must be true or false")
    return value == "true"


def frame(records):
    result = pd.DataFrame(records)
    for name in KEY[:2]:
        result[name] = pd.to_datetime(result[name])
    return result


def checked_model(raw):
    if hashlib.sha256(raw).hexdigest() != MODEL_SHA:
        raise ValueError("frozen fold3 model file differs")
    packet = json.loads(raw)
    if (packet["payload_sha256"] != PAYLOAD_SHA or packet["payload_sha256"] != spo.sha(
            {k: v for k, v in packet.items() if k != "payload_sha256"})
            or packet["contract_sha256"] != spo.sha(specialist.CONTRACT) or packet["target"] != TARGET
            or packet["fold"] != 3 or packet["model"]["cutoff"] != RELEASE["training_cutoff"]):
        raise ValueError("frozen specialist envelope differs")
    return packet


def predict_current(inputs, packet):
    """New execution contract; the historical held-out predictor remains unchanged."""
    spo.validate_population(inputs)
    if (len(inputs) != 50 or any(not inputs[k].eq(v).all() for k, v in TARGET.items())
            or inputs[specialist.DAY].nunique() != 1
            or not inputs[specialist.DAY].ge(pd.Timestamp(RELEASE["first_decision_date"])).all()
            or packet.get("payload_sha256") != PAYLOAD_SHA
            or spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"}) != PAYLOAD_SHA):
        raise ValueError("current inference population/model identity differs")
    return spo.predict(inputs, packet["model"])


def read_packet(path):
    if path.stat().st_size > 4 * 1024**2:
        raise ValueError("shadow packet exceeds bounded input size")
    value = json.loads(path.read_bytes())
    if value.get("sha256") != spo.sha({k: v for k, v in value.items() if k != "sha256"}):
        raise ValueError("immutable shadow packet hash differs")
    return value


def publish(path, payload):
    packet = {**payload, "sha256": spo.sha(payload)}
    path.parent.mkdir(parents=True, exist_ok=True)
    _immutable_json(path, packet)
    return read_packet(path)


def paired_result(predictions, finance):
    """Ten fixed slots per arm; UNKNOWN never changes the denominator."""
    if finance.duplicated(list(spo.KEY)).any() or len(finance) != 50:
        raise ValueError("finance changed the original fifty rows")
    rows = predictions.merge(finance, on=list(spo.KEY), validate="one_to_one", suffixes=("", "_finance"))
    if len(rows) != 50 or not rows.valuation_policy_sha256.eq(RELEASE["valuation_policy_sha256"]).all():
        raise ValueError("finance population or valuation policy differs")
    result = {}
    for name, rank in (("native", "selection_effective_rank"), ("specialist", "advisory_rank")):
        slots = rows.loc[rows[rank].le(10)]
        if len(slots) != 10:
            raise ValueError("each arm requires its original ten slots")
        known = slots.valuation_status.isin(["AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"])
        values = slots.hypothetical_liquidation_net_bps
        if (known & values.isna()).any() or not pd.Series(values[known]).map(lambda v: float('-inf') < v < float('inf')).all():
            raise ValueError("known valuation lost its numeric value")
        if not values.loc[slots.valuation_status.eq("CASH_ENTRY_NOT_EXECUTABLE")].eq(0).all():
            raise ValueError("nonexecuted entry cash slot must remain zero")
        result[name + "_unknown_slots"] = int((~known).sum())
        result[name + "_net_bps"] = float(values.mean()) if known.all() else None
    complete = all(result[n + "_unknown_slots"] == 0 for n in ("native", "specialist"))
    result.update(status="COMPLETE" if complete else "UNKNOWN", actual_fill_proven=False,
                  delta_bps=result["specialist_net_bps"] - result["native_net_bps"] if complete else None,
                  intervention_count=int((rows.advisory_rank.le(10) & rows.selection_effective_rank.gt(10)).sum()))
    return result


class LstmSpoShadowService:
    def __init__(self, *, root=None, source=None, now=None):
        base = root if root is not None else os.getenv("AISTOCK_ADVISORY_MODEL_ROOT", "").strip()
        self.root = Path(base) / SCHEMA if base else None
        self.source = source
        self.now = now or (lambda: datetime.now(timezone.utc))

    def _release(self):
        if self.root is None or not (self.root / "release.json").exists():
            return None
        if not self.root.is_absolute() or self.root.resolve() != self.root.absolute():
            raise ValueError("shadow root must be absolute and not redirected")
        release = read_packet(self.root / "release.json")
        if any(release.get(k) != v for k, v in RELEASE.items()):
            raise ValueError("shadow release identity or policy differs")
        created = datetime.fromisoformat(release["created_at"])
        if created.utcoffset() is None or created > self.now():
            raise ValueError("shadow release has an invalid creation clock")
        checked_model((self.root / "model.json").read_bytes())
        return release

    def _source(self):
        if self.source is None:
            from .lstm_spo_shadow_source_v1 import LstmSpoShadowSource
            self.source = LstmSpoShadowSource(now=self.now)
        return self.source

    def capture(self, publication, release):
        d, t = publication["decision_date"], publication["target_date"]
        published = datetime.fromisoformat(publication["created_at"])
        if (publication["program_id"] != PROGRAM or publication["binding_version_id"] != BINDING
                or date.fromisoformat(d) >= date.fromisoformat(t) or published.utcoffset() is None
                or published > self.now() or published < clock(d, 15)):
            raise ValueError("publication target, binding or observed clock differs")
        if date.fromisoformat(d) < date.fromisoformat(RELEASE["first_decision_date"]):
            return dict(status="BEFORE_TRIAL", decision_date=d)
        directory = self.root / "days" / d
        path = directory / "prediction.json"
        if path.exists():
            stored = read_packet(path)
            if stored["release_sha256"] != release["sha256"] or stored["publication"] != publication:
                raise ValueError("publication changed after a frozen prediction")
            return dict(status="PREDICTION_REUSED", decision_date=d, sha256=stored["sha256"])
        if (directory / "missed.json").exists():
            missed = read_packet(directory / "missed.json")
            if missed["publication"] != publication or missed["release_sha256"] != release["sha256"]:
                raise ValueError("publication changed after missed capture")
            return dict(status="MISSED_CAPTURE_WINDOW", decision_date=d)
        if self.now() >= clock(t, 9, 30):
            miss = directory / "missed.json"
            if not miss.exists():
                publish(miss, dict(status="MISSED_CAPTURE_WINDOW", publication=publication,
                                   observed_at=self.now().isoformat(), release_sha256=release["sha256"]))
            return dict(status="MISSED_CAPTURE_WINDOW", decision_date=d)
        if self.now() < clock(d, 15):
            return dict(status="WAITING_D_CLOSE", decision_date=d)
        inputs, receipt = self._source().inputs(publication)
        if (not inputs[KEY[0]].eq(pd.Timestamp(d)).all() or not inputs[KEY[1]].eq(pd.Timestamp(t)).all()):
            raise ValueError("prediction input dates differ from frozen publication")
        prediction = predict_current(inputs, checked_model((self.root / "model.json").read_bytes()))
        self._source().verify_capture(receipt)
        observed = self.now()
        if observed >= clock(t, 9, 30):
            raise ValueError("prediction finished after target open; no retrospective capture")
        stored = publish(path, dict(schema=SCHEMA, release_sha256=release["sha256"], publication=publication,
            observed_at=observed.isoformat(), model_sha256=MODEL_SHA, inputs=_records(inputs),
            predictions=_records(prediction), source_receipt=receipt, physical_fit_count=0,
            historical_vintage_proven=False, native_policy_changed=False))
        return dict(status="PREDICTED", decision_date=d, sha256=stored["sha256"])

    def evaluate(self, path, release):
        prediction = read_packet(path)
        if prediction["release_sha256"] != release["sha256"]:
            raise ValueError("prediction release changed")
        directory = path.parent / "evaluations"
        prior = [read_packet(p) for p in sorted(directory.glob("*.json"))] if directory.exists() else []
        if any(p["prediction_sha256"] != prediction["sha256"] for p in prior):
            raise ValueError("evaluation prediction identity changed")
        if any(p["result"]["status"] == "COMPLETE" for p in prior):
            return dict(status="EVALUATION_REUSED", decision_date=path.parent.name)
        # The source checks calendar maturity before querying any financial outcomes.
        value = self._source().finance(prediction)
        if value is None:
            return dict(status="IMMATURE", decision_date=path.parent.name)
        finance, receipt = value
        result = paired_result(frame(prediction["predictions"]), finance)
        body = dict(prediction_sha256=prediction["sha256"], result=result, finance=_records(finance), source_receipt=receipt)
        identity = spo.sha(body)
        target = directory / (identity + ".json")
        if not target.exists():
            publish(target, dict(**body, observed_at=self.now().isoformat()))
        return dict(status=result["status"], decision_date=path.parent.name, evaluation=identity)

    def run_once(self):
        release = self._release()
        if release is None:
            return dict(status="NOT_CONFIGURED", results=[])
        if not enabled():
            return dict(status="PAUSED", results=[], release_sha256=release["sha256"])
        budget, results = EntryWorkBudget(), []
        with _daily_lock(self.root / "run", budget):
            try:
                publications = self._source().publications()
                if not publications:
                    results.append(dict(status="WAITING_NATIVE_PUBLICATION"))
                for publication in publications:
                    try:
                        budget.check()
                        results.append(self.capture(publication, release))
                    except Exception as exc:
                        results.append(self._failure("PREDICTION", exc, decision_date=publication["decision_date"]))
            except Exception as exc:
                results.append(self._failure("PREDICTION", exc))
            # Oldest unfinished days first, bounded, without replacing first complete observations.
            paths = sorted((self.root / "days").glob("*/prediction.json"))
            pending = []
            for path in paths:
                evaluated = path.parent / "evaluations"
                if not any(read_packet(p)["result"]["status"] == "COMPLETE" for p in evaluated.glob("*.json")):
                    pending.append(path)
            for path in pending[:20]:
                try:
                    budget.check()
                    results.append(self.evaluate(path, release))
                except Exception as exc:
                    results.append(self._failure("EVALUATION", exc, decision_date=path.parent.name))
            result = dict(status="FAILED" if any(r["status"] == "FAILED" for r in results) else "SHADOW_ONLY",
                          observed_at=self.now().isoformat(), release_sha256=release["sha256"], results=results,
                          pending_evaluations=len(pending), physical_fit_count=0, database_writes=0)
            # Failures are durable and visible; same failure content is retained once per local day.
            failures = [r for r in results if r["status"] == "FAILED"]
            if failures:
                day = self.now().astimezone(TZ).date().isoformat()
                error_path = self.root / "failures" / day / (spo.sha(failures) + ".json")
                if not error_path.exists():
                    publish(error_path, result)
            waiting = [r for r in results if r["status"] == "WAITING_DATA"]
            if waiting:
                day = self.now().astimezone(TZ).date().isoformat()
                wait_path = self.root / "waiting" / day / (spo.sha(waiting) + ".json")
                if not wait_path.exists():
                    publish(wait_path, result)
            return result

    @staticmethod
    def _failure(stage, error, **context):
        from backend.services.trading_core.errors import DataUnavailableError
        if isinstance(error, DataUnavailableError):
            return dict(status="WAITING_DATA", stage=stage, reason_code=error.error_code,
                        message=str(error), context=error.context, **context)
        return dict(status="FAILED", stage=stage, error_type=type(error).__name__, message=str(error), **context)

    def status(self, *, program_id, target_date=None):
        from .lstm_top10_business_review_v1 import CONTRACT, review_day, summarize
        if program_id != PROGRAM:
            return dict(status="NOT_APPLICABLE", program_id=program_id)
        release = self._release()
        if release is None:
            return dict(status="NOT_CONFIGURED", program_id=program_id)
        days, details = [], None
        for path in sorted((self.root / "days").glob("*/prediction.json")):
            p = read_packet(path)
            if p["release_sha256"] != release["sha256"]:
                raise ValueError("status prediction release identity differs")
            evaluations = [read_packet(v) for v in (path.parent / "evaluations").glob("*.json")]
            if any(e["prediction_sha256"] != p["sha256"] for e in evaluations):
                raise ValueError("status evaluation parent identity differs")
            evaluations.sort(key=lambda e: e["observed_at"])
            complete = [e for e in evaluations if e["result"]["status"] == "COMPLETE"]
            latest = (complete or evaluations)[0 if complete else -1] if evaluations else None
            day = dict(decision_date=path.parent.name, target_date=p["publication"]["target_date"],
                       prediction_sha256=p["sha256"], observed_at=p["observed_at"],
                       evaluation=latest["result"] if latest else dict(status="IMMATURE_OR_PENDING"),
                       evaluation_versions=len(evaluations))
            days.append(day)
            if target_date is None or str(target_date) == day["target_date"]:
                details = dict(**day, predictions=p["predictions"], source_receipt=p["source_receipt"],
                               business_review=review_day(p, latest))
        complete = [d["evaluation"] for d in days if d["evaluation"]["status"] == "COMPLETE"]
        failures = sorted((self.root / "failures").glob("*/*.json"), key=lambda p: p.stat().st_mtime)
        waits = sorted((self.root / "waiting").glob("*/*.json"), key=lambda p: p.stat().st_mtime)
        last_waiting = read_packet(waits[-1]) if waits else None
        if last_waiting:
            finished = {d["decision_date"] for d in days} | {p.parent.name for p in (self.root / "days").glob("*/missed.json")}
            if all(r.get("decision_date") in finished for r in last_waiting["results"] if r["status"] == "WAITING_DATA"):
                last_waiting = None
        return dict(status="SHADOW_ONLY" if enabled() else "PAUSED", program_id=PROGRAM, release=release, days=days, selected=details,
                    business_contract=CONTRACT, business_summary=summarize(days),
                    captured_days=len(days), complete_days=len(complete),
                    mean_delta_bps=sum(v["delta_bps"] for v in complete)/len(complete) if complete else None,
                    missed=[read_packet(p) for p in sorted((self.root / "days").glob("*/missed.json"))],
                    last_failure=read_packet(failures[-1]) if failures else None,
                    last_waiting=last_waiting,
                    independent_economic_confirmation=False, production_ranking_replaced=False)

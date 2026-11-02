"""Replay existing development files through public LSTM/valuation/display code.

Virtual clocks and storage are explicit offline adapters. They never relax the
production release's prospective capture guard or create a live recommendation.
"""
from datetime import datetime
import math
from pathlib import Path

import numpy as np
import pandas as pd

from . import lstm_specialist_spo_v1 as specialist
from . import lstm_spo_shadow_v1 as shadow
from . import lstm_top10_business_review_v1 as business
from backend.services.data_integrity.inputs import InputReadinessError

SCHEMA = "advisory_lstm_historical_engineering_replay_v1"
KEY = list(specialist.KEY)
DAY = specialist.DAY


def aware(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("historical replay requires an explicit aware virtual clock")
    return value.astimezone(shadow.TZ)


def records(frame):
    return shadow._records(frame)


def normalized(records_value):
    """Use the established JSON serializer, including explicit null for NaN."""
    from scripts.advisory_lstm_rank_value_review import json_bytes
    import json
    return json.loads(json_bytes(records_value))


def compare_predictions(actual, saved):
    columns = [*KEY, "selection_effective_rank", "advisory_rank", "selected", "no_rank_advice"]
    order = [*KEY[:-1], "selection_effective_rank"]
    a, b = (frame.sort_values(order).reset_index(drop=True) for frame in (actual, saved))
    pd.testing.assert_frame_equal(a[columns], b[columns], check_dtype=False)
    np.testing.assert_allclose(a.decision_score, b.decision_score, rtol=1e-10, atol=1e-10, equal_nan=True)
    difference = (a.decision_score - b.decision_score).abs().dropna()
    return float(difference.max()) if len(difference) else 0.


def verify_fees(finance):
    """Independent arithmetic on the already fixed per-stock valuation contract."""
    for row in finance.itertuples(index=False):
        value = row.hypothetical_liquidation_net_bps
        if row.valuation_status == "AVAILABLE":
            ratio = row.valuation_gross_terminal_ratio
            expected = 10000 * (ratio * (1 - 5.95 / 10000) / (1 + .95 / 10000) - 1)
            if not math.isfinite(expected) or not math.isclose(value, expected, abs_tol=1e-8):
                raise ValueError("saved net valuation contradicts the frozen buy/sell costs")
        elif row.valuation_status == "CASH_ENTRY_NOT_EXECUTABLE" and value != 0:
            raise ValueError("unexecuted cash slot is not zero")
        elif row.valuation_status not in {"AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"} and pd.notna(value):
            raise ValueError("unknown or immature slot acquired a numeric payoff")


def mature_at(info, at):
    return info["horizon_end"] <= info["fold_end"] and at >= shadow.clock(info["horizon_end"], 15)


class HistoricalReplay:
    def __init__(self, *, root, source, clock, plan_sha256):
        self.root, self.source, self.clock = Path(root), source, clock
        self.plan_sha256 = plan_sha256
        if not self.root.is_absolute() or self.root.resolve() != self.root.absolute():
            raise ValueError("replay output must be absolute and not redirected")

    def path(self, day):
        self.source.day(day)
        return self.root / "days" / day / "prediction.json"

    def prediction(self, day):
        packet = shadow.read_packet(self.path(day))
        info = self.source.day(day)
        if (packet.get("schema") != SCHEMA or packet.get("plan_sha256") != self.plan_sha256
                or packet.get("replay_context", {}).get("fold") != info["fold"]
                or packet.get("publication", {}).get("decision_date") != day
                or packet["publication"].get("target_date") != info["target_date"]):
            raise ValueError("historical packet changed its plan/fold/date identity")
        return packet

    def capture(self, day):
        info, now = self.source.day(day), aware(self.clock())
        path = self.path(day)
        if path.exists():
            return dict(status="PREDICTION_REUSED", sha256=self.prediction(day)["sha256"])
        missed = path.parent / "missed.json"
        if missed.exists():
            value = shadow.read_packet(missed)
            if value["plan_sha256"] != self.plan_sha256 or value["decision_date"] != day:
                raise ValueError("historical missed capture identity differs")
            return dict(status="MISSED_CAPTURE_WINDOW")
        if now >= shadow.clock(info["target_date"], 9, 30):
            shadow.publish(missed, dict(schema=SCHEMA, plan_sha256=self.plan_sha256, decision_date=day,
                clock_is_simulated=True, observed_at=now.isoformat(), status="MISSED_CAPTURE_WINDOW"))
            return dict(status="MISSED_CAPTURE_WINDOW")
        if now < shadow.clock(day, 15):
            return dict(status="WAITING_D_CLOSE")
        try:
            token = self.source.token()
            inputs = self.source.inputs(day)
            packet = self.source.model(info["fold"])
            predictions = specialist.predict_specialist(inputs, packet)
            error = compare_predictions(predictions, self.source.saved(day))
            self.source.verify(token)
        except InputReadinessError as exc:
            return dict(status="WAITING_DATA", reason_code=exc.error_code)
        finished = aware(self.clock())
        if finished >= shadow.clock(info["target_date"], 9, 30):
            raise ValueError("historical computation crossed target open; no prospective packet")
        context = dict(schema=SCHEMA, evidence="EXISTING_DEVELOPMENT_NON_VINTAGE",
            clock_is_simulated=True, fold=info["fold"], training_cutoff=packet["model"]["cutoff"],
            original_fold_end=info["fold_end"], source_binding_sha256=token,
            program_identity_use="CURRENT_UI_CONTAINER_NOT_HISTORICAL_PROGRAM_EXISTENCE",
            independent_oos_evidence=False, historical_pit_proven=False,
            scenario_is_fault_injection=bool(getattr(self.source, "scenario_is_fault_injection", False)))
        value = shadow.publish(path, normalized(dict(schema=SCHEMA, plan_sha256=self.plan_sha256,
            publication=dict(program_id=shadow.PROGRAM, binding_version_id=shadow.BINDING,
                decision_date=day, target_date=info["target_date"]),
            observed_at=finished.isoformat(), replay_context=context,
            model_payload_sha256=packet["payload_sha256"], inputs=records(inputs), predictions=records(predictions),
            prediction_max_abs_error=error, physical_fit_count=0, native_policy_changed=False)))
        return dict(status="PREDICTED", sha256=value["sha256"], max_abs_error=error)

    def evaluation(self, day):
        if not mature_at(self.source.day(day), aware(self.clock())):
            return None
        prediction = self.prediction(day)
        paths = (self.path(day).parent / "evaluations").glob("*.json")
        versions = sorted((shadow.read_packet(path) for path in paths), key=lambda p: p["observed_at"])
        versions = [v for v in versions if aware(datetime.fromisoformat(v["observed_at"])) <= aware(self.clock())]
        if any(v["prediction_sha256"] != prediction["sha256"] for v in versions):
            raise ValueError("historical evaluation parent differs")
        complete = [v for v in versions if v["result"]["status"] == "COMPLETE"]
        return complete[0] if complete else versions[-1] if versions else None

    def evaluate(self, day):
        prediction = self.prediction(day)
        if not mature_at(self.source.day(day), aware(self.clock())):
            return dict(status="IMMATURE")
        previous = self.evaluation(day)
        if previous and previous["result"]["status"] == "COMPLETE":
            return dict(status="EVALUATION_REUSED")
        token = self.source.token()
        finance = self.source.finance_at(day, aware(self.clock()))
        if finance is None:
            return dict(status="IMMATURE")
        verify_fees(finance)
        result = shadow.paired_result(shadow.frame(prediction["predictions"]), finance)
        self.source.verify(token)
        body = normalized(dict(prediction_sha256=prediction["sha256"], finance=records(finance), result=result))
        path = self.path(day).parent / "evaluations" / (shadow.spo.sha(body) + ".json")
        if not path.exists():
            shadow.publish(path, dict(**body, observed_at=aware(self.clock()).isoformat()))
        return dict(status=result["status"])

    def status(self, selected_day):
        days, selected = [], None
        for day in self.source.days:
            if not self.path(day).exists():
                continue
            prediction, evaluation = self.prediction(day), self.evaluation(day)
            if aware(datetime.fromisoformat(prediction["observed_at"])) > aware(self.clock()):
                continue
            review = business.review_day(prediction, evaluation)
            days.append(dict(decision_date=day, target_date=prediction["publication"]["target_date"],
                observed_at=prediction["observed_at"], evaluation={k:v for k,v in review.items() if k not in {"rows", "changes", "components"}}))
            if day == selected_day:
                context = dict(prediction["replay_context"], evaluation_as_of=(evaluation["observed_at"] if evaluation
                    else shadow.clock(self.source.day(day)["fold_end"], 15).isoformat()))
                selected = dict(decision_date=day, target_date=prediction["publication"]["target_date"],
                    observed_at=prediction["observed_at"], prediction_sha256=prediction["sha256"],
                    predictions=prediction["predictions"], evaluation=review,
                    business_review=review, replay_context=context)
        summary = business.summarize(days)
        return normalized(dict(ok=True, program_id=shadow.PROGRAM, status="SHADOW_ONLY",
            replay_context=selected["replay_context"] if selected else dict(schema=SCHEMA, clock_is_simulated=True),
            business_contract=business.CONTRACT, business_summary=summary, selected=selected, days=days,
            captured_days=len(days), complete_days=summary["complete_pairs"], mean_delta_bps=summary["mean_delta_bps"],
            independent_economic_confirmation=False, production_ranking_replaced=False, missed=[],
            last_waiting=None, last_failure=None))

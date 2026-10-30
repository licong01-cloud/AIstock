"""Frozen stock-probability research input contract; no fit, prediction or DB."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

CONTRACT = "QE-STOCK-PROBABILITY-ENTRY-GATE-V1"
SCHEMA = "qe_stock_probability_gate_v1"
MANIFEST_FILE = "qe_stock_probability_manifest.json"
RISK_FILE = "qe_stock_probability.parquet"
ACTION_FILE = "qe_stock_probability_actions.jsonl"
DATASET_SHA = "590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394"
DATASET_FILE_SHA = "321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73"
THRESHOLDS = {"F": 0.004557943883386495, "G": 0.004670301317604409}
RISK_HASHES = {
    "F": "a06cc11535e537bb43c0d96071ce7d755bf4ff4f919f0ff76b2a66a61be5b317",
    "G": "f2c303fda66a839b5c1c80985fe0d931c76c15a2d10b632d872430ea30de4000",
}
PREDICTION_HASHES = {
    "215535ded832bda646be7a86ceec0a6685ac75b7d4903b504007581e808c9322",
    "9b87acc8f7be7e4a7f51766ba7d73897f41bb6576289cbf493111805baa8fab0",
    "29a9b34e8dc6168b37e4964f8cb0039892bcda09ac9660490b8060466f2f6b1c",
}


class StockProbabilityContractError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise StockProbabilityContractError("QE_STOCK_PROBABILITY_" + code)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def symbol_key(value):
    value = str(value).upper()
    if re.fullmatch(r"(?:SH|SZ)\d{6}", value):
        value = value[2:] + "." + value[:2]
    require(bool(re.fullmatch(r"\d{6}\.(?:SH|SZ)", value)), "SYMBOL_INVALID")
    return value


def validate_manifest(manifest):
    m = manifest
    require(isinstance(m, dict) and m.get("schema_version") == SCHEMA
            and m.get("contract") == CONTRACT, "CONTRACT_INVALID")
    payload = {k: v for k, v in m.items() if k != "manifest_sha256"}
    require(m.get("manifest_sha256") == canonical_sha(payload), "MANIFEST_HASH_MISMATCH")
    arm = m.get("arm")
    require(arm in THRESHOLDS and m.get("threshold") == THRESHOLDS[arm], "THRESHOLD_INVALID")
    require(m.get("policy") == "block_new_buy_and_add_only"
            and m.get("unknown_policy") == "block_buy_keep_base_hold_sell"
            and m.get("training_cutoff") == "2025-12-31", "POLICY_INVALID")
    require(m.get("dataset_manifest_sha256") == DATASET_SHA, "DATASET_IDENTITY_MISMATCH")
    require(m.get("risk") == {"path": RISK_FILE, "sha256": RISK_HASHES[arm], "rows": 967083},
            "RISK_ASSET_NOT_ADMITTED")
    require(m.get("prediction_sha256") in PREDICTION_HASHES, "PREDICTION_NOT_ADMITTED")
    require((m.get("start"), m.get("end"), m.get("decision_days")) ==
            ("2026-01-05", "2026-09-29", 180), "ACCOUNT_WINDOW_INVALID")
    b = m.get("dataset_binding")
    require(isinstance(b, dict) and b.get("schema_version") == "qe_direct_v2_dataset_binding_v3"
            and b.get("release_id") == "qe_hmm_full_v2_20260930" and b.get("cutoff") == "2026-09-30",
            "DATASET_BINDING_INVALID")
    for key in ("provider_uri_day", "provider_uri_1min", "candidate_root"):
        require(isinstance(b.get(key), str) and bool(b[key]), "DATASET_BINDING_INVALID")
    for key in ("day_pins", "minute_pins", "selection_pins"):
        require(isinstance(b.get(key), dict) and bool(b[key]), "PIT_BINDING_INVALID")
    return m


def checked_calendar(manifest):
    """Verify the physical frozen release and metadata at the actual worker."""
    b = manifest["dataset_binding"]
    checks = [(Path(b["candidate_root"]) / "qe_dataset_manifest.json", DATASET_FILE_SHA)]
    for provider, pin, freq in (("provider_uri_day", "day_pins", "day"),
                                 ("provider_uri_1min", "minute_pins", "1min")):
        root = Path(b[provider])
        checks += [(root / "calendars" / f"{freq}.txt", b[pin]["calendar_sha256"]),
                   (root / "meta_export.json", b[pin]["meta_export_sha256"])]
    checks.append((Path(b["provider_uri_day"]) / "instruments" / b["selection_pins"]["instruments_file"],
                   b["selection_pins"]["instruments_sha256"]))
    for path, expected in checks:
        require(sha256(path) == expected, "PHYSICAL_DATASET_HASH_MISMATCH")
    path = Path(b["provider_uri_day"]) / "calendars/day.txt"
    calendar = pd.DatetimeIndex(pd.to_datetime(path.read_text().splitlines()))
    require(calendar.is_unique and calendar.is_monotonic_increasing
            and calendar.equals(calendar.normalize()), "CALENDAR_INVALID")
    return calendar


def validate_risk_frame(frame, calendar):
    required = {"signal_date", "input_as_of", "instrument", "probability", "availability", "reason"}
    require(set(frame.columns) == required, "RISK_COLUMNS_INVALID")
    frame = frame.copy()
    for col in ("signal_date", "input_as_of"):
        frame[col] = pd.to_datetime(frame[col], errors="raise")
        require(frame[col].notna().all() and frame[col].equals(frame[col].dt.normalize()), "RISK_TIME_INVALID")
    require(not frame.duplicated(["signal_date", "instrument"]).any(), "DUPLICATE_RISK_KEY")
    require(frame.instrument.map(lambda v: bool(re.fullmatch(r"\d{6}\.(?:SH|SZ)", str(v)))).all(),
            "SYMBOL_INVALID")
    available, unknown = frame.availability.eq("available"), frame.availability.eq("unknown")
    p = pd.to_numeric(frame.probability, errors="raise")
    require((available | unknown).all() and np.isfinite(p[available]).all()
            and p[available].between(0, 1).all() and frame.loc[available, "reason"].isna().all(),
            "AVAILABLE_PROBABILITY_INVALID")
    require(p[unknown].isna().all() and frame.loc[unknown, "reason"].map(
        lambda v: isinstance(v, str) and bool(v.strip())).all(), "UNKNOWN_PROBABILITY_INVALID")
    dates = pd.DatetimeIndex(frame.signal_date.unique()).sort_values()
    require(dates.isin(calendar).all(), "SIGNAL_DATE_INVALID")
    positions = calendar.get_indexer(dates)
    require((positions > 0).all(), "INPUT_ASOF_MISSING")
    prior = dict(zip(dates, calendar[positions - 1]))
    require(frame.input_as_of.equals(frame.signal_date.map(prior)), "EXTRA_LAG_OR_FUTURE_INPUT")
    # A rectangular full stock-day panel distinguishes structural omission from NA.
    require(len(frame) == len(dates) * frame.instrument.nunique(), "RISK_ROWS_MISSING")
    return frame.set_index(["signal_date", "instrument"]).sort_index()


class StockProbabilityInputs:
    def __init__(self, manifest_file, prediction_file="frozen_prediction.pkl"):
        manifest_path = Path(manifest_file)
        self.manifest = validate_manifest(json.loads(manifest_path.read_text(encoding="utf-8")))
        m = self.manifest
        self.calendar = checked_calendar(m)
        self.threshold = m["threshold"]
        risk_path = manifest_path.parent / RISK_FILE
        require(sha256(risk_path) == m["risk"]["sha256"], "RISK_HASH_MISMATCH")
        self.frame = validate_risk_frame(pd.read_parquet(risk_path), self.calendar)
        days = self.frame.index.get_level_values("signal_date").unique()
        expected = self.calendar[(self.calendar >= "2026-01-05") & (self.calendar <= "2026-09-30")]
        require(days.equals(expected) and len(days) == 181 and len(self.frame) == m["risk"]["rows"],
                "RISK_WINDOW_INCOMPLETE")
        require(sha256(prediction_file) == m["prediction_sha256"], "PREDICTION_HASH_MISMATCH")
        pred = pd.read_pickle(prediction_file)  # admitted original SHA; never arbitrary untrusted pickle
        require(isinstance(pred.index, pd.MultiIndex) and pred.index.names == ["datetime", "instrument"]
                and pred.index.is_unique, "PREDICTION_INDEX_INVALID")
        pdates = pd.DatetimeIndex(pred.index.get_level_values("datetime"))
        require(pdates.unique().sort_values().equals(expected), "PREDICTION_DATES_INCOMPLETE")
        keys = pd.MultiIndex.from_arrays([pdates, pred.index.get_level_values("instrument").map(symbol_key)],
                                         names=self.frame.index.names)
        require((self.frame.index.get_indexer(keys) >= 0).all(), "PREDICTION_RISK_COVERAGE_MISSING")
        self.decision_dates = self.calendar[(self.calendar >= m["start"]) & (self.calendar <= m["end"])]
        require(len(self.decision_dates) == m["decision_days"], "ACCOUNT_WINDOW_INCOMPLETE")
        self._day_cache = {}

    def validate_time(self, decision_date, signal_date):
        day, signal = pd.Timestamp(decision_date).normalize(), pd.Timestamp(signal_date).normalize()
        require(day in self.decision_dates, "DECISION_DATE_INVALID")
        pos = self.calendar.get_loc(day)
        require(pos > 0 and signal == self.calendar[pos - 1], "DECISION_TIME_INVALID")
        return day, signal

    def evaluate(self, instrument, decision_date, signal_date):
        _, signal = self.validate_time(decision_date, signal_date)
        if signal not in self._day_cache:
            require(signal in self.frame.index.get_level_values(0), "RISK_SIGNAL_DATE_MISSING")
            self._day_cache = {signal: self.frame.xs(signal).to_dict("index")}
        sid = symbol_key(instrument)
        rows = self._day_cache[signal]
        require(sid in rows, "RISK_STOCK_MISSING")
        row = rows[sid]
        available = row["availability"] == "available"
        p = float(row["probability"]) if available else None
        allowed = available and p <= self.threshold
        return {"instrument": str(instrument), "probability": p, "availability": row["availability"],
                "reason": row["reason"], "allowed": allowed,
                "action": "ALLOW_BUY" if allowed else ("RISK_WARNING_BLOCK" if available else "UNKNOWN_BLOCK")}


def validate_config(manifest, config_path):
    import yaml
    conf = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    b = manifest["dataset_binding"]
    require(conf["qlib_init"]["provider_uri"] == {"day": b["provider_uri_day"], "1min": b["provider_uri_1min"]}
            and conf.get("market") == b["selection_pins"]["instrument_name"], "EXECUTION_PROVIDER_MISMATCH")
    port = conf["port_analysis_config"]
    require(str(port["backtest"]["start_time"]) == manifest["start"]
            and str(port["backtest"]["end_time"]) == manifest["end"], "EXECUTION_WINDOW_MISMATCH")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    parser.add_argument("--prediction", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    inputs = StockProbabilityInputs(args.manifest, args.prediction)
    validate_config(inputs.manifest, args.config)
    print(json.dumps({"status": "STOCK_PROBABILITY_INPUTS_VERIFIED", "rows": len(inputs.frame),
                      "arm": inputs.manifest["arm"], "decision_days": len(inputs.decision_dates),
                      "new_fits": 0, "new_AF_inferences": 0}))

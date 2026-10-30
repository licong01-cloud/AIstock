"""Read-only physical-window checks for an explicitly enabled prediction replay."""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re

import pandas as pd
import yaml

PARAM = "qe_terminal_replay"
SCHEMA = "qe_terminal_replay_v1"
TERMINAL = "2026-09-30"


def mask_template_expressions(text):
    """Parse configuration without evaluating the unused frozen model recipe.

    Equal-length placeholders preserve YAML node source offsets. The actual
    template remains untouched for the existing runner's own rendering.
    """
    return re.sub(r"\{\{[^\r\n]*?\}\}", lambda match: "T" * len(match.group()), text)


def load_config(text):
    return yaml.safe_load(mask_template_expressions(text))


def require(ok, code):
    if not ok:
        raise ValueError("QE_TERMINAL_REPLAY_" + code)


def normalize_spec(value):
    fields = {"schema_version", "start", "terminal_date", "expected_sessions",
              "dataset_manifest_sha256", "prediction_sha256"}
    require(isinstance(value, dict) and set(value) == fields, "SPEC_FIELDS")
    require(value["schema_version"] == SCHEMA and value["terminal_date"] == TERMINAL, "VERSION_OR_TERMINAL")
    start = value["start"]
    require(isinstance(start, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", start), "START")
    try:
        start_date = date.fromisoformat(start)
    except ValueError as error:
        raise ValueError("QE_TERMINAL_REPLAY_START") from error
    require(start_date <= date.fromisoformat(TERMINAL), "START")
    require(type(value["expected_sessions"]) is int and value["expected_sessions"] > 0, "SESSION_COUNT")
    for key in ("dataset_manifest_sha256", "prediction_sha256"):
        require(isinstance(value[key], str) and re.fullmatch("[0-9a-f]{64}", value[key]), "IDENTITY")
    return dict(value)


def checked_calendar(path, digest):
    raw = Path(path).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == digest, "CALENDAR_HASH")
    values = pd.DatetimeIndex(pd.to_datetime(raw.decode("utf-8").splitlines()))
    require(len(values) > 1 and not values.isna().any() and values.is_unique
            and values.is_monotonic_increasing, "CALENDAR_GRID")
    return values


def validate(envelope, config, prediction):
    require(isinstance(envelope, dict) and set(envelope) == {"spec", "dataset_binding"}, "ENVELOPE")
    spec = normalize_spec(envelope["spec"])
    binding = envelope["dataset_binding"]
    require(binding["cutoff"] == spec["terminal_date"], "BINDING_CUTOFF")
    require(config["qlib_init"]["provider_uri"] == {
        "day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}, "PROVIDERS")
    require(config["market"] == binding["selection_pins"]["instrument_name"], "PIT")
    port = config["port_analysis_config"]
    require(str(port["backtest"]["start_time"]) == spec["start"]
            and str(port["backtest"]["end_time"]) == spec["terminal_date"]
            and port["backtest"]["exchange_kwargs"]["freq"] == "1min", "CONFIG_WINDOW")
    executor = port["executor"]
    inner = executor["kwargs"]["inner_executor"]
    require((executor["class"], executor["module_path"], inner["class"], inner["module_path"]) == (
        "BoundedTerminalNestedExecutor", "qe_terminal_interval_executor",
        "BoundedTerminalSimulatorExecutor", "qe_terminal_interval_executor"), "EXECUTORS")
    days = checked_calendar(Path(binding["provider_uri_day"]) / "calendars/day.txt", binding["day_pins"]["calendar_sha256"])
    minutes = checked_calendar(Path(binding["provider_uri_1min"]) / "calendars/1min.txt", binding["minute_pins"]["calendar_sha256"])
    require(days[-1] == pd.Timestamp(TERMINAL) and minutes[-1] == pd.Timestamp(TERMINAL + " 15:00:00"), "PHYSICAL_TERMINAL")
    selected = days[(days >= spec["start"]) & (days <= spec["terminal_date"])]
    require(len(selected) == spec["expected_sessions"] and str(selected[0].date()) == spec["start"], "SESSION_COUNT")
    raw = Path(prediction).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == spec["prediction_sha256"], "PREDICTION_HASH")
    # Only load the user-owned frozen pickle after its approved source hash matches.
    frame = pd.read_pickle(prediction)
    require(isinstance(frame, (pd.Series, pd.DataFrame)) and isinstance(frame.index, pd.MultiIndex)
            and set(frame.index.names) == {"datetime", "instrument"} and frame.index.is_unique, "PREDICTION_PANEL")
    dates = pd.DatetimeIndex(frame.index.get_level_values("datetime"))
    require(not dates.isna().any() and (dates == days[-2]).any(), "TERMINAL_PREVIOUS_SIGNAL")
    require(dates.max() <= days[-1], "PREDICTION_FUTURE")
    return {"contract": SCHEMA, "start": spec["start"], "end": TERMINAL, "days": len(selected),
            "last_signal_date": str(days[-2].date()), "last_minute": str(minutes[-1]),
            "prediction_sha256": spec["prediction_sha256"], "prediction_rows": len(frame),
            "dataset_manifest_sha256": spec["dataset_manifest_sha256"],
            "new_fits": 0, "new_inferences": 0, "new_backtests": 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("envelope")
    parser.add_argument("--config", required=True)
    parser.add_argument("--prediction", required=True)
    args = parser.parse_args()
    envelope = json.loads(Path(args.envelope).read_text(encoding="utf-8"))
    config = load_config(Path(args.config).read_text(encoding="utf-8"))
    print("QE_TERMINAL_REPLAY_PREFLIGHT " + json.dumps(validate(envelope, config, args.prediction)), flush=True)


if __name__ == "__main__":
    main()

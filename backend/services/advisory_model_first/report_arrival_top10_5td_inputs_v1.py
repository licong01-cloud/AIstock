"""Frozen literal-org-day arrival proxy; NON_VINTAGE exploratory inputs only."""

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES
from backend.services.advisory_model_first.peer_residual_top10_5td_v1 import validate_population

SCREEN_ROOT = "X:/AIstock_temp/ADVISORY/p12-report-information-inventory-20261024"
SCREEN_SHA = "c7f630429e4ab61d7146e2ba5b3589934d5cfacae94ac3f32fcd4f84f4327fbb"
CONTRACT_SHA = "026e80c0153fb56657ce31cc25787db28ae556815e796ad0b933a0fbaa0181d6"
BINDING = "589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c"
EVIDENCE_PINS = {
    "inventory_receipt.json": SCREEN_SHA,
    "inventory_contract.json": CONTRACT_SHA,
    "audit_receipt.json": "8e45ab97d1cda1c7730508c2c207ac7c2b8c882a88550a12b662b6ce83101adc",
    "input_validation_receipt.json": "c78c764da5c8995bc23de44fb0dfdfa778acb1a2e921fc93355c8520c7bf160a",
    "delivery_receipt.json": "40c92fca59cf5470c5c474b45ebdec735901bb31b02a8bb094d3369b2ca6d50c",
}
NEW = ("arrival_acceleration_proxy",)
COUNTS = ("recent_org_days", "baseline_org_days", "recent_literal_org_count", "baseline_literal_org_count")
EXACT = ("input_status", "recent_event_ids_json", "baseline_event_ids_json")
PANEL_COLUMNS = (
    "ts_code",
    "org_key",
    "report_date",
    "provider_create_time",
    "captured_at",
    "capture_id",
    "row_ordinal",
    "source_row_sha256",
    "vintage_status",
)
DAY = "decision_as_of_trade_date"
EVENT_KEY = ["ts_code", "org_key", "report_date"]


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def stat(path):
    s = Path(path).stat()
    return [s.st_size, s.st_mtime_ns]


def screen_evidence():
    root = node_path(SCREEN_ROOT)
    if any(file_sha(root / name) != value for name, value in EVIDENCE_PINS.items()):
        raise ValueError("screen evidence identity drift")
    receipt = json.loads((root / "inventory_receipt.json").read_bytes())
    audit = json.loads((root / "audit_receipt.json").read_bytes())
    tests = json.loads((root / "input_validation_receipt.json").read_bytes())
    dependent = {
        "inventory_rows.parquet": receipt["rows_sha256"],
        "event_ledger.parquet": receipt["events_sha256"],
        "inventory.py": receipt["code_sha256"],
        "audit.py": audit["audit_code_sha256"],
    }
    if any(file_sha(root / name) != value for name, value in dependent.items()):
        raise ValueError("screen dependent evidence drift")
    if (
        receipt["support"]["decision"] != "SUPPORTS_ONE_FUTURE_F1_PROPOSAL_ONLY"
        or receipt["new_fits"]
        or receipt["outcome_labels_read"]
        or not receipt["source_leases_unchanged"]
        or receipt["historical_pit_established"]
        or tests["passed"] != 13
        or not all(t["passed"] for t in tests["tests"])
        or audit["status"] != "PASS"
        or audit["outcome_labels_read"]
        or audit["new_fits"]
    ):
        raise ValueError("expected frozen label-free supported screen")
    return receipt


def sources(plan, expected=None):
    screen_evidence()
    c = json.loads((node_path(SCREEN_ROOT) / "inventory_contract.json").read_bytes())
    if plan["binding"]["binding_hash"] != BINDING:
        raise ValueError("original parent binding differs")
    # P12's old EPS overlap artifact is not a source or eligibility input here.
    stamps = {name: c["source_stamps"][name] for name in ("panel", "population")}
    paths = {name: node_path(item["path"]) for name, item in stamps.items()}
    paths["manifest"] = node_path(c["manifest_path"])
    pins = {"manifest": c["manifest_sha256"], "panel": c["panel_sha256"]}
    current = {name: stat(p) for name, p in paths.items()}
    if expected is not None and expected != current:
        raise ValueError("source stat lease drift")
    for name, item in stamps.items():
        if current[name] != [item["size"], item["mtime_ns"]]:
            raise ValueError("source no longer matches frozen input generation")
    if any(file_sha(paths[name]) != digest for name, digest in pins.items()):
        raise ValueError("source content drift")
    return paths, pins, current


def nonempty(series):
    return series.notna() & series.astype("string").str.strip().ne("").fillna(False)


def build_events(panel):
    """Count presence, not prediction rows or distinct publications."""
    p = panel.loc[:, list(PANEL_COLUMNS)].copy()
    if not p.ts_code.astype("string").str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").fillna(False).all():
        raise ValueError("unassignable source stock identity")
    ordinal = pd.to_numeric(p.row_ordinal, errors="raise")
    if (
        not nonempty(p.capture_id).all()
        or not np.isfinite(ordinal).all()
        or not ordinal.ge(0).all()
        or not ordinal.eq(np.floor(ordinal)).all()
        or not p.source_row_sha256.astype("string").str.fullmatch("[0-9a-f]{64}").fillna(False).all()
        or p.duplicated(["capture_id", "row_ordinal"]).any()
    ):
        raise ValueError("source occurrence reference invalid or duplicate")
    report = pd.to_datetime(p.report_date, errors="raise").dt.normalize()
    update = pd.to_datetime(p.provider_create_time, errors="raise").dt.normalize()
    p["report_date"] = report
    p["assumed_available_date"] = pd.concat([report, update], axis=1).max(axis=1, skipna=False) + pd.Timedelta(days=1)
    valid = nonempty(p.org_key) & report.notna() & update.notna()
    events = (
        p.loc[valid]
        .groupby(EVENT_KEY, sort=True, dropna=False)
        .agg(
            assumed_available_date=("assumed_available_date", "min"),
            source_rows=("capture_id", "size"),
        )
        .reset_index()
    )
    events["event_id"] = [
        hashlib.sha256(
            (
                json.dumps(
                    [str(s), str(o), str(d.date())], ensure_ascii=False, sort_keys=True, allow_nan=False, default=str
                )
                + "\n"
            ).encode()
        ).hexdigest()
        for s, o, d in events[EVENT_KEY].itertuples(index=False, name=None)
    ]
    return events, p.loc[~valid].copy()


def stock_context(events, invalid, day):
    day = pd.Timestamp(day)
    tainted = not invalid.empty and bool(
        (invalid.report_date.isna() | invalid.report_date.between(day - pd.Timedelta(days=119), day)).any()
    )
    visible = events.loc[events.assumed_available_date.le(day)]
    recent = visible.loc[visible.report_date.between(day - pd.Timedelta(days=29), day)]
    baseline = visible.loc[visible.report_date.between(day - pd.Timedelta(days=119), day - pd.Timedelta(days=30))]
    n, b = len(recent), len(baseline)
    status = "AVAILABLE_NON_VINTAGE_ARRIVAL_PROXY"
    if tainted:
        status = "UNKNOWN_SOURCE_IDENTITY_OR_CLOCK"
    elif n == 0:
        status = "NO_RECENT_OBSERVED_ORG_DAY_UNKNOWN"
    elif b == 0:
        status = "NO_BASELINE_OBSERVED_ORG_DAY_UNKNOWN"
    return dict(
        arrival_acceleration_proxy=float(np.log1p(n) - np.log1p(b / 3)) if status.startswith("AVAILABLE") else np.nan,
        input_status=status,
        recent_org_days=n,
        baseline_org_days=b,
        recent_literal_org_count=int(recent.org_key.nunique()),
        baseline_literal_org_count=int(baseline.org_key.nunique()),
        recent_snapshot_source_rows_diagnostic_only=int(recent.source_rows.sum()),
        recent_event_ids_json=json.dumps(sorted(recent.event_id)),
        baseline_event_ids_json=json.dumps(sorted(baseline.event_id)),
    )


def symbol(value):
    text = str(value).upper()
    return text[2:] + "." + text[:2] if len(text) == 8 and text[:2] in ("SH", "SZ", "BJ") else text


def verify_screen_alignment(frame):
    evidence = screen_evidence()
    screen = pd.read_parquet(
        node_path(SCREEN_ROOT) / "inventory_rows.parquet",
        columns=[*IDENTITY, *NEW, *COUNTS, *EXACT, "inside_development"],
    )
    screen = screen.loc[screen.inside_development].reset_index(drop=True)
    part = frame.reset_index(drop=True)
    if len(part) != evidence["development_rows"] or not part[list(IDENTITY)].equals(screen[list(IDENTITY)]):
        raise ValueError("original screen population differs")
    actual, expected = part[list(NEW)].to_numpy(float), screen[list(NEW)].to_numpy(float)
    if not np.allclose(actual, expected, rtol=0, atol=1e-12, equal_nan=True):
        raise ValueError("screen measurements differ")
    if not np.array_equal(part[list(COUNTS)].to_numpy(float), screen[list(COUNTS)].to_numpy(float)):
        raise ValueError("screen integer counts differ")
    if not part[list(EXACT)].equals(screen[list(EXACT)]):
        raise ValueError("screen unknown/event identity differs")
    finite = np.isfinite(actual) & np.isfinite(expected)
    return dict(
        rows=len(part),
        numeric_columns=len(NEW),
        exact_columns=len(EXACT) + len(COUNTS),
        exact_equal_including_nan=bool(np.array_equal(actual, expected, equal_nan=True)),
        values_equal_with_declared_tolerance=True,
        rtol=0,
        atol=1e-12,
        maximum_absolute_error=float(np.abs(actual[finite] - expected[finite]).max()) if finite.any() else 0.0,
        screen_sha256=SCREEN_SHA,
    )


def read_panel(plan, inputs, expected=None, budget=lambda: None):
    paths, pins, leases = sources(plan, expected)
    c = json.loads((node_path(SCREEN_ROOT) / "inventory_contract.json").read_bytes())
    validate_population(inputs)
    if not inputs[DAY].between(pd.Timestamp(c["development_start"]), pd.Timestamp(c["development_end"])).all():
        raise ValueError("input D outside registered development")
    result = inputs.loc[:, [*IDENTITY, "source_id", "parent_score", *INPUT_FEATURES]].reset_index(drop=True).copy()
    panel = pd.read_parquet(paths["panel"], columns=list(PANEL_COLUMNS))
    if len(panel) != c["expected_panel_rows"] or not panel.vintage_status.eq("NON_VINTAGE").all():
        raise ValueError("panel row/evidence class differs")
    budget()
    events, invalid = build_events(panel)
    budget()
    groups = {stock: group for stock, group in events.groupby("ts_code", sort=False)}
    bad = {stock: group for stock, group in invalid.groupby("ts_code", sort=False)}
    cache, contexts = {}, []
    for n, row in enumerate(result.itertuples(index=False), 1):
        day = pd.Timestamp(getattr(row, DAY))
        key = (symbol(row.instrument), day)
        if key not in cache:
            cache[key] = stock_context(groups.get(key[0], events.iloc[:0]), bad.get(key[0], invalid.iloc[:0]), day)
        contexts.append(cache[key])
        if n % 1000 == 0:
            budget()
    result = pd.concat([result, pd.DataFrame(contexts)], axis=1)
    if not result[list(IDENTITY)].equals(inputs[list(IDENTITY)].reset_index(drop=True)):
        raise ValueError("original population changed")
    sources(plan, leases)
    strict = pd.to_datetime(panel.captured_at, utc=True, errors="raise").lt(
        (pd.Timestamp(c["development_end"]) + pd.Timedelta(days=1)).tz_localize("UTC")
    )
    return result, dict(
        binding_hash=BINDING,
        source_pins=pins,
        source_stamps=leases,
        unique_stock_days=len(cache),
        minimum_consumed_D=str(result[DAY].min().date()),
        maximum_consumed_D=str(result[DAY].max().date()),
        source_rows=c["expected_panel_rows"],
        source_columns=list(PANEL_COLUMNS),
        event_count=len(events),
        invalid_source_rows=len(invalid),
        evidence_class="NON_VINTAGE",
        historical_pit_established=False,
        strict_capture_known_historical_rows=int(strict.sum()),
        sql_count=0,
        label_reads=0,
    )


def verify_consumed(plan, receipt):
    _, pins, _ = sources(plan, receipt["source_stamps"])
    if pins != receipt["source_pins"] or receipt["binding_hash"] != BINDING:
        raise ValueError("consumed source identity differs")

"""Frozen next-year EPS revision proxy; exploratory NON_VINTAGE inputs only."""

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES
from backend.services.advisory_model_first.peer_residual_top10_5td_v1 import validate_population

SCREEN_ROOT = "X:/AIstock_temp/ADVISORY/p10-report-revision-20261024"
SCREEN_SHA = "7d27623513717d6638f53d2d8b228b7779385813311a73eae7702335268352b2"
CONTRACT_SHA = "99992ed7de9ad750c3981cd84dad393f07cea1afa5a46fb4af6a23e4042aef84"
BINDING = "589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c"
EVIDENCE_PINS = {
    "screen_receipt.json": SCREEN_SHA,
    "signal_contract.json": CONTRACT_SHA,
    "screen_tests.json": "caf10bd83e204bcf8d3062d1ed39c19735d8651d2d573ce4fec51d1b72b02f07",
    "audit_receipt.json": "7dda7997390261eae1eebac374ed39b3aec8c141513229c24993f3248718cb67",
}
NEW = ("eps_revision_proxy",)
NUMERIC = (*NEW, "valid_literal_org_count", "unknown_literal_org_count", "latest_report_age", "changed_pair_count")
EXACT = ("input_status", "strict_capture_observed", "pair_trace_json", "target_forecast_period")
DAY = "decision_as_of_trade_date"


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
    receipt = json.loads((root / "screen_receipt.json").read_bytes())
    audit = json.loads((root / "audit_receipt.json").read_bytes())
    tests = json.loads((root / "screen_tests.json").read_bytes())
    dependent = {
        "screen_rows.parquet": receipt["rows_sha256"],
        "screen_report_revisions.py": receipt["source_code_sha256"],
        "audit_screen.py": audit["source_code_sha256"],
    }
    if any(file_sha(root / name) != value for name, value in dependent.items()):
        raise ValueError("screen dependent evidence drift")
    if (
        receipt["results"]["decision"] != "SUPPORTS_ONE_F1"
        or receipt["new_fits"]
        or receipt["outcome_labels_read"]
        or not receipt["original_population_unchanged"]
        or receipt["historical_pit_established"]
        or not tests["passed"]
        or not audit["passed"]
        or audit["outcome_labels_read"]
    ):
        raise ValueError("expected frozen label-free supported screen")
    return receipt


def sources(plan, expected=None):
    screen_evidence()
    c = json.loads((node_path(SCREEN_ROOT) / "signal_contract.json").read_bytes())
    if plan["binding"]["binding_hash"] != BINDING:
        raise ValueError("original parent binding differs")
    paths = {name: node_path(item["path"]) for name, item in c["source_stamps"].items()}
    paths["manifest"] = node_path(c["manifest_path"])
    pins = {"manifest": c["manifest_sha256"], "panel": c["accepted_panel_sha256"]}
    current = {name: stat(p) for name, p in paths.items()}
    if expected is not None and expected != current:
        raise ValueError("source stat lease drift")
    for name, item in c["source_stamps"].items():
        if current[name] != [item["size"], item["mtime_ns"]]:
            raise ValueError("source no longer matches frozen input generation")
    if any(file_sha(paths[name]) != digest for name, digest in pins.items()):
        raise ValueError("source content drift")
    return paths, pins, current


def revision(new, old):
    if not np.isfinite([new, old]).all():
        return np.nan
    # Scale before subtraction to avoid overflow; mathematically identical.
    scale = max(abs(new), abs(old))
    if scale == 0:
        return 0.0
    a, b = new / scale, old / scale
    return 2 * (a - b) / (abs(a) + abs(b))


def add_assumed_clock(panel):
    p = panel.copy()
    report = pd.to_datetime(p.report_date, errors="raise").dt.normalize()
    update = pd.to_datetime(p.provider_create_time, errors="raise").dt.normalize()
    p["assumed_available_date"] = pd.concat([report, update], axis=1).max(axis=1, skipna=False) + pd.Timedelta(days=1)
    return p


def day_value(rows):
    values = rows.eps.to_numpy(float)
    refs = rows.sort_values(["capture_id", "row_ordinal"])[
        ["capture_id", "row_ordinal", "source_row_sha256", "report_lineage_key"]
    ].to_dict("records")
    if not np.isfinite(values).all():
        return np.nan, "EPS_UNKNOWN", refs
    if len(np.unique(values)) != 1:
        return np.nan, "DATE_CONFLICT", refs
    return float(values[0]), "KNOWN", refs


def stock_context(target, day, contract):
    """target contains one stock and the predeclared next-year Q4 only."""
    day = pd.Timestamp(day)
    empty = dict(
        eps_revision_proxy=np.nan,
        valid_literal_org_count=0,
        unknown_literal_org_count=0,
        latest_report_age=np.nan,
        changed_pair_count=0,
        strict_capture_observed=False,
        pair_trace_json="[]",
    )
    if target.empty:
        return dict(empty, input_status="NO_TARGET_PERIOD_RECORD_IN_SNAPSHOT")
    if not target.quarter.eq(f"{day.year + 1}Q4").all() or target.ts_code.nunique() != 1:
        raise ValueError("stock/forecast period mixed in pairing")
    visible = target.loc[target.assumed_available_date.le(day)].copy()
    if visible.empty:
        return dict(empty, input_status="NO_ASSUMED_VISIBLE_TARGET_RECORD")
    items, values, ages = [], [], []
    for org, group in visible.groupby("org_key", dropna=False, sort=True):
        days = sorted(group.report_date.dropna().unique())
        item = dict(org_key=None if pd.isna(org) else str(org), reason="UNKNOWN")
        if pd.isna(org) or not str(org).strip() or not days:
            item["reason"] = "IDENTITY_UNKNOWN"
            items.append(item)
            continue
        current_day = pd.Timestamp(days[-1])
        current = group.loc[group.report_date.eq(current_day)]
        new, new_reason, new_refs = day_value(current)
        age = int((day - current_day).days)
        item.update(
            current_report_date=current_day.date().isoformat(),
            age_calendar_days=age,
            current_refs=new_refs,
            current_eps=new if np.isfinite(new) else None,
        )
        if age > contract["max_current_age_calendar_days"]:
            item["reason"] = "STALE_CURRENT_REPORT"
        elif new_reason != "KNOWN":
            item["reason"] = "CURRENT_" + new_reason
        elif len(days) < 2:
            item["reason"] = "NO_PRIOR_ASSUMED_VISIBLE_REPORT"
        else:
            prior_day = pd.Timestamp(days[-2])
            prior = group.loc[group.report_date.eq(prior_day)]
            old, old_reason, old_refs = day_value(prior)
            gap = int((current_day - prior_day).days)
            item.update(
                prior_report_date=prior_day.date().isoformat(),
                gap_calendar_days=gap,
                prior_refs=old_refs,
                prior_eps=old if np.isfinite(old) else None,
            )
            if gap > contract["max_pair_gap_calendar_days"]:
                item["reason"] = "PRIOR_REPORT_TOO_OLD"
            elif old_reason != "KNOWN":
                item["reason"] = "PRIOR_" + old_reason
            else:
                val = revision(new, old)
                if not np.isfinite(val) or abs(val) > 2 + 1e-12 or gap <= 0:
                    raise ValueError("invalid revision or report ordering")
                item.update(reason="VALID_PROXY_BASIS_CLOCK_UNVERIFIED", revision=float(val))
                values.append(val)
                ages.append(age)
        items.append(item)
    strict = bool(
        (pd.to_datetime(visible.captured_at, utc=True) < (day + pd.Timedelta(days=1)).tz_localize("UTC")).any()
    )
    if not values:
        reasons = sorted(set(i["reason"] for i in items))
        status = reasons[0] if len(reasons) == 1 else "MULTIPLE_UNKNOWN_ORG_REASONS"
        return dict(
            empty,
            input_status=status,
            unknown_literal_org_count=len(items),
            strict_capture_observed=strict,
            pair_trace_json=json.dumps(items, ensure_ascii=False, allow_nan=False),
        )
    return dict(
        eps_revision_proxy=float(np.median(values)),
        valid_literal_org_count=len(values),
        unknown_literal_org_count=len(items) - len(values),
        latest_report_age=min(ages),
        changed_pair_count=int(np.count_nonzero(values)),
        strict_capture_observed=strict,
        input_status="AVAILABLE_PROXY_BASIS_CLOCK_UNVERIFIED",
        pair_trace_json=json.dumps(items, ensure_ascii=False, allow_nan=False),
    )


def symbol(value):
    text = str(value).upper()
    return text[2:] + "." + text[:2] if len(text) == 8 and text[:2] in ("SH", "SZ", "BJ") else text


def verify_screen_alignment(frame):
    evidence = screen_evidence()
    screen = pd.read_parquet(node_path(SCREEN_ROOT) / "screen_rows.parquet")
    screen = screen.loc[screen.inside_development].reset_index(drop=True)
    part = frame.reset_index(drop=True)
    if len(part) != evidence["results"]["development_rows"] or not part[list(IDENTITY)].equals(screen[list(IDENTITY)]):
        raise ValueError("original screen population differs")
    actual, expected = part[list(NUMERIC)].to_numpy(float), screen[list(NUMERIC)].to_numpy(float)
    if not np.allclose(actual, expected, rtol=1e-12, atol=1e-12, equal_nan=True):
        raise ValueError("screen measurements differ")
    if not part[list(EXACT)].equals(screen[list(EXACT)]):
        raise ValueError("screen unknown/trace/period differs")
    return dict(
        rows=len(part),
        numeric_columns=len(NUMERIC),
        exact_columns=len(EXACT),
        exact_equal_including_nan=bool(np.array_equal(actual, expected, equal_nan=True)),
        values_equal_with_declared_tolerance=True,
        rtol=1e-12,
        atol=1e-12,
        screen_sha256=SCREEN_SHA,
    )


def read_panel(plan, inputs, expected=None, budget=lambda: None):
    paths, pins, leases = sources(plan, expected)
    c = json.loads((node_path(SCREEN_ROOT) / "signal_contract.json").read_bytes())
    validate_population(inputs)
    if not inputs[DAY].between(pd.Timestamp(c["development_start"]), pd.Timestamp(c["development_end"])).all():
        raise ValueError("input D outside registered development")
    # Rebuild only the original fields on recovery, avoiding duplicate derived columns.
    result = inputs.loc[:, [*IDENTITY, "source_id", "parent_score", *INPUT_FEATURES]].reset_index(drop=True).copy()
    panel = pd.read_parquet(paths["panel"], columns=c["panel_columns"])
    if len(panel) != c["expected_panel_rows"] or not panel.vintage_status.eq("NON_VINTAGE").all():
        raise ValueError("panel row/evidence class differs")
    budget()
    panel = add_assumed_clock(panel)
    years = sorted(result[DAY].dt.year.unique())
    panel = panel.loc[panel.quarter.isin([f"{year + 1}Q4" for year in years])]
    groups = {(stock, quarter): g for (stock, quarter), g in panel.groupby(["ts_code", "quarter"], sort=False)}
    empty = panel.iloc[:0]
    cache, contexts = {}, []
    for n, row in enumerate(result.itertuples(index=False), 1):
        day = pd.Timestamp(getattr(row, DAY))
        key = (symbol(row.instrument), day)
        quarter = f"{day.year + 1}Q4"
        if key not in cache:
            cache[key] = stock_context(groups.get((key[0], quarter), empty), day, c)
        contexts.append(dict(cache[key], target_forecast_period=quarter))
        if n % 1000 == 0:
            budget()
    result = pd.concat([result, pd.DataFrame(contexts)], axis=1)
    if not result[list(IDENTITY)].equals(inputs[list(IDENTITY)].reset_index(drop=True)):
        raise ValueError("original population changed")
    sources(plan, leases)
    return result, dict(
        binding_hash=BINDING,
        source_pins=pins,
        source_stamps=leases,
        unique_stock_days=len(cache),
        minimum_consumed_D=str(result[DAY].min().date()),
        maximum_consumed_D=str(result[DAY].max().date()),
        source_rows=c["expected_panel_rows"],
        evidence_class=c["evidence_class"],
        historical_pit_established=False,
        strict_capture_observed_rows=int(result.strict_capture_observed.sum()),
        sql_count=0,
        label_reads=0,
    )


def verify_consumed(plan, receipt):
    _, pins, _ = sources(plan, receipt["source_stamps"])
    if pins != receipt["source_pins"] or receipt["binding_hash"] != BINDING:
        raise ValueError("consumed source identity differs")

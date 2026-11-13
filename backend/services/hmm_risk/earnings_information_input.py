"""Frozen financial observations; explicit backcast semantics, never DB/latest."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import re

import pandas as pd

from .contracts import canonical_sha256
from .monthly_release_input import _Files

PACKAGE_PIN = "bbdaa3542d89fbbf8558a446da574914860babaa1e7e59dd62589257965d6f1c"
RAW_PIN = "64302167dd4c0445e2b273cde9c6fefa3f8f3c4bf3f06ff3d04df069e9e59ea7"
AUTHORITY_PIN = "e6b984ccaadec5bddc8ada28c4a57d2331f1cc5739b958629b946ef547a2c84e"
PROJECTION = dict(
    schema_version="hmm_financial_source_projection_v1",
    source_dataset="market.tushare_fina_indicator_raw",
    source_api="fina_indicator_vip",
    security_identity_id="szse_300114_302132",
    source_ts_code="300114.SZ",
    canonical_ts_code="302132.SZ",
    effective_start="2010-08-27",
    effective_end="2025-02-16",
    evidence_file_sha256=AUTHORITY_PIN,
    evidence_row_hash="4a23cbb8068f46ba9d10644e31442f82cf0dc2fa7db6898c813dc648311f6a42",
)
SOURCE_CUTOFF = pd.Timestamp("2026-10-30T23:59:59.999999+08:00")


def need(condition, message):
    if not condition:
        raise ValueError("hmm_earnings_input_invalid: " + message)


def canonical_symbol(symbol, day):
    need(isinstance(symbol, str) and re.fullmatch(r"\d{6}\.(SZ|SH|BJ)", symbol), "invalid raw security code")
    if symbol == PROJECTION["source_ts_code"]:
        need(date(2010, 8, 27) <= day <= date(2025, 2, 16), "financial alias outside approved interval")
        return PROJECTION["canonical_ts_code"]
    need(symbol not in {"000022.SZ", "000043.SZ"}, "unapproved historical financial alias")
    return symbol


def choose_version(rows):
    """Consensus of the entire earliest local timestamp tie; never best/latest."""
    eligible = [r for r in rows if pd.Timestamp(r["first_seen"]) <= SOURCE_CUTOFF]
    if not eligible:
        return None
    stamp = min(pd.Timestamp(r["first_seen"]) for r in eligible)
    tied = [r for r in eligible if pd.Timestamp(r["first_seen"]) == stamp]
    values = {r["value"] for r in tied}
    reason = (
        "AMBIGUOUS_EARNINGS_VERSION"
        if len(values) != 1
        else ("MISSING_EARNINGS_FIELD" if tied[0]["value"] is None else None)
    )
    return dict(
        symbol=tied[0]["symbol"],
        ann=tied[0]["ann"],
        period=tied[0]["period"],
        value=tied[0]["value"] if reason is None else None,
        reason=reason,
        first_seen=stamp.isoformat(),
        selected_lineages=sorted(r["lineage"] for r in tied),
        all_lineages=sorted(r["lineage"] for r in rows),
    )


class Disclosures:
    """Prefix maximum report-period/announcement, with no finite-value backfill."""

    def __init__(self, events, calendar):
        need(calendar == sorted(set(calendar)), "calendar must be unique/sorted")
        self.calendar, self.series = calendar, {}
        self.sessions = set(calendar)
        grouped = defaultdict(list)
        for row in events:
            if row["period"] <= row["ann"]:
                grouped[(row["symbol"], row["ann"], row["period"])].append(row)
        selected = [choose_version(rows) for rows in grouped.values()]
        by_symbol = defaultdict(list)
        for event in selected:
            if event is not None:
                by_symbol[event["symbol"]].append(event)
        for symbol, rows in by_symbol.items():
            dates, best, current = [], [], None
            for row in sorted(rows, key=lambda r: (r["ann"], r["period"])):
                if current is None or (row["period"], row["ann"]) > (current["period"], current["ann"]):
                    current = row
                dates.append(row["ann"])
                best.append(current)
            self.series[symbol] = dates, best
        self.events = sorted((r for r in selected if r is not None), key=lambda r: (r["symbol"], r["ann"], r["period"]))

    def at(self, symbol, as_of):
        need(as_of in self.sessions, "as-of is not a frozen open session")
        dates, best = self.series.get(symbol, ([], []))
        at = bisect_left(dates, as_of) - 1  # ann strictly before as_of, including weekends.
        if at < 0:
            return dict(value=None, reason="NO_EARNINGS_DISCLOSURE")
        event = best[at]
        age = bisect_right(self.calendar, as_of) - bisect_right(self.calendar, event["period"])
        reason = "STALE_EARNINGS_DISCLOSURE" if age > 252 else event["reason"]
        return dict(
            **{k: v for k, v in event.items() if k not in {"value", "reason"}},
            age_sessions=age,
            value=event["value"] if reason is None else None,
            reason=reason,
        )


def load(package_root, authority_path, projection, calendar):
    """Authenticate every shared member, all raw rows and the separate namespace."""
    need(projection == PROJECTION, "financial namespace projection differs")
    files = _Files(Path(package_root))
    manifest_path = files.path("manifest.json")
    need(files.digest(manifest_path) == PACKAGE_PIN, "financial manifest SHA differs")
    manifest = files.read(manifest_path)
    need(
        manifest.get("schema_version") == "aistock_shared_financial_raw_observations_v1"
        and manifest.get("announcement_date_window") == dict(start="2022-03-21", end="2026-10-30", closed="both")
        and len(manifest.get("files", {})) == 11,
        "financial manifest schema/window differs",
    )
    for relative, pin in manifest["files"].items():
        files.verify((relative, pin["sha256"], pin["bytes"]))
    raw_path = files.path("tushare_fina_indicator_raw.parquet")
    need(files.digest(raw_path) == RAW_PIN, "selected financial source SHA differs")
    authority_path = files.external(Path(authority_path))
    need(files.digest(authority_path) == AUTHORITY_PIN, "financial projection evidence SHA differs")
    authority = files.read(authority_path)
    evidence = [r for r in authority["rows"] if r.get("row_hash") == PROJECTION["evidence_row_hash"]]
    need(
        len(evidence) == 1
        and all(
            evidence[0].get(k) == PROJECTION[k]
            for k in ("security_identity_id", "source_ts_code", "canonical_ts_code", "effective_start", "effective_end")
        ),
        "financial projection evidence row differs",
    )
    frame = pd.read_parquet(raw_path)
    need(len(frame) == 182385 and frame.raw_observation_id.is_unique, "raw count/identity differs")
    events, reasons, exceptional = [], Counter(), []
    for row in frame.itertuples(index=False):
        payload = json.loads(row.raw_payload)
        digest = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()
        need(digest == row.source_row_hash == row.payload_recomputed_sha256, "raw payload hash differs")
        ann, period = row.ann_date.isoformat(), row.report_period.isoformat()
        need(
            payload["ts_code"] == row.ts_code
            and payload["ann_date"] == ann.replace("-", "")
            and payload["end_date"] == period.replace("-", "")
            and row.source_api == "fina_indicator_vip"
            and "2022-03-21" <= ann <= "2026-10-30",
            "raw identity/date projection differs",
        )
        need(
            pd.isna(row.published_at)
            and row.publication_time_quality == "UNKNOWN"
            and row.vintage_quality == "NON_VINTAGE",
            "source time/vintage semantics differ",
        )
        first = pd.Timestamp(row.first_seen_at)
        need(not pd.isna(first) and first.tzinfo is not None, "invalid first local observation")
        value = payload["netprofit_yoy"]
        need(value is None or type(value) in (int, float) and math.isfinite(value), "nonfinite earnings field")
        in_domain = re.fullmatch(r"\d{6}\.(SZ|SH)", row.ts_code) is not None
        record = dict(
            symbol=canonical_symbol(row.ts_code, row.ann_date) if in_domain else row.ts_code,
            ann=ann,
            period=period,
            value=value,
            first_seen=first.isoformat(),
            lineage=str(row.raw_observation_id),
        )
        reason = (
            "INVALID_FINANCIAL_REPORT_TIMING"
            if period > ann
            else (
                "POST_SOURCE_CUTOFF_OBSERVATION"
                if first > SOURCE_CUTOFF
                else "OUTSIDE_SHSZ_EQUITY_DOMAIN"
                if not in_domain
                else None
            )
        )
        reasons[reason or "ELIGIBLE_SOURCE_ROW"] += 1
        if reason:
            exceptional.append(dict(**record, reason=reason, source_row_hash=digest))
        if period <= ann and in_domain:
            events.append(record)
    state = Disclosures(events, calendar)
    reasons.update(r["reason"] for r in state.events if r["reason"])
    files.unchanged()
    state.files = files
    state.evidence = dict(
        package_manifest_file_sha256=PACKAGE_PIN,
        selected_file_sha256=RAW_PIN,
        raw_rows=len(frame),
        event_count=len(state.events),
        reasons=dict(reasons),
        exceptional_records=exceptional,
        financial_source_projection=projection,
        projection_sha256=canonical_sha256(projection),
        historical_as_known=False,
        vintage="NON_VINTAGE",
        events_sha256=canonical_sha256(state.events),
        consumed_files={str(p): digest for p, digest in files.hashes.items()},
    )
    return state

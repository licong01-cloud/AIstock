"""Advisory consumption projection; never changes source facts or PIT eligibility."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date

from backend.services.market_data.instrument_validator import normalize_ts_code
from .catalog import RULE_VERSION, SCOPE_VERSION
from .contracts import DENSE_TRADED_BASIS, digest, effective_partition
from .stock_scope import is_supported_stock_code

POLICY = "advisory_decision_members_missing_chip_v1"
# Only dated security-key validators can support this projection. Aggregate,
# sparse/reference and unverified source contracts retain their original gates.
STOCK_DATASETS = frozenset({"bak_basic", "cyq_perf", "daily_basic", "moneyflow_ts",
                            "kline_daily_raw", "kline_minute_raw", "adj_factor", "stk_limit"})


def _canonical_gap_code(code, row):
    from backend.data_service.security_source_identity import load_default_security_source_identity_manifest
    day = date.fromisoformat(str(row.get("trade_date", row.get("fact_date"))))
    matches = {r.canonical_ts_code for r in load_default_security_source_identity_manifest().rows
               if r.source_dataset == "market." + row["dataset"] and r.source_ts_code == code
               and r.effective_start is not None and r.effective_end is not None
               and r.effective_start <= day <= r.effective_end}
    if len(matches) > 1:
        raise ValueError("ambiguous dated source security identity")
    return next(iter(matches), code)


def scope_identity(members, day):
    codes = sorted({normalize_ts_code(code) for code in members})
    if not codes or any(not is_supported_stock_code(code) for code in codes):
        raise ValueError("Advisory decision PIT members are empty or invalid")
    return {"policy": POLICY, "decision_date": str(day), "member_count": len(codes),
            "universe_sha256": digest(codes)}


def resolve_scope(conn, pit, day):
    """Match native inference: decision-day members, over their full lookback.

    Do NOT filter each historical row by that day's eligibility: a restored
    member can still need its old observations for the frozen model window.
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT ts_code FROM market.stock_universe_pit_spans
            WHERE universe_key=%s AND rule_version=%s
              AND eligible_start<=%s AND eligible_end>=%s ORDER BY ts_code""",
            (pit["universe_key"], pit["rule_version"], day, day))
        members = frozenset(normalize_ts_code(row[0]) for row in cur.fetchall())
    return scope_identity(members, day), members


def _validated_gaps(row):
    """Require a complete, disjoint key ledger, not a truncated UI gap sample."""
    metadata = row.get("metadata")
    validation = metadata.get("validation") if isinstance(metadata, Mapping) else None
    if not isinstance(validation, Mapping) or validation.get("state") != "INCOMPLETE":
        return None
    checks = validation.get("checks")
    if row["dataset"] in {"bak_basic", "cyq_perf"} and (
            not isinstance(checks, Mapping) or checks.get("coverage_basis") != DENSE_TRADED_BASIS):
        return None
    counts = validation.get("counts")
    if not isinstance(counts, list) or len(counts) != 1 or not isinstance(counts[0], Mapping):
        return None
    c = counts[0]
    values = [c.get(k) for k in ("expected", "valid", "legal_na", "missing", "invalid", "unverified")]
    if (any(type(v) is not int or v < 0 for v in values) or values[0] <= 0
            or sum(values[1:]) != values[0] or c["unverified"] != 0):
        return None
    groups = validation.get("gaps")
    if not isinstance(groups, list):
        return None
    gaps, seen = [], set()
    for group in groups:
        if not isinstance(group, Mapping):
            return None
        kind, symbols = group.get("classification"), group.get("symbols")
        if kind not in {"MISSING", "INVALID"} or not isinstance(symbols, list):
            return None
        for symbol in symbols:
            if not isinstance(symbol, str):
                return None
            code = _canonical_gap_code(normalize_ts_code(symbol), row)
            if not is_supported_stock_code(code) or code in seen:
                return None
            seen.add(code)
            gaps.append((code, kind))
    if (sum(k == "MISSING" for _, k in gaps) != c["missing"]
            or sum(k == "INVALID" for _, k in gaps) != c["invalid"] or not gaps):
        return None
    return sorted(gaps), digest(validation)


def partition_projection(row, scope, members):
    effective = effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
    if (scope.get("policy") != POLICY or not effective["current"]
            or effective["state"] != "INCOMPLETE" or row["dataset"] not in STOCK_DATASETS):
        return None
    validated = _validated_gaps(row)
    if validated is None:
        return None
    gaps, validation_digest = validated
    outside, missing = [], []
    for code, kind in gaps:
        if code not in members:
            outside.append({"symbol": code, "classification": kind})
        elif row["dataset"] == "cyq_perf" and kind == "MISSING":
            # User-approved missing-input policy: known source gaps remain NaN.
            # No row deletion, source-complete claim, zero or forward filling.
            missing.append(code)
        else:
            return None
    return {"policy": POLICY, "scope_sha256": digest(scope), "source_state": "INCOMPLETE",
            "validation_sha256": validation_digest, "outside_scope": outside,
            "missing_as_nan": missing}


def verify_projection(ref, row, scope):
    """Revalidate the immutable partition proof even at detached artifact readback."""
    proof = ref.get("consumer_projection")
    if not isinstance(proof, Mapping) or not isinstance(scope, Mapping):
        return False
    outside = proof.get("outside_scope", [])
    validated = _validated_gaps(row)
    if validated is None or not isinstance(outside, list):
        return False
    # The PIT membership hash is separately matched to the actual native read;
    # verify_current also independently reloads membership in the DB snapshot.
    excluded = {g.get("symbol") for g in outside if isinstance(g, Mapping)}
    reconstructed_members = {code for code, _ in validated[0]} - excluded
    return partition_projection(row, scope, reconstructed_members) == proof


def verify_missing_chip_values(binding, frame):
    """Raw captured input must preserve audited source gaps before frozen processing."""
    import pandas as pd
    missing = [(r["fact_date"], s) for r in binding["partition_refs"]
               for s in r.get("consumer_projection", {}).get("missing_as_nan", [])]
    if not missing:
        return
    if frame is None or frame.empty or not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("missing chip policy requires observable raw input rows")
    chip_columns = [c for c in frame.columns if str(c).startswith("cp_")]
    if not chip_columns:
        raise ValueError("missing chip source columns are absent from raw input")
    codes = frame.index.get_level_values("instrument").map(normalize_ts_code)
    days = pd.to_datetime(frame.index.get_level_values("datetime")).strftime("%Y-%m-%d")
    for day, code in missing:
        selected = frame.loc[(codes == code) & (days == day), chip_columns]
        if len(selected) != 1 or not selected.isna().all().all():
            raise ValueError(f"audited missing chip input was filled or omitted: {code}/{day}")

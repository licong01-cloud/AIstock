"""Bounded SOURCE-only, additive official quote repair; no DB writes or policy inference."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib

import pandas as pd

from .canonical import canonical_json_bytes
from .errors import SourceManifestError
from .sw_l2_quote_policy import quote_is_expected

REQUIRED = ("pct_change", "vol", "amount")


def _finite(value):
    try:
        return value is not None and Decimal(str(value)).is_finite()
    except InvalidOperation:
        return False


def repair_quote_partition(rows, *, codes, calendar, cas, provider=None):
    """Only requested missing/non-finite fields can be filled from sw_daily.

    Raw provider rows, exact request, capture time and units are CAS-pinned.
    Empty replies remain unresolved; no quote-availability exception is made.
    Each request covers at most one code/month (<=31 source observations).
    """
    from .monthly_industry_facts import QUOTE_FIELDS
    keys = [(code, day.isoformat()) for day in calendar for code in codes if quote_is_expected(code, day)]
    key_set = set(keys)
    facts = {(row["index_code"], row["trade_date"]): dict(row) for row in rows}
    if len(facts) != len(rows):
        raise SourceManifestError("official industry quote keys are duplicated")
    missing = {key: [field for field in REQUIRED if not _finite(facts.get(key, {}).get(field))]
               for key in keys}
    missing = {key: fields for key, fields in missing.items() if fields}
    requests = []
    changed = set()
    if provider is not None:
        for code in sorted({key[0] for key in missing}):
            dates = sorted(day for item, day in missing if item == code)
            params = {"ts_code": code, "start_date": dates[0].replace("-", ""),
                      "end_date": dates[-1].replace("-", ""), "fields": "ts_code,trade_date," + ",".join(QUOTE_FIELDS)}
            try:
                response = provider.sw_daily(**params)
            except Exception as exc:
                raise SourceManifestError(f"official quote provider request failed: {code}/{dates[0]}..{dates[-1]}") from exc
            if response is None or not isinstance(response, pd.DataFrame) or len(response) > 31:
                raise SourceManifestError(f"official quote provider response exceeds code/month scope: {code}")
            captured = []
            seen = set()
            for raw in response.to_dict(orient="records"):
                if set(raw) != {"ts_code", "trade_date", *QUOTE_FIELDS}:
                    raise SourceManifestError("official quote provider fields differ")
                try:
                    day = datetime.strptime(str(raw["trade_date"]), "%Y%m%d").date().isoformat()
                except ValueError as exc:
                    raise SourceManifestError("official quote provider date is invalid") from exc
                key = (str(raw["ts_code"]), day)
                if key[0] != code or not dates[0] <= day <= dates[-1] or key not in key_set or key in seen:
                    raise SourceManifestError("official quote provider key is outside request or duplicated")
                seen.add(key)
                values = {}
                for field in QUOTE_FIELDS:
                    value = raw[field]
                    if value is None or pd.isna(value):
                        values[field] = None
                    elif not _finite(value):
                        raise SourceManifestError("official quote provider numeric value is invalid")
                    else:
                        values[field] = str(value)
                if any(values[field] is not None and Decimal(values[field]) < 0 for field in ("vol", "amount")):
                    raise SourceManifestError("official quote provider volume/amount is negative")
                captured.append({"index_code": code, "trade_date": day, **values})
                if key not in missing:
                    continue  # Context rows must not replace already finite facts.
                target = facts.setdefault(key, {"index_code": code, "trade_date": day, **dict.fromkeys(QUOTE_FIELDS)})
                if any(_finite(target[field]) and values[field] is not None and
                       Decimal(str(target[field])) != Decimal(values[field]) for field in QUOTE_FIELDS):
                    raise SourceManifestError("official quote provider conflicts with an existing finite fact")
                for field in QUOTE_FIELDS:
                    if not _finite(target[field]) and values[field] is not None:
                        target[field] = values[field]
                        changed.add(key)
            payload = {"schema_version": "aistock_industry_quote_provider_response_v1", "endpoint": "sw_daily",
                       "request": params, "captured_at": datetime.now(timezone.utc).isoformat(),
                       "quote_units": "sw_daily_native_v1", "rows": captured, "database_write": False}
            import json
            payload["provider_rows"] = json.loads(response.to_json(orient="records", date_format="iso"))
            payload["normalization"] = "dates_iso_numeric_decimal_strings_v1"
            ref = cas.put_json(payload)
            requests.append({"response_ref": ref.as_dict(), "row_count": len(captured), "ts_code": code})
    unresolved = [{"index_code": code, "trade_date": day,
                   "missing_fields": [field for field in REQUIRED if not _finite(facts.get((code, day), {}).get(field))]}
                  for code, day in keys if any(not _finite(facts.get((code, day), {}).get(field)) for field in REQUIRED)]
    return [facts[key] for key in sorted(facts, key=lambda key: (key[1], key[0]))], {
        "expected_key_count": len(keys), "repaired_key_count": len(changed),
        "unresolved_key_count": len(unresolved), "unresolved_keys": unresolved,
        "provider_requests": requests,
    }


def assert_additive_quote_refresh(cas, before, after):
    """A SOURCE amendment cannot silently revise any existing finite quote."""
    for field in ("schema_version", "source_dataset", "projection_sha256", "start_date", "end_date",
                  "quote_units", "quote_taxonomy", "quote_start_date", "database_write"):
        if before.get(field) != after.get(field):
            raise SourceManifestError("quote refresh source identity changed")
    old_parts = {part["partition_key"]: part for part in before["partitions"]}
    new_parts = {part["partition_key"]: part for part in after["partitions"]}
    if old_parts.keys() != new_parts.keys():
        raise SourceManifestError("quote refresh partition scope changed")
    for key, part in old_parts.items():
        old = cas.get_json_bounded(part["ref"], max_bytes=16 * 1024 * 1024)["rows"]
        new = cas.get_json_bounded(new_parts[key]["ref"], max_bytes=16 * 1024 * 1024)["rows"]
        indexed = {(row["index_code"], row["trade_date"]): row for row in new}
        for row in old:
            actual = indexed.get((row["index_code"], row["trade_date"]))
            if actual is None or any(_finite(value) and (not _finite(actual.get(field)) or Decimal(str(value)) != Decimal(str(actual[field])))
                                     for field, value in row.items() if field not in {"index_code", "trade_date"}):
                raise SourceManifestError("quote refresh would change or remove an existing finite fact")


def quote_source_sha256(source):
    return hashlib.sha256(canonical_json_bytes(source)).hexdigest()

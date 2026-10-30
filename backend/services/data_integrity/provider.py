"""Compact identities of completed formal provider queries, not history PIT."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from .contracts import digest


def announcement_spec():
    """Existing shared ANN identity; display de-dup never becomes a new fact."""
    from backend.services.tushare_dataset_specs import DatasetSpec, QueryMode
    return DatasetSpec(name="anns_metadata", tushare_api="anns_metadata", target_table="market.anns",
        primary_keys=["ts_code", "ann_date", "title"], query_mode=QueryMode.BY_DATE,
        columns={"ts_code": "text", "ann_date": "date", "title": "text"}, date_column="ann_date")


def record_ann_scan(conn, day, raw_rows, audit, *, source, job_id):
    """Record completed pagination, never turn a failed page into zero events."""
    from backend.services.data_refresh_audit import DataRefreshAuditRepository
    repo = DataRefreshAuditRepository()
    facts = [{"ann_date": r[0], "ts_code": r[1], "title": r[3]} for r in raw_rows]
    complete = audit.get("pagination_complete") is True
    metadata = {"mode": "by_date", "pagination_complete": complete,
                "provider_scan": provider_scan(announcement_spec(), facts,
                    parameters={"ann_date": str(day), "source": source}, complete=complete)}
    if not complete:
        repo.record_failure(dataset="anns_metadata", trade_date=day, data_source=source,
            error_message="announcement pagination incomplete", failure_category="pagination_incomplete",
            quality_status="error", metadata=metadata, job_id=job_id, conn=conn)
    else:
        repo.record_success(dataset="anns_metadata", trade_date=day, data_source=source,
            row_count=len(facts), written_rows=len(facts), job_id=job_id,
            quality_status="ok" if facts else "empty_valid", metadata=metadata, conn=conn)


def value_identity(value: Any, kind: str):
    if value is None:
        return None
    if kind == "date":
        if isinstance(value, (date, datetime)):
            return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
        text = str(value).replace("-", "")
        return datetime.strptime(text, "%Y%m%d").date().isoformat()
    if kind in {"numeric", "bigint", "integer", "int", "float", "double precision"}:
        return str(Decimal(str(value)).normalize())
    return str(value)


def row_identity(spec, row, fields):
    return digest({field: value_identity(row.get(field), spec.columns[field]) for field in fields})


def provider_scan(spec, facts, *, parameters, complete: bool = True):
    from .stock_scope import filter_dataset_rows
    identities = {}
    for row in filter_dataset_rows(spec, facts):
        key = tuple(value_identity(row.get(field), spec.columns[field]) for field in spec.primary_keys)
        if any(value is None and field not in spec.nullable_identity_columns for field, value in zip(spec.primary_keys, key)):
            raise ValueError("provider scan has no stable primary identity")
        # Only a compact independent expected-key set. Shared DB facts remain
        # authoritative: no per-price evidence archive or provider equality gate.
        entry = {"key": list(key)}
        previous = identities.get(key)
        if previous is not None and previous != entry:
            raise ValueError("provider scan has conflicting primary identities")
        identities[key] = entry
    entries = sorted(identities.values(), key=lambda entry: str(entry["key"]))
    entries = scoped_scan_entries(spec, {"entries": entries})
    body = {"schema_version": "aistock_completed_provider_scan_v1", "dataset": spec.name,
            "query_mode": spec.query_mode.value, "parameters": parameters, "complete": complete,
            "key_columns": list(spec.primary_keys), "entries": entries}
    return {**body, "scan_sha256": digest(body)}


def merge_scans(spec, scans, *, parameters):
    entries = {tuple(entry["key"]): entry for scan in scans for entry in scan["entries"]}
    body = provider_scan(spec, [], parameters=parameters)
    body.pop("scan_sha256")
    body["entries"] = sorted(entries.values(), key=lambda entry: str(entry["key"]))
    return {**body, "scan_sha256": digest(body)}


def scoped_scan_entries(spec, scan):
    from .stock_scope import STOCK_APIS, is_excluded_stock_code
    if spec.tushare_api not in STOCK_APIS:
        return scan['entries']
    if 'ts_code' in spec.primary_keys:
        index = spec.primary_keys.index('ts_code')
        return [entry for entry in scan['entries'] if not is_excluded_stock_code(entry['key'][index])]
    if spec.name in {'tushare_forecast_raw', 'tushare_express_raw', 'tushare_fina_indicator_raw'}:
        # Official source_record_key(): dataset:ts_code:ann_date:period[:extras].
        # Raw ingestion projects keys before scanning, so ts_code need not be a
        # separate key column. Preserve the signed legacy scan, only project its
        # supported equity scope. Unknown/foreign key formats remain required.
        index = spec.primary_keys.index('source_record_key')
        prefix = spec.name + ':'
        return [entry for entry in scan['entries']
                if not (str(entry['key'][index]).startswith(prefix)
                        and is_excluded_stock_code(str(entry['key'][index])[len(prefix):].split(':', 1)[0]))]
    return scan['entries']


def verify_scan(spec, scan, actual_rows):
    if (scan.get("schema_version") != "aistock_completed_provider_scan_v1" or scan.get("dataset") != spec.name
            or scan.get("complete") is not True or scan.get("key_columns") != spec.primary_keys
            or scan.get("scan_sha256") != digest({k: v for k, v in scan.items() if k != "scan_sha256"})):
        raise ValueError("completed provider scan identity differs")
    from .stock_scope import filter_dataset_rows
    actual = {tuple(value_identity(row.get(field), spec.columns[field]) for field in spec.primary_keys): row
              for row in filter_dataset_rows(spec, actual_rows)}
    missing, invalid = [], []
    for entry in scoped_scan_entries(spec, scan):
        key = tuple(entry["key"])
        row = actual.get(key)
        if row is None:
            missing.append(list(key))
    return missing, invalid

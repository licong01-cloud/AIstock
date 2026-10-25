"""Bounded, source-only report_rc history. No signal, dedupe winner or PIT claim."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
from uuid import uuid4

import pandas as pd

from backend.services.tushare_rate_limiter import get_limiter
from backend.services.tushare_sync_engine import _http_post, TUSHARE_DATAAPI_URL

FIELDS = (
    "ts_code",
    "name",
    "report_date",
    "report_title",
    "report_type",
    "classify",
    "org_name",
    "author_name",
    "quarter",
    "op_rt",
    "op_pr",
    "tp",
    "np",
    "eps",
    "pe",
    "rd",
    "roe",
    "ev_ebitda",
    "rating",
    "max_price",
    "min_price",
    "imp_dg",
    "create_time",
)
CONTRACT = {
    "schema_version": "aistock_report_rc_raw_capture_v1",
    "api_name": "report_rc",
    "start_date": "2023-07-01",
    "end_date": "2025-09-30",
    "fields": FIELDS,
    "page_size": 3000,
    "attempts_per_page": 3,
    "timeout_seconds": 30,
    "rate_per_minute": 120,
    "workers": 4,
    "vintage_status": "NON_VINTAGE",
    "normalization_version": "report_rc_preserve_occurrences_v1",
    "source_url": "https://tushare.pro/document/2?doc_id=292",
    "raw_non_finite_json_encoding": "string-sentinel-preserving-source-value",
}
# Standardization may evolve without rewriting the frozen capture contract.
NORMALIZATION_VERSION = "report_rc_preserve_occurrences_v2"


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


def encoded(value):
    return (
        json.dumps(json_safe(value), ensure_ascii=False, sort_keys=True, allow_nan=False, default=str) + "\n"
    ).encode("utf-8")


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def file_identity(path):
    raw = path.read_bytes()
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def create_json(path, value):
    """Atomic no-replace creation; a concurrent writer can never win by overwriting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with tmp.open("xb") as f:
            f.write(encoded(value))
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def validate_page(response):
    if not isinstance(response, dict) or response.get("code") != 0:
        raise ValueError("report_rc provider protocol failure")
    data = response.get("data")
    if not isinstance(data, dict):
        raise ValueError("report_rc missing data envelope")
    fields, items = data.get("fields"), data.get("items")
    if (
        not isinstance(fields, list)
        or not all(isinstance(f, str) for f in fields)
        or len(fields) != len(set(fields))
        or not set(FIELDS).issubset(fields)
        or not isinstance(items, list)
        or any(not isinstance(r, list) or len(r) != len(fields) for r in items)
    ):
        raise ValueError("report_rc response fields/row width differs")
    return [dict(zip(fields, r)) for r in items]


class SourceClient:
    def __init__(self, token):
        if not token:
            raise ValueError("TUSHARE_TOKEN is absent at configured credential location")
        self._token = token
        self._pacer = threading.Lock()
        self._last = 0.0

    def __call__(self, params):
        get_limiter("report_rc", CONTRACT["rate_per_minute"]).acquire()
        with self._pacer:
            wait = 60 / CONTRACT["rate_per_minute"] - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
        response = _http_post(
            TUSHARE_DATAAPI_URL + "/report_rc",
            {"api_name": "report_rc", "token": self._token, "params": params, "fields": ",".join(FIELDS)},
            CONTRACT["timeout_seconds"],
        )
        if response.status_code != 200:
            raise ValueError(f"report_rc HTTP {response.status_code}")
        return validate_page(response.json())


def collect_day(root, batch_id, day, query, *, page_size=3000, retries=3):
    """Resume only proven pages; failed requests cannot create a successful empty day."""
    root = Path(root)
    target = date.fromisoformat(day)
    completion = root / "days" / f"{day}.json"
    if completion.exists():
        done = json.loads(completion.read_bytes())
        if (
            done["batch_id"] != batch_id
            or done["contract_sha256"] != digest(CONTRACT)
            or done["page_size"] != page_size
        ):
            raise ValueError("resume contract/batch differs")
        for identity in done["pages"]:
            if file_identity(Path(identity["path"])) != identity:
                raise ValueError("completed page hash differs")
        return done
    pages = []
    page_hashes = set()
    offset, total = 0, 0
    while True:
        params = {"report_date": target.strftime("%Y%m%d"), "limit": page_size, "offset": offset}
        page_path = root / "pages" / day / f"{offset:08d}.json"
        if page_path.exists():
            packet = json.loads(page_path.read_bytes())
            if (
                packet["params"] != params
                or packet["batch_id"] != batch_id
                or packet["contract_sha256"] != digest(CONTRACT)
            ):
                raise ValueError("cached page contract differs")
            rows = packet["rows"]
            if digest(rows) != packet["source_rows_sha256"]:
                raise ValueError("cached page rows hash differs")
        else:
            for attempt in range(retries):
                started = utcnow()
                try:
                    rows = query(params)
                    if len(rows) > page_size or any(
                        str(r.get("report_date", "")).replace("-", "") != params["report_date"] for r in rows
                    ):
                        raise ValueError("report_rc date or page size differs")
                    captured = utcnow()
                    packet = {
                        "schema_version": CONTRACT["schema_version"],
                        "batch_id": batch_id,
                        "contract_sha256": digest(CONTRACT),
                        "params": params,
                        "request_started_at": started,
                        "captured_at": captured,
                        "source_rows_sha256": digest(rows),
                        "rows": rows,
                        "vintage_status": "NON_VINTAGE",
                        "publication_available_at": None,
                    }
                    packet["capture_id"] = digest({k: v for k, v in packet.items() if k != "rows"})
                    create_json(page_path, packet)
                    break
                except Exception as exc:
                    create_json(
                        root / "attempts" / f"{day}-{offset}-{uuid4().hex}.json",
                        {
                            "date": day,
                            "params": params,
                            "started_at": started,
                            "finished_at": utcnow(),
                            "attempt": attempt + 1,
                            "status": "FAILED",
                            "error_type": type(exc).__name__,
                            "batch_id": batch_id,
                        },
                    )
                    if attempt + 1 >= retries:
                        raise
                    time.sleep(2**attempt)
        source_hash = digest(rows)
        if rows and source_hash in page_hashes:
            raise ValueError("repeated report_rc page: offset ignored or unstable pagination")
        page_hashes.add(source_hash)
        pages.append(file_identity(page_path))
        total += len(rows)
        if len(rows) < page_size:
            done = {
                "schema_version": "aistock_report_rc_day_receipt_v1",
                "batch_id": batch_id,
                "contract_sha256": digest(CONTRACT),
                "report_date": day,
                "page_size": page_size,
                "rows": total,
                "pages": pages,
                "status": "SUCCESS_NONEMPTY" if total else "SUCCESS_EMPTY",
                "empty_semantics": "CURRENT_PROVIDER_SNAPSHOT_NOT_PROOF_OF_NO_HISTORICAL_REPORT",
                "completed_at": utcnow(),
            }
            create_json(completion, done)
            return done
        offset += page_size
        if offset > 300000:
            raise ValueError("report_rc pagination did not terminate")


def capture(root, batch_id, client):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    contract_file = root / "capture_contract.json"
    if contract_file.exists():
        prior = json.loads(contract_file.read_bytes())
        if prior != json.loads(encoded({"batch_id": batch_id, "contract": CONTRACT})):
            raise ValueError("capture contract differs; start another immutable version")
    else:
        create_json(contract_file, {"batch_id": batch_id, "contract": CONTRACT})
    lock = root / "collector.lock"
    # O_EXCL is intentional. Never remove another running collector's lock.
    with lock.open("x") as f:
        f.write(str(os.getpid()))
    try:
        start, end = map(date.fromisoformat, (CONTRACT["start_date"], CONTRACT["end_date"]))
        days = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
        result, errors = [], []
        with ThreadPoolExecutor(max_workers=CONTRACT["workers"]) as pool:
            futures = {pool.submit(collect_day, root, batch_id, day, client): day for day in days}
            for future in as_completed(futures):
                day = futures[future]
                try:
                    result.append(future.result())
                except Exception as exc:
                    errors.append({"date": day, "status": "FAILED", "error_type": type(exc).__name__})
                if (len(result) + len(errors)) % 20 == 0:
                    print(
                        json.dumps(
                            {
                                "completed_days": len(result),
                                "failed_days": len(errors),
                                "expected_days": len(days),
                                "rows": sum(r["rows"] for r in result),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
        summary = {
            "completed_days": len(result),
            "expected_days": len(days),
            "rows": sum(r["rows"] for r in result),
            "failed_dates": errors,
            "complete": len(result) == len(days),
            "batch_id": batch_id,
            "finished_at": utcnow(),
        }
        create_json(root / f"collection-result-{uuid4().hex}.json", summary)
        return summary
    finally:
        lock.unlink()


def raw_records(root):
    for path in sorted((Path(root) / "days").glob("*.json")):
        day = json.loads(path.read_bytes())
        for identity in day["pages"]:
            page = Path(identity["path"])
            if file_identity(page) != identity:
                raise ValueError("raw page identity changed")
            packet = json.loads(page.read_bytes())
            for n, row in enumerate(packet["rows"]):
                yield {
                    "row": row,
                    "capture_id": packet["capture_id"],
                    "row_ordinal": n,
                    "batch_id": packet["batch_id"],
                    "captured_at": packet["captured_at"],
                }


def normalize_rows(records):
    rows = []
    for record in records:
        source = record["row"]
        row = {f: source.get(f) for f in FIELDS}
        row.update({k: v for k, v in record.items() if k != "row"})
        row.update(
            source_row_sha256=digest(source),
            raw_payload_json=encoded(source).decode().strip(),
            provider_report_id=source.get("report_id") or source.get("id"),
            org_key=str(source.get("org_name") or "").strip() or None,
            org_alias_status="LITERAL_NAME_ONLY_ALIAS_UNKNOWN",
            eps_unit="SOURCE_YUAN_PER_SHARE",
            eps_currency="UNKNOWN_PROVIDER_HAS_NO_CURRENCY_FIELD",
            eps_basis_status="UNKNOWN",
            vintage_status="NON_VINTAGE",
            publication_available_at=None,
            publication_time_quality="HISTORICAL_FIRST_PUBLICATION_UNKNOWN",
            normalization_version=NORMALIZATION_VERSION,
        )
        try:
            eps = float(source.get("eps"))
            if isinstance(source.get("eps"), bool) or not math.isfinite(eps):
                raise ValueError("not finite")
            row["eps"] = eps
            row["eps_quality"] = "FINITE_SOURCE_VALUE"
        except (TypeError, ValueError, OverflowError):
            row["eps"] = None
            row["eps_quality"] = "NON_FINITE_OR_MISSING"
        row["report_lineage_key"] = digest(
            {
                f: source.get(f)
                for f in ("ts_code", "org_name", "report_date", "report_title", "author_name", "report_type")
            }
        )
        rows.append(row)
    panel = pd.DataFrame(rows)
    if panel.empty:
        return panel
    panel["report_date"] = pd.to_datetime(panel["report_date"], format="%Y%m%d", errors="coerce")
    panel["provider_create_time"] = pd.to_datetime(panel["create_time"], errors="coerce")
    panel["captured_at"] = pd.to_datetime(panel["captured_at"], utc=True)
    for column in (
        "ts_code",
        "name",
        "report_title",
        "report_type",
        "classify",
        "org_name",
        "author_name",
        "quarter",
        "rating",
        "imp_dg",
        "create_time",
    ):
        panel[column] = panel[column].astype("string")
    panel["duplicate_source_row"] = panel.duplicated(["source_row_sha256"], keep=False)
    panel["forecast_period_quality"] = (
        panel["quarter"]
        .str.fullmatch(r"20\d{2}Q[1-4]", na=False)
        .map({True: "STANDARD_QUARTER_SYNTAX", False: "UNKNOWN_MISSING_OR_NONSTANDARD"})
    )
    key = ["report_lineage_key", "quarter"]
    panel["same_report_quarter_conflict"] = panel.groupby(key, dropna=False)["eps"].transform("nunique").gt(1)
    panel["create_time_after_report_day"] = panel["provider_create_time"].dt.normalize().gt(panel["report_date"])
    identity = panel[["ts_code", "org_key", "quarter", "report_title"]].fillna("").astype(str)
    panel["record_quality"] = (
        identity.apply(lambda s: s.str.strip().ne(""))
        .all(axis=1)
        .map({True: "IDENTITY_PRESENT", False: "IDENTITY_UNKNOWN"})
    )
    return panel


def persist_dev(conn, root, *, day_receipts=None):
    """Explicit DEV target only. Append captured occurrences; retain duplicate payloads."""
    from psycopg2.extras import Json, execute_values

    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != "aistock_dev":
            raise ValueError("report_rc DEV persistence target differs")
        cur.execute("SELECT count(*) FROM market.tushare_report_rc_raw_v1")
        before = cur.fetchone()[0]
        paths = list(day_receipts) if day_receipts is not None else sorted((Path(root) / "days").glob("*.json"))
        for path in paths:
            day = json.loads(path.read_bytes())
            for identity in day["pages"]:
                page = Path(identity["path"])
                if file_identity(page) != identity:
                    raise ValueError("persistence source page hash differs")
                packet = json.loads(page.read_bytes())
                capture_id = packet["capture_id"]
                cur.execute(
                    """INSERT INTO market.tushare_report_rc_capture_v1
                    (capture_id,batch_id,report_date,fetch_params,source_rows_sha256,captured_at,row_count,packet_sha256)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(capture_id) DO NOTHING""",
                    (
                        capture_id,
                        packet["batch_id"],
                        day["report_date"],
                        Json(packet["params"]),
                        packet["source_rows_sha256"],
                        packet["captured_at"],
                        len(packet["rows"]),
                        identity["sha256"],
                    ),
                )
                cur.execute(
                    "SELECT packet_sha256 FROM market.tushare_report_rc_capture_v1 WHERE capture_id=%s", (capture_id,)
                )
                if cur.fetchone()[0] != identity["sha256"]:
                    raise ValueError("immutable database capture differs")
                values = [
                    (
                        capture_id,
                        n,
                        source.get("ts_code"),
                        day["report_date"],
                        source.get("org_name"),
                        source.get("quarter"),
                        source.get("report_title"),
                        digest(source),
                        Json(source),
                    )
                    for n, source in enumerate(packet["rows"])
                ]
                if values:
                    execute_values(
                        cur,
                        """INSERT INTO market.tushare_report_rc_raw_v1
                        (capture_id,row_ordinal,ts_code,report_date,org_name,quarter,report_title,source_row_sha256,raw_payload)
                        VALUES %s ON CONFLICT(capture_id,row_ordinal) DO NOTHING""",
                        values,
                        page_size=1000,
                    )
                cur.execute(
                    "SELECT row_ordinal,source_row_sha256,raw_payload FROM market.tushare_report_rc_raw_v1 WHERE capture_id=%s ORDER BY row_ordinal",
                    (capture_id,),
                )
                if cur.fetchall() != [(n, digest(r), r) for n, r in enumerate(packet["rows"])]:
                    raise ValueError("immutable raw database readback differs")
        cur.execute("SELECT count(*) FROM market.tushare_report_rc_raw_v1")
        after = cur.fetchone()[0]
    conn.commit()
    return {
        "database": "aistock_dev",
        "before_rows": before,
        "after_rows": after,
        "inserted_rows": after - before,
        "exact_row_readback": True,
    }

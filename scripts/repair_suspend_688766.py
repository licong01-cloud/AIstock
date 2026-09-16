"""Build authority, audit, and repair the bounded 688766.SH suspension interval.

The upstream Tushare ``suspend_d`` endpoint exposes sparse transition rows for
this event.  It does not materialize every exchange trading day in the
continuous suspension interval.  This utility binds the correction to frozen
official disclosure PDFs, validates the market calendar and price evidence,
then applies only the exact rows authorized by BUG-1558.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import psycopg2


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.repair_minute_via_minute_api import (  # noqa: E402
    _database_config,
    _discover_env_file,
)


SCHEMA_VERSION = "local_data_suspend_interval_authority_v1"
RECEIPT_SCHEMA_VERSION = "local_data_suspend_interval_repair_receipt_v1"
SYMBOL = "688766.SH"
SUSPEND_START = dt.date(2025, 11, 25)
SUSPEND_END = dt.date(2025, 12, 8)
RESUME_DATE = dt.date(2025, 12, 9)
SUSPENDED_TRADING_DATES = (
    dt.date(2025, 11, 25),
    dt.date(2025, 11, 26),
    dt.date(2025, 11, 27),
    dt.date(2025, 11, 28),
    dt.date(2025, 12, 1),
    dt.date(2025, 12, 2),
    dt.date(2025, 12, 3),
    dt.date(2025, 12, 4),
    dt.date(2025, 12, 5),
    dt.date(2025, 12, 8),
)
PRODUCTION_CONFIRMATION = "APPLY_BUG_1558_688766_SUSPEND_PRODUCTION"


@dataclass(frozen=True)
class DocumentSpec:
    filename: str
    url: str
    sha256: str
    size: int
    role: str


DOCUMENTS = (
    DocumentSpec(
        "2025-082-initial-suspension.pdf",
        "https://static.cninfo.com.cn/finalpage/2025-11-26/1224826077.PDF",
        "c86337fcdeb8421fb4680b4793ad0df49983ee790b54a1611e0fe9eb40be2c8a",
        108753,
        "initial_disclosure_conflicting_start_date",
    ),
    DocumentSpec(
        "2025-083-suspension-progress.pdf",
        "https://static.cninfo.com.cn/finalpage/2025-12-02/1224839861.PDF",
        "88e62071b8a8da068a2536740869649a404d639bbbf891f507ebdc83c610fd97",
        75892,
        "progress_disclosure_confirms_2025_11_25_start",
    ),
    DocumentSpec(
        "2025-088-resumption.pdf",
        "https://static.cninfo.com.cn/finalpage/2025-12-09/1224858827.PDF",
        "dbf5e4bbdf63cfd9d7f5465d31da2e916a4c7e1a81ad473f9faa0ab27a2b5b91",
        139876,
        "final_disclosure_confirms_interval_and_resume",
    ),
    DocumentSpec(
        "2025-090-pre-suspension-shareholders.pdf",
        "https://static.cninfo.com.cn/finalpage/2025-12-09/1224858822.PDF",
        "02a06f550f0518d7955e51be9613e5e6f5308e5fa0aaab172d28d62d5b1650ab",
        92501,
        "corroborates_2025_11_24_as_last_trading_day",
    ),
)


class RepairError(RuntimeError):
    """Raised when the bounded repair cannot prove a safe result."""


def canonical_json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def payload_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(payload))


def build_authority(
    source_dir: Path,
    output: Path,
    *,
    documents: Sequence[DocumentSpec] = DOCUMENTS,
) -> Mapping[str, Any]:
    source_dir = source_dir.expanduser().resolve(strict=True)
    frozen_documents: list[dict[str, Any]] = []
    for spec in documents:
        path = (source_dir / spec.filename).resolve(strict=True)
        if path.parent != source_dir:
            raise RepairError(f"source document escapes source directory: {path}")
        observed_size = path.stat().st_size
        observed_hash = sha256_file(path)
        if observed_size != spec.size or observed_hash != spec.sha256:
            raise RepairError(
                f"source document identity mismatch: {spec.filename}; "
                f"size={observed_size}, sha256={observed_hash}"
            )
        frozen_documents.append(
            {
                "filename": spec.filename,
                "path": str(path),
                "role": spec.role,
                "sha256": observed_hash,
                "size": observed_size,
                "url": spec.url,
            }
        )

    authority = {
        "schema_version": SCHEMA_VERSION,
        "symbol": SYMBOL,
        "event_type": "FULL_DAY_SUSPENSION",
        "suspend_start_date": SUSPEND_START.isoformat(),
        "suspend_end_date": SUSPEND_END.isoformat(),
        "resume_date": RESUME_DATE.isoformat(),
        "suspended_trading_dates": [value.isoformat() for value in SUSPENDED_TRADING_DATES],
        "database_rows": [
            *[
                {
                    "trade_date": value.isoformat(),
                    "ts_code": SYMBOL,
                    "suspend_type": "S",
                    "suspend_timing": None,
                }
                for value in SUSPENDED_TRADING_DATES
            ],
            {
                "trade_date": RESUME_DATE.isoformat(),
                "ts_code": SYMBOL,
                "suspend_type": "R",
                "suspend_timing": None,
            },
        ],
        "superseded_database_rows": [
            {
                "trade_date": "2025-11-26",
                "ts_code": SYMBOL,
                "suspend_type": "R",
                "suspend_timing": None,
                "classification": "CONTRADICTED_BY_FINAL_DISCLOSURE_AND_NO_TRADE_EVIDENCE",
            }
        ],
        "source_resolution": {
            "initial_disclosure_start_date": "2025-11-26",
            "resolved_start_date": "2025-11-25",
            "resolution": "LATER_FINAL_DISCLOSURES_AND_LAST_TRADED_DAY_CONTROL",
            "tushare_sparse_transition_observation": {
                "2025-11-25": [{"suspend_type": "S", "suspend_timing": None}],
                "2025-11-26": [
                    {"suspend_type": "R", "suspend_timing": None},
                    {"suspend_type": "S", "suspend_timing": "09:30-09:30"},
                ],
                "2025-12-08": [{"suspend_type": "S", "suspend_timing": None}],
            },
        },
        "source_documents": frozen_documents,
        "database_write_performed": False,
    }
    _write_exclusive(output, authority)
    return authority


def load_authority(path: Path) -> tuple[Mapping[str, Any], str]:
    path = path.expanduser().resolve(strict=True)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RepairError(f"authority is not valid UTF-8 JSON: {path}") from exc
    if raw != canonical_json_bytes(payload):
        raise RepairError("authority is not canonical JSON with exactly one trailing newline")
    expected = {
        "schema_version": SCHEMA_VERSION,
        "symbol": SYMBOL,
        "suspend_start_date": SUSPEND_START.isoformat(),
        "suspend_end_date": SUSPEND_END.isoformat(),
        "resume_date": RESUME_DATE.isoformat(),
        "suspended_trading_dates": [value.isoformat() for value in SUSPENDED_TRADING_DATES],
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise RepairError(f"authority field mismatch: {key}")
    expected_rows = desired_rows()
    observed_rows = {
        (str(row.get("trade_date")), str(row.get("suspend_type")), row.get("suspend_timing"))
        for row in payload.get("database_rows", [])
        if row.get("ts_code") == SYMBOL
    }
    if observed_rows != expected_rows:
        raise RepairError("authority database_rows do not match the bounded repair contract")
    for item in payload.get("source_documents", []):
        source = Path(str(item.get("path", ""))).resolve(strict=True)
        if source.stat().st_size != item.get("size") or sha256_file(source) != item.get("sha256"):
            raise RepairError(f"frozen source document changed: {source}")
    return payload, hashlib.sha256(raw).hexdigest()


def desired_rows() -> set[tuple[str, str, str | None]]:
    return {
        *((value.isoformat(), "S", None) for value in SUSPENDED_TRADING_DATES),
        (RESUME_DATE.isoformat(), "R", None),
    }


def derive_plan(existing: Iterable[tuple[str, str, str | None]]) -> Mapping[str, Any]:
    existing_set = set(existing)
    allowed_preimage = desired_rows() | {
        ("2025-11-26", "S", "09:30-09:30"),
        ("2025-11-26", "R", None),
    }
    unexpected = sorted(existing_set - allowed_preimage)
    if unexpected:
        raise RepairError(f"unexpected suspend_d preimage rows: {unexpected}")
    existing_keys = {(trade_date, suspend_type) for trade_date, suspend_type, _ in existing_set}
    inserts = sorted(
        row for row in desired_rows() if (row[0], row[1]) not in existing_keys
    )
    update_full_day = ("2025-11-26", "S", "09:30-09:30") in existing_set
    delete_superseded = ("2025-11-26", "R", None) in existing_set
    return {
        "insert_rows": inserts,
        "update_2025_11_26_s_to_full_day": update_full_day,
        "delete_superseded_2025_11_26_r": delete_superseded,
    }


def _read_state(cur: Any, *, lock: bool) -> Mapping[str, Any]:
    cur.execute(
        """
        SELECT cal_date::text
          FROM market.trading_calendar
         WHERE is_trading=TRUE AND cal_date BETWEEN %s AND %s
         ORDER BY cal_date
        """,
        (SUSPEND_START, SUSPEND_END),
    )
    trading_dates = [row[0] for row in cur.fetchall()]
    expected_dates = [value.isoformat() for value in SUSPENDED_TRADING_DATES]
    if trading_dates != expected_dates:
        raise RepairError(f"trading calendar mismatch: {trading_dates}")

    cur.execute(
        """
        SELECT trade_date::text, COUNT(*)::int,
               COALESCE(SUM(volume_hand), 0)::text
          FROM market.kline_daily_raw
         WHERE ts_code=%s AND trade_date BETWEEN DATE '2025-11-24' AND %s
         GROUP BY trade_date
         ORDER BY trade_date
        """,
        (SYMBOL, RESUME_DATE),
    )
    daily = {row[0]: {"rows": row[1], "volume_hand": row[2]} for row in cur.fetchall()}
    if int(daily.get("2025-11-24", {}).get("volume_hand", 0)) <= 0:
        raise RepairError("last pre-suspension trading day lacks positive daily volume")
    if int(daily.get(RESUME_DATE.isoformat(), {}).get("volume_hand", 0)) <= 0:
        raise RepairError("resume date lacks positive daily volume")
    traded_inside = [
        value.isoformat()
        for value in SUSPENDED_TRADING_DATES
        if int(daily.get(value.isoformat(), {}).get("volume_hand", 0)) > 0
    ]
    if traded_inside:
        raise RepairError(f"positive daily volume exists inside suspension interval: {traded_inside}")

    suffix = " FOR UPDATE" if lock else ""
    cur.execute(
        """
        SELECT trade_date::text, suspend_type, suspend_timing
          FROM market.suspend_d
         WHERE ts_code=%s AND trade_date BETWEEN %s AND %s
         ORDER BY trade_date, suspend_type
        """ + suffix,
        (SYMBOL, SUSPEND_START, RESUME_DATE),
    )
    existing = [(row[0], row[1], row[2]) for row in cur.fetchall()]
    plan = derive_plan(existing)
    return {
        "trading_dates": trading_dates,
        "daily": daily,
        "existing_rows": existing,
        "plan": plan,
    }


def audit(target_db: str, authority_path: Path) -> Mapping[str, Any]:
    _, authority_sha256 = load_authority(authority_path)
    conn = psycopg2.connect(**_database_config(target_db, _discover_env_file(None)))
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            state = _read_state(cur, lock=False)
    finally:
        conn.close()
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "operation": "audit",
        "target_db": target_db,
        "symbol": SYMBOL,
        "authority_path": str(authority_path.expanduser().resolve()),
        "authority_sha256": authority_sha256,
        **state,
        "database_write_performed": False,
    }


def apply_repair(
    target_db: str,
    authority_path: Path,
    receipt_path: Path,
    *,
    confirmation: str | None,
) -> Mapping[str, Any]:
    if target_db == "production" and confirmation != PRODUCTION_CONFIRMATION:
        raise RepairError(
            "production repair requires --confirm " + PRODUCTION_CONFIRMATION
        )
    _, authority_sha256 = load_authority(authority_path)
    conn = psycopg2.connect(**_database_config(target_db, _discover_env_file(None)))
    try:
        with conn:
            with conn.cursor() as cur:
                state_before = _read_state(cur, lock=True)
                plan = state_before["plan"]
                for trade_date, suspend_type, suspend_timing in plan["insert_rows"]:
                    cur.execute(
                        """
                        INSERT INTO market.suspend_d
                            (trade_date, ts_code, suspend_type, suspend_timing)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (trade_date, SYMBOL, suspend_type, suspend_timing),
                    )
                if plan["update_2025_11_26_s_to_full_day"]:
                    cur.execute(
                        """
                        UPDATE market.suspend_d
                           SET suspend_timing=NULL
                         WHERE trade_date=DATE '2025-11-26' AND ts_code=%s
                           AND suspend_type='S' AND suspend_timing='09:30-09:30'
                        """,
                        (SYMBOL,),
                    )
                    if cur.rowcount != 1:
                        raise RepairError("expected exactly one 2025-11-26 S row update")
                if plan["delete_superseded_2025_11_26_r"]:
                    cur.execute(
                        """
                        DELETE FROM market.suspend_d
                         WHERE trade_date=DATE '2025-11-26' AND ts_code=%s
                           AND suspend_type='R' AND suspend_timing IS NULL
                        """,
                        (SYMBOL,),
                    )
                    if cur.rowcount != 1:
                        raise RepairError("expected exactly one superseded 2025-11-26 R row deletion")

                state_after = _read_state(cur, lock=False)
                if set(state_after["existing_rows"]) != desired_rows():
                    raise RepairError("transactional suspend_d readback does not match authority")

        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "operation": "apply",
            "target_db": target_db,
            "symbol": SYMBOL,
            "authority_path": str(authority_path.expanduser().resolve()),
            "authority_sha256": authority_sha256,
            "before": state_before,
            "after": state_after,
            "inserted_rows": len(plan["insert_rows"]),
            "updated_rows": int(plan["update_2025_11_26_s_to_full_day"]),
            "deleted_rows": int(plan["delete_superseded_2025_11_26_r"]),
            "database_write_performed": True,
            "ddl_performed": False,
        }
        _write_exclusive(receipt_path, receipt)
        return receipt
    finally:
        conn.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build-authority")
    build.add_argument("--source-dir", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)

    audit_parser = sub.add_parser("audit")
    audit_parser.add_argument("--target-db", choices=("dev", "production"), required=True)
    audit_parser.add_argument("--authority", type=Path, required=True)

    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("--target-db", choices=("dev", "production"), required=True)
    apply_parser.add_argument("--authority", type=Path, required=True)
    apply_parser.add_argument("--receipt", type=Path, required=True)
    apply_parser.add_argument("--confirm")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "build-authority":
        payload = build_authority(args.source_dir, args.output)
        print(json.dumps({"authority": str(args.output.resolve()), "sha256": payload_sha256(payload)}))
        return 0
    if args.command == "audit":
        print(json.dumps(audit(args.target_db, args.authority), ensure_ascii=False, default=str))
        return 0
    if args.command == "apply":
        receipt = apply_repair(
            args.target_db,
            args.authority,
            args.receipt,
            confirmation=args.confirm,
        )
        print(json.dumps(receipt, ensure_ascii=False, default=str))
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scripts import repair_pt_next_018_source_data as repair


def _canonical_file(path: Path, payload: dict) -> tuple[str, str]:
    unsigned = dict(payload)
    canonical = repair.sha256_bytes(repair.canonical_json_bytes(unsigned))
    body = {**unsigned, "canonical_sha256": canonical}
    path.write_bytes(repair.canonical_json_bytes(body) + b"\n")
    return canonical, hashlib.sha256(path.read_bytes()).hexdigest()


def _authority_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = (tmp_path / "authority").resolve()
    root.mkdir()
    rows = [
        {"ts_code": "300506.SZ", "trade_date": "2026-08-31", "adj_factor": 6.4229},
        {"ts_code": "688109.SH", "trade_date": "2026-08-31", "adj_factor": 1.5459},
    ]
    series = []
    for row in rows:
        series.append(
            {
                "ts_code": row["ts_code"],
                "start": row["trade_date"],
                "end": row["trade_date"],
                "row_count": 1,
                "ordered_rows_sha256": repair.sha256_bytes(repair.canonical_json_bytes([row])),
                "rows": [row],
            }
        )
    adj = {
        "schema_version": repair.ADJ_AUTHORITY_SCHEMA,
        "provider": "tushare",
        "cutoff_trade_date": "2026-08-31",
        "request_id": "PT-NEXT-018",
        "diagnosis_sha256": "d" * 64,
        "series": series,
        "safety": {},
    }
    events = []
    for event_id, symbol, record_date in (
        ("RIGHTS_ISSUE:000970.SZ:2022-02-15", "000970.SZ", "2022-02-15"),
        ("RIGHTS_ISSUE:600008.SH:2020-09-18", "600008.SH", "2020-09-18"),
        ("RIGHTS_ISSUE:601236.SH:2021-07-26", "601236.SH", "2021-07-26"),
    ):
        events.append(
            {
                "schema_version": repair.RIGHTS_EVENT_SCHEMA,
                "event_id": event_id,
                "event_key": f"{symbol} RIGHTS_ISSUE {record_date}",
                "event_type": "RIGHTS_ISSUE",
                "symbol": symbol,
                "announcement_date": record_date,
                "disclosure_available_at": f"{record_date}T00:00:00Z",
                "source_time_quality": "fixture",
                "record_date": record_date,
                "payment_start_date": record_date,
                "payment_end_date": record_date,
                "ex_right_date": record_date,
                "resume_date": record_date,
                "listing_date": record_date,
                "entitlement_ratio": 0.3,
                "subscription_price": 1.0,
                "actual_offered_quantity": 1,
                "actual_subscribed_quantity": 1,
                "issue_success_status": "SUCCESS",
                "rounding_rule": "fixture",
                "source_url": "https://example.invalid/a.pdf",
                "source_content_sha256": "a" * 64,
                "source_file_size": 100,
                "source_document_type": "fixture",
                "source_record_key": "fixture",
                "captured_at": "2026-09-12T00:00:00Z",
                "versions": [{"source_content_sha256": "a" * 64}],
            }
        )
    rights = {
        "schema_version": repair.RIGHTS_AUTHORITY_SCHEMA,
        "event_count": 3,
        "events": events,
        "ddl_performed": False,
        "outcomes_read": False,
        "production_database_written": False,
        "runtime_action_performed": False,
    }
    adj_canonical, adj_file = _canonical_file(root / "adj_factor_restatement_authority.json", adj)
    rights_canonical, rights_file = _canonical_file(root / "rights_issue_authority.json", rights)
    monkeypatch.setattr(repair, "EXPECTED_ADJ_CANONICAL_SHA256", adj_canonical)
    monkeypatch.setattr(repair, "EXPECTED_ADJ_FILE_SHA256", adj_file)
    monkeypatch.setattr(repair, "EXPECTED_RIGHTS_CANONICAL_SHA256", rights_canonical)
    monkeypatch.setattr(repair, "EXPECTED_RIGHTS_FILE_SHA256", rights_file)
    monkeypatch.setattr(
        repair,
        "EXPECTED_ADJ_SERIES",
        {"300506.SZ": ("2026-08-31", "2026-08-31", 1), "688109.SH": ("2026-08-31", "2026-08-31", 1)},
    )
    return root


def test_load_authorities_requires_exact_content_addressed_whole_series(tmp_path, monkeypatch) -> None:
    root = _authority_fixture(tmp_path, monkeypatch)
    bundle = repair.load_authorities(root)
    assert len(bundle.adj_rows) == 2
    assert len(bundle.rights_events) == 3
    assert bundle.cutoff_trade_date == "2026-08-31"

    path = root / "adj_factor_restatement_authority.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(repair.RepairError, match="file identity differs"):
        repair.load_authorities(root)


def test_database_config_separates_dev_and_production_targets(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                "TDX_DB_DEV_HOST=127.0.0.1", "TDX_DB_DEV_PORT=5433", "TDX_DB_DEV_NAME=aistock_dev",
                "TDX_DB_DEV_USER=postgres", "TDX_DB_DEV_PASSWORD=dev",
                "TDX_DB_HOST=127.0.0.1", "TDX_DB_PORT=5432", "TDX_DB_NAME=aistock",
                "TDX_DB_USER=postgres", "TDX_DB_PASSWORD=prod",
            ]
        ),
        encoding="utf-8",
    )
    assert repair.database_config("dev", env)["dbname"] == "aistock_dev"
    assert repair.database_config("production", env)["dbname"] == "aistock"
    text = env.read_text(encoding="utf-8").replace("TDX_DB_DEV_PORT=5433", "TDX_DB_DEV_PORT=5432")
    env.write_text(text, encoding="utf-8")
    with pytest.raises(repair.RepairError, match="dev database target identity is invalid"):
        repair.database_config("dev", env)


def test_expected_rights_fact_preserves_authority_and_early_visibility(tmp_path, monkeypatch) -> None:
    bundle = repair.load_authorities(_authority_fixture(tmp_path, monkeypatch))
    event = bundle.rights_events[0]
    fact = repair._expected_fact(event, bundle)
    assert fact["event_key"] == event["event_id"]
    assert fact["source_time_quality"] == "OBSERVED"
    assert fact["source_available_at"] == event["disclosure_available_at"]
    assert fact["facts"]["event"] == event
    assert fact["facts"]["authority_canonical_sha256"] == bundle.rights_canonical_sha256


def test_apply_confirmation_and_receipt_are_fail_closed(tmp_path, monkeypatch) -> None:
    bundle = repair.load_authorities(_authority_fixture(tmp_path, monkeypatch))
    config = {"host": "127.0.0.1", "port": 5433, "dbname": "aistock_dev", "user": "u", "password": "p"}
    with pytest.raises(repair.RepairError, match="confirmation differs"):
        repair.apply_repair(
            target="dev",
            config=config,
            authority=bundle,
            expected_plan_digest="b" * 64,
            expected_target_identity=repair.target_identity_sha256(config),
            confirmation="wrong",
            receipt_path=(tmp_path / "receipt.json").resolve(),
            expected_source_commit=None,
        )


def test_canonical_json_rejects_nan() -> None:
    with pytest.raises(ValueError):
        repair.canonical_json_bytes({"value": float("nan")})

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.financial_event_source_readiness import (
    NOT_READY_NEXT_TASK,
    NOT_READY_STATE,
    build_financial_event_source_bundle,
    deliver_financial_event_source_bundle,
    inspect_financial_event_source_bundle,
)
from backend.services.event_signal.tushare_event_raw_sync import source_row_hash


class FakeConnection:
    def __init__(self) -> None:
        self.session: dict[str, object] = {}
        self.rolled_back = False
        self.closed = False

    def set_session(self, **kwargs: object) -> None:
        self.session = kwargs

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def _parent(path: Path) -> None:
    pd.DataFrame(
        {
            "arm_id": ["CURRENT_IC_PARENT"] * 4,
            "decision_as_of_trade_date": [
                dt.date(2024, 7, 8),
                dt.date(2024, 7, 8),
                dt.date(2024, 7, 9),
                dt.date(2024, 7, 9),
            ],
            "instrument": ["000001.SZ", "000002.SZ"] * 2,
            "score": [2.0, 1.0, 1.5, 0.5],
            "economic_net_excess_bps": [10_000.0, -10_000.0, 20_000.0, -20_000.0],
        }
    ).to_parquet(path, index=False)


def _raw(source: str, raw_id: int, instrument: str) -> dict:
    if source == "tushare_forecast":
        payload = {"type": "首亏"}
    elif source == "tushare_express":
        payload = {"n_income": -10, "yoy_dedu_np": -20}
    else:
        payload = {"dt_netprofit_yoy": -60}
    return {
        "raw_observation_id": raw_id,
        "source_record_key": f"{source}:{raw_id}",
        "source_row_hash": source_row_hash(payload),
        "ts_code": instrument,
        "ann_date": dt.date(2024, 7, 5),
        "report_period": dt.date(2024, 6, 30),
        "first_seen_at": dt.datetime(2026, 5, 6, tzinfo=dt.timezone.utc),
        "raw_payload": payload,
    }


def _margin_receipt(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "status": "COMPLETE",
                "selected_trial_count": 0,
                "next_task": "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN",
                "sealed_holdout_accessed": False,
                "runtime_eligible": False,
                "factor_catalog_written": False,
                "strategy_package_written": False,
                "final_model_written": False,
                "position_weight_output": False,
                "receipt_id": "fixture",
                "request_sha256": "1" * 64,
                "source_identity_sha256": "2" * 64,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _snapshot() -> dict:
    return {
        "database_identity": {
            "database_name": "aistock",
            "snapshot_id": "1:2:",
            "transaction_read_only": "on",
            "transaction_isolation": "repeatable read",
        },
        "trading_calendar": [
            dt.date(2024, 7, 4),
            dt.date(2024, 7, 5),
            dt.date(2024, 7, 8),
            dt.date(2024, 7, 9),
        ],
        "source_start": dt.date(2024, 7, 4),
        "raw_rows_by_source": {
            "tushare_forecast": [_raw("tushare_forecast", 1, "000001.SZ")],
            "tushare_express": [_raw("tushare_express", 2, "000001.SZ")],
            "tushare_fina_indicator": [_raw("tushare_fina_indicator", 3, "000002.SZ")],
        },
        "diagnostic_event_signal_row_count": 3,
        "diagnostic_pit_mismatch_count": 0,
        "database_query_count": 8,
        "database_write_count": 0,
    }


def _build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, FakeConnection]:
    parent = tmp_path / "parent.parquet"
    margin_receipt = tmp_path / "margin_receipt.json"
    _parent(parent)
    _margin_receipt(margin_receipt)
    connection = FakeConnection()
    monkeypatch.setattr(
        "backend.services.advisory_model_first.financial_event_source_readiness.read_database_snapshot",
        lambda *_args, **_kwargs: _snapshot(),
    )
    bundle = build_financial_event_source_bundle(
        parent_path=parent,
        margin_receipt_path=margin_receipt,
        repository_root=tmp_path,
        output_root=tmp_path / "artifacts",
        registry_path=tmp_path / "trial_registry.jsonl",
        route_path=tmp_path / "current_route.md",
        connection_factory=lambda: connection,
        expectation=None,
        expected_margin_receipt_sha256=None,
        require_clean_main=False,
    )
    return bundle, connection


def test_bundle_is_closed_target_free_and_inspectable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bundle, connection = _build(monkeypatch, tmp_path)
    inspected = inspect_financial_event_source_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["source_state"] == NOT_READY_STATE
    assert inspected["next_task"] == NOT_READY_NEXT_TASK
    assert connection.session == {
        "isolation_level": "REPEATABLE READ",
        "readonly": True,
        "autocommit": False,
    }
    assert connection.rolled_back is True
    assert connection.closed is True
    projection = pd.read_parquet(bundle / "event_source_projection.parquet")
    assert "raw_payload" not in projection
    assert "economic_net_excess_bps" not in projection


def test_exact_retry_reuses_bundle_without_temporary_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first, _ = _build(monkeypatch, tmp_path)
    second, _ = _build(monkeypatch, tmp_path)
    assert second == first
    bundle_root = first.parent
    assert not [path for path in bundle_root.iterdir() if path.name.startswith(".financial-event-source-")]


def test_mutated_bundle_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bundle, _ = _build(monkeypatch, tmp_path)
    receipt = bundle / "source_readiness_receipt.json"
    receipt.write_text(receipt.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        inspect_financial_event_source_bundle(bundle)
    assert exc_info.value.reason_code == "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID"


def test_delivery_is_registry_and_route_exact_noop(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bundle, _ = _build(monkeypatch, tmp_path)
    registry = tmp_path / "trial_registry.jsonl"
    route = tmp_path / "current_route.md"
    first = deliver_financial_event_source_bundle(
        bundle_path=bundle,
        registry_path=registry,
        route_path=route,
    )
    second = deliver_financial_event_source_bundle(
        bundle_path=bundle,
        registry_path=registry,
        route_path=route,
    )
    assert first["registry"]["appended_count"] == 1
    assert second["registry"]["duplicate_noop_count"] == 1
    assert second["route_write"] == "exact_noop"
    assert NOT_READY_NEXT_TASK in route.read_text(encoding="utf-8")

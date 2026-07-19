from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date

import pandas as pd
import pytest

from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.models import EvaluationSpec
from backend.services.hmm_evolution.universe import (
    LEGACY_QE_ST_PIT_UNIVERSE_KEY,
    QEExecutionUniverseResolver,
    QELoopUniverseRepository,
    SourceLoopRiskPolicySnapshot,
    SourceLoopUniverseContract,
    _parse_source_risk_policy_snapshot,
)
from backend.services.hmm_evolution import universe as universe_module
from backend.services.quantevolver.qe_dataset_contract import QE_ST_PIT_UNIVERSE_KEY
from backend.services.quantevolver.stock_pool_sync import (
    StockPoolInterval,
    StockPoolSnapshot,
)
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_RULE_VERSION


class _LoopRepository:
    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract:
        assert base_loop_ref == "qe_task/Loop8"
        return SourceLoopUniverseContract(
            task_id="qe_task",
            loop_name="Loop8",
            stock_pool="filtered_pool_fixture",
            risk_policy={
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
                "policy_version": "stock_event_risk_policy_v1",
                "strict_data_ready": True,
                "st_universe_key": LEGACY_QE_ST_PIT_UNIVERSE_KEY,
                "visible_time_mode": "next_trading_session",
            },
        )


class _Cursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, _sql, _params=None):
        return None

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, row):
        self.row = row

    def cursor(self, **_kwargs):
        return _Cursor(self.row)


def _conn_factory(row):
    @contextmanager
    def factory(**_kwargs):
        yield _Connection(row)

    return factory


def _spec(*, topk: int = 1) -> EvaluationSpec:
    return EvaluationSpec(
        base_loop_ref="qe_task/Loop8",
        window_start=date(2025, 1, 2),
        window_end=date(2025, 1, 3),
        as_of={"policy": "explicit", "requested_date": "2025-01-03"},
        label_horizon_days=10,
        topk=topk,
        market_forward_return={"mode": "disabled", "horizon_trading_days": 10},
    )


def _pool() -> StockPoolSnapshot:
    return StockPoolSnapshot(
        filename="filtered_pool_fixture.txt",
        instrument_name="filtered_pool_fixture",
        sha256="a" * 64,
        intervals=(
            StockPoolInterval("600000.SH", date(2025, 1, 2), date(2025, 1, 3)),
            StockPoolInterval("000002.SZ", date(2025, 1, 2), date(2025, 1, 3)),
        ),
    )


def _risk_snapshot(
    *,
    universe_key: str = LEGACY_QE_ST_PIT_UNIVERSE_KEY,
) -> SourceLoopRiskPolicySnapshot:
    return SourceLoopRiskPolicySnapshot(
        snapshot=StockPoolSnapshot(
            filename="qe_event_risk_policy.json",
            instrument_name=universe_key,
            sha256="b" * 64,
            intervals=(
                StockPoolInterval("600000.SH", date(2025, 1, 2), date(2025, 1, 3)),
            ),
        ),
        artifact_sha256="b" * 64,
        dataset_contract_id=None,
        universe_key=universe_key,
        binding_mode="legacy_frozen_runtime_artifact_v1",
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        scope="st_only_active",
        source_fingerprint_sha256="f" * 64,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
    )


def _risk_policy_payload(
    *,
    universe_key: str = LEGACY_QE_ST_PIT_UNIVERSE_KEY,
    dataset_contract_id: str | None = None,
) -> dict:
    return {
        "enabled": True,
        "contract": "stock_event_risk_policy_v1",
        "providers": ["st_pit"],
        "hard_actions": ["block_buy", "force_exit"],
        "visible_time_mode": "next_trading_session",
        "strict_data_ready": True,
        "dataset_contract_id": dataset_contract_id,
        "st_universe_key": universe_key,
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "span_count": 1,
        "active_spans": [
            {
                "ts_code": "600000.SH",
                "eligible_start": "2025-01-02",
                "eligible_end": "2025-01-03",
                "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            }
        ],
        "state": {
            "status": "ready",
            "dirty": False,
            "universe_key": universe_key,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "source_fingerprint_sha256": "f" * 64,
        },
    }


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = [
        (trade_date, symbol, score)
        for trade_date in (date(2025, 1, 2), date(2025, 1, 3))
        for symbol, score in (
            ("600000.SH", 3.0),
            ("000001.SZ", 2.0),
            ("000002.SZ", 1.0),
        )
    ]
    predictions = pd.DataFrame(rows, columns=["trade_date", "symbol", "score"])
    labels = pd.DataFrame(
        [(trade_date, symbol, 10, 0.1) for trade_date, symbol, _score in rows],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    return predictions, labels


def test_loop_repository_reads_stock_pool_and_strict_st_pit_from_persisted_config(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        universe_module,
        "get_conn",
        _conn_factory(
            {
                "config_json": {
                    "stock_pool": "filtered_pool_fixture",
                    "model_params": {
                        "stock_pool": "filtered_pool_fixture",
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "hard_actions": ["block_buy", "force_exit"],
                            "policy_version": "stock_event_risk_policy_v1",
                            "strict_data_ready": True,
                            "st_universe_key": LEGACY_QE_ST_PIT_UNIVERSE_KEY,
                            "visible_time_mode": "next_trading_session",
                        },
                    },
                }
            }
        ),
    )

    contract = QELoopUniverseRepository().load("qe_task/Loop8")

    assert contract.stock_pool == "filtered_pool_fixture"
    assert contract.risk_policy["strict_data_ready"] is True
    assert contract.risk_policy["st_universe_key"] == LEGACY_QE_ST_PIT_UNIVERSE_KEY


def test_loop_repository_rejects_missing_persisted_universe_key(monkeypatch) -> None:
    monkeypatch.setattr(
        universe_module,
        "get_conn",
        _conn_factory(
            {
                "config_json": {
                    "stock_pool": "filtered_pool_fixture",
                    "model_params": {
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "strict_data_ready": True,
                        }
                    },
                }
            }
        ),
    )

    with pytest.raises(InvalidSpecError, match="does not persist its ST-PIT universe key"):
        QELoopUniverseRepository().load("qe_task/Loop8")


def test_loop_repository_rejects_conflicting_stock_pool_declarations(monkeypatch) -> None:
    monkeypatch.setattr(
        universe_module,
        "get_conn",
        _conn_factory(
            {
                "config_json": {
                    "stock_pool": "filtered_pool_a",
                    "model_params": {
                        "stock_pool": "filtered_pool_b",
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "strict_data_ready": True,
                        },
                    },
                }
            }
        ),
    )

    with pytest.raises(InvalidSpecError, match="conflicting stock_pool"):
        QELoopUniverseRepository().load("qe_task/Loop8")


def test_parser_accepts_exact_frozen_legacy_runtime_artifact() -> None:
    raw = json.dumps(_risk_policy_payload()).encode("utf-8")

    snapshot = _parse_source_risk_policy_snapshot(raw, task_id="qe_task", loop_name="Loop8")

    assert snapshot.universe_key == LEGACY_QE_ST_PIT_UNIVERSE_KEY
    assert snapshot.binding_mode == "legacy_frozen_runtime_artifact_v1"
    assert snapshot.snapshot.intervals == (
        StockPoolInterval("600000.SH", date(2025, 1, 2), date(2025, 1, 3)),
    )


def test_parser_rejects_unknown_legacy_runtime_universe_key() -> None:
    raw = json.dumps(_risk_policy_payload(universe_key="unknown_st_pit_v1")).encode("utf-8")

    with pytest.raises(InvalidSpecError, match="unknown universe key"):
        _parse_source_risk_policy_snapshot(raw, task_id="qe_task", loop_name="Loop8")


def test_parser_accepts_dataset_bound_runtime_artifact() -> None:
    dataset_contract_id = "dataset_contract_v2"
    universe_key = f"shsz_st_pit_qe_dataset_{dataset_contract_id}"
    raw = json.dumps(
        _risk_policy_payload(
            universe_key=universe_key,
            dataset_contract_id=dataset_contract_id,
        )
    ).encode("utf-8")

    snapshot = _parse_source_risk_policy_snapshot(raw, task_id="qe_task", loop_name="Loop8")

    assert snapshot.dataset_contract_id == dataset_contract_id
    assert snapshot.universe_key == universe_key
    assert snapshot.binding_mode == "immutable_dataset_runtime_artifact_v1"


def test_resolver_intersects_source_pool_with_exact_runtime_st_pit_artifact() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(),
    )

    resolved = resolver.resolve(
        evaluation_spec=_spec(),
        predictions=predictions,
        labels=labels,
    )

    assert set(resolved.predictions["symbol"]) == {"600000.SH"}
    assert set(resolved.labels["symbol"]) == {"600000.SH"}
    assert resolved.evidence["prediction_row_count_before"] == 6
    assert resolved.evidence["prediction_row_count_after"] == 2
    assert resolved.evidence["excluded_prediction_row_count"] == 4
    assert resolved.evidence["stock_pool"]["sha256"] == "a" * 64
    assert resolved.evidence["st_pit"]["universe_key"] == LEGACY_QE_ST_PIT_UNIVERSE_KEY
    assert resolved.evidence["st_pit"]["artifact_sha256"] == "b" * 64
    assert resolved.evidence["st_pit"]["binding_mode"] == "legacy_frozen_runtime_artifact_v1"
    assert len(str(resolved.evidence["universe_hash"])) == 64


def test_resolver_rejects_persisted_policy_runtime_artifact_identity_drift() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(
            universe_key=QE_ST_PIT_UNIVERSE_KEY
        ),
    )

    with pytest.raises(InvalidSpecError, match="differs from its frozen runtime artifact"):
        resolver.resolve(
            evaluation_spec=_spec(),
            predictions=predictions,
            labels=labels,
        )


def test_resolver_fails_when_any_day_has_fewer_eligible_symbols_than_topk() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(),
    )

    with pytest.raises(InvalidSpecError, match="smaller than TopK"):
        resolver.resolve(
            evaluation_spec=_spec(topk=2),
            predictions=predictions,
            labels=labels,
        )

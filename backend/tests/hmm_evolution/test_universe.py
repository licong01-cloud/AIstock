from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.models import EvaluationSpec
from backend.services.hmm_evolution.universe import (
    QEExecutionUniverseResolver,
    QELoopUniverseRepository,
    SourceLoopUniverseContract,
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
                "strict_data_ready": True,
                "st_universe_key": QE_ST_PIT_UNIVERSE_KEY,
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


class _PitService:
    def metadata(self, **kwargs):
        assert kwargs["universe_key"] == QE_ST_PIT_UNIVERSE_KEY
        assert kwargs["ensure"] is False
        return {
            "universe_rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "universe_scope": "sh_sz_bj",
            "universe_fingerprint_sha256": "f" * 64,
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
            "coverage_semantics": "st_pit_buy_eligible_suspend_excluded_non_warmup_v1",
        }

    def build_eligible_mask(self, dates, symbols, **kwargs):
        assert kwargs["universe_key"] == QE_ST_PIT_UNIVERSE_KEY
        assert kwargs["ensure"] is False
        mask = np.ones((len(dates), len(symbols)), dtype=bool)
        mask[:, symbols.index("000002.SZ")] = False
        return mask


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
                            "strict_data_ready": True,
                            "st_universe_key": QE_ST_PIT_UNIVERSE_KEY,
                        },
                    },
                }
            }
        ),
    )

    contract = QELoopUniverseRepository().load("qe_task/Loop8")

    assert contract.stock_pool == "filtered_pool_fixture"
    assert contract.risk_policy["strict_data_ready"] is True


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


def test_resolver_intersects_source_pool_with_immutable_st_pit() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        pit_service=_PitService(),  # type: ignore[arg-type]
        stock_pool_loader=lambda _stock_pool: _pool(),
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
    assert resolved.evidence["st_pit"]["universe_key"] == QE_ST_PIT_UNIVERSE_KEY
    assert len(str(resolved.evidence["universe_hash"])) == 64


def test_resolver_fails_when_any_day_has_fewer_eligible_symbols_than_topk() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        pit_service=_PitService(),  # type: ignore[arg-type]
        stock_pool_loader=lambda _stock_pool: _pool(),
    )

    with pytest.raises(InvalidSpecError, match="smaller than TopK"):
        resolver.resolve(
            evaluation_spec=_spec(topk=2),
            predictions=predictions,
            labels=labels,
        )

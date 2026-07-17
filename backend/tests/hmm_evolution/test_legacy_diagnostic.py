from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from backend.services.hmm_evolution.errors import MarketDataUnavailableError
from backend.services.hmm_evolution.market_repository import MarketReturnRead, MarketWatermark
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceFileNotFound
from scripts.diagnostics import hmm_offline_diagnostic as diagnostic


def _loop(*, has_hmm: bool = True) -> diagnostic.LoopInfo:
    return diagnostic.LoopInfo(
        loop_index=2,
        loop_id="Loop2",
        status="completed",
        experiment_id="exp-2",
        node_id="node-1",
        label="h20",
        has_hmm=has_hmm,
        snapshot_id="snapshot-2",
        model_path=None,
        annualized_return=None,
        max_drawdown=None,
        sharpe=None,
        ic=None,
        rank_ic=None,
    )


class _ArtifactClient:
    def __init__(self) -> None:
        self.requested_paths: list[str] = []

    async def download_workspace_file_bytes(
        self,
        task_id: str,
        loop_id: str,
        file_path: str,
    ) -> bytes:
        self.requested_paths.append(file_path)
        if file_path == "run.log":
            return b"Recorder abcdef starts running under Experiment 123"
        if file_path == "hmm_sector_coefficients.json":
            return json.dumps(
                {
                    "daily_coefficients": {"2026-01-05": {"S1": 1.0}},
                    "stock_sector_map": {"A": "S1"},
                }
            ).encode("utf-8")
        raise QEWorkspaceFileNotFound(task_id, loop_id, file_path, "https://qe.invalid")


class _MarketRepository:
    def __init__(self, *, empty: bool = False) -> None:
        self.empty = empty
        self.horizons: list[int] = []

    def resolve_watermark(self, *, policy: str, requested_date: date | None) -> MarketWatermark:
        assert policy == "latest_common_completed"
        assert requested_date is None
        return MarketWatermark(
            requested_policy=policy,
            requested_date=None,
            resolved_as_of_date=date(2026, 1, 30),
            dataset_max_dates={
                "market.trading_calendar": date(2026, 1, 30),
                "market.kline_daily_raw": date(2026, 1, 30),
            },
            calendar_start_date=date(2020, 1, 1),
            calendar_end_date=date(2026, 1, 30),
            pit_mapping_symbol_count=2,
            pit_market_symbol_count=2,
            read_only_transaction={
                "transaction_read_only": True,
                "isolation_level": "repeatable_read",
                "write_relations": [],
            },
        )

    def read_forward_returns(
        self,
        *,
        symbols: list[str],
        trade_dates: list[date],
        horizon_trading_days: int,
        as_of_date: date,
    ) -> MarketReturnRead:
        self.horizons.append(horizon_trading_days)
        rows: list[tuple[Any, ...]] = []
        if not self.empty:
            rows = [
                (trade_dates[0], symbol, horizon_trading_days, 0.01 * index, date(2026, 1, 20))
                for index, symbol in enumerate(symbols, start=1)
            ]
        return MarketReturnRead(
            returns=pd.DataFrame(
                rows,
                columns=["trade_date", "symbol", "horizon_days", "future_return", "label_date"],
            ),
            price_row_count=len(rows) * 2,
            requested_symbol_count=len(symbols),
            requested_date_count=len(trade_dates),
            horizon_trading_days=horizon_trading_days,
            as_of_date=as_of_date,
            read_only_transaction={
                "transaction_read_only": True,
                "isolation_level": "repeatable_read",
                "write_relations": [],
            },
        )


def test_source_has_no_plaintext_db_or_forbidden_config_download() -> None:
    source = Path(diagnostic.__file__).read_text(encoding="utf-8")
    assert "psycopg2" not in source
    assert "DB_DEFAULT" not in source
    assert "conf.yaml" not in source
    assert "except Exception" not in source


def test_safe_float_does_not_swallow_unexpected_runtime_errors() -> None:
    class _BrokenFloat:
        def __float__(self) -> float:
            raise RuntimeError("unexpected conversion failure")

    with pytest.raises(RuntimeError, match="unexpected conversion failure"):
        diagnostic.safe_float(_BrokenFloat())


def test_optional_artifact_absence_is_explicit_and_conf_is_never_requested(
    tmp_path: Path,
) -> None:
    client = _ArtifactClient()
    artifacts = asyncio.run(
        diagnostic.download_loop_artifacts(client, "qe-task", _loop(), tmp_path)
    )

    assert "conf.yaml" not in client.requested_paths
    assert "hmm_sector_coefficients.json" in client.requested_paths
    assert {item["reason_code"] for item in artifacts["warnings"]} == {
        "qe_workspace_file_not_found"
    }
    assert not list(tmp_path.rglob("conf.yaml"))


def test_replacement_wrapper_uses_canonical_evaluator_and_preserves_h20(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade_date = pd.Timestamp("2026-01-05")
    index = pd.MultiIndex.from_tuples(
        [(trade_date, symbol) for symbol in ("A", "B", "C", "D")],
        names=["datetime", "instrument"],
    )
    predictions = pd.Series([4.0, 3.0, 2.0, 1.0], index=index)
    labels = pd.Series([-0.1, -0.2, 0.3, 0.0], index=index)
    payload = {
        "daily_coefficients": {"2026-01-05": {"S1": 1.0, "S2": 2.0}},
        "stock_sector_map": {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
    }
    original = diagnostic.evaluate_candidate
    calls: list[dict[str, Any]] = []

    def _spy(**kwargs: Any):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(diagnostic, "evaluate_candidate", _spy)
    replacements, days, _sectors = diagnostic.compute_replacements(
        predictions,
        labels,
        payload,
        2,
        "h20-oracle",
        label_horizon_days=20,
    )

    assert len(calls) == 1
    assert calls[0]["label_horizon_days"] == 20
    assert calls[0]["market_forward_return_mode"] == "disabled"
    assert "label_20d" in replacements.columns
    assert "label_10d" not in replacements.columns
    assert "net_enter_minus_drop_label_20d" in days.columns


def test_market_enrichment_uses_trading_day_repository_and_records_coverage() -> None:
    replacements = pd.DataFrame(
        [
            {"date": "2026-01-05", "symbol": "A", "replacement_type": "entered_by_hmm"},
            {"date": "2026-01-05", "symbol": "B", "replacement_type": "dropped_by_hmm"},
        ]
    )
    repository = _MarketRepository()

    enriched = diagnostic.enrich_db_forward_returns(
        replacements,
        [5, 10, 20],
        repository=repository,
    )

    assert repository.horizons == [5, 10, 20]
    assert enriched[["db_ret_5d", "db_ret_10d", "db_ret_20d"]].notna().all().all()
    evidence = enriched.attrs["market_return_evidence"]
    assert evidence["watermark"]["read_only_transaction"]["transaction_read_only"] is True
    assert evidence["horizons"]["10"]["replacement_row_coverage_ratio"] == 1.0


def test_market_enrichment_fails_loudly_when_no_horizon_has_returns() -> None:
    replacements = pd.DataFrame(
        [{"date": "2026-01-05", "symbol": "A", "replacement_type": "entered_by_hmm"}]
    )
    with pytest.raises(MarketDataUnavailableError, match="no trading-day market returns"):
        diagnostic.enrich_db_forward_returns(
            replacements,
            [10],
            repository=_MarketRepository(empty=True),
        )


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("h20", 20),
        ("label_10d", 10),
        ("Ref($close, -20) / Ref($close, -1) - 1", 20),
    ],
)
def test_label_horizon_is_explicit_or_unambiguously_inferred(
    label: str,
    expected: int,
) -> None:
    assert diagnostic.resolve_label_horizon(label, None) == expected


def test_unknown_label_horizon_fails_loudly() -> None:
    with pytest.raises(RuntimeError, match="pass --label-horizon-days explicitly"):
        diagnostic.resolve_label_horizon("LABEL0", None)

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.historical_price_replay import (
    AdvisoryHistoricalPriceReplayService,
    HistoricalPriceDaySnapshot,
    read_historical_price_replay_artifact,
)
from backend.services.advisory_model_first.historical_price_replay_contracts import (
    build_historical_price_replay_request,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source(path: Path) -> Path:
    rows = []
    for day, target in ((date(2026, 1, 5), date(2026, 1, 6)), (date(2026, 1, 6), date(2026, 1, 7))):
        for index in range(20):
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "target_trade_date": target,
                    "instrument": f"{index:06d}.SZ",
                    "entry_gap_return": 9.99,  # Forbidden label must never enter the snapshot.
                    "entry_gap_calibrated_q10": -0.02,
                    "entry_gap_calibrated_q50": 0.0,
                    "entry_gap_calibrated_q90": 0.02,
                }
            )
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _request(source: Path):
    return build_historical_price_replay_request(
        price_range_bundle_id="a" * 64,
        price_range_manifest_sha256="b" * 64,
        prediction_source_sha256=_sha256(source),
        decision_start_trade_date=date(2026, 1, 5),
        decision_end_trade_date=date(2026, 1, 6),
        replay_as_of_date=date(2026, 1, 8),
        pit_universe_key="canonical-pit-v1",
    )


class _Source:
    def __init__(self, prediction_path: Path) -> None:
        self.prediction_path = prediction_path
        self.calls = 0

    def load_day(self, *, symbols, decision_trade_date, target_trade_date, pit_universe_key):
        assert self.prediction_path.exists(), "prediction snapshot must freeze before outcomes"
        assert pit_universe_key == "canonical-pit-v1"
        self.calls += 1
        contexts = {
            symbol: PriceRangeRealtimeContext(
                symbol=symbol,
                decision_raw_close=10.0,
                decision_price_trade_date=decision_trade_date,
                decision_price_source="market.kline_daily_raw.close_li",
                price_unit_divisor=1000.0,
                target_raw_price_multiplier=1.0,
                corporate_action_source="none",
                board_type="MAIN",
                list_date=date(2020, 1, 1),
                listed_trading_days=99,
                target_is_st=False,
                tick_size=0.01,
            )
            for symbol in symbols
        }
        opens = {symbol: 10.0 for symbol in symbols}
        suspended = frozenset({symbols[0]}) if self.calls == 1 else frozenset()
        opens.pop(symbols[0], None) if suspended else None
        missing = {symbols[-1]: "target_market_row_missing_unexplained"} if self.calls == 2 else {}
        for symbol in missing:
            opens.pop(symbol, None)
        return HistoricalPriceDaySnapshot(
            contexts=contexts,
            context_unavailable={},
            raw_open_by_symbol=opens,
            suspended_symbols=suspended,
            market_unavailable=missing,
        )


def test_batch_replay_freezes_predictions_before_database_outcomes(tmp_path):
    source_path = _source(tmp_path / "predictions.parquet")
    request = _request(source_path)
    prediction_path = (
        tmp_path
        / "out"
        / "price_range_historical_replays"
        / request.replay_id
        / "prediction"
        / "prediction.json"
    )
    data_source = _Source(prediction_path)
    service = AdvisoryHistoricalPriceReplayService(
        data_source=data_source,
        now_provider=lambda: datetime(2026, 1, 8, 1, tzinfo=timezone.utc),
    )
    receipt = service.run(
        request=request, prediction_source_path=source_path, output_root=tmp_path / "out"
    )
    assert receipt.status == "PUBLISHED"
    assert receipt.evidence_level == "HISTORICAL_REPLAY"
    assert receipt.metrics.decision_date_count == 2
    assert receipt.metrics.candidate_count == 40
    assert receipt.metrics.not_applicable_count == 1
    assert receipt.metrics.market_unavailable_count == 1
    assert receipt.metrics.model_available_market_available_count == 38
    assert receipt.metrics.calibrated_coverage == 1.0
    frozen = json.loads(prediction_path.read_text(encoding="utf-8"))
    assert frozen["realized_outcome_accessed"] is False
    assert all("entry_gap_return" not in row for row in frozen["rows"])
    assert data_source.calls == 2


def test_batch_replay_exact_retry_is_immutable(tmp_path):
    source_path = _source(tmp_path / "predictions.parquet")
    request = _request(source_path)
    prediction_path = (
        tmp_path / "out" / "price_range_historical_replays" / request.replay_id / "prediction" / "prediction.json"
    )
    data_source = _Source(prediction_path)
    service = AdvisoryHistoricalPriceReplayService(data_source=data_source)
    first = service.run(request=request, prediction_source_path=source_path, output_root=tmp_path / "out")
    second = service.run(request=request, prediction_source_path=source_path, output_root=tmp_path / "out")
    assert first.receipt_sha256 == second.receipt_sha256
    assert second.status == "ALREADY_MATERIALIZED"
    assert data_source.calls == 2
    artifact = read_historical_price_replay_artifact(
        tmp_path / "out" / "price_range_historical_replays" / request.replay_id
    )
    assert artifact.receipt.receipt_sha256 == first.receipt_sha256

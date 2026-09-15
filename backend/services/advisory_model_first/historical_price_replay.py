from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

import pandas as pd
from pydantic import ValidationError

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.historical_price_replay_contracts import (
    AdvisoryHistoricalPriceOutcomeRowV1,
    AdvisoryHistoricalPricePredictionRowV1,
    AdvisoryHistoricalPriceReplayMetricsV1,
    AdvisoryHistoricalPriceReplayReceiptV1,
    AdvisoryHistoricalPriceReplayRequestV1,
    build_historical_price_replay_receipt,
    build_historical_price_replay_request,
)
from backend.services.advisory_model_first.price_range_calibration_bundle import (
    validate_calibrated_daily_price_envelope_bundle,
)
from backend.services.advisory_model_first.price_range_contracts import (
    canonical_json_sha256,
)
from backend.services.advisory_model_first.price_range_inference import _entry_band
from backend.services.advisory_model_first.price_range_regulatory import (
    resolve_regulatory_price_range,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext,
    _board_type,
    _project_target_st,
    _target_raw_price_multiplier,
)
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_UNIVERSE_KEY


HISTORICAL_REPLAY_ROOT = "price_range_historical_replays"
PRICE_UNIT_DIVISOR = 1000.0
PREDICTION_COLUMNS = (
    "decision_as_of_trade_date",
    "target_trade_date",
    "instrument",
    "entry_gap_calibrated_q10",
    "entry_gap_calibrated_q50",
    "entry_gap_calibrated_q90",
)


@dataclass(frozen=True)
class HistoricalPriceDaySnapshot:
    contexts: Mapping[str, PriceRangeRealtimeContext]
    context_unavailable: Mapping[str, str]
    raw_open_by_symbol: Mapping[str, float]
    suspended_symbols: frozenset[str]
    market_unavailable: Mapping[str, str]


@dataclass(frozen=True)
class AdvisoryHistoricalPriceReplayArtifact:
    path: Path
    request: AdvisoryHistoricalPriceReplayRequestV1
    receipt: AdvisoryHistoricalPriceReplayReceiptV1
    outcome: dict[str, Any]


class PostgresHistoricalPriceReplaySource:
    """Read historical D/T rows without requiring the current-day refresh clock."""

    def __init__(self, *, connection_context_factory: Callable[[], Any] | None = None) -> None:
        self._connection_context_factory = connection_context_factory or (
            lambda: get_conn(autocommit=False, manage_transaction=False)
        )

    def load_day(
        self,
        *,
        symbols: Sequence[str],
        decision_trade_date: date,
        target_trade_date: date,
        pit_universe_key: str,
    ) -> HistoricalPriceDaySnapshot:
        normalized = tuple(sorted({str(value).strip().upper() for value in symbols}))
        if len(normalized) != len(symbols) or not normalized:
            raise _replay_error(
                "historical replay day contains missing or duplicate symbols",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_IDENTITY_MISMATCH",
            )
        try:
            with self._connection_context_factory() as conn:
                cursor = conn.cursor()
                try:
                    conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                    contexts, unavailable = self._contexts(
                        cursor,
                        symbols=normalized,
                        decision_trade_date=decision_trade_date,
                        target_trade_date=target_trade_date,
                        pit_universe_key=pit_universe_key,
                    )
                    cursor.execute(
                        """
                        SELECT ts_code, open_li
                        FROM market.kline_daily_raw
                        WHERE trade_date = %s AND ts_code = ANY(%s)
                        ORDER BY ts_code
                        """,
                        (target_trade_date, list(normalized)),
                    )
                    open_rows = cursor.fetchall()
                    cursor.execute(
                        """
                        SELECT ts_code, suspend_type
                        FROM market.suspend_d
                        WHERE trade_date = %s AND ts_code = ANY(%s)
                        ORDER BY ts_code, suspend_type
                        """,
                        (target_trade_date, list(normalized)),
                    )
                    suspend_rows = cursor.fetchall()
                    conn.rollback()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    cursor.close()
        except AdvisoryModelFirstError:
            raise
        except Exception as exc:
            raise _replay_error(
                "historical replay database read failed",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
                context={"error_type": type(exc).__name__},
            ) from exc

        opens: dict[str, float] = {}
        for symbol_value, open_value in open_rows:
            symbol = str(symbol_value).strip().upper()
            try:
                value = float(open_value) / PRICE_UNIT_DIVISOR
            except (TypeError, ValueError, OverflowError):
                value = float("nan")
            if symbol in opens or not math.isfinite(value) or value <= 0.0:
                raise _replay_error(
                    "historical replay target open is duplicate or invalid",
                    "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
                    context={"symbol": symbol},
                )
            opens[symbol] = value
        suspend_types: dict[str, set[str]] = {}
        for symbol_value, suspend_type_value in suspend_rows:
            suspend_types.setdefault(str(symbol_value).strip().upper(), set()).add(
                str(suspend_type_value).strip().upper()
            )
        conflicting = sorted(symbol for symbol, values in suspend_types.items() if len(values) != 1)
        if conflicting:
            raise _replay_error(
                "historical replay suspend rows conflict",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
                context={"symbols": conflicting},
            )
        suspended = frozenset(symbol for symbol, values in suspend_types.items() if values == {"S"})
        if set(opens) & suspended:
            raise _replay_error(
                "historical replay target is both traded and suspended",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
            )
        missing = {
            symbol: "target_market_row_missing_unexplained"
            for symbol in normalized
            if symbol not in opens and symbol not in suspended
        }
        return HistoricalPriceDaySnapshot(
            contexts=contexts,
            context_unavailable=unavailable,
            raw_open_by_symbol=opens,
            suspended_symbols=suspended,
            market_unavailable=missing,
        )

    @staticmethod
    def _contexts(
        cursor: Any,
        *,
        symbols: Sequence[str],
        decision_trade_date: date,
        target_trade_date: date,
        pit_universe_key: str,
    ) -> tuple[dict[str, PriceRangeRealtimeContext], dict[str, str]]:
        cursor.execute(
            """
            SELECT status, dirty, start_date, end_date
            FROM market.stock_universe_pit_state
            WHERE universe_key = %s
            """,
            (pit_universe_key,),
        )
        state = cursor.fetchone()
        if (
            state is None
            or str(state[0] or "").lower() != "ready"
            or bool(state[1])
            or state[2] is None
            or state[3] is None
            or state[2] > decision_trade_date
            or state[3] < decision_trade_date
        ):
            raise _replay_error(
                "historical PIT universe is not ready for the decision date",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_PIT_UNAVAILABLE",
                context={"decision_trade_date": decision_trade_date.isoformat()},
            )
        cursor.execute(
            """
            SELECT price.ts_code, price.close_li, basic.list_date,
                   CASE
                     WHEN basic.list_date IS NULL THEN NULL
                     WHEN basic.list_date < %s - INTERVAL '14 days' THEN 99
                     ELSE (
                       SELECT COUNT(*) FROM market.trading_calendar cal
                       WHERE cal.is_trading = TRUE
                         AND cal.cal_date BETWEEN basic.list_date AND %s
                     )
                   END AS listed_trading_days
            FROM market.kline_daily_raw price
            LEFT JOIN market.stock_basic basic ON basic.ts_code = price.ts_code
            WHERE price.trade_date = %s AND price.ts_code = ANY(%s)
            ORDER BY price.ts_code
            """,
            (target_trade_date, target_trade_date, decision_trade_date, list(symbols)),
        )
        decision_rows = {str(row[0]).upper(): row for row in cursor.fetchall()}
        cursor.execute(
            """
            SELECT DISTINCT ON (ts_code)
                   ts_code, event_kind, action_date, source_pub_date,
                   source_imp_date, source_effective_date
            FROM market.stock_universe_pit_events
            WHERE universe_key = %s AND ts_code = ANY(%s)
              AND action_date <= %s
              AND COALESCE(source_pub_date, source_imp_date,
                           source_effective_date, action_date) <= %s
            ORDER BY ts_code, action_date DESC, event_id DESC
            """,
            (pit_universe_key, list(symbols), target_trade_date, decision_trade_date),
        )
        st_events = {str(row[0]).upper(): row for row in cursor.fetchall()}
        cursor.execute(
            """
            SELECT ts_code, end_date, ann_date, div_proc, stk_div,
                   stk_bo_rate, stk_co_rate, cash_div, cash_div_tax, imp_ann_date
            FROM market.dividend
            WHERE ex_date = %s AND ts_code = ANY(%s) AND div_proc = '实施'
            ORDER BY ts_code, imp_ann_date DESC NULLS LAST, end_date DESC, ann_date DESC
            """,
            (target_trade_date, list(symbols)),
        )
        dividends: dict[str, list[tuple[Any, ...]]] = {}
        for row in cursor.fetchall():
            dividends.setdefault(str(row[0]).upper(), []).append(row)

        contexts: dict[str, PriceRangeRealtimeContext] = {}
        unavailable: dict[str, str] = {}
        for symbol in symbols:
            row = decision_rows.get(symbol)
            if row is None or row[1] is None:
                unavailable[symbol] = "decision_raw_close_unavailable"
                continue
            try:
                close = float(row[1]) / PRICE_UNIT_DIVISOR
                list_date = pd.Timestamp(row[2]).date()
                listed_days = int(row[3])
                multiplier, source = _target_raw_price_multiplier(
                    symbol=symbol,
                    decision_raw_close=close,
                    rows=dividends.get(symbol, []),
                    decision_as_of_trade_date=decision_trade_date,
                )
                target_is_st = _project_target_st(st_events.get(symbol))
            except (AdvisoryModelFirstError, TypeError, ValueError, OverflowError) as exc:
                unavailable[symbol] = f"pit_price_context_unavailable:{type(exc).__name__}"
                continue
            if not math.isfinite(close) or close <= 0.0:
                unavailable[symbol] = "decision_raw_close_invalid"
                continue
            contexts[symbol] = PriceRangeRealtimeContext(
                symbol=symbol,
                decision_raw_close=close,
                decision_price_trade_date=decision_trade_date,
                decision_price_source="market.kline_daily_raw.close_li",
                price_unit_divisor=PRICE_UNIT_DIVISOR,
                target_raw_price_multiplier=multiplier,
                corporate_action_source=source,
                board_type=_board_type(symbol),
                list_date=list_date,
                listed_trading_days=listed_days,
                target_is_st=target_is_st,
                tick_size=0.01,
            )
        return contexts, unavailable


class AdvisoryHistoricalPriceReplayService:
    def __init__(
        self,
        *,
        data_source: Any | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._data_source = data_source or PostgresHistoricalPriceReplaySource()
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def run(
        self,
        *,
        request: AdvisoryHistoricalPriceReplayRequestV1,
        prediction_source_path: str | Path,
        output_root: str | Path,
    ) -> AdvisoryHistoricalPriceReplayReceiptV1:
        root = Path(output_root).resolve() / HISTORICAL_REPLAY_ROOT / request.replay_id
        prediction_path = root / "prediction"
        settlement_path = root / "settlement"
        if settlement_path.exists():
            return read_historical_price_replay_artifact(root).receipt.model_copy(
                update={"status": "ALREADY_MATERIALIZED"}
            )
        predictions, prediction_sha256 = self._freeze_predictions(
            request=request,
            prediction_source_path=prediction_source_path,
            target=prediction_path,
        )
        rows: list[AdvisoryHistoricalPriceOutcomeRowV1] = []
        for (decision_day, target_day), group in _group_predictions(predictions):
            snapshot = self._data_source.load_day(
                symbols=tuple(row.symbol for row in group),
                decision_trade_date=decision_day,
                target_trade_date=target_day,
                pit_universe_key=request.pit_universe_key,
            )
            rows.extend(_settle_day(group, snapshot=snapshot))
        ordered = tuple(sorted(rows, key=lambda row: (row.decision_as_of_trade_date, row.symbol)))
        metrics = _metrics(ordered)
        outcome_payload: dict[str, Any] = {
            "schema_version": "advisory_historical_price_replay_outcome_v1",
            "replay_id": request.replay_id,
            "request_sha256": request.request_sha256,
            "prediction_snapshot_sha256": prediction_sha256,
            "evidence_level": "HISTORICAL_REPLAY",
            "decision_use": "NAVIGATION_ONLY",
            "realized_outcome_accessed": True,
            "database_written": False,
            "binding_activated": False,
            "sealed_holdout_consumed": False,
            "metrics": metrics.model_dump(mode="json"),
            "rows": [row.model_dump(mode="json") for row in ordered],
        }
        outcome_sha256 = canonical_json_sha256(outcome_payload)
        return self._publish_settlement(
            target=settlement_path,
            request=request,
            prediction_sha256=prediction_sha256,
            outcome_sha256=outcome_sha256,
            outcome_payload={**outcome_payload, "outcome_sha256": outcome_sha256},
            metrics=metrics,
        )

    @staticmethod
    def _freeze_predictions(
        *,
        request: AdvisoryHistoricalPriceReplayRequestV1,
        prediction_source_path: str | Path,
        target: Path,
    ) -> tuple[tuple[AdvisoryHistoricalPricePredictionRowV1, ...], str]:
        if target.exists():
            payload = _read_json(target / "prediction.json")
            return _validate_prediction_snapshot(request=request, payload=payload)
        source = Path(prediction_source_path).resolve()
        try:
            source_sha256 = _sha256_file(source)
        except OSError as exc:
            raise _replay_error(
                "historical prediction source is unavailable",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
                context={"error_type": type(exc).__name__},
            ) from exc
        if source_sha256 != request.prediction_source_sha256:
            raise _replay_error(
                "historical prediction source hash changed",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_IDENTITY_MISMATCH",
            )
        try:
            # Intentionally exclude entry_gap_return and every other outcome column.
            frame = pd.read_parquet(source, columns=list(PREDICTION_COLUMNS))
            decisions = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.date
            selected = frame[
                (decisions >= request.decision_start_trade_date) & (decisions <= request.decision_end_trade_date)
            ].copy()
            selected["decision_as_of_trade_date"] = pd.to_datetime(selected["decision_as_of_trade_date"]).dt.date
            selected["target_trade_date"] = pd.to_datetime(selected["target_trade_date"]).dt.date
            rows = tuple(
                AdvisoryHistoricalPricePredictionRowV1(
                    decision_as_of_trade_date=item.decision_as_of_trade_date,
                    target_trade_date=item.target_trade_date,
                    symbol=str(item.instrument).strip().upper(),
                    calibrated_gap_q10=float(item.entry_gap_calibrated_q10),
                    calibrated_gap_q50=float(item.entry_gap_calibrated_q50),
                    calibrated_gap_q90=float(item.entry_gap_calibrated_q90),
                )
                for item in selected.sort_values(["decision_as_of_trade_date", "instrument"]).itertuples(index=False)
            )
        except Exception as exc:
            raise _replay_error(
                "historical prediction source cannot be read",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
                context={"error_type": type(exc).__name__},
            ) from exc
        if selected.empty:
            raise _replay_error(
                "historical replay date range has no frozen predictions",
                "ADVISORY_HISTORICAL_PRICE_REPLAY_DATA_UNAVAILABLE",
            )
        digest = canonical_json_sha256([row.model_dump(mode="json") for row in rows])
        payload = {
            "schema_version": "advisory_historical_price_prediction_snapshot_v1",
            "request_sha256": request.request_sha256,
            "prediction_snapshot_sha256": digest,
            "source_columns": list(PREDICTION_COLUMNS),
            "forbidden_source_columns": ["entry_gap_return"],
            "realized_outcome_accessed": False,
            "rows": [row.model_dump(mode="json") for row in rows],
        }
        _publish_directory(target, {"request.json": request.model_dump(mode="json"), "prediction.json": payload})
        published = _read_json(target / "prediction.json")
        return _validate_prediction_snapshot(request=request, payload=published)

    def _publish_settlement(
        self,
        *,
        target: Path,
        request: AdvisoryHistoricalPriceReplayRequestV1,
        prediction_sha256: str,
        outcome_sha256: str,
        outcome_payload: Mapping[str, Any],
        metrics: AdvisoryHistoricalPriceReplayMetricsV1,
    ) -> AdvisoryHistoricalPriceReplayReceiptV1:
        published_at = self._now_provider().astimezone(timezone.utc)
        temporary = Path(tempfile.mkdtemp(prefix=".historical-price-settlement-", dir=target.parent))
        try:
            _write_json(temporary / "outcome.json", dict(outcome_payload))
            manifest = {
                "schema_version": "advisory_historical_price_replay_manifest_v1",
                "replay_id": request.replay_id,
                "request_sha256": request.request_sha256,
                "prediction_snapshot_sha256": prediction_sha256,
                "outcome_sha256": outcome_sha256,
                "files": {"outcome.json": _file_identity(temporary / "outcome.json")},
            }
            _write_json(temporary / "manifest.json", manifest)
            receipt = build_historical_price_replay_receipt(
                status="PUBLISHED",
                replay_id=request.replay_id,
                request_sha256=request.request_sha256,
                prediction_snapshot_sha256=prediction_sha256,
                outcome_sha256=outcome_sha256,
                manifest_sha256=_sha256_file(temporary / "manifest.json"),
                published_at=published_at,
                metrics=metrics,
            )
            _write_json(temporary / "receipt.json", receipt.model_dump(mode="json"))
            try:
                os.replace(temporary, target)
            except OSError:
                if not target.exists():
                    raise
                return read_historical_price_replay_artifact(target.parent).receipt.model_copy(
                    update={"status": "ALREADY_MATERIALIZED"}
                )
            return read_historical_price_replay_artifact(target.parent).receipt
        finally:
            if temporary.exists():
                for child in temporary.iterdir():
                    child.unlink(missing_ok=True)
                temporary.rmdir()


def prepare_historical_price_replay_request(
    *,
    model_root: str | Path,
    price_range_bundle_id: str,
    decision_start_trade_date: date,
    decision_end_trade_date: date,
    replay_as_of_date: date,
    pit_universe_key: str = DEFAULT_ST_PIT_UNIVERSE_KEY,
) -> tuple[AdvisoryHistoricalPriceReplayRequestV1, Path]:
    bundle_path = Path(model_root).resolve() / "price_range_bundles" / price_range_bundle_id
    manifest = validate_calibrated_daily_price_envelope_bundle(bundle_path, expected_bundle_id=price_range_bundle_id)
    if manifest.get("schema_version") != "advisory_price_range_bundle_v4":
        raise _replay_error(
            "historical price replay requires a frozen v4 bundle",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_IDENTITY_MISMATCH",
        )
    source = bundle_path / "calibrated_test_predictions.parquet"
    request = build_historical_price_replay_request(
        price_range_bundle_id=price_range_bundle_id,
        price_range_manifest_sha256=_sha256_file(bundle_path / "manifest.json"),
        prediction_source_sha256=_sha256_file(source),
        decision_start_trade_date=decision_start_trade_date,
        decision_end_trade_date=decision_end_trade_date,
        replay_as_of_date=replay_as_of_date,
        pit_universe_key=pit_universe_key,
    )
    return request, source


def read_historical_price_replay_artifact(
    root: str | Path,
) -> AdvisoryHistoricalPriceReplayArtifact:
    path = Path(root).resolve()
    prediction_path = path / "prediction"
    settlement_path = path / "settlement"
    try:
        request = AdvisoryHistoricalPriceReplayRequestV1.model_validate(_read_json(prediction_path / "request.json"))
        prediction = _read_json(prediction_path / "prediction.json")
        outcome = _read_json(settlement_path / "outcome.json")
        manifest = _read_json(settlement_path / "manifest.json")
        receipt_payload = _read_json(settlement_path / "receipt.json")
        receipt = AdvisoryHistoricalPriceReplayReceiptV1.model_validate(receipt_payload)
    except (OSError, ValueError, ValidationError) as exc:
        raise _replay_error(
            "historical price replay artifact cannot be read",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
            context={"error_type": type(exc).__name__},
        ) from exc
    prediction_rows, prediction_sha256 = _validate_prediction_snapshot(request=request, payload=prediction)
    try:
        outcome_rows = tuple(
            AdvisoryHistoricalPriceOutcomeRowV1.model_validate(item) for item in outcome.get("rows") or ()
        )
    except (TypeError, ValidationError) as exc:
        raise _replay_error(
            "historical price replay outcome rows are invalid",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
            context={"error_type": type(exc).__name__},
        ) from exc
    expected_keys = tuple((row.decision_as_of_trade_date, row.target_trade_date, row.symbol) for row in prediction_rows)
    actual_keys = tuple((row.decision_as_of_trade_date, row.target_trade_date, row.symbol) for row in outcome_rows)
    if actual_keys != expected_keys:
        raise _replay_error(
            "historical price replay outcome identities differ from predictions",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
        )
    try:
        recomputed_metrics = _metrics(outcome_rows)
    except ValidationError as exc:
        raise _replay_error(
            "historical price replay outcome metrics are invalid",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
            context={"error_type": type(exc).__name__},
        ) from exc
    outcome_without_hash = {key: value for key, value in outcome.items() if key != "outcome_sha256"}
    outcome_sha256 = canonical_json_sha256(outcome_without_hash)
    if (
        outcome.get("schema_version") != "advisory_historical_price_replay_outcome_v1"
        or outcome.get("replay_id") != request.replay_id
        or outcome.get("request_sha256") != request.request_sha256
        or outcome.get("evidence_level") != "HISTORICAL_REPLAY"
        or outcome.get("decision_use") != "NAVIGATION_ONLY"
        or outcome.get("realized_outcome_accessed") is not True
        or outcome.get("database_written") is not False
        or outcome.get("binding_activated") is not False
        or outcome.get("sealed_holdout_consumed") is not False
        or outcome.get("outcome_sha256") != outcome_sha256
        or manifest.get("schema_version") != "advisory_historical_price_replay_manifest_v1"
        or manifest.get("replay_id") != request.replay_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("prediction_snapshot_sha256") != prediction_sha256
        or manifest.get("outcome_sha256") != outcome_sha256
        or manifest.get("files") != {"outcome.json": _file_identity(settlement_path / "outcome.json")}
        or receipt.request_sha256 != request.request_sha256
        or receipt.replay_id != request.replay_id
        or receipt.prediction_snapshot_sha256 != prediction_sha256
        or receipt.outcome_sha256 != outcome_sha256
        or receipt.manifest_sha256 != _sha256_file(settlement_path / "manifest.json")
        or recomputed_metrics.model_dump(mode="json") != outcome.get("metrics")
        or receipt.metrics != recomputed_metrics
    ):
        raise _replay_error(
            "historical price replay artifact hashes are inconsistent",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
        )
    return AdvisoryHistoricalPriceReplayArtifact(path=path, request=request, receipt=receipt, outcome=outcome)


def _validate_prediction_snapshot(
    *,
    request: AdvisoryHistoricalPriceReplayRequestV1,
    payload: Mapping[str, Any],
) -> tuple[tuple[AdvisoryHistoricalPricePredictionRowV1, ...], str]:
    if (
        payload.get("schema_version") != "advisory_historical_price_prediction_snapshot_v1"
        or payload.get("request_sha256") != request.request_sha256
        or payload.get("source_columns") != list(PREDICTION_COLUMNS)
        or payload.get("forbidden_source_columns") != ["entry_gap_return"]
        or payload.get("realized_outcome_accessed") is not False
    ):
        raise _replay_error(
            "historical prediction snapshot contract is inconsistent",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
        )
    try:
        rows = tuple(AdvisoryHistoricalPricePredictionRowV1.model_validate(item) for item in payload.get("rows") or ())
    except (TypeError, ValidationError) as exc:
        raise _replay_error(
            "historical prediction snapshot rows are invalid",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
            context={"error_type": type(exc).__name__},
        ) from exc
    if not rows:
        raise _replay_error(
            "historical prediction snapshot is empty",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
        )
    keys = [(row.decision_as_of_trade_date, row.symbol) for row in rows]
    per_day: dict[date, list[AdvisoryHistoricalPricePredictionRowV1]] = {}
    for row in rows:
        per_day.setdefault(row.decision_as_of_trade_date, []).append(row)
    if len(keys) != len(set(keys)) or {len(group) for group in per_day.values()} != {20}:
        raise _replay_error(
            "historical replay requires one exact Top20 for every represented decision date",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_IDENTITY_MISMATCH",
        )
    if any(len({row.target_trade_date for row in group}) != 1 for group in per_day.values()):
        raise _replay_error(
            "historical replay decision date has multiple target dates",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_IDENTITY_MISMATCH",
        )
    if any(
        row.decision_as_of_trade_date < request.decision_start_trade_date
        or row.decision_as_of_trade_date > request.decision_end_trade_date
        for row in rows
    ):
        raise _replay_error(
            "historical prediction snapshot exceeds its requested decision window",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_CLOCK_INVALID",
        )
    if any(row.target_trade_date >= request.replay_as_of_date for row in rows):
        raise _replay_error(
            "historical replay includes an outcome not mature at replay as-of",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_CLOCK_INVALID",
        )
    normalized = [row.model_dump(mode="json") for row in rows]
    digest = canonical_json_sha256(normalized)
    if payload.get("prediction_snapshot_sha256") != digest:
        raise _replay_error(
            "historical prediction snapshot hash is inconsistent",
            "ADVISORY_HISTORICAL_PRICE_REPLAY_ARTIFACT_CONFLICT",
        )
    return rows, digest


def _group_predictions(
    rows: Sequence[AdvisoryHistoricalPricePredictionRowV1],
) -> Sequence[tuple[tuple[date, date], tuple[AdvisoryHistoricalPricePredictionRowV1, ...]]]:
    grouped: dict[tuple[date, date], list[AdvisoryHistoricalPricePredictionRowV1]] = {}
    for row in rows:
        grouped.setdefault((row.decision_as_of_trade_date, row.target_trade_date), []).append(row)
    return tuple((key, tuple(value)) for key, value in sorted(grouped.items()))


def _settle_day(
    predictions: Sequence[AdvisoryHistoricalPricePredictionRowV1],
    *,
    snapshot: HistoricalPriceDaySnapshot,
) -> list[AdvisoryHistoricalPriceOutcomeRowV1]:
    output: list[AdvisoryHistoricalPriceOutcomeRowV1] = []
    for prediction in predictions:
        symbol = prediction.symbol
        base = {
            "decision_as_of_trade_date": prediction.decision_as_of_trade_date,
            "target_trade_date": prediction.target_trade_date,
            "symbol": symbol,
        }
        if symbol in snapshot.suspended_symbols:
            output.append(
                AdvisoryHistoricalPriceOutcomeRowV1(
                    **base,
                    model_prediction_status="AVAILABLE",
                    market_outcome_status="NOT_APPLICABLE",
                    market_outcome_reason="target_authoritatively_suspended",
                )
            )
            continue
        if symbol in snapshot.market_unavailable:
            output.append(
                AdvisoryHistoricalPriceOutcomeRowV1(
                    **base,
                    model_prediction_status="AVAILABLE",
                    market_outcome_status="UNAVAILABLE",
                    market_outcome_reason=snapshot.market_unavailable[symbol],
                )
            )
            continue
        actual_open = snapshot.raw_open_by_symbol[symbol]
        context = snapshot.contexts.get(symbol)
        if context is None:
            output.append(
                AdvisoryHistoricalPriceOutcomeRowV1(
                    **base,
                    model_prediction_status="UNAVAILABLE",
                    model_prediction_reason=snapshot.context_unavailable.get(symbol, "pit_price_context_unavailable"),
                    market_outcome_status="AVAILABLE",
                    market_outcome_reason="target_open_observed",
                    actual_open=actual_open,
                )
            )
            continue
        try:
            regulatory = resolve_regulatory_price_range(context, target_trade_date=prediction.target_trade_date)
            low, mid, high = _entry_band(
                symbol=symbol,
                context=context,
                regulatory=regulatory,
                entry_gaps=(
                    prediction.calibrated_gap_q10,
                    prediction.calibrated_gap_q50,
                    prediction.calibrated_gap_q90,
                ),
            )
        except AdvisoryModelFirstError as exc:
            output.append(
                AdvisoryHistoricalPriceOutcomeRowV1(
                    **base,
                    model_prediction_status="UNAVAILABLE",
                    model_prediction_reason=exc.reason_code,
                    market_outcome_status="AVAILABLE",
                    market_outcome_reason="target_open_observed",
                    actual_open=actual_open,
                )
            )
            continue
        output.append(
            AdvisoryHistoricalPriceOutcomeRowV1(
                **base,
                model_prediction_status="AVAILABLE",
                market_outcome_status="AVAILABLE",
                market_outcome_reason="target_open_observed",
                actual_open=actual_open,
                decision_reference_price=context.decision_raw_close,
                calibrated_low=low,
                calibrated_mid=mid,
                calibrated_high=high,
                calibrated_gap_q10=prediction.calibrated_gap_q10,
                calibrated_gap_q50=prediction.calibrated_gap_q50,
                calibrated_gap_q90=prediction.calibrated_gap_q90,
                actual_entry_gap_return=(
                    actual_open / (context.decision_raw_close * context.target_raw_price_multiplier) - 1.0
                ),
                model_space_covered=(
                    prediction.calibrated_gap_q10
                    <= actual_open / (context.decision_raw_close * context.target_raw_price_multiplier) - 1.0
                    <= prediction.calibrated_gap_q90
                ),
                model_space_lower_miss=(
                    actual_open / (context.decision_raw_close * context.target_raw_price_multiplier) - 1.0
                    < prediction.calibrated_gap_q10
                ),
                model_space_upper_miss=(
                    actual_open / (context.decision_raw_close * context.target_raw_price_multiplier) - 1.0
                    > prediction.calibrated_gap_q90
                ),
                covered=low <= actual_open <= high,
                lower_miss=actual_open < low,
                upper_miss=actual_open > high,
                interval_width_bps=(high - low) / context.decision_raw_close * 10_000.0,
                absolute_mid_error_bps=abs(actual_open - mid) / context.decision_raw_close * 10_000.0,
            )
        )
    return output


def _metrics(
    rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
) -> AdvisoryHistoricalPriceReplayMetricsV1:
    supported = [
        row for row in rows if row.model_prediction_status == "AVAILABLE" and row.market_outcome_status == "AVAILABLE"
    ]
    payload: dict[str, Any] = {
        "decision_date_count": len({row.decision_as_of_trade_date for row in rows}),
        "candidate_count": len(rows),
        "market_available_count": sum(row.market_outcome_status == "AVAILABLE" for row in rows),
        "not_applicable_count": sum(row.market_outcome_status == "NOT_APPLICABLE" for row in rows),
        "market_unavailable_count": sum(row.market_outcome_status == "UNAVAILABLE" for row in rows),
        "model_available_market_available_count": len(supported),
        "model_unavailable_count": sum(row.model_prediction_status == "UNAVAILABLE" for row in rows),
        "crossing_count": 0,
        "tick_rounding_rescue_count": sum(not bool(row.model_space_covered) and bool(row.covered) for row in supported),
        "tick_rounding_harm_count": sum(bool(row.model_space_covered) and not bool(row.covered) for row in supported),
    }
    if supported:
        widths = [float(row.interval_width_bps) for row in supported if row.interval_width_bps is not None]
        errors = [float(row.absolute_mid_error_bps) for row in supported if row.absolute_mid_error_bps is not None]
        payload.update(
            calibrated_coverage=mean(float(bool(row.covered)) for row in supported),
            lower_miss_rate=mean(float(bool(row.lower_miss)) for row in supported),
            upper_miss_rate=mean(float(bool(row.upper_miss)) for row in supported),
            mean_interval_width_bps=mean(widths),
            median_interval_width_bps=median(widths),
            mean_absolute_mid_error_bps=mean(errors),
            median_absolute_mid_error_bps=median(errors),
            model_space_coverage=mean(float(bool(row.model_space_covered)) for row in supported),
            model_space_lower_miss_rate=mean(float(bool(row.model_space_lower_miss)) for row in supported),
            model_space_upper_miss_rate=mean(float(bool(row.model_space_upper_miss)) for row in supported),
        )
    return AdvisoryHistoricalPriceReplayMetricsV1(**payload)


def _publish_directory(target: Path, files: Mapping[str, Any]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        for name, payload in files.items():
            _write_json(temporary / name, payload)
        try:
            os.replace(temporary, target)
        except OSError:
            if not target.exists():
                raise
    finally:
        if temporary.exists():
            for child in temporary.iterdir():
                child.unlink(missing_ok=True)
            temporary.rmdir()


def _write_json(path: Path, payload: Any) -> None:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _file_identity(path: Path) -> dict[str, Any]:
    return {"sha256": _sha256_file(path), "size_bytes": path.stat().st_size}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _replay_error(
    message: str, reason_code: str, *, context: Mapping[str, Any] | None = None
) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code, context=dict(context or {}))

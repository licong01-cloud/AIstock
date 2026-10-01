from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prospective_price_evaluation_contracts import (
    AdvisoryPriceOutcomeRefreshAuditV1,
    AdvisoryPriceProspectiveOutcomeCandidateV1,
    AdvisoryPriceProspectiveSettlementMetricsV1,
    AdvisoryPriceProspectiveSettlementReceiptV1,
    AdvisoryPriceProspectiveSettlementV1,
    build_settlement,
    build_settlement_receipt,
)
from backend.services.advisory_model_first.prospective_price_prediction import (
    AdvisoryPriceProspectivePredictionArtifact,
    read_prospective_prediction_artifact,
)


SETTLEMENT_ROOT_NAME = "price_range_prospective_settlements"
SHANGHAI = ZoneInfo("Asia/Shanghai")
PRICE_UNIT_DIVISOR = 1000.0


@dataclass(frozen=True)
class AdvisoryPriceOutcomeSnapshot:
    refresh_audits: tuple[AdvisoryPriceOutcomeRefreshAuditV1, AdvisoryPriceOutcomeRefreshAuditV1]
    raw_open_by_symbol: Mapping[str, float]
    suspended_symbols: frozenset[str]


@dataclass(frozen=True)
class AdvisoryPriceProspectiveSettlementArtifact:
    path: Path
    settlement: AdvisoryPriceProspectiveSettlementV1
    manifest: dict[str, Any]
    receipt: AdvisoryPriceProspectiveSettlementReceiptV1


class PostgresAdvisoryPriceOutcomeSource:
    """Read one target-date outcome snapshot without mutating refresh state."""

    def __init__(self, *, connection_context_factory: Callable[[], Any] | None = None) -> None:
        self._connection_context_factory = connection_context_factory or (
            lambda: get_conn(autocommit=False, manage_transaction=False)
        )

    def load(self, *, symbols: Sequence[str], target_trade_date: date) -> AdvisoryPriceOutcomeSnapshot:
        normalized = tuple(sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}))
        if len(normalized) != len(symbols):
            raise _outcome_error(
                "outcome request contains missing or duplicate symbols",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH",
            )
        try:
            with self._connection_context_factory() as conn:
                cursor = conn.cursor()
                try:
                    conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                    audits = self._read_audits(cursor, target_trade_date=target_trade_date)
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
            raise _outcome_error(
                "prospective outcome database read failed",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                context={"error_type": type(exc).__name__},
            ) from exc

        raw_open_by_symbol: dict[str, float] = {}
        for symbol_value, open_value in open_rows:
            symbol = str(symbol_value).strip().upper()
            if symbol in raw_open_by_symbol:
                raise _outcome_error(
                    "prospective outcome contains duplicate daily rows",
                    "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                    context={"symbol": symbol},
                )
            try:
                raw_open = float(open_value) / PRICE_UNIT_DIVISOR
            except (TypeError, ValueError, OverflowError) as exc:
                raise _outcome_error(
                    "prospective outcome open price is invalid",
                    "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                    context={"symbol": symbol},
                ) from exc
            if not math.isfinite(raw_open) or raw_open <= 0.0:
                raise _outcome_error(
                    "prospective outcome open price is invalid",
                    "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                    context={"symbol": symbol},
                )
            raw_open_by_symbol[symbol] = raw_open

        suspend_types: dict[str, set[str]] = {}
        for symbol_value, suspend_type_value in suspend_rows:
            symbol = str(symbol_value).strip().upper()
            suspend_types.setdefault(symbol, set()).add(str(suspend_type_value).strip().upper())
        conflicting = sorted(symbol for symbol, values in suspend_types.items() if len(values) > 1)
        if conflicting:
            raise _outcome_error(
                "prospective outcome contains conflicting suspend identities",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                context={"symbols": conflicting},
            )
        suspended = frozenset(symbol for symbol, values in suspend_types.items() if values == {"S"})
        both = sorted(set(raw_open_by_symbol) & suspended)
        if both:
            raise _outcome_error(
                "prospective outcome is both traded and authoritatively suspended",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                context={"symbols": both},
            )
        missing = sorted(set(normalized) - set(raw_open_by_symbol) - set(suspended))
        if missing:
            raise _outcome_error(
                "prospective outcome is missing after refresh completion",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_UNEXPLAINED_MISSING",
                context={"symbols": missing},
            )
        return AdvisoryPriceOutcomeSnapshot(
            refresh_audits=audits,
            raw_open_by_symbol=raw_open_by_symbol,
            suspended_symbols=suspended,
        )

    @staticmethod
    def _read_audits(
        cursor: Any,
        *,
        target_trade_date: date,
    ) -> tuple[AdvisoryPriceOutcomeRefreshAuditV1, AdvisoryPriceOutcomeRefreshAuditV1]:
        cursor.execute(
            """
            SELECT DISTINCT ON (dataset)
                   dataset, trade_date, data_source, status, quality_status,
                   row_count, refreshed_at
            FROM market.dataset_date_refresh_audit
            WHERE dataset = ANY(%s) AND trade_date = %s
            ORDER BY dataset, refreshed_at DESC
            """,
            (["kline_daily_raw", "suspend_d"], target_trade_date),
        )
        rows = cursor.fetchall()
        if {str(row[0]) for row in rows} != {"kline_daily_raw", "suspend_d"}:
            raise _outcome_error(
                "prospective target refresh audits are not complete",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE",
            )
        audits: list[AdvisoryPriceOutcomeRefreshAuditV1] = []
        for row in rows:
            if str(row[3]).lower() != "success" or str(row[4]).lower() in {
                "error",
                "empty_invalid",
                "low_coverage",
            }:
                raise _outcome_error(
                    "prospective target refresh audit is not usable",
                    "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                    context={
                        "dataset": str(row[0]),
                        "status": str(row[3]),
                        "quality_status": str(row[4]),
                    },
                )
            audits.append(
                AdvisoryPriceOutcomeRefreshAuditV1(
                    dataset=str(row[0]),
                    trade_date=row[1],
                    data_source=str(row[2]),
                    status="success",
                    quality_status=str(row[4]),
                    row_count=int(row[5] or 0),
                    refreshed_at=row[6],
                )
            )
        ordered = tuple(sorted(audits, key=lambda item: item.dataset))
        return ordered  # type: ignore[return-value]


class AdvisoryPriceProspectiveEvaluationService:
    def __init__(
        self,
        *,
        outcome_source: Any | None = None,
        artifact_reader: Callable[[str | Path], AdvisoryPriceProspectivePredictionArtifact] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._outcome_source = outcome_source or PostgresAdvisoryPriceOutcomeSource()
        self._artifact_reader = artifact_reader or read_prospective_prediction_artifact
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def settle(
        self,
        *,
        prediction_artifact_path: str | Path,
        model_root: str | Path,
    ) -> AdvisoryPriceProspectiveSettlementReceiptV1:
        artifact = self._artifact_reader(prediction_artifact_path)
        request = artifact.request
        target = Path(model_root).resolve() / SETTLEMENT_ROOT_NAME / request.request_id
        if target.exists():
            existing = read_settlement_artifact(target, status="ALREADY_MATERIALIZED")
            actual_identity = {
                "request_id": existing.settlement.request_id,
                "request_sha256": existing.settlement.request_sha256,
                "prediction_bundle_id": existing.settlement.prediction_bundle_id,
                "prediction_sha256": existing.settlement.prediction_sha256,
                "price_range_bundle_id": existing.settlement.price_range_bundle_id,
                "package_id": existing.settlement.package_id,
                "review_policy_sha256": existing.settlement.review_policy_sha256,
                "target_trade_date": existing.settlement.target_trade_date,
            }
            expected_identity = {
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "prediction_bundle_id": artifact.receipt.prediction_bundle_id,
                "prediction_sha256": artifact.receipt.prediction_sha256,
                "price_range_bundle_id": request.price_range_bundle_id,
                "package_id": request.package_id,
                "review_policy_sha256": request.review_policy_sha256,
                "target_trade_date": request.target_trade_date,
            }
            if actual_identity != expected_identity:
                raise _outcome_error(
                    "existing settlement differs from the current prospective prediction",
                    "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT",
                )
            return existing.receipt

        now = _aware_utc(self._now_provider(), field="now")
        not_before = datetime.combine(
            request.target_trade_date,
            time(hour=18),
            tzinfo=SHANGHAI,
        ).astimezone(timezone.utc)
        if now < not_before:
            raise _outcome_error(
                "prospective target outcome is not mature before 18:00 Asia/Shanghai",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE",
                context={"not_before": not_before.isoformat()},
            )
        candidates = _prediction_candidates(artifact)
        symbols = tuple(str(row["symbol"]).strip().upper() for row in candidates)
        snapshot = self._outcome_source.load(
            symbols=symbols,
            target_trade_date=request.target_trade_date,
        )
        observed_symbols = set(snapshot.raw_open_by_symbol) | set(snapshot.suspended_symbols)
        if (
            observed_symbols != set(symbols)
            or set(snapshot.raw_open_by_symbol) & set(snapshot.suspended_symbols)
        ):
            raise _outcome_error(
                "prospective outcome snapshot differs from the frozen candidate identity",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH",
            )
        outcome_rows = tuple(
            _settle_candidate(row, snapshot=snapshot)
            for row in sorted(candidates, key=lambda value: str(value["symbol"]).upper())
        )
        metrics = _settlement_metrics(outcome_rows)
        settlement = build_settlement(
            request_id=request.request_id,
            request_sha256=request.request_sha256,
            prediction_bundle_id=artifact.receipt.prediction_bundle_id,
            prediction_sha256=artifact.receipt.prediction_sha256,
            price_range_bundle_id=request.price_range_bundle_id,
            package_id=request.package_id,
            review_policy_sha256=request.review_policy_sha256,
            decision_as_of_trade_date=request.decision_as_of_trade_date,
            target_trade_date=request.target_trade_date,
            predicted_at=artifact.receipt.published_at,
            settled_at=now,
            refresh_audits=snapshot.refresh_audits,
            candidates=outcome_rows,
            metrics=metrics,
        )
        return _publish_settlement(target, settlement=settlement)


def read_settlement_artifact(
    path: str | Path,
    *,
    status: str = "ALREADY_MATERIALIZED",
) -> AdvisoryPriceProspectiveSettlementArtifact:
    target = Path(path).resolve()
    try:
        settlement = AdvisoryPriceProspectiveSettlementV1.model_validate(_read_json(target / "settlement.json"))
        manifest = _read_json(target / "manifest.json")
        receipt_payload = _read_json(target / "receipt.json")
        receipt = AdvisoryPriceProspectiveSettlementReceiptV1.model_validate(
            {**receipt_payload, "status": status}
        )
    except (OSError, ValueError, ValidationError) as exc:
        raise _outcome_error(
            "prospective settlement artifact cannot be read",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT",
            context={"error_type": type(exc).__name__},
        ) from exc
    expected_file = _file_identity(target / "settlement.json")
    expected_receipt = {
        "request_id": settlement.request_id,
        "settlement_id": settlement.settlement_id,
        "settlement_sha256": settlement.settlement_sha256,
        "manifest_sha256": _sha256_file(target / "manifest.json"),
        "published_at": receipt_payload.get("published_at"),
        "target_trade_date": settlement.target_trade_date.isoformat(),
        "candidate_count": settlement.metrics.candidate_count,
        "market_available_count": settlement.metrics.market_available_count,
        "not_applicable_count": settlement.metrics.not_applicable_count,
        "realized_outcome_accessed": True,
        "database_written": False,
        "binding_activated": False,
        "sealed_holdout_consumed": False,
    }
    actual_receipt = {key: receipt_payload.get(key) for key in expected_receipt}
    if (
        manifest.get("schema_version") != "advisory_price_prospective_settlement_manifest_v1"
        or manifest.get("request_id") != settlement.request_id
        or manifest.get("settlement_id") != settlement.settlement_id
        or manifest.get("settlement_sha256") != settlement.settlement_sha256
        or manifest.get("files") != {"settlement.json": expected_file}
        or receipt_payload.get("status") != "PUBLISHED"
        or receipt.published_at != settlement.settled_at
        or actual_receipt != expected_receipt
    ):
        raise _outcome_error(
            "prospective settlement artifact hashes are inconsistent",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT",
        )
    return AdvisoryPriceProspectiveSettlementArtifact(
        path=target,
        settlement=settlement,
        manifest=manifest,
        receipt=receipt,
    )


def _prediction_candidates(artifact: AdvisoryPriceProspectivePredictionArtifact) -> list[Mapping[str, Any]]:
    envelope = artifact.prediction.get("price_envelope")
    rows = envelope.get("candidates") if isinstance(envelope, Mapping) else None
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise _outcome_error(
            "prospective prediction candidate payload is invalid",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
        )
    if len(rows) != artifact.request.candidate_count:
        raise _outcome_error(
            "prospective prediction candidate count differs from request",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH",
        )
    return rows


def _settle_candidate(
    candidate: Mapping[str, Any],
    *,
    snapshot: AdvisoryPriceOutcomeSnapshot,
) -> AdvisoryPriceProspectiveOutcomeCandidateV1:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    availability_status = candidate.get("availability_status")
    if availability_status not in {"AVAILABLE", "UNAVAILABLE"}:
        raise _outcome_error(
            "prospective prediction has an unknown availability status",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
            context={"symbol": symbol},
        )
    model_available = availability_status == "AVAILABLE"
    if symbol in snapshot.suspended_symbols:
        return AdvisoryPriceProspectiveOutcomeCandidateV1(
            symbol=symbol,
            model_prediction_status="AVAILABLE" if model_available else "UNAVAILABLE",
            model_prediction_reason=(
                None
                if model_available
                else str(candidate.get("reason_code") or "model_prediction_unavailable")
            ),
            market_outcome_status="NOT_APPLICABLE",
            market_outcome_reason="target_authoritatively_suspended",
        )
    actual = snapshot.raw_open_by_symbol[symbol]
    if not model_available:
        return AdvisoryPriceProspectiveOutcomeCandidateV1(
            symbol=symbol,
            model_prediction_status="UNAVAILABLE",
            model_prediction_reason=str(candidate.get("reason_code") or "model_prediction_unavailable"),
            market_outcome_status="AVAILABLE",
            market_outcome_reason="target_open_observed",
            actual_open=actual,
        )
    band = candidate.get("calibrated_entry_price_range")
    if not isinstance(band, Mapping):
        raise _outcome_error(
            "available prospective prediction lacks its calibrated price interval",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
            context={"symbol": symbol},
        )
    try:
        low = float(band["low"])
        mid = float(band["mid"])
        high = float(band["high"])
        reference = float(candidate["decision_reference_price"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise _outcome_error(
            "prospective calibrated price interval is invalid",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
            context={"symbol": symbol},
        ) from exc
    if (
        not all(math.isfinite(value) and value > 0.0 for value in (low, mid, high, reference))
        or not low <= mid <= high
    ):
        raise _outcome_error(
            "prospective calibrated price interval is invalid",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
            context={"symbol": symbol},
        )
    return AdvisoryPriceProspectiveOutcomeCandidateV1(
        symbol=symbol,
        model_prediction_status="AVAILABLE",
        market_outcome_status="AVAILABLE",
        market_outcome_reason="target_open_observed",
        actual_open=actual,
        decision_reference_price=reference,
        calibrated_low=low,
        calibrated_mid=mid,
        calibrated_high=high,
        covered=low <= actual <= high,
        lower_miss=actual < low,
        upper_miss=actual > high,
        interval_width_bps=(high - low) / reference * 10_000.0,
        absolute_mid_error_bps=abs(actual - mid) / reference * 10_000.0,
    )


def _settlement_metrics(
    candidates: Sequence[AdvisoryPriceProspectiveOutcomeCandidateV1],
) -> AdvisoryPriceProspectiveSettlementMetricsV1:
    supported = [
        row
        for row in candidates
        if row.market_outcome_status == "AVAILABLE" and row.model_prediction_status == "AVAILABLE"
    ]
    base = {
        "candidate_count": len(candidates),
        "market_available_count": sum(row.market_outcome_status == "AVAILABLE" for row in candidates),
        "not_applicable_count": sum(row.market_outcome_status == "NOT_APPLICABLE" for row in candidates),
        "model_available_market_available_count": len(supported),
        "model_unavailable_count": sum(row.model_prediction_status == "UNAVAILABLE" for row in candidates),
        "crossing_count": 0,
    }
    if not supported:
        return AdvisoryPriceProspectiveSettlementMetricsV1(**base)
    covered = [float(bool(row.covered)) for row in supported]
    lower = [float(bool(row.lower_miss)) for row in supported]
    upper = [float(bool(row.upper_miss)) for row in supported]
    widths = [float(row.interval_width_bps) for row in supported if row.interval_width_bps is not None]
    errors = [float(row.absolute_mid_error_bps) for row in supported if row.absolute_mid_error_bps is not None]
    return AdvisoryPriceProspectiveSettlementMetricsV1(
        **base,
        calibrated_coverage=mean(covered),
        lower_miss_rate=mean(lower),
        upper_miss_rate=mean(upper),
        mean_interval_width_bps=mean(widths),
        median_interval_width_bps=median(widths),
        mean_absolute_mid_error_bps=mean(errors),
        median_absolute_mid_error_bps=median(errors),
    )


def _publish_settlement(
    target: Path,
    *,
    settlement: AdvisoryPriceProspectiveSettlementV1,
) -> AdvisoryPriceProspectiveSettlementReceiptV1:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        _write_json(temporary / "settlement.json", settlement.model_dump(mode="json"))
        manifest = {
            "schema_version": "advisory_price_prospective_settlement_manifest_v1",
            "request_id": settlement.request_id,
            "settlement_id": settlement.settlement_id,
            "settlement_sha256": settlement.settlement_sha256,
            "files": {"settlement.json": _file_identity(temporary / "settlement.json")},
        }
        _write_json(temporary / "manifest.json", manifest)
        receipt = build_settlement_receipt(
            status="PUBLISHED",
            request_id=settlement.request_id,
            settlement_id=settlement.settlement_id,
            settlement_sha256=settlement.settlement_sha256,
            manifest_sha256=_sha256_file(temporary / "manifest.json"),
            published_at=settlement.settled_at,
            target_trade_date=settlement.target_trade_date,
            candidate_count=settlement.metrics.candidate_count,
            market_available_count=settlement.metrics.market_available_count,
            not_applicable_count=settlement.metrics.not_applicable_count,
        )
        _write_json(temporary / "receipt.json", receipt.model_dump(mode="json"))
        try:
            os.replace(temporary, target)
        except OSError:
            if not target.exists():
                raise
            return read_settlement_artifact(target, status="ALREADY_MATERIALIZED").receipt
        return read_settlement_artifact(target, status="PUBLISHED").receipt
    finally:
        if temporary.exists():
            for name in ("settlement.json", "manifest.json", "receipt.json"):
                (temporary / name).unlink(missing_ok=True)
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


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise _outcome_error(
            f"{field} must be timezone-aware",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
        )
    return value.astimezone(timezone.utc)


def _outcome_error(
    message: str,
    reason_code: str,
    *,
    context: Mapping[str, Any] | None = None,
) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code, context=dict(context or {}))

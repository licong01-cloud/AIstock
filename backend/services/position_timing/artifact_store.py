"""Filesystem-owned state for position timing advice.

Immutable evidence uses content-addressed, no-replace files.  Mutable user
intent uses atomic replacement.  The implementation does not expose a current
route pointer and never writes another subsystem's registry.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

from .contracts import (
    AlertEmissionAuthorizedEventV1,
    CardIssuedEventV1,
    OutcomeEvaluatedEventV1,
    PositionTimingCardSetV1,
    PositionTimingIntentV1,
    canonical_json_bytes,
    canonical_sha256,
)


DEFAULT_ARTIFACT_ROOT = Path("F:/Dev/AIstock_model_artifacts/position_timing_advice_v1")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


class PositionTimingArtifactError(RuntimeError):
    code = "POSITION_TIMING_ARTIFACT_ERROR"


class CardSetIdentityConflict(PositionTimingArtifactError):
    code = "CARD_SET_IDENTITY_CONFLICT"


class ImmutableArtifactConflict(PositionTimingArtifactError):
    code = "IMMUTABLE_ARTIFACT_CONFLICT"


class PositionTimingArtifactStore:
    def __init__(self, root: str | Path | None = None) -> None:
        configured = root or os.getenv("POSITION_TIMING_ARTIFACT_ROOT") or DEFAULT_ARTIFACT_ROOT
        self.root = Path(configured).resolve()

    def list_intents(self) -> tuple[PositionTimingIntentV1, ...]:
        folder = self.root / "intents"
        if not folder.exists():
            return ()
        intents = [self._read_intent(path) for path in folder.glob("*.json")]
        return tuple(sorted(intents, key=lambda item: item.canonical_symbol))

    def get_intent(self, canonical_symbol: str) -> PositionTimingIntentV1 | None:
        path = self._intent_path(canonical_symbol)
        if not path.exists():
            return None
        return self._read_intent(path)

    def put_intent(self, intent: PositionTimingIntentV1) -> PositionTimingIntentV1:
        path = self._intent_path(intent.canonical_symbol)
        self._atomic_replace(path, canonical_json_bytes(intent) + b"\n")
        return intent

    def publish_policy_snapshot(self, *, name: str, payload: dict[str, Any]) -> tuple[Path, str]:
        safe_name = self._safe_name(name)
        artifact_sha256 = canonical_sha256(payload)
        path = self.root / "policy_snapshots" / f"{safe_name}-{artifact_sha256}.json"
        lock_path = self.root / "locks" / f"policy-{safe_name}-{artifact_sha256}.lock"
        with _exclusive_file_lock(lock_path):
            self._publish_immutable(path, canonical_json_bytes(payload) + b"\n")
        return path, artifact_sha256

    def get_card_set(self, *, decision_trade_date: date) -> PositionTimingCardSetV1 | None:
        date_root = self.root / "cards" / decision_trade_date.isoformat()
        if not date_root.exists():
            return None
        paths = sorted(date_root.glob("*/card_set-*.json"))
        if not paths:
            return None
        if len(paths) != 1:
            raise CardSetIdentityConflict(
                f"multiple card sets exist for LEGACY_PORTFOLIO/{decision_trade_date.isoformat()}"
            )
        try:
            card_set = PositionTimingCardSetV1.model_validate_json(paths[0].read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise PositionTimingArtifactError(f"invalid immutable card-set artifact: {paths[0]}") from exc
        self._validate_card_set_integrity(card_set=card_set, path=paths[0])
        return card_set

    def publish_card_set(self, card_set: PositionTimingCardSetV1) -> tuple[PositionTimingCardSetV1, str, bool]:
        date_root = self.root / "cards" / card_set.decision_trade_date.isoformat()
        lock_path = self.root / "locks" / f"cards-{card_set.decision_trade_date.isoformat()}.lock"
        created = False
        with _exclusive_file_lock(lock_path):
            existing = self.get_card_set(decision_trade_date=card_set.decision_trade_date)
            if existing is not None:
                if existing.semantic_identity_sha256 != card_set.semantic_identity_sha256:
                    raise CardSetIdentityConflict(
                        "a different immutable card-set identity already owns this position source/date"
                    )
                selected = existing
            else:
                artifact_sha256 = canonical_sha256(card_set)
                path = date_root / card_set.card_set_id / f"card_set-{artifact_sha256}.json"
                self._validate_card_set_integrity(card_set=card_set, path=path)
                self._publish_immutable(path, canonical_json_bytes(card_set) + b"\n")
                selected = card_set
                created = True

        artifact_sha256 = canonical_sha256(selected)
        # If publication succeeded but event append was interrupted, a retry
        # reaches this loop again and fills only the missing idempotency keys.
        events: list[tuple[dict[str, Any], str]] = []
        for card in selected.cards:
            card_sha256 = canonical_sha256(card)
            event = CardIssuedEventV1(
                event_id=f"evt_{canonical_sha256({'event_type': 'CARD_ISSUED', 'card_id': card.card_id})[:24]}",
                idempotency_key=card.card_id,
                occurred_at=selected.created_at,
                card_id=card.card_id,
                card_set_id=selected.card_set_id,
                canonical_symbol=card.canonical_symbol,
                decision_trade_date=card.decision_trade_date,
                target_trade_date=card.target_trade_date,
                card_artifact_sha256=card_sha256,
                event_payload={
                    "pre_action_qty": card.pre_action_qty,
                    "planned_full_notional_cny": card.planned_full_notional_cny,
                    "planned_trigger_deltas": [trigger.planned_delta_qty for trigger in card.triggers],
                    "reference_price_raw": card.reference_price_raw,
                    "holding_trading_days": card.holding_trading_days,
                    "holding_age_bucket": card.holding_age_bucket,
                    "primary_source_role": card.primary_source_role,
                    "action": card.action,
                    "action_side": (
                        "BUY"
                        if card.requested_delta_qty > 0
                        else "SELL"
                        if card.requested_delta_qty < 0
                        else "NONE"
                    ),
                    "market_regime": card.market_regime,
                    "st_flag": card.st_flag,
                    "delist_flag": card.delist_flag,
                    "delist_context_status": card.delist_context_status,
                    "sizing_identity_sha256": canonical_sha256(
                        {
                            "planned_full_notional_cny": card.planned_full_notional_cny,
                            "desired_target_exposure": card.desired_target_exposure,
                            "requested_delta_qty": card.requested_delta_qty,
                        }
                    ),
                    "board_lot_identity_sha256": card.board_lot_identity.get("identity_sha256"),
                    "cost_policy_sha256": card.cost_policy_sha256,
                },
            )
            events.append((event.model_dump(mode="python"), event.idempotency_key))
        self.append_events(events)
        return selected, artifact_sha256, created

    def latest_card_set(self) -> PositionTimingCardSetV1 | None:
        root = self.root / "cards"
        if not root.exists():
            return None
        dates: list[date] = []
        for child in root.iterdir():
            if not child.is_dir():
                continue
            try:
                dates.append(date.fromisoformat(child.name))
            except ValueError:
                raise PositionTimingArtifactError(f"invalid decision-date directory: {child}") from None
        if not dates:
            return None
        return self.get_card_set(decision_trade_date=max(dates))

    def append_event(self, payload: dict[str, Any], *, idempotency_key: str) -> bool:
        return self.append_events([(payload, idempotency_key)]) == 1

    def append_events(self, events: list[tuple[dict[str, Any], str]]) -> int:
        if not events:
            return 0
        pending: dict[tuple[str, str], dict[str, Any]] = {}
        for raw_payload, expected_key in events:
            payload = _validated_event_payload(raw_payload)
            idempotency_key = str(payload.get("idempotency_key") or "")
            if idempotency_key != expected_key:
                raise ValueError("event idempotency_key argument does not match payload")
            logical_key = (str(payload["event_type"]), idempotency_key)
            prior = pending.get(logical_key)
            if prior is not None and canonical_sha256(prior) != canonical_sha256(payload):
                raise ImmutableArtifactConflict(f"event batch contains a conflicting key: {logical_key}")
            pending[logical_key] = payload

        lock_path = self.root / "locks" / "events-global.lock"
        appended = 0
        with _exclusive_file_lock(lock_path):
            existing_by_key: dict[tuple[str, str], dict[str, Any]] = {}
            events_root = self.root / "events"
            if events_root.exists():
                for path in sorted(events_root.glob("*.jsonl")):
                    with path.open("r", encoding="utf-8") as handle:
                        for line_number, line in enumerate(handle, start=1):
                            if not line.strip():
                                continue
                            try:
                                existing = _validated_event_payload(json.loads(line))
                            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                                raise PositionTimingArtifactError(
                                    f"invalid event log {path}:{line_number}"
                                ) from exc
                            logical_key = (
                                str(existing["event_type"]),
                                str(existing["idempotency_key"]),
                            )
                            if logical_key in existing_by_key:
                                raise PositionTimingArtifactError(
                                    f"duplicate event idempotency key: {logical_key}"
                                )
                            existing_by_key[logical_key] = existing

            append_by_month: dict[str, list[dict[str, Any]]] = {}
            for logical_key, payload in pending.items():
                existing = existing_by_key.get(logical_key)
                if existing is not None:
                    if canonical_sha256(existing) != canonical_sha256(payload):
                        raise ImmutableArtifactConflict(
                            f"event idempotency key has conflicting payload: {logical_key}"
                        )
                    continue
                occurred_at = payload.get("occurred_at")
                if not isinstance(occurred_at, datetime):
                    raise ValueError("validated event occurred_at must be a datetime")
                append_by_month.setdefault(occurred_at.strftime("%Y-%m"), []).append(payload)

            for month, payloads in append_by_month.items():
                path = events_root / f"{month}.jsonl"
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("ab") as handle:
                    for payload in payloads:
                        handle.write(canonical_json_bytes(payload) + b"\n")
                        appended += 1
                    handle.flush()
                    os.fsync(handle.fileno())
        return appended

    def event_counts(self) -> dict[str, int]:
        if not (self.root / "events").exists():
            return {}
        lock_path = self.root / "locks" / "events-global.lock"
        if not lock_path.exists():
            raise PositionTimingArtifactError("event log exists without its global lock identity")
        with _exclusive_file_lock(lock_path):
            return self._event_counts_unlocked()

    def _event_counts_unlocked(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        root = self.root / "events"
        if not root.exists():
            return counts
        seen_keys: set[tuple[str, str]] = set()
        for path in sorted(root.glob("*.jsonl")):
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise PositionTimingArtifactError(f"malformed event log {path}:{line_number}") from exc
                    try:
                        payload = _validated_event_payload(payload)
                    except (TypeError, ValueError) as exc:
                        raise PositionTimingArtifactError(f"invalid event contract {path}:{line_number}") from exc
                    event_type = str(payload.get("event_type") or "UNKNOWN")
                    key = (event_type, str(payload.get("idempotency_key") or ""))
                    if key in seen_keys:
                        raise PositionTimingArtifactError(f"duplicate event idempotency key: {key}")
                    seen_keys.add(key)
                    counts[event_type] = counts.get(event_type, 0) + 1
        return counts

    def _intent_path(self, canonical_symbol: str) -> Path:
        return self.root / "intents" / f"{self._safe_name(canonical_symbol)}.json"

    @staticmethod
    def _read_intent(path: Path) -> PositionTimingIntentV1:
        try:
            intent = PositionTimingIntentV1.model_validate_json(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise PositionTimingArtifactError(f"invalid timing intent artifact: {path}") from exc
        expected_hash = canonical_sha256(
            {
                "canonical_symbol": intent.canonical_symbol,
                "planned_full_notional_cny": intent.planned_full_notional_cny,
                "desired_target_exposure": intent.desired_target_exposure,
                "updated_at": intent.updated_at,
            }
        )
        if path.stem != intent.canonical_symbol or intent.intent_sha256 != expected_hash:
            raise PositionTimingArtifactError(f"timing intent semantic identity mismatch: {path}")
        return intent

    @staticmethod
    def _validate_card_set_integrity(*, card_set: PositionTimingCardSetV1, path: Path) -> None:
        if canonical_sha256(card_set.input_identity) != card_set.input_identity_sha256:
            raise PositionTimingArtifactError(f"card-set input identity mismatch: {path}")
        if canonical_sha256(card_set.policy_identity) != card_set.policy_identity_sha256:
            raise PositionTimingArtifactError(f"card-set policy identity mismatch: {path}")
        if canonical_sha256(card_set.cards) != card_set.cards_sha256:
            raise PositionTimingArtifactError(f"card-set cards identity mismatch: {path}")
        expected_semantic = canonical_sha256(
            {
                "position_source": card_set.position_source,
                "decision_trade_date": card_set.decision_trade_date,
                "target_trade_date": card_set.target_trade_date,
                "input_identity_sha256": card_set.input_identity_sha256,
                "policy_identity_sha256": card_set.policy_identity_sha256,
            }
        )
        expected_set_id = f"ptset_{expected_semantic[:24]}"
        if card_set.semantic_identity_sha256 != expected_semantic or card_set.card_set_id != expected_set_id:
            raise PositionTimingArtifactError(f"card-set semantic identity mismatch: {path}")
        if path.parent.name != card_set.card_set_id or path.parent.parent.name != card_set.decision_trade_date.isoformat():
            raise PositionTimingArtifactError(f"card-set path identity mismatch: {path}")
        expected_filename = f"card_set-{canonical_sha256(card_set)}.json"
        if path.name != expected_filename:
            raise PositionTimingArtifactError(f"card-set content-addressed filename mismatch: {path}")
        seen_symbols: set[str] = set()
        for card in card_set.cards:
            if card.canonical_symbol in seen_symbols:
                raise PositionTimingArtifactError(f"duplicate symbol in immutable card set: {card.canonical_symbol}")
            seen_symbols.add(card.canonical_symbol)
            expected_card_id = f"ptcard_{canonical_sha256({'card_set_id': card_set.card_set_id, 'symbol': card.canonical_symbol})[:24]}"
            if (
                card.card_id != expected_card_id
                or card.card_set_id != card_set.card_set_id
                or card.decision_trade_date != card_set.decision_trade_date
                or card.target_trade_date != card_set.target_trade_date
            ):
                raise PositionTimingArtifactError(f"card identity mismatch: {card.card_id}")

    @staticmethod
    def _safe_name(value: str) -> str:
        normalized = str(value or "").strip()
        if not _SAFE_NAME.fullmatch(normalized):
            raise ValueError(f"unsafe artifact name: {value!r}")
        return normalized

    @staticmethod
    def _publish_immutable(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temp_path = Path(temp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temp_path, path)
            except FileExistsError:
                if path.read_bytes() != content:
                    raise ImmutableArtifactConflict(
                        f"immutable artifact conflicts with existing content: {path}"
                    ) from None
        finally:
            if temp_path.exists():
                temp_path.unlink()

    @staticmethod
    def _atomic_replace(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temp_path = Path(temp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
        finally:
            if temp_path.exists():
                temp_path.unlink()


@contextmanager
def _exclusive_file_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _validated_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    event_type = str(payload.get("event_type") or "")
    model_by_type = {
        "CARD_ISSUED": CardIssuedEventV1,
        "ALERT_EMISSION_AUTHORIZED": AlertEmissionAuthorizedEventV1,
        "OUTCOME_EVALUATED": OutcomeEvaluatedEventV1,
    }
    model = model_by_type.get(event_type)
    if model is None:
        raise ValueError(f"unsupported position timing event_type: {event_type!r}")
    return model.model_validate(payload).model_dump(mode="python")


__all__ = [
    "CardSetIdentityConflict",
    "DEFAULT_ARTIFACT_ROOT",
    "ImmutableArtifactConflict",
    "PositionTimingArtifactError",
    "PositionTimingArtifactStore",
]

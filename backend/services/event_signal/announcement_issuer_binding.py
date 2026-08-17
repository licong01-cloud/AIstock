"""Deterministic issuer binding for provider announcement observations.

``market.anns`` is a raw provider-observation table.  One issuer document may
appear under index symbols or unrelated lookup symbols, so its ``ts_code`` is
not sufficient authority for an actionable security signal.  This module
keeps the raw row intact and records an explicit binding decision.  Anything
other than one exact issuer match is auditable but suppressed.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


ISSUER_BINDING_SCHEMA_VERSION = "announcement_issuer_binding_v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")

# The provider can return the same issuer document under several lookup
# symbols.  A candidate is authoritative only when the provider issuer name
# exactly matches the canonical stock name.  Ambiguity is preserved rather
# than guessed.  Historical aliases require a separate authoritative dataset.
ISSUER_BINDING_PROJECTION_SQL = """
            a.name AS announcement_issuer_name,
            COALESCE(
                issuer_binding.exact_name_candidate_ts_codes,
                ARRAY[]::text[]
            ) AS issuer_candidate_ts_codes,
"""

ISSUER_BINDING_LATERAL_SQL = """
          LEFT JOIN LATERAL (
                SELECT array_agg(DISTINCT peer.ts_code ORDER BY peer.ts_code)
                           AS exact_name_candidate_ts_codes
                  FROM market.anns peer
                  JOIN market.stock_basic stock
                    ON stock.ts_code = peer.ts_code
                   AND stock.name = peer.name
                   AND (stock.list_date IS NULL OR stock.list_date <= peer.ann_date)
                   AND (stock.delist_date IS NULL OR stock.delist_date >= peer.ann_date)
                 WHERE peer.ann_date = a.ann_date
                   AND peer.name = a.name
                   AND peer.title = a.title
          ) issuer_binding ON TRUE
"""


class IssuerBindingStatus(str, Enum):
    EXACT = "EXACT"
    DUPLICATE_PROVIDER_ALIAS = "DUPLICATE_PROVIDER_ALIAS"
    AMBIGUOUS = "AMBIGUOUS"
    UNRESOLVED = "UNRESOLVED"
    PIT_TIME_INCONSISTENT = "PIT_TIME_INCONSISTENT"
    TERMINAL_EVIDENCE_UNCONFIRMED = "TERMINAL_EVIDENCE_UNCONFIRMED"


@dataclass(frozen=True)
class IssuerBindingDecision:
    status: IssuerBindingStatus
    reason_code: str
    source_ts_code: str
    resolved_ts_code: str | None
    candidate_ts_codes: tuple[str, ...]
    known_date: dt.date | None
    effective_trade_date: dt.date | None
    actionable: bool
    fact_status: str
    signal_status: str
    digest: str

    def evidence(self) -> dict[str, Any]:
        return {
            "schema_version": ISSUER_BINDING_SCHEMA_VERSION,
            "status": self.status.value,
            "reason_code": self.reason_code,
            "source_ts_code": self.source_ts_code,
            "resolved_ts_code": self.resolved_ts_code,
            "candidate_ts_codes": list(self.candidate_ts_codes),
            "known_date": self.known_date.isoformat() if self.known_date else None,
            "effective_trade_date": (
                self.effective_trade_date.isoformat() if self.effective_trade_date else None
            ),
            "actionable": self.actionable,
            "fact_status": self.fact_status,
            "signal_status": self.signal_status,
            "binding_digest": self.digest,
        }


def _date(value: Any, *, shanghai: bool = False) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=SHANGHAI)
        return value.astimezone(SHANGHAI).date() if shanghai else value.date()
    if isinstance(value, dt.date):
        return value
    parsed = dt.datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI).date() if shanghai else parsed.date()


def _candidate_codes(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values: Iterable[Any] = (value,)
    else:
        values = value
    return tuple(sorted({str(item).strip() for item in values if str(item).strip()}))


def _digest_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def resolve_announcement_issuer_binding(
    row: Mapping[str, Any],
    *,
    require_terminal_cross_check: bool = False,
) -> IssuerBindingDecision:
    source_ts_code = str(row.get("ts_code") or "").strip()
    candidates = _candidate_codes(row.get("issuer_candidate_ts_codes"))
    resolved = candidates[0] if len(candidates) == 1 else None
    available_date = _date(row.get("available_at"), shanghai=True)
    known_date = available_date or _date(row.get("ann_date"))
    effective_date = _date(row.get("effective_trade_date"))

    status: IssuerBindingStatus
    reason_code: str
    if not candidates:
        status = IssuerBindingStatus.UNRESOLVED
        reason_code = "announcement_issuer_binding_unresolved"
    elif len(candidates) > 1:
        status = IssuerBindingStatus.AMBIGUOUS
        reason_code = "announcement_issuer_binding_ambiguous"
    elif resolved != source_ts_code:
        status = IssuerBindingStatus.DUPLICATE_PROVIDER_ALIAS
        reason_code = "announcement_issuer_binding_provider_alias"
    elif known_date is None or effective_date is None or effective_date < known_date:
        status = IssuerBindingStatus.PIT_TIME_INCONSISTENT
        reason_code = "announcement_issuer_binding_pit_time_inconsistent"
    else:
        cross_check = dict(row.get("ann_signal_evidence") or {}).get("st_cross_check") or {}
        if (
            require_terminal_cross_check
            and row.get("event_type") == "stock_delisting_confirmed"
            and not (bool(cross_check.get("matched")) and bool(cross_check.get("terminal")))
        ):
            status = IssuerBindingStatus.TERMINAL_EVIDENCE_UNCONFIRMED
            reason_code = "announcement_terminal_evidence_cross_check_missing"
        else:
            status = IssuerBindingStatus.EXACT
            reason_code = "announcement_issuer_binding_exact"

    actionable = status is IssuerBindingStatus.EXACT
    fact_status = (
        "ACTIVE"
        if actionable
        else "SUPERSEDED"
        if status is IssuerBindingStatus.DUPLICATE_PROVIDER_ALIAS
        else "UNKNOWN"
    )
    original_signal_status = str(row.get("ann_signal_status") or "ACTIVE")
    signal_status = original_signal_status if actionable else "SUPPRESSED"
    payload = {
        "schema_version": ISSUER_BINDING_SCHEMA_VERSION,
        "status": status.value,
        "reason_code": reason_code,
        "source_ts_code": source_ts_code,
        "resolved_ts_code": resolved,
        "candidate_ts_codes": list(candidates),
        "known_date": known_date,
        "effective_trade_date": effective_date,
        "actionable": actionable,
        "fact_status": fact_status,
        "signal_status": signal_status,
    }
    return IssuerBindingDecision(
        status=status,
        reason_code=reason_code,
        source_ts_code=source_ts_code,
        resolved_ts_code=resolved,
        candidate_ts_codes=candidates,
        known_date=known_date,
        effective_trade_date=effective_date,
        actionable=actionable,
        fact_status=fact_status,
        signal_status=signal_status,
        digest=_digest_payload(payload),
    )


def attach_announcement_issuer_bindings(
    rows: Iterable[Mapping[str, Any]],
    *,
    require_terminal_cross_check: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    enriched: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in rows:
        copied = dict(row)
        decision = resolve_announcement_issuer_binding(
            copied,
            require_terminal_cross_check=require_terminal_cross_check,
        )
        evidence = copied.get("ann_signal_evidence") or {}
        if not isinstance(evidence, dict):
            evidence = {"source_signal_evidence": evidence}
        evidence = dict(evidence)
        evidence["issuer_binding"] = decision.evidence()
        detail = copied.get("classification_detail") or {}
        if not isinstance(detail, dict):
            detail = {"source_classification_detail": detail}
        detail = dict(detail)
        detail["issuer_binding"] = decision.evidence()
        copied["ann_signal_evidence"] = evidence
        copied["classification_detail"] = detail
        copied["issuer_fact_status"] = decision.fact_status
        copied["ann_signal_status"] = decision.signal_status
        copied["issuer_binding_decision"] = decision.evidence()
        counts[decision.status.value] += 1
        enriched.append(copied)
    return enriched, dict(sorted(counts.items()))

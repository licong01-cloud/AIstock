"""Fail-closed, file-only rights-issue authority for PT-NEXT-018.

The reader is intentionally isolated from databases, network clients, dataset
activation and runtime advice.  It verifies the immutable candidate manifest,
the content-addressed authority and every referenced official source document.
Account participation is a separately frozen counterfactual; a factor change
never creates shares or proves that a holder subscribed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from .action_value import ActionValueError
from .action_value_data import file_reference
from .artifact_store import PositionTimingArtifactStore
from .contracts import canonical_json_bytes, canonical_sha256


CANDIDATE_MANIFEST_SCHEMA = "qe_dataset_manifest_v1"
AUTHORITY_SCHEMA = "position_timing_rights_issue_authority_v1"
EVENT_SCHEMA = "position_timing_rights_issue_event_v1"
AUTHORITY_RELATIVE_PATH = (
    Path("components")
    / "position_timing_source_authority_v1"
    / "rights_issue_authority.json"
)
_SYMBOL = re.compile(r"^\d{6}\.(SH|SZ)$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


RIGHTS_ISSUE_PARTICIPATION_POLICY: Mapping[str, Any] = {
    "schema_version": "position_timing_pattern_rights_issue_participation_policy_never_subscribe_v1",
    "applies_to": "ALL_R0_TO_R7_CANDIDATES_COMPARATORS_AND_COST_SCENARIOS",
    "participation_decision": "NEVER_SUBSCRIBE",
    "subscription_cash": "ZERO_NO_ACCOUNT_OR_EXTERNAL_CASH",
    "cash_insufficient": "NOT_APPLICABLE_NO_SUBSCRIPTION",
    "partial_subscription": "NOT_APPLICABLE_NO_SUBSCRIPTION",
    "quantity_rounding": "NO_ENTITLEMENT_MATERIALIZATION",
    "share_credit_date": "NO_ACCOUNT_SHARES_CREDITED_AUTHORITY_LISTING_DATE_RETAINED",
    "sellable_date": "NO_ACCOUNT_SELLABLE_INCREMENT_AUTHORITY_LISTING_DATE_RETAINED",
    "wealth_semantics": "RAW_PRICE_ACCOUNT_WEALTH_BEARING_NON_PARTICIPATION_DILUTION",
    "feature_price_semantics": "CANDIDATE_FACTOR_ONLY_FOR_MECHANICAL_EX_RIGHT_CONTINUITY",
    "factor_account_inference": "FORBIDDEN",
    "external_cash_injection": "FORBIDDEN",
    "selection_basis": "PREREGISTERED_BEFORE_FORMAL_OUTCOME_READ_NOT_RETURN_SELECTED",
}
RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256 = canonical_sha256(
    RIGHTS_ISSUE_PARTICIPATION_POLICY
)


def _validated_sha256(value: Any, *, code: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256.fullmatch(normalized):
        raise ActionValueError(code)
    return normalized


def _parsed_date(value: Any, *, code: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc


def _parsed_datetime(value: Any, *, code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ActionValueError(code)
    return parsed


def _positive_decimal(value: Any, *, code: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ActionValueError(code)
    return parsed


@dataclass(frozen=True)
class RightsIssueEvent:
    symbol: str
    event_id: str
    event_key: tuple[str, str, str]
    announcement_date: date
    disclosure_available_at: datetime
    record_date: date
    payment_start_date: date
    payment_end_date: date
    ex_right_date: date
    resume_date: date
    listing_date: date
    entitlement_ratio: Decimal
    subscription_price: Decimal
    actual_offered_quantity: int
    actual_subscribed_quantity: int
    rounding_rule: str
    issue_success_status: str
    source_record_key: str
    source_url: str
    source_content_sha256: str
    source_document_type: str
    source_file_size: int
    event_sha256: str


@dataclass(frozen=True)
class RightsIssueAuthority:
    candidate_root: Path
    candidate_manifest_reference: Mapping[str, Any]
    candidate_dataset_manifest_sha256: str
    candidate_revision: str
    authority_path: Path
    authority_reference: Mapping[str, Any]
    authority_canonical_sha256: str
    source_documents_sha256: str
    events: tuple[RightsIssueEvent, ...]
    _by_symbol: Mapping[str, tuple[RightsIssueEvent, ...]] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        keys = [event.event_key for event in self.events]
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_EVENT_ORDER_INVALID")
        grouped: dict[str, list[RightsIssueEvent]] = {}
        for event in self.events:
            grouped.setdefault(event.symbol, []).append(event)
        object.__setattr__(
            self,
            "_by_symbol",
            {symbol: tuple(values) for symbol, values in grouped.items()},
        )

    def between(
        self, symbol: str, start_exclusive: date, end_inclusive: date
    ) -> tuple[RightsIssueEvent, ...]:
        return tuple(
            event
            for event in self._by_symbol.get(str(symbol).upper(), ())
            if start_exclusive < event.ex_right_date <= end_inclusive
        )


def freeze_rights_issue_participation_policy(*, timing_root: Path) -> Path:
    """Publish the preregistered no-subscription policy before outcome reads."""

    root = timing_root.resolve()
    path = (
        root
        / "research"
        / "pattern_strategy_v1"
        / "rights_issue_policies"
        / f"{RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256}.json"
    ).resolve()
    if not path.is_relative_to(root):
        raise ActionValueError("RIGHTS_ISSUE_POLICY_PATH_OUTSIDE_OWNER")
    PositionTimingArtifactStore._publish_immutable(
        path, canonical_json_bytes(RIGHTS_ISSUE_PARTICIPATION_POLICY)
    )
    if file_reference(path)["sha256"] != RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256:
        raise ActionValueError("RIGHTS_ISSUE_POLICY_IDENTITY_MISMATCH")
    return path


def open_rights_issue_authority(
    *,
    candidate_root: Path,
    expected_candidate_manifest_sha256: str,
    expected_authority_canonical_sha256: str,
) -> RightsIssueAuthority:
    """Open only the authority pinned by one explicit immutable candidate."""

    root = candidate_root.resolve()
    expected_manifest = _validated_sha256(
        expected_candidate_manifest_sha256,
        code="PATTERN_CANDIDATE_MANIFEST_EXPECTED_IDENTITY_INVALID",
    )
    expected_authority = _validated_sha256(
        expected_authority_canonical_sha256,
        code="RIGHTS_ISSUE_AUTHORITY_EXPECTED_IDENTITY_INVALID",
    )
    manifest_path = (root / "qe_dataset_manifest.json").resolve()
    if not manifest_path.is_relative_to(root):
        raise ActionValueError("PATTERN_CANDIDATE_MANIFEST_PATH_INVALID")
    manifest_reference = file_reference(manifest_path)
    if manifest_reference["sha256"] != expected_manifest:
        raise ActionValueError(
            "PATTERN_CANDIDATE_MANIFEST_IDENTITY_MISMATCH",
            expected=expected_manifest,
            actual=manifest_reference["sha256"],
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_CANDIDATE_MANIFEST_UNAVAILABLE") from exc
    manifest_identity = {
        key: value for key, value in manifest.items() if key != "dataset_manifest_sha256"
    }
    dataset_manifest_sha256 = _validated_sha256(
        manifest.get("dataset_manifest_sha256"),
        code="PATTERN_CANDIDATE_DATASET_IDENTITY_INVALID",
    )
    rights_component = (manifest.get("components") or {}).get("rights_issue_authority")
    if (
        manifest.get("schema_version") != CANDIDATE_MANIFEST_SCHEMA
        or manifest.get("availability_status") != "CANDIDATE_READY"
        or dataset_manifest_sha256 != canonical_sha256(manifest_identity)
        or not isinstance(manifest.get("revision"), str)
        or not manifest.get("revision")
        or not isinstance(rights_component, Mapping)
    ):
        raise ActionValueError("PATTERN_CANDIDATE_MANIFEST_CONTRACT_MISMATCH")

    relative_text = str(rights_component.get("path") or "")
    relative = Path(relative_text)
    if (
        relative.is_absolute()
        or relative.as_posix() != AUTHORITY_RELATIVE_PATH.as_posix()
    ):
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_MANIFEST_PATH_INVALID")
    authority_path = (root / relative).resolve()
    if not authority_path.is_relative_to(root):
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_PATH_OUTSIDE_CANDIDATE")
    authority_reference = file_reference(authority_path)
    if (
        authority_reference["sha256"]
        != _validated_sha256(
            rights_component.get("sha256"),
            code="RIGHTS_ISSUE_AUTHORITY_MANIFEST_IDENTITY_INVALID",
        )
        or authority_reference["size_bytes"] != int(rights_component.get("size", -1))
    ):
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_MANIFEST_IDENTITY_MISMATCH")
    try:
        payload = json.loads(authority_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_UNAVAILABLE") from exc
    canonical_identity = {
        key: value for key, value in payload.items() if key != "canonical_sha256"
    }
    declared_authority = _validated_sha256(
        payload.get("canonical_sha256"),
        code="RIGHTS_ISSUE_AUTHORITY_CANONICAL_IDENTITY_INVALID",
    )
    raw_events = payload.get("events")
    if (
        payload.get("schema_version") != AUTHORITY_SCHEMA
        or declared_authority != canonical_sha256(canonical_identity)
        or declared_authority != expected_authority
        or not isinstance(raw_events, list)
        or payload.get("event_count") != len(raw_events)
        or payload.get("outcomes_read") is not False
        or payload.get("production_database_written") is not False
        or payload.get("runtime_action_performed") is not False
        or payload.get("ddl_performed") is not False
    ):
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_CONTRACT_MISMATCH")

    events: list[RightsIssueEvent] = []
    document_refs: list[dict[str, Any]] = []
    for item in raw_events:
        event, references = _parse_event(item, authority_path=authority_path)
        events.append(event)
        document_refs.extend(references)
    ordered_events = tuple(sorted(events, key=lambda event: event.event_key))
    if tuple(events) != ordered_events:
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_EVENT_ORDER_INVALID")
    if file_reference(authority_path) != authority_reference:
        raise ActionValueError("RIGHTS_ISSUE_AUTHORITY_CHANGED_WHILE_READING")
    return RightsIssueAuthority(
        candidate_root=root,
        candidate_manifest_reference=manifest_reference,
        candidate_dataset_manifest_sha256=dataset_manifest_sha256,
        candidate_revision=str(manifest["revision"]),
        authority_path=authority_path,
        authority_reference=authority_reference,
        authority_canonical_sha256=declared_authority,
        source_documents_sha256=canonical_sha256(document_refs),
        events=ordered_events,
    )


def _parse_event(
    item: Any, *, authority_path: Path
) -> tuple[RightsIssueEvent, list[dict[str, Any]]]:
    if not isinstance(item, Mapping):
        raise ActionValueError("RIGHTS_ISSUE_EVENT_SCHEMA_INVALID")
    symbol = str(item.get("symbol") or "").upper()
    record_date = _parsed_date(item.get("record_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID")
    event_key = (symbol, "RIGHTS_ISSUE", record_date.isoformat())
    raw_key = item.get("event_key")
    announcement_date = _parsed_date(
        item.get("announcement_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID"
    )
    payment_start = _parsed_date(
        item.get("payment_start_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID"
    )
    payment_end = _parsed_date(
        item.get("payment_end_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID"
    )
    ex_right = _parsed_date(
        item.get("ex_right_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID"
    )
    resume = _parsed_date(item.get("resume_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID")
    listing = _parsed_date(item.get("listing_date"), code="RIGHTS_ISSUE_EVENT_DATE_INVALID")
    disclosed = _parsed_datetime(
        item.get("disclosure_available_at"), code="RIGHTS_ISSUE_EVENT_PIT_INVALID"
    )
    try:
        offered = int(item.get("actual_offered_quantity"))
        subscribed = int(item.get("actual_subscribed_quantity"))
        source_size = int(item.get("source_file_size"))
    except (TypeError, ValueError) as exc:
        raise ActionValueError("RIGHTS_ISSUE_EVENT_QUANTITY_INVALID") from exc
    source_hash = _validated_sha256(
        item.get("source_content_sha256"), code="RIGHTS_ISSUE_EVENT_SOURCE_IDENTITY_INVALID"
    )
    versions = item.get("versions")
    if (
        item.get("schema_version") != EVENT_SCHEMA
        or item.get("event_type") != "RIGHTS_ISSUE"
        or not _SYMBOL.fullmatch(symbol)
        or item.get("event_id") != f"RIGHTS_ISSUE:{symbol}:{record_date.isoformat()}"
        or tuple(str(value) for value in raw_key or ()) != event_key
        or not announcement_date <= record_date < payment_start <= payment_end < ex_right
        or resume != ex_right
        or listing < ex_right
        or disclosed.date() > record_date
        or item.get("issue_success_status") != "SUCCESS"
        or offered <= 0
        or not 0 < subscribed <= offered
        or source_size <= 0
        or not str(item.get("rounding_rule") or "").strip()
        or not str(item.get("source_time_quality") or "").strip()
        or str(item.get("source_time_quality")).upper() == "BACKFILL_UNKNOWN"
        or not str(item.get("source_record_key") or "").strip()
        or not str(item.get("source_url") or "").startswith("https://")
        or not str(item.get("source_document_type") or "").strip()
        or not isinstance(versions, list)
        or not versions
    ):
        raise ActionValueError("RIGHTS_ISSUE_EVENT_CONTRACT_MISMATCH", symbol=symbol)

    references: list[dict[str, Any]] = []
    primary_matches = 0
    seen_paths: set[str] = set()
    seen_keys: set[str] = set()
    for version in versions:
        if not isinstance(version, Mapping):
            raise ActionValueError("RIGHTS_ISSUE_SOURCE_VERSION_INVALID", symbol=symbol)
        relative_text = str(version.get("relative_path") or "")
        relative = Path(relative_text)
        record_key = str(version.get("source_record_key") or "")
        if (
            relative.is_absolute()
            or not relative_text
            or relative_text in seen_paths
            or not record_key
            or record_key in seen_keys
            or not str(version.get("source_url") or "").startswith("https://")
            or not str(version.get("source_document_type") or "").strip()
        ):
            raise ActionValueError("RIGHTS_ISSUE_SOURCE_VERSION_INVALID", symbol=symbol)
        source_path = (authority_path.parent / relative).resolve()
        if not source_path.is_relative_to(authority_path.parent.resolve()):
            raise ActionValueError("RIGHTS_ISSUE_SOURCE_PATH_OUTSIDE_AUTHORITY", symbol=symbol)
        reference = file_reference(source_path)
        expected_hash = _validated_sha256(
            version.get("source_content_sha256"),
            code="RIGHTS_ISSUE_SOURCE_VERSION_IDENTITY_INVALID",
        )
        try:
            expected_size = int(version.get("source_file_size"))
        except (TypeError, ValueError) as exc:
            raise ActionValueError("RIGHTS_ISSUE_SOURCE_VERSION_IDENTITY_INVALID") from exc
        if reference["sha256"] != expected_hash or reference["size_bytes"] != expected_size:
            raise ActionValueError(
                "RIGHTS_ISSUE_SOURCE_VERSION_IDENTITY_MISMATCH",
                symbol=symbol,
                source_record_key=record_key,
            )
        references.append(
            {
                "symbol": symbol,
                "source_record_key": record_key,
                "relative_path": relative.as_posix(),
                "sha256": reference["sha256"],
                "size_bytes": reference["size_bytes"],
            }
        )
        if (
            record_key == item.get("source_record_key")
            and expected_hash == source_hash
            and expected_size == source_size
            and version.get("source_url") == item.get("source_url")
            and version.get("source_document_type") == item.get("source_document_type")
        ):
            primary_matches += 1
        seen_paths.add(relative_text)
        seen_keys.add(record_key)
    if primary_matches != 1:
        raise ActionValueError("RIGHTS_ISSUE_PRIMARY_SOURCE_UNBOUND", symbol=symbol)

    event = RightsIssueEvent(
        symbol=symbol,
        event_id=str(item["event_id"]),
        event_key=event_key,
        announcement_date=announcement_date,
        disclosure_available_at=disclosed,
        record_date=record_date,
        payment_start_date=payment_start,
        payment_end_date=payment_end,
        ex_right_date=ex_right,
        resume_date=resume,
        listing_date=listing,
        entitlement_ratio=_positive_decimal(
            item.get("entitlement_ratio"), code="RIGHTS_ISSUE_EVENT_RATIO_INVALID"
        ),
        subscription_price=_positive_decimal(
            item.get("subscription_price"), code="RIGHTS_ISSUE_EVENT_PRICE_INVALID"
        ),
        actual_offered_quantity=offered,
        actual_subscribed_quantity=subscribed,
        rounding_rule=str(item["rounding_rule"]),
        issue_success_status=str(item["issue_success_status"]),
        source_record_key=str(item["source_record_key"]),
        source_url=str(item["source_url"]),
        source_content_sha256=source_hash,
        source_document_type=str(item["source_document_type"]),
        source_file_size=source_size,
        event_sha256=canonical_sha256(item),
    )
    return event, references


def rights_issue_application_audit(
    authority: RightsIssueAuthority,
    *,
    symbols: Sequence[str],
    start: date,
    end: date,
) -> Mapping[str, Any]:
    """Freeze the shared no-subscription decision for every in-scope event."""

    normalized = tuple(sorted({str(symbol).upper() for symbol in symbols}))
    if not normalized or start > end:
        raise ActionValueError("RIGHTS_ISSUE_APPLICATION_SCOPE_INVALID")
    scoped = tuple(
        event
        for event in authority.events
        if event.symbol in normalized and start <= event.ex_right_date <= end
    )
    outside = tuple(event.event_id for event in authority.events if event not in scoped)
    if outside:
        raise ActionValueError(
            "RIGHTS_ISSUE_AUTHORITY_SCOPE_MISMATCH", outside_event_ids=outside
        )
    applications = [
        {
            "event_id": event.event_id,
            "event_sha256": event.event_sha256,
            "symbol": event.symbol,
            "record_date": event.record_date.isoformat(),
            "payment_start_date": event.payment_start_date.isoformat(),
            "payment_end_date": event.payment_end_date.isoformat(),
            "ex_right_date": event.ex_right_date.isoformat(),
            "listing_date": event.listing_date.isoformat(),
            "participation_decision": "NOT_SUBSCRIBED",
            "subscribed_quantity": 0,
            "subscription_cash_cny": "0",
            "partial_subscription_status": "NOT_APPLICABLE_NO_SUBSCRIPTION",
            "cash_insufficient_status": "NOT_APPLICABLE_NO_SUBSCRIPTION",
            "credited_quantity": 0,
            "sellable_quantity_increment": 0,
            "factor_account_quantity_inference": False,
        }
        for event in scoped
    ]
    identity = {
        "schema_version": "position_timing_pattern_rights_issue_application_audit_v1",
        "authority_file_sha256": authority.authority_reference["sha256"],
        "authority_canonical_sha256": authority.authority_canonical_sha256,
        "source_documents_sha256": authority.source_documents_sha256,
        "policy_sha256": RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256,
        "scope": {
            "symbols_sha256": canonical_sha256(normalized),
            "symbol_count": len(normalized),
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
        "event_count": len(scoped),
        "applications": applications,
        "account_quantity_change": 0,
        "account_cash_change_cny": "0",
        "outcomes_read": False,
    }
    return {**identity, "application_sha256": canonical_sha256(identity)}


def combined_corporate_action_source_snapshot(
    *,
    dividend_snapshot_sha256: str,
    authority: RightsIssueAuthority,
) -> Mapping[str, Any]:
    identity = {
        "schema_version": "position_timing_pattern_corporate_action_source_snapshot_v1",
        "action_types": ("DIVIDEND", "RIGHTS_ISSUE"),
        "dividend_snapshot_sha256": _validated_sha256(
            dividend_snapshot_sha256,
            code="PATTERN_DIVIDEND_SNAPSHOT_IDENTITY_INVALID",
        ),
        "rights_issue_authority_file_sha256": authority.authority_reference["sha256"],
        "rights_issue_authority_canonical_sha256": authority.authority_canonical_sha256,
        "rights_issue_source_documents_sha256": authority.source_documents_sha256,
        "rights_issue_event_ids": tuple(event.event_id for event in authority.events),
        "rights_issue_event_count": len(authority.events),
    }
    return {**identity, "snapshot_sha256": canonical_sha256(identity)}


__all__ = [
    "AUTHORITY_RELATIVE_PATH",
    "RIGHTS_ISSUE_PARTICIPATION_POLICY",
    "RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256",
    "RightsIssueAuthority",
    "RightsIssueEvent",
    "combined_corporate_action_source_snapshot",
    "freeze_rights_issue_participation_policy",
    "open_rights_issue_authority",
    "rights_issue_application_audit",
]

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Mapping, Sequence

import pandas as pd

from .entry_price_contracts import (
    AdvisoryEntryPriceEnvelopeV2, EntryPriceCandidateV2, EntryPriceScope, availability,
)
from .errors import AdvisoryModelFirstError
from .price_range_inference import predict_entry_quantiles, project_entry_price
from .price_range_runtime_bundle import LoadedAdvisoryPriceRangeBundle
from .realtime_feature_source import PriceRangeRealtimeContext


@dataclass(frozen=True)
class ScoredEntryPrice:
    """Continuous predictions retained for historical scoring, not trading actions."""

    candidate: EntryPriceCandidateV2
    raw_gaps: tuple[float, float, float] | None
    calibrated_gaps: tuple[float, float, float] | None


@dataclass(frozen=True)
class EntryPriceDayResult:
    envelope: AdvisoryEntryPriceEnvelopeV2
    scored: tuple[ScoredEntryPrice, ...]
    contexts: Mapping[str, PriceRangeRealtimeContext]
    candidate_source_sha256: str
    feature_values_sha256: str
    input_identity: Mapping[str, Any]


@dataclass(frozen=True)
class PreparedEntryPriceDay:
    decision_date: date
    candidates: pd.DataFrame
    parent: Any
    price: LoadedAdvisoryPriceRangeBundle
    pit_kwargs: Mapping[str, str]
    frozen_input_ids: Mapping[str, str]
    list_created_at: Any
    list_status: str
    provenance_identity: Mapping[str, Any] = field(default_factory=dict)


def unavailable_entry_candidate(
    *, symbol: str, reason_code: str, message: str,
) -> EntryPriceCandidateV2:
    return EntryPriceCandidateV2(
        symbol=symbol,
        entry_price={"status": "UNAVAILABLE", "reason_code": reason_code, "message": message},
        **_unavailable_auxiliaries(),
    )


def score_entry_price_bundle(
    bundle: LoadedAdvisoryPriceRangeBundle,
    features: pd.DataFrame,
    *,
    contexts: Mapping[str, PriceRangeRealtimeContext],
    context_unavailable: Sequence[Mapping[str, Any]],
    decision_as_of_trade_date: date,
    target_trade_date: date,
) -> list[ScoredEntryPrice]:
    """Score one frozen PIT candidate group without executing M1/M3 or writing state."""
    required_identity = {"instrument", "decision_as_of_trade_date", "target_trade_date"}
    if not required_identity.issubset(features.columns) or target_trade_date <= decision_as_of_trade_date:
        _identity_error("entry features require explicit decision/target dates and instruments")
    symbols = features["instrument"].tolist()
    if any(not isinstance(symbol, str) or not symbol.strip() for symbol in symbols):
        _identity_error("entry instruments must be nonempty strings")
    if len(set(symbols)) != len(symbols):
        _identity_error("entry candidate group contains duplicate symbols")
    for column, expected in (
        ("decision_as_of_trade_date", decision_as_of_trade_date),
        ("target_trade_date", target_trade_date),
    ):
        dates = pd.to_datetime(features[column], errors="coerce")
        if dates.isna().any() or not dates.dt.date.eq(expected).all():
            _identity_error("entry feature dates differ from frozen inference clock")
    failures: dict[str, Mapping[str, Any]] = {}
    for failure in context_unavailable:
        symbol = failure.get("symbol")
        if symbol not in symbols or symbol in failures or symbol in contexts:
            _identity_error("entry context failure has duplicate, conflicting or foreign identity")
        if not failure.get("reason_code") or not failure.get("message"):
            _identity_error("entry context failure requires a typed reason")
        failures[symbol] = failure
    if set(contexts) - set(symbols):
        _identity_error("entry contexts contain foreign candidates")
    if not symbols:
        return []
    predictions, calibrated = predict_entry_quantiles(bundle, features)
    result = []
    for index, symbol in enumerate(symbols):
        raw_gaps = tuple(float(predictions[name][index]) for name in (
            "entry_gap_q10", "entry_gap_q50", "entry_gap_q90",
        ))
        calibrated_gaps = (
            tuple(float(values[index]) for values in calibrated) if calibrated is not None else None
        )
        context = contexts.get(symbol)
        if context is None:
            failure = failures.get(symbol) or {
                "reason_code": "ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
                "message": "entry price PIT context is unavailable",
            }
            candidate = unavailable_entry_candidate(
                symbol=symbol, reason_code=failure["reason_code"], message=failure["message"],
            )
        else:
            if context.symbol != symbol or context.decision_price_trade_date > decision_as_of_trade_date:
                _identity_error("entry context has foreign identity or future decision price")
            try:
                projected = project_entry_price(
                    symbol=symbol, context=context, entry_gaps=raw_gaps,
                    calibrated_entry_gaps=calibrated_gaps,
                    calibration_spec=bundle.calibration_spec, target_trade_date=target_trade_date,
                )
                candidate = EntryPriceCandidateV2(
                    symbol=symbol,
                    decision_reference_price=projected["decision_reference_price"],
                    decision_price_trade_date=projected["decision_price_trade_date"],
                    target_raw_price_multiplier=projected["target_raw_price_multiplier"],
                    tick_size=projected["tick_size"],
                    regulatory_price_range=projected["regulatory_price_range"],
                    entry_price={
                        "status": "AVAILABLE", "raw_range": projected["entry_price_range"],
                        "calibrated_range": projected["calibrated_entry_price_range"],
                        "calibration": projected["entry_gap_calibration"],
                    },
                    **_unavailable_auxiliaries(),
                )
            except AdvisoryModelFirstError as exc:
                candidate = unavailable_entry_candidate(
                    symbol=symbol, reason_code=exc.reason_code, message=str(exc),
                )
        result.append(ScoredEntryPrice(candidate, raw_gaps, calibrated_gaps))
    return result


def _unavailable_auxiliaries() -> dict[str, dict[str, str]]:
    return {
        role: {"status": "UNAVAILABLE", "reason_code": "OUTCOME_ROLE_UNAVAILABLE"}
        for role in ("take_profit", "protective", "stop_loss")
    }


def _identity_error(message: str) -> None:
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ENTRY_PRICE_INPUT_IDENTITY_MISMATCH")


def merge_auxiliary_prices(
    entry: AdvisoryEntryPriceEnvelopeV2, legacy_payload: Mapping[str, Any] | None,
) -> AdvisoryEntryPriceEnvelopeV2:
    """Attach already computed, exact-compatible auxiliaries; never execute M3."""
    from pydantic import ValidationError
    from .daily_price_envelope_contracts import AdvisoryDailyPriceEnvelopeV1

    if not legacy_payload:
        return entry
    try:
        legacy = AdvisoryDailyPriceEnvelopeV1.model_validate(legacy_payload)
    except ValidationError:
        # A malformed auxiliary is local to that role, not a replacement for entry.
        return _auxiliary_failure(entry, "OUTCOME_ROLE_PAYLOAD_INVALID")
    if legacy.availability_status == "UNAVAILABLE":
        return entry
    expected = (
        entry.package_id, entry.package_manifest_sha256, entry.style_profile_hash,
        entry.review_policy_sha256, entry.price_range_bundle_id,
        entry.decision_as_of_trade_date, entry.target_trade_date,
        entry.training_lineage.parent_bundle_id if entry.training_lineage else None,
        entry.training_lineage.outcome_bundle_id if entry.training_lineage else None,
    )
    actual = (
        legacy.package_id, legacy.package_manifest_sha256, legacy.style_profile_hash,
        legacy.review_policy_sha256, legacy.price_range_bundle_id,
        legacy.decision_as_of_trade_date, legacy.target_trade_date,
        legacy.parent_bundle_id, legacy.outcome_bundle_id,
    )
    if expected != actual:
        return _auxiliary_failure(entry, "OUTCOME_ROLE_IDENTITY_MISMATCH")
    old_by_symbol = {row.symbol: row for row in legacy.candidates}
    rows = []
    available_roles = 0
    for row in entry.candidates:
        old = old_by_symbol.get(row.symbol)
        payload = row.model_dump(mode="json")
        payload.update(_unavailable_auxiliaries())
        if (
            row.entry_price.status == "AVAILABLE" and old is not None
            and old.availability_status == "AVAILABLE"
        ):
            same_projection = (
                row.entry_price.raw_range == old.entry_price_range
                and row.entry_price.calibrated_range == old.calibrated_entry_price_range
                and row.entry_price.calibration == old.entry_gap_calibration
                and row.decision_price_trade_date == old.decision_price_trade_date
                and row.decision_reference_price == old.decision_reference_price
                and row.target_raw_price_multiplier == old.target_raw_price_multiplier
                and row.regulatory_price_range == old.regulatory_price_range
                and row.tick_size == old.tick_size
            )
            for name, value in (
                ("take_profit", old.take_profit_price), ("protective", old.protective_price),
                ("stop_loss", old.stop_loss_price),
            ):
                if same_projection:
                    payload[name] = {
                        "status": "AVAILABLE", "payload": value.model_dump(mode="json"),
                        "source_identity": {
                            "outcome_bundle_id": legacy.outcome_bundle_id,
                            "review_policy_sha256": legacy.review_policy_sha256,
                        },
                    }
                    available_roles += 1
                else:
                    payload[name] = {"status": "UNAVAILABLE", "reason_code": "OUTCOME_ROLE_PROJECTION_MISMATCH"}
        rows.append(payload)
    payload = entry.as_payload()
    payload.update(candidates=rows, auxiliary_availability=availability(available_roles, len(rows) * 3))
    return AdvisoryEntryPriceEnvelopeV2.model_validate(payload)


def _auxiliary_failure(entry: AdvisoryEntryPriceEnvelopeV2, reason: str) -> AdvisoryEntryPriceEnvelopeV2:
    payload = entry.as_payload()
    for row in payload["candidates"]:
        for name in ("take_profit", "protective", "stop_loss"):
            row[name] = {"status": "UNAVAILABLE", "reason_code": reason}
    payload["auxiliary_availability"] = "UNAVAILABLE"
    return AdvisoryEntryPriceEnvelopeV2.model_validate(payload)


class AdvisoryEntryPriceService:
    """Read persisted daily inputs and evaluate an explicitly identified frozen price model."""

    def __init__(
        self, *, program_service=None, selection_service=None, review_source=None,
        feature_source=None, parent_loader=None, price_loader=None, pit_authority_resolver=None,
    ) -> None:
        from .model_inference import AdvisoryModelShadowService
        from .model_bundle import load_frozen_research_bundle
        from .price_range_runtime_bundle import load_frozen_price_range_bundle

        self._reader = AdvisoryModelShadowService(
            program_service=program_service, selection_service=selection_service,
            review_source=review_source, feature_source=feature_source,
        )
        self._programs = self._reader._program_service
        self._features = self._reader._feature_source
        self._parent_loader = parent_loader or load_frozen_research_bundle
        self._price_loader = price_loader or load_frozen_price_range_bundle
        self._pit_authority_resolver = pit_authority_resolver

    def evaluate(
        self, **kwargs,
    ) -> AdvisoryEntryPriceEnvelopeV2:
        return self.evaluate_day(**kwargs).envelope

    def prepare_day(
        self, *, model_root, program_id: str, binding_version_id: str,
        target_trade_date: date, scope: EntryPriceScope, role_binding_sha256: str,
        frozen_input_ids=None, capture_as_of: datetime | None = None,
        legacy_provenance=None,
    ) -> PreparedEntryPriceDay:
        from .feature_schema_v1 import FEATURE_SCHEMA_HASH
        from .model_binding_resolution import AdvisoryModelBindingResolutionV1
        from .model_inference import (
            _candidate_rows_for_recommendation_list, _resolve_decision_date,
            _validate_review_policy_identity, build_frozen_candidate_frame,
        )
        from .prospective_price_prediction import _require_next_trading_day
        from backend.services.selection_center.canonical_pit_runtime import (
            has_canonical_pit_runtime_profile, require_canonical_pit_generation_current,
            require_canonical_pit_runtime_binding,
        )

        program = self._programs.get_program(program_id)
        binding = self._programs.active_binding(program_id)
        if (
            binding.get("binding_version_id") != binding_version_id
            or tuple(binding.get("package_ids") or ()) != (scope.package_id,)
            or tuple(program.package_ids) != (scope.package_id,)
            or program.review_policy_sha256 != scope.review_policy_sha256
            or int(program.target_count) != scope.target_count
            or scope.feature_schema_sha256 != FEATURE_SCHEMA_HASH
        ):
            _identity_error("entry Program binding or feature/policy scope differs")
        config = binding.get("runtime_config_json") or {}
        declared_universe = binding.get("universe_selection") or config.get("universe_selection")
        if declared_universe != scope.universe_selection.model_dump(mode="json"):
            _identity_error("entry Program lacks matching explicit universe selection")
        version, items, selection = self._reader.entry_candidate_context(
            program_id=program_id, target_trade_date=target_trade_date,
            binding=binding, package_ids=(scope.package_id,), frozen_input_ids=frozen_input_ids,
            allow_empty=True, published_only=capture_as_of is not None,
        )
        if capture_as_of is not None:
            created = version.get("created_at")
            if not created or version.get("version_status") != "PUBLISHED":
                _identity_error("natural capture requires an already published input list")
            created = created if isinstance(created, datetime) else datetime.fromisoformat(str(created))
            if capture_as_of.utcoffset() is None or created.utcoffset() is None or created > capture_as_of:
                _identity_error("natural capture list publication is unproven or in the future")
        if selection.manifest_sha256_by_package.get(scope.package_id) != scope.package_manifest_sha256:
            _identity_error("entry Selection manifest differs from frozen price model")
        decision = _resolve_decision_date(list_version=version, selection_run=selection)
        _require_next_trading_day(
            self._programs.calendar_provider, decision_date=decision, target_trade_date=target_trade_date,
        )
        parent = self._parent_loader(
            model_root=model_root, bundle_id=scope.parent_bundle_id,
            expected_package_id=scope.package_id, expected_manifest_sha256=scope.package_manifest_sha256,
            expected_selection_runtime_semantics_hash=scope.selection_runtime_semantics_hash,
        )
        if (
            parent.manifest_file_sha256 != scope.parent_bundle_manifest_sha256
            or parent.manifest.get("style_profile_hash") != scope.style_profile_hash
            or parent.manifest.get("style_profile_id") != scope.style_profile_id
            or parent.manifest.get("feature_schema_hash") != scope.feature_schema_sha256
        ):
            _identity_error("entry frozen parent provenance differs")
        price = self._price_loader(
            model_root=model_root, price_range_bundle_id=scope.price_range_bundle_id,
            price_range_bundle_manifest_sha256=scope.price_range_bundle_manifest_sha256,
            expected_package_id=scope.package_id, expected_manifest_sha256=scope.package_manifest_sha256,
            expected_style_profile_hash=scope.style_profile_hash,
            expected_parent_bundle_id=scope.parent_bundle_id, expected_outcome_bundle_id=scope.outcome_bundle_id,
        )
        resolution = AdvisoryModelBindingResolutionV1(
            program_id=program_id, binding_version_id=binding_version_id, package_id=scope.package_id,
            manifest_sha256=scope.package_manifest_sha256, style_profile_id=scope.style_profile_id,
            style_profile_hash=scope.style_profile_hash,
            selection_runtime_semantics_hash=scope.selection_runtime_semantics_hash,
            feature_schema_version=scope.feature_schema_version, feature_schema_hash=scope.feature_schema_sha256,
            bundle_id=scope.parent_bundle_id, bundle_manifest_sha256=scope.parent_bundle_manifest_sha256,
            component_roles=scope.component_roles.model_dump(), descriptor_sha256=role_binding_sha256,
        )
        summary = version.get("summary_json") or {}
        receipt = summary.get("advisory_universe_receipt") or {}
        if receipt.get("universe_selection") != declared_universe:
            from .entry_price_legacy_provenance import LegacyExploratoryInputs
            if ("advisory_universe_receipt" in summary or capture_as_of is not None
                    or not isinstance(legacy_provenance, LegacyExploratoryInputs)
                    or legacy_provenance.request.study_type != "EXPLORATORY_SCREEN"
                    or legacy_provenance.request.decision_use != "NAVIGATION_ONLY"
                    or legacy_provenance.request.evidence_level != "HISTORICAL_REPLAY"
                    or legacy_provenance.request.data_identity.qualification != "CONSUMED_OR_NON_VINTAGE"
                    or legacy_provenance.request.scope != scope
                    or legacy_provenance.request.request_sha256 != role_binding_sha256):
                _identity_error("entry frozen list universe differs from Program scope")
        rows = (
            _candidate_rows_for_recommendation_list(selection.aggregate_results, items)
            if scope.universe_selection.mode != "stock_universe" else selection.aggregate_results
        )
        if not rows and any(str(item.get("action") or "").upper() != "EXIT" for item in items):
            _identity_error("empty Selection input conflicts with persisted recommendation candidates")
        candidates = build_frozen_candidate_frame(
            rows, program_id=program_id, binding_version_id=binding_version_id,
            decision_date=decision, target_trade_date=target_trade_date,
            target_count=scope.target_count, bundle=parent, resolution=resolution,
        ) if rows else pd.DataFrame(columns=["instrument", "decision_as_of_trade_date", "target_trade_date"])
        _validate_review_policy_identity(
            list_items=items, expected_symbols=candidates["instrument"].tolist(),
            review_policy_sha256=scope.review_policy_sha256,
        )
        provenance = {"kind": "NATIVE_LIST_RECEIPT", "native_receipt_present": True}
        archive_ref = summary.get("advisory_frozen_input_archive")
        run_archive_ref = selection.runtime_config.get("advisory_frozen_input_archive")
        if archive_ref is not None or run_archive_ref is not None:
            if archive_ref != run_archive_ref:
                _identity_error("entry run/list frozen input archive reference differs")
            from backend.services.selection_center.advisory_input_archive import validate_archive_reference
            validate_archive_reference(archive_ref, run=selection, program_id=program_id,
                                       binding_version_id=binding_version_id,
                                       review_policy_sha256=scope.review_policy_sha256)
        if not receipt or (legacy_provenance is not None and target_trade_date in legacy_provenance.metadata):
            legacy_provenance.validate_live_day(
                version=version, items=items, selection=selection, decision=decision, target=target_trade_date,
                candidates=candidates, request_sha256=role_binding_sha256)
            provenance = legacy_provenance.identity(target_trade_date)
        pit_kwargs = {}
        if has_canonical_pit_runtime_profile(selection.runtime_config):
            lease = require_canonical_pit_runtime_binding(selection.runtime_config, trade_date=decision)
            kwargs = {"authority_resolver": self._pit_authority_resolver} if self._pit_authority_resolver is not None else {}
            require_canonical_pit_generation_current(selection.runtime_config, **kwargs)
            pit_kwargs["pit_universe_key"] = lease.universe_key
        return PreparedEntryPriceDay(
            decision_date=decision, candidates=candidates, parent=parent, price=price, pit_kwargs=pit_kwargs,
            frozen_input_ids={
                "list_version_id": str(version["list_version_id"]),
                "review_run_id": str(version["review_run_id"]), "selection_run_id": str(selection.run_id),
            },
            list_created_at=version.get("created_at"), list_status=str(version.get("version_status") or ""),
            provenance_identity=provenance,
        )

    def evaluate_day(
        self, *, model_root, program_id: str, binding_version_id: str,
        target_trade_date: date, scope: EntryPriceScope, role_binding_sha256: str,
        evidence_state: str = "EXPERIMENTAL", frozen_input_ids=None, capture_as_of: datetime | None = None,
        legacy_provenance=None,
    ) -> EntryPriceDayResult:
        from .shared_feature_builder import build_advisory_feature_matrix

        prepared = self.prepare_day(
            model_root=model_root, program_id=program_id, binding_version_id=binding_version_id,
            target_trade_date=target_trade_date, scope=scope, role_binding_sha256=role_binding_sha256,
            frozen_input_ids=frozen_input_ids, capture_as_of=capture_as_of,
            legacy_provenance=legacy_provenance,
        )
        decision, candidates, parent, price = prepared.decision_date, prepared.candidates, prepared.parent, prepared.price
        identity = dict(program_id=program_id, binding_version_id=binding_version_id, scope=scope,
                        role_binding_sha256=role_binding_sha256, decision=decision, target_trade_date=target_trade_date,
                        price=price, evidence_state=evidence_state)
        inputs = {**prepared.frozen_input_ids, "list_created_at": str(prepared.list_created_at), "list_status": prepared.list_status,
                  "candidate_provenance": dict(prepared.provenance_identity),
                  "pit_input": dict(prepared.pit_kwargs), "feature_source": "DATABASE_DAILY_AS_OF_DECISION"}
        if candidates.empty:
            return EntryPriceDayResult(envelope=_entry_envelope(projected=(), **identity), scored=(), contexts={},
                candidate_source_sha256=_frame_sha256(candidates), feature_values_sha256=_frame_sha256(candidates), input_identity=inputs)
        realtime = self._features.load(
            symbols=candidates["instrument"].tolist(), decision_as_of_trade_date=decision,
            target_trade_date=target_trade_date,
            continuation_cutoff=date.fromisoformat(parent.manifest["continuation_cutoff"]),
            hmm_models=parent.hmm_models, **prepared.pit_kwargs,
        )
        built = build_advisory_feature_matrix(
            candidates=candidates, candidate_daily=realtime.candidate_daily,
            candidate_static=realtime.candidate_static, market_daily=realtime.market_daily,
            benchmark_daily=realtime.benchmark_daily, suspend_rows=realtime.suspend_rows,
            hmm_states=realtime.hmm_states, component_roles=scope.component_roles.model_dump(),
            incomplete_candidate_policy="drop_candidate",
        )
        feature_symbols = set(built.features["instrument"])
        if feature_symbols - set(candidates["instrument"]):
            _identity_error("entry feature builder produced foreign candidates")
        scored = score_entry_price_bundle(
            price, built.features,
            contexts={k: v for k, v in realtime.price_range_contexts.items() if k in feature_symbols},
            context_unavailable=tuple(row for row in realtime.price_range_unavailable if row["symbol"] in feature_symbols),
            decision_as_of_trade_date=decision, target_trade_date=target_trade_date,
        )
        by_symbol = {row.candidate.symbol: row.candidate for row in scored}
        projected = tuple(
            by_symbol[symbol] if symbol in by_symbol else unavailable_entry_candidate(
                symbol=symbol, reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                message="required historical feature is unavailable; candidate retained",
            )
            for symbol in candidates["instrument"]
        )
        envelope = _entry_envelope(projected=projected, **identity)
        scored_by_symbol = {row.candidate.symbol: row for row in scored}
        return EntryPriceDayResult(
            envelope=envelope,
            scored=tuple(scored_by_symbol.get(row.symbol, ScoredEntryPrice(row, None, None)) for row in projected),
            contexts=realtime.price_range_contexts,
            candidate_source_sha256=_frame_sha256(candidates),
            feature_values_sha256=_frame_sha256(built.features),
            input_identity=inputs,
        )


def _entry_envelope(*, program_id, binding_version_id, scope, role_binding_sha256, decision, target_trade_date, price, evidence_state, projected):
    count = sum(row.entry_price.status == "AVAILABLE" for row in projected)
    return AdvisoryEntryPriceEnvelopeV2(
            program_id=program_id, binding_version_id=binding_version_id, package_id=scope.package_id,
            package_manifest_sha256=scope.package_manifest_sha256, style_profile_hash=scope.style_profile_hash,
            review_policy_sha256=scope.review_policy_sha256, universe_identity_sha256=scope.universe_identity_sha256,
            candidate_projection_sha256=scope.candidate_projection_sha256,
            feature_schema_sha256=scope.feature_schema_sha256, role_binding_sha256=role_binding_sha256,
            price_range_bundle_id=scope.price_range_bundle_id,
            price_range_bundle_manifest_sha256=scope.price_range_bundle_manifest_sha256,
            training_lineage={"parent_bundle_id": scope.parent_bundle_id, "outcome_bundle_id": scope.outcome_bundle_id},
            decision_as_of_trade_date=decision, target_trade_date=target_trade_date,
            nominal_coverage=float((price.calibration_spec or {}).get("nominal_coverage", 0.8)),
            calibration_state="CALIBRATED_INTERVAL" if price.calibration_spec is not None else "UNCALIBRATED",
            evidence_state=evidence_state, candidates=projected, candidate_count=len(projected),
            available_count=count, unavailable_count=len(projected) - count,
            availability_status=availability(count, len(projected)), auxiliary_availability="UNAVAILABLE",
            reason_code=None if count else "ADVISORY_ENTRY_PRICE_CANDIDATES_UNAVAILABLE" if projected else "NO_CANDIDATES",
            message=None if count else "all entry candidates are unavailable; inspect row errors" if projected else "verified published candidate roster is empty",
        )


def _frame_sha256(frame: pd.DataFrame) -> str:
    return sha256(frame.to_json(orient="split", date_format="iso", double_precision=15).encode("utf-8")).hexdigest()

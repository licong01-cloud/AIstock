"""Exact R3 candidate projection shared by R4 outcome and Phase 1 bridge."""

from __future__ import annotations

from datetime import datetime, time
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import psycopg2.extras
from pydantic import BaseModel, ConfigDict

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeCandidateFactV1,
    HistoricalRangeLineageIdentity,
)
from backend.services.advisory_phase1.capture_foundation import (
    RetrospectiveObservationCapturePlan,
)
from backend.services.advisory_phase1.observation_capture import (
    Phase1GObservationRowBundle,
    materialize_retrospective_observation_row_bundle,
)
from backend.services.advisory_phase1.outcome_engine import (
    OutcomeOwner,
    OwnerType,
)
from backend.services.advisory_phase1.retrospective_contracts import (
    HistoricalRangeArtifactReference,
    HistoricalRangeCaptureScope,
    HistoricalRangeLineageProjection,
)
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
)


_MARKET_TZ = ZoneInfo("Asia/Shanghai")
_RANGE_STAGES = (
    "alpha_raw",
    "hmm_adjusted",
    "risk_policy_adjusted",
    "selection_effective",
)


class HistoricalRangeProjectionPolicyProvider(Protocol):
    def load(self, policy_bundle_hash: str) -> Any: ...


class HistoricalRangeCandidateProjectionV1(BaseModel):
    """One exact candidate materialization whose owner is used by R4 valuation."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    candidate_artifact_ref: HistoricalRangeArtifactRefV1
    candidate_payload: HistoricalRangeCandidateArtifactPayloadV2
    candidate_fact: HistoricalRangeCandidateFactV1
    lineage: HistoricalRangeLineageIdentity
    capture_plan: RetrospectiveObservationCapturePlan
    stage_payload: dict[str, Any]
    observation_rows: Phase1GObservationRowBundle
    owner: OutcomeOwner


class PostgresHistoricalRangeCandidateProjectionLoader:
    """Load exact R3 refs and derive the same observation owner used by capture."""

    def __init__(
        self,
        *,
        conn_factory: Any,
        artifact_store: HistoricalRangeArtifactStore,
        policy_provider: HistoricalRangeProjectionPolicyProvider,
    ) -> None:
        self._conn_factory = conn_factory
        self._artifact_store = artifact_store
        self._policy_provider = policy_provider
        self._projection_cache: dict[tuple[str, str, str, str], HistoricalRangeCandidateProjectionV1] = {}
        self._candidate_payload_cache: dict[str, HistoricalRangeCandidateArtifactPayloadV2] = {}

    def load(
        self,
        *,
        candidate_id: str,
        range_run_id: str,
        policy_bundle_ref: HistoricalRangeArtifactRefV1,
        policy_bundle_hash: str,
    ) -> HistoricalRangeCandidateProjectionV1:
        cache_key = (
            candidate_id,
            range_run_id,
            policy_bundle_ref.semantic_content_hash,
            policy_bundle_hash,
        )
        cached = self._projection_cache.get(cache_key)
        if cached is not None:
            return cached
        row = self._load_row(candidate_id=candidate_id, range_run_id=range_run_id)
        candidate_ref = HistoricalRangeArtifactRefV1.model_validate(row["candidate_artifact_ref"])
        if candidate_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
            raise ValueError("R3 candidate row lacks an exact candidate artifact ref")
        candidate_payload = self._candidate_payload_cache.get(candidate_ref.semantic_content_hash)
        if candidate_payload is None:
            envelope = self._artifact_store.load(candidate_ref)
            candidate_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(envelope.payload)
            self._candidate_payload_cache[candidate_ref.semantic_content_hash] = candidate_payload
        if candidate_payload.range_run_id != range_run_id or candidate_payload.day_run_id != str(row["day_run_id"]):
            raise ValueError("R3 candidate artifact belongs to another range/day")
        matches = tuple(item for item in candidate_payload.candidates if item.candidate_id == candidate_id)
        if len(matches) != 1 or matches[0].membership_status != "INCLUDED":
            raise ValueError("R3 candidate artifact lacks the exact included candidate")
        candidate_fact = matches[0]
        policy = self._policy_provider.load(policy_bundle_hash)
        if (
            policy.bundle.policy_bundle_hash != policy_bundle_hash
            or policy_bundle_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST
            or policy_bundle_ref.payload_sha256 != policy_bundle_hash
        ):
            raise ValueError("range policy provider/ref/hash closure is invalid")

        request_ref = HistoricalRangeArtifactRefV1.model_validate(row["historical_range_request_ref"])
        frozen_json = dict(row["frozen_program_json"])
        frozen_ref_raw = frozen_json.get("artifact_ref")
        if not isinstance(frozen_ref_raw, dict):
            raise ValueError("R3 frozen Program lacks its immutable artifact ref")
        frozen_ref = HistoricalRangeArtifactRefV1.model_validate(frozen_ref_raw)
        if (
            request_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST
            or frozen_ref.artifact_kind is not HistoricalRangeArtifactKind.FROZEN_PROGRAM
        ):
            raise ValueError("R3 request/frozen Program artifact kinds are invalid")
        signal_source_hash = canonical_json_sha256(
            [item.model_dump(mode="json") for item in candidate_payload.source_revision_refs]
        )
        oos_interval_hash = canonical_json_sha256(
            {
                "schema_version": "advisory_phase1_retrospective_oos_interval_v1",
                "historical_range_request_hash": request_ref.semantic_content_hash,
                "range_run_id": range_run_id,
                "date_start": row["start_trade_date"],
                "date_end": row["end_trade_date"],
                "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
                "signal_source_revision_set_hash": signal_source_hash,
            }
        )
        lineage = HistoricalRangeLineageIdentity(
            historical_range_request_ref=request_ref,
            historical_range_frozen_program_ref=frozen_ref,
            range_run_id=range_run_id,
            range_day_run_id=str(row["day_run_id"]),
            candidate_artifact_ref=candidate_ref,
            package_id=candidate_payload.package_id,
            manifest_sha256=candidate_payload.manifest_sha256,
            code_release_hash=candidate_payload.code_release_hash,
            signal_source_revision_set_hash=signal_source_hash,
            oos_interval_hash=oos_interval_hash,
        )
        phase1_lineage = HistoricalRangeLineageProjection.model_validate(lineage.model_dump(mode="json"))
        phase1_policy_ref = HistoricalRangeArtifactReference.model_validate(policy_bundle_ref.model_dump(mode="json"))
        range_scope = HistoricalRangeCaptureScope(
            historical_range_request_ref=phase1_lineage.historical_range_request_ref,
            historical_range_frozen_program_ref=(phase1_lineage.historical_range_frozen_program_ref),
            range_run_id=range_run_id,
            historical_range_policy_bundle_ref=phase1_policy_ref,
            historical_range_policy_bundle_hash=policy_bundle_hash,
            selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            signal_source_revision_set_id=f"hrsrs_{signal_source_hash[:20]}",
            signal_source_revision_set_hash=signal_source_hash,
            oos_interval_hash=oos_interval_hash,
        )
        decision_date = candidate_payload.decision_trade_date
        target_date = policy.calendar.next_trading_day(decision_date)
        cutoff = datetime.combine(decision_date, time(15, 0), tzinfo=_MARKET_TZ)
        stable_semantics_hash = canonical_json_sha256(
            {
                "schema_version": "advisory_phase1_retrospective_signal_semantics_v1",
                "package_id": candidate_payload.package_id,
                "manifest_sha256": candidate_payload.manifest_sha256,
                "alpha_mode": candidate_payload.alpha_mode.value,
                "selection_semantics_hash": candidate_payload.selection_semantics_hash,
                "package_effective_config_hash": candidate_payload.runtime_profile_hash,
                "calendar_hash": policy.bundle.calendar_hash,
                "historical_range_policy_bundle_hash": policy_bundle_hash,
            }
        )
        signal_scope_hash = canonical_json_sha256(
            {
                "stable_signal_semantics_hash": stable_semantics_hash,
                "decision_cutoff_ts": cutoff,
                "target_trade_date": target_date,
                "symbol": candidate_fact.symbol,
            }
        )
        range_signal_context_hash = canonical_json_sha256(
            {
                "range_lineage_identity_hash": lineage.range_lineage_identity_hash,
                "candidate_input_hash": candidate_payload.candidate_input_hash,
                "stage_closure_hash": candidate_payload.stage_closure_hash,
                "historical_range_policy_bundle_hash": policy_bundle_hash,
            }
        )
        evidence_bundle_hash = canonical_json_sha256(
            {
                "candidate_artifact_hash": candidate_ref.semantic_content_hash,
                "source_read_receipt_hashes": (candidate_payload.source_read_receipt_hashes),
                "signal_source_revision_set_hash": signal_source_hash,
                "stage_closure_hash": candidate_payload.stage_closure_hash,
            }
        )
        metadata = candidate_payload.stage_trace.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, dict) else {}
        hmm_metadata = metadata.get("hmm")
        hmm_metadata = dict(hmm_metadata) if isinstance(hmm_metadata, dict) else {}
        hmm_status = str(candidate_payload.stage_trace["hmm_adjusted"]["status"])
        if hmm_status == "NOT_APPLICABLE":
            hmm_snapshot_id = None
            hmm_snapshot_hash = None
        else:
            hmm_snapshot_id = str(hmm_metadata.get("model_snapshot_id") or "")
            if not hmm_snapshot_id:
                raise ValueError("complete R3 HMM stage lacks frozen snapshot identity")
            hmm_snapshot_hash = canonical_json_sha256(hmm_metadata)
        risk_metadata = metadata.get("risk")
        risk_metadata = dict(risk_metadata) if isinstance(risk_metadata, dict) else {}
        stage_payload = {stage: dict(candidate_payload.stage_trace[stage]) for stage in _RANGE_STAGES}
        plan = RetrospectiveObservationCapturePlan(
            canonical_signal_id=f"acs_{signal_scope_hash[:20]}",
            symbol=candidate_fact.symbol,
            decision_as_of_trade_date=decision_date,
            selection_as_of_trade_date=decision_date,
            target_trade_date=target_date,
            decision_cutoff_ts=cutoff,
            alpha_mode=candidate_payload.alpha_mode.value,
            selection_runtime_semantics_hash=(candidate_payload.selection_semantics_hash),
            package_effective_config_hash=candidate_payload.runtime_profile_hash,
            calendar_version=policy.bundle.calendar_version,
            calendar_hash=policy.bundle.calendar_hash,
            stable_signal_semantics_hash=stable_semantics_hash,
            canonical_signal_scope_hash=signal_scope_hash,
            lineage=phase1_lineage,
            range_scope=range_scope,
            signal_source_revision_set_id=range_scope.signal_source_revision_set_id,
            signal_source_revision_set_hash=signal_source_hash,
            range_signal_context_hash=range_signal_context_hash,
            evidence_bundle_hash=evidence_bundle_hash,
            stage_payload_hash=str(candidate_payload.stage_closure_hash),
            runtime_profile_version_id=(f"hrrpv_{candidate_payload.runtime_profile_hash[:20]}"),
            runtime_profile_version_hash=candidate_payload.runtime_profile_hash,
            hmm_snapshot_id=hmm_snapshot_id,
            hmm_snapshot_hash=hmm_snapshot_hash,
            hmm_snapshot_status=("NOT_APPLICABLE" if hmm_status == "NOT_APPLICABLE" else "FROZEN"),
            risk_policy_hash=canonical_json_sha256(risk_metadata),
            universe_policy_hash=candidate_payload.universe_identity_hash,
            symbol_normalization_policy_hash=(policy.market_data.symbol_normalization_policy_hash),
            evidence_available_at=row["day_finished_at"],
            selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        )
        observation_rows = materialize_retrospective_observation_row_bundle(
            plan=plan,
            stage_payload=stage_payload,
            candidate_fact=candidate_fact.model_dump(mode="python"),
            created_by_capture_batch_id=f"owner_{candidate_id}"[:160],
        )
        selection_stage = next(
            item for item in observation_rows.stage_evidence_rows if item["stage"] == "selection_effective"
        )
        owner = OutcomeOwner(
            owner_type=OwnerType.CANDIDATE,
            owner_key=candidate_id,
            canonical_signal_id=plan.canonical_signal_id,
            observation_version_id=str(observation_rows.observation_version["observation_version_id"]),
            candidate_stage_evidence_id=str(selection_stage["stage_evidence_id"]),
            symbol=candidate_fact.symbol,
            decision_as_of_trade_date=decision_date,
        )
        projection = HistoricalRangeCandidateProjectionV1(
            candidate_artifact_ref=candidate_ref,
            candidate_payload=candidate_payload,
            candidate_fact=candidate_fact,
            lineage=lineage,
            capture_plan=plan,
            stage_payload=stage_payload,
            observation_rows=observation_rows,
            owner=owner,
        )
        self._projection_cache[cache_key] = projection
        return projection

    def _load_row(self, *, candidate_id: str, range_run_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT candidate.artifact_ref AS candidate_artifact_ref,
                           day.day_run_id, day.finished_at AS day_finished_at,
                           run.frozen_program_json,
                           batch.request_artifact_ref AS historical_range_request_ref,
                           batch.start_trade_date, batch.end_trade_date
                    FROM app.advisory_historical_range_candidate AS candidate
                    JOIN app.advisory_historical_range_day_run AS day
                      ON day.day_run_id = candidate.day_run_id
                    JOIN app.advisory_historical_range_run AS run
                      ON run.range_run_id = day.range_run_id
                    JOIN app.advisory_historical_range_batch AS batch
                      ON batch.batch_id = run.batch_id
                    WHERE candidate.candidate_id = %s
                      AND run.range_run_id = %s
                      AND candidate.membership_status = 'INCLUDED'
                      AND day.status = 'COMPLETE'
                    """,
                    (candidate_id, range_run_id),
                )
                row = cur.fetchone()
            conn.rollback()
        if row is None:
            raise ValueError("exact successful R3 candidate lineage is unavailable")
        result = dict(row)
        if (
            result.get("candidate_artifact_ref") is None
            or result.get("historical_range_request_ref") is None
            or result.get("day_finished_at") is None
        ):
            raise ValueError("successful R3 candidate lineage is incomplete")
        return result

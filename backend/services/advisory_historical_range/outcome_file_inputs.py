"""Prepare immutable R3 subject inputs once; reuse the native owner projection.

Only run/day control-plane metadata uses SQL. Candidate, episode and list facts
come from the original content-addressed artifacts, never a rerun of Selection.
"""
from __future__ import annotations

import json
import os
import tempfile
from collections import OrderedDict
from typing import Any

import psycopg2.extras

from .artifact_store import HistoricalRangeArtifactStore
from .canonical import canonical_json_sha256
from .models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeDayReceiptPayloadV2,
)
from .outcome_evaluator import PostgresHistoricalRangeOutcomeSubjectInputProvider
from .retrospective_projection import PostgresHistoricalRangeCandidateProjectionLoader
from .summary_service import PostgresHistoricalRangeSummaryContextProvider


class HistoricalRangeOutcomeReadCache(HistoricalRangeArtifactStore):
    """Operation-local cache of validated, sealed artifacts; no asset copies.

    Cache keys include the complete ref. A changed file fails instead of being
    silently refreshed, and every caller still checks its own run/day/payload
    and upstream contracts. This is not exported to shared selection or LocalSIM.
    """
    def __init__(self, *, root, max_bytes=32 * 1024 * 1024):
        super().__init__(root=root)
        self._cache = OrderedDict()
        self._cached_bytes = 0
        self._max_bytes = max_bytes

    def load(self, ref):
        key = canonical_json_sha256(ref.model_dump(mode="json"))
        path = (self.root / ref.relative_path).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("outcome artifact escapes its registered root")
        stat = path.stat()
        stamp = (stat.st_size, stat.st_mtime_ns)
        cached = self._cache.get(key)
        if cached is not None:
            if cached[0] != stamp:
                raise ValueError("sealed outcome input changed after verification")
            self._cache.move_to_end(key)
            return cached[1].model_copy(deep=True)
        envelope = super().load(ref)
        after = path.stat()
        if stamp != (after.st_size, after.st_mtime_ns):
            raise ValueError("sealed outcome input changed during verification")
        if stat.st_size <= self._max_bytes:
            while self._cache and (self._cached_bytes + stat.st_size > self._max_bytes or len(self._cache) >= 256):
                _, (old_stamp, _) = self._cache.popitem(last=False)
                self._cached_bytes -= old_stamp[0]
            self._cache[key] = (stamp, envelope)
            self._cached_bytes += stat.st_size
        return envelope.model_copy(deep=True)


def _command_ref_path(store, batch_id, idempotency_key):
    key = canonical_json_sha256({"batch_id": batch_id, "idempotency_key": idempotency_key})
    path = store.root / "outcome-command-refs" / f"{key}.json"
    if not path.resolve().is_relative_to(store.root):
        raise ValueError("outcome command index escapes its registered root")
    return path


def load_frozen_outcome_command(*, store, batch_id, api_request, resolved_request_hash):
    path = _command_ref_path(store, batch_id, api_request.operation_idempotency_key)
    if not path.exists():
        return None
    ref = HistoricalRangeArtifactRefV1.model_validate(json.loads(path.read_text(encoding="utf-8")))
    envelope = store.load(ref)
    payload = envelope.payload
    if envelope.resolved_request_hash != resolved_request_hash:
        raise ValueError("frozen outcome command batch request identity differs")
    if (ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST or
            payload.get("schema_version") != "advisory_frozen_outcome_command_v1" or
            payload.get("batch_id") != batch_id or
            payload.get("api_request_hash") != canonical_json_sha256(api_request.model_dump(mode="json"))):
        raise ValueError("frozen outcome command differs from exact API request")
    return payload["plan"]


def publish_frozen_outcome_command(*, store, batch_id, api_request, plan, resolved_request_hash):
    """Immutable plan before operation registration; a crash cannot rebind its data."""
    policy_refs = {canonical_json_sha256(item.policy_bundle_ref.model_dump(mode="json")): item.policy_bundle_ref for item in plan.requests}
    stored = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="advisory_frozen_outcome_command_v1",
        payload_schema_version="advisory_frozen_outcome_command_v1",
        resolved_request_hash=resolved_request_hash,
        payload={"schema_version": "advisory_frozen_outcome_command_v1", "batch_id": batch_id,
                 "api_request_hash": canonical_json_sha256(api_request.model_dump(mode="json")),
                 "plan": {"request_hash": plan.request_hash, "requests": [item.model_dump(mode="json") for item in plan.requests]}},
        upstream_refs=tuple(policy_refs[key] for key in sorted(policy_refs)),
    )
    path = _command_ref_path(store, batch_id, api_request.operation_idempotency_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    contents = json.dumps(stored.ref.model_dump(mode="json"), sort_keys=True).encode("utf-8")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".outcome-command-", delete=False) as stream:
            temporary = stream.name
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != contents:
                raise ValueError("outcome command idempotency key already has another frozen plan") from None
    finally:
        if temporary is not None:
            os.unlink(temporary)
    return stored.ref


class FrozenHistoricalRangeOutcomeInputs:
    def __init__(self, *, conn_factory, artifact_store: HistoricalRangeArtifactStore, range_run_ids: tuple[str, ...]) -> None:
        if not range_run_ids:
            raise ValueError("file outcome preparation requires explicit range run identities")
        self._metadata: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self._candidate_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self._store = artifact_store
        self._candidate_payloads: dict[str, HistoricalRangeCandidateArtifactPayloadV2] = {}
        starts = []
        for run_id in range_run_ids:
            with conn_factory() as conn:
                conn.set_session(readonly=True, autocommit=False)
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT run.frozen_program_json, run.final_receipt_ref,
                               batch.request_artifact_ref AS historical_range_request_ref,
                               batch.start_trade_date, batch.end_trade_date
                        FROM app.advisory_historical_range_run run
                        JOIN app.advisory_historical_range_batch batch ON batch.batch_id = run.batch_id
                        WHERE run.range_run_id = %s
                    """, (run_id,))
                    header = cur.fetchone()
                    if header is None:
                        raise ValueError("frozen outcome run identity is unavailable")
                    header = dict(header)
                    cur.execute("""
                        SELECT day_run_id, decision_trade_date, finished_at AS day_finished_at,
                               candidate_artifact_ref, day_receipt_ref
                        FROM app.advisory_historical_range_day_run
                        WHERE range_run_id = %s AND status IN ('COMPLETE', 'VALID_NO_CANDIDATE')
                        ORDER BY decision_trade_date, day_run_id
                    """, (run_id,))
                    days = tuple(dict(row) for row in cur.fetchall())
                conn.rollback()
            if not days:
                raise ValueError("frozen outcome run has no successful day artifacts")
            starts.append(header["start_trade_date"])
            for day in days:
                self._index_day(run_id, header, day)
            if header["final_receipt_ref"] is not None:
                self._put(run_id, "RANGE", run_id, HistoricalRangeArtifactRefV1.model_validate(header["final_receipt_ref"]), {
                    "symbol": None, "decision_trade_date": min(day["decision_trade_date"] for day in days),
                    "strategy_rank": None, "list_version_id": None, "episode_closed": None,
                })
        self.start_trade_date = min(starts)

    @staticmethod
    def _ref_key(ref: HistoricalRangeArtifactRefV1) -> str:
        return canonical_json_sha256(ref.model_dump(mode="json"))

    def _put(self, run_id, kind, subject_id, ref, metadata) -> None:
        key = (run_id, kind, subject_id, self._ref_key(ref))
        if key in self._metadata and self._metadata[key] != metadata:
            raise ValueError("conflicting frozen outcome subject metadata")
        self._metadata[key] = metadata

    def _index_day(self, run_id, header, day) -> None:
        ref = HistoricalRangeArtifactRefV1.model_validate(day["candidate_artifact_ref"])
        payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(self._store.load(ref).payload)
        day_ref = HistoricalRangeArtifactRefV1.model_validate(day["day_receipt_ref"])
        receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(self._store.load(day_ref).payload)
        if (payload.range_run_id != run_id or payload.day_run_id != day["day_run_id"] or
                payload.decision_trade_date != day["decision_trade_date"] or receipt.range_run_id != run_id or
                receipt.day_run_id != day["day_run_id"] or receipt.candidate_artifact_ref != ref or day["day_finished_at"] is None):
            raise ValueError("frozen outcome run/day/candidate identity differs")
        self._candidate_payloads[ref.semantic_content_hash] = payload
        active_symbols = {item.symbol for item in receipt.items if item.action.value in {"ENTER", "HOLD"}}
        for candidate in payload.candidates:
            if candidate.membership_status != "INCLUDED":
                continue
            key = (run_id, candidate.candidate_id)
            if key in self._candidate_rows:
                raise ValueError("duplicate candidate identity across frozen days")
            self._candidate_rows[key] = {**header, **day, "candidate_artifact_ref": ref.model_dump(mode="json")}
            self._put(run_id, "CANDIDATE", candidate.candidate_id, ref, {
                "symbol": candidate.symbol, "end_decision_trade_date": day["decision_trade_date"],
                "decision_trade_date": day["decision_trade_date"], "episode_closed": False,
                "strategy_rank": candidate.selection_effective_rank,
                "list_version_id": receipt.list_version.list_version_id if candidate.symbol in active_symbols else None,
            })
        for episode in receipt.episode_snapshots:
            self._put(run_id, "EPISODE", episode.episode_id, day_ref, {
                "symbol": episode.symbol, "end_decision_trade_date": episode.exit_decision_trade_date or episode.decision_trade_date,
                "decision_trade_date": episode.enter_decision_trade_date,
                "episode_closed": episode.recommendation_state == "EXITED", "strategy_rank": None,
                "list_version_id": episode.list_version_id,
            })
        self._put(run_id, "LIST_VERSION", receipt.list_version.list_version_id, day_ref, {
            "symbol": None, "decision_trade_date": day["decision_trade_date"], "strategy_rank": None,
            "list_version_id": receipt.list_version.list_version_id, "episode_closed": None,
        })

    def metadata(self, *, range_run_id, subject_type, subject_id, subject_ref) -> dict[str, Any]:
        key = (range_run_id, subject_type.value, subject_id, self._ref_key(subject_ref))
        if key not in self._metadata:
            raise ValueError("outcome subject does not belong to the exact frozen run/day/ref")
        return dict(self._metadata[key])


class FileHistoricalRangeCandidateProjectionLoader(PostgresHistoricalRangeCandidateProjectionLoader):
    """Use exactly the native observation/owner projection; replace metadata IO only."""

    def __init__(self, *, inputs, artifact_store, policy_provider) -> None:
        super().__init__(conn_factory=None, artifact_store=artifact_store, policy_provider=policy_provider)
        self._inputs = inputs
        self._candidate_payload_cache = inputs._candidate_payloads

    def _load_row(self, *, candidate_id, range_run_id):
        return dict(self._inputs._candidate_rows[(range_run_id, candidate_id)])


class FileHistoricalRangeOutcomeSubjectInputProvider(PostgresHistoricalRangeOutcomeSubjectInputProvider):
    def __init__(self, *, inputs, **kwargs) -> None:
        super().__init__(conn_factory=None, **kwargs)
        self._inputs = inputs

    def _metadata(self, work_item):
        return self._inputs.metadata(range_run_id=work_item.range_run_id, subject_type=work_item.subject_type, subject_id=work_item.subject_id, subject_ref=work_item.subject_ref)


class FileHistoricalRangeSummaryContextProvider(PostgresHistoricalRangeSummaryContextProvider):
    def __init__(self, *, inputs, artifact_store, source_provider) -> None:
        super().__init__(conn_factory=None, artifact_store=artifact_store, source_provider=source_provider)
        self._inputs = inputs

    def _subject_metadata(self, fact, *, subject_ref):
        metadata = self._inputs.metadata(range_run_id=fact.range_run_id, subject_type=fact.subject_type, subject_id=fact.subject_id, subject_ref=subject_ref)
        if fact.subject_type.value != "EPISODE":
            metadata["episode_closed"] = None
        return metadata

    def _load_summary_industry_at_t(self, *, symbol, decision_trade_date):
        return self._source_provider.load_industry_at_t(symbol=symbol, decision_trade_date=decision_trade_date)[0]

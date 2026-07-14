from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.evidence_projection import ProjectedSelectionScoreArtifact
from backend.services.advisory_phase1.readiness_plan import REASON_PACKAGE_LINEAGE_HASH_MISMATCH, Phase1EError
from backend.services.advisory_phase1.readiness_plan_postgres import PostgresPhase1EInputProvider


NOW = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)


class _Snapshot:
    def __init__(self, *, artifact: ProjectedSelectionScoreArtifact) -> None:
        self.artifact = artifact
        self.exact_artifact_ids: list[str] = []
        self.selection_artifact_list_called = False
        self.closed = False
        self.run = SimpleNamespace(
            program_id="program",
            decision_trade_date=date(2026, 7, 10),
            package_id="pkg",
            manifest_sha256="a" * 64,
            evidence_id="dse",
            artifact_id="artifact",
            artifact_payload_hash="d" * 64,
        )

    def get_historical_receipt(self, _receipt_ref):
        return SimpleNamespace(), SimpleNamespace(program_runs=[self.run])

    def list_binding_versions(self, _program_id):
        return [
            SimpleNamespace(
                activation_status="ACTIVE",
                effective_from_trade_date=date(2026, 1, 1),
                effective_to_trade_date=None,
            )
        ]

    def get(self, _package_id):
        return SimpleNamespace(package_id="pkg")

    def get_daily_selection_evidence(self, _evidence_id):
        return SimpleNamespace(evidence_id="dse")

    def get_selection_score_artifact(self, artifact_id):
        self.exact_artifact_ids.append(artifact_id)
        return self.artifact

    def list(self, **_kwargs):
        self.selection_artifact_list_called = True
        raise AssertionError("Phase 1E must not find historical artifacts through a truncated latest-artifact list")

    def postgres_now(self):
        return NOW

    def list_source_events(self, **_kwargs):
        return []


class _Projection:
    def __init__(self, snapshot: _Snapshot) -> None:
        self.snapshot_value = snapshot

    @contextmanager
    def snapshot(self):
        try:
            yield self.snapshot_value
        finally:
            self.snapshot_value.closed = True


def _artifact(*, package_id: str = "pkg") -> ProjectedSelectionScoreArtifact:
    return ProjectedSelectionScoreArtifact(
        artifact_id="artifact",
        package_id=package_id,
        manifest_sha256="a" * 64,
        trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        runtime_config_hash="b" * 64,
        scores_json=[],
        artifact_sha256="c" * 64,
        score_count=0,
        universe_count=0,
        top_score_symbol=None,
        status="SUCCEEDED",
        artifact_contract_version="selection_score_artifact_v2",
        artifact_payload_sha256="d" * 64,
        artifact_input_context_hash="e" * 64,
        source_revision_set_hash="f" * 64,
        asset_closure_hash="0" * 64,
    )


def _request() -> SimpleNamespace:
    return SimpleNamespace(
        historical_batch_receipt_ref="receipt",
        program_id="program",
        decision_trade_date=date(2026, 7, 10),
    )


def test_provider_pins_exact_artifact_for_historical_audit() -> None:
    snapshot = _Snapshot(artifact=_artifact())
    provider = PostgresPhase1EInputProvider(projection=_Projection(snapshot), policy=SimpleNamespace())

    evidence = provider.resolve_program_date(request=_request(), batch_request=SimpleNamespace())

    assert snapshot.exact_artifact_ids == ["artifact"]
    assert snapshot.selection_artifact_list_called is False
    assert evidence.selection_artifact.artifact_id == "artifact"
    assert evidence.audit_readers.score_artifact.list(package_id="pkg", manifest_sha256="a" * 64) == [evidence.selection_artifact]
    provider.close_program_date()
    assert snapshot.closed is True


def test_provider_rejects_exact_artifact_with_mismatched_lineage() -> None:
    snapshot = _Snapshot(artifact=_artifact(package_id="wrong-package"))
    provider = PostgresPhase1EInputProvider(projection=_Projection(snapshot), policy=SimpleNamespace())

    with pytest.raises(Phase1EError) as error:
        provider.resolve_program_date(request=_request(), batch_request=SimpleNamespace())

    assert error.value.reason_code == REASON_PACKAGE_LINEAGE_HASH_MISMATCH
    assert snapshot.closed is True

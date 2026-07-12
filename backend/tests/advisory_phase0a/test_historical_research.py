from __future__ import annotations

import ast
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase0a.historical_research import (
    HISTORICAL_RESEARCH_DATA_SOURCE,
    HISTORICAL_RESEARCH_ORIGIN,
    HISTORICAL_RESEARCH_SCOPE,
    REASON_HISTORICAL_DATE_REQUIRED,
    REASON_PROGRAM_EVIDENCE_INVALID,
    REASON_RESEARCH_RUN_CONFLICT,
    HistoricalAdvisoryResearchRunner,
    HistoricalResearchBatchRequest,
    HistoricalResearchCandidate,
    HistoricalResearchInputUnavailable,
    HistoricalResearchProgramContext,
    HistoricalResearchRunStatus,
    HistoricalSelectionEvidence,
    InMemoryHistoricalResearchRepository,
)
from backend.services.advisory_phase0a.historical_research_postgres import PersistedHistoricalSelectionEvidenceAdapter
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError
from backend.tests.strategy_package.test_prospective_selection_evidence import _prospective_capture_fixture


class _Calendar:
    def require_completed_historical_trading_date(self, *, decision_trade_date: date, requested_at: datetime) -> None:
        if decision_trade_date >= requested_at.date():
            raise RuntimeConfigInvalidError(
                "decision date must precede the request date",
                context={"reason_code": REASON_HISTORICAL_DATE_REQUIRED},
            )


class _Resolver:
    def __init__(self, *, broken_program_ids: set[str] | None = None) -> None:
        self.broken_program_ids = broken_program_ids or set()

    def resolve(self, *, program_id: str, decision_trade_date: date, cursor=None) -> HistoricalResearchProgramContext:
        if program_id in self.broken_program_ids:
            raise RuntimeConfigInvalidError("binding is invalid", context={"reason_code": "BINDING_INVALID"})
        return HistoricalResearchProgramContext(
            program_id=program_id,
            binding_version_id=f"bind_{program_id}",
            binding_payload_hash="a" * 64,
            package_id=f"pkg_{program_id}",
            manifest_sha256="b" * 64,
            policy_hash="c" * 64,
            effective_runtime_config_hash="d" * 64,
        )


class _EvidenceAdapter:
    def __init__(self, evidence_by_program: dict[str, HistoricalSelectionEvidence]) -> None:
        self.evidence_by_program = evidence_by_program

    def load(self, *, context: HistoricalResearchProgramContext, decision_trade_date: date, cursor=None) -> HistoricalSelectionEvidence:
        try:
            return self.evidence_by_program[context.program_id]
        except KeyError as exc:
            raise HistoricalResearchInputUnavailable("v2 DSE is not available") from exc


def _request(*program_ids: str, decision_trade_date: date = date(2026, 7, 10)) -> HistoricalResearchBatchRequest:
    return HistoricalResearchBatchRequest(
        decision_trade_date=decision_trade_date,
        program_ids=list(program_ids),
        requested_at=datetime(2026, 7, 12, 10, tzinfo=UTC),
    )


def _evidence(*, candidate_outcome: str = "CANDIDATES_PRESENT", score: float = 1.0, source_hash: str = "f" * 64) -> HistoricalSelectionEvidence:
    candidates = []
    if candidate_outcome == "CANDIDATES_PRESENT":
        candidates = [
            HistoricalResearchCandidate(
                symbol="000001.SZ",
                rank=1,
                score=score,
                stock_name="Unit Stock",
                component_scores={"alpha": score},
            )
        ]
    return HistoricalSelectionEvidence(
        evidence_id=f"dse_{score}",
        evidence_hash=f"{int(score * 10):x}" * 64,
        artifact_id=f"ssa_{score}",
        artifact_payload_hash="e" * 64,
        source_watermark_hash=source_hash,
        candidate_outcome=candidate_outcome,
        candidates=candidates,
    )


def _runner(
    *,
    evidence_by_program: dict[str, HistoricalSelectionEvidence],
    broken_program_ids: set[str] | None = None,
    now_provider=None,
):
    repository = InMemoryHistoricalResearchRepository()
    return (
        HistoricalAdvisoryResearchRunner(
            repository=repository,
            trading_date_resolver=_Calendar(),
            program_resolver=_Resolver(broken_program_ids=broken_program_ids),
            evidence_adapter=_EvidenceAdapter(evidence_by_program),
            now_provider=now_provider or (lambda: datetime(2026, 7, 12, 10, tzinfo=UTC)),
        ),
        repository,
    )


def test_historical_research_runs_independent_programs_and_keeps_only_research_fields() -> None:
    runner, _repository = _runner(
        evidence_by_program={
            "program_single": _evidence(score=1.0),
            "program_native_multi": _evidence(candidate_outcome="VALID_NO_CANDIDATE", score=2.0),
        }
    )

    receipt = runner.run(_request("program_native_multi", "program_single"))

    assert receipt.status is HistoricalResearchRunStatus.COMPLETE
    assert [row.program_id for row in receipt.program_runs] == ["program_native_multi", "program_single"]
    by_program = {row.program_id: row for row in receipt.program_runs}
    assert by_program["program_single"].candidate_outcome == "CANDIDATES_PRESENT"
    assert by_program["program_single"].research_candidates[0].model_dump() == {
        "symbol": "000001.SZ",
        "rank": 1,
        "score": 1.0,
        "stock_name": "Unit Stock",
        "component_scores": {"alpha": 1.0},
    }
    assert by_program["program_native_multi"].candidate_outcome == "VALID_NO_CANDIDATE"
    assert by_program["program_native_multi"].research_candidates == []


def test_waiting_input_resumes_without_creating_a_second_program_run() -> None:
    runner, repository = _runner(evidence_by_program={})
    request = _request("program_single")

    first = runner.run(request)
    assert first.status is HistoricalResearchRunStatus.WAITING_INPUT
    first_run = first.program_runs[0]
    assert first_run.reason_codes == ["ADVISORY_PHASE0A2D_PROGRAM_INPUT_UNAVAILABLE"]

    adapter = runner._evidence_adapter
    assert isinstance(adapter, _EvidenceAdapter)
    adapter.evidence_by_program["program_single"] = _evidence(score=1.0)
    second = runner.run(request)

    assert second.status is HistoricalResearchRunStatus.COMPLETE
    assert second.program_runs[0].program_run_id == first_run.program_run_id
    assert repository.get_program_run(program_id="program_single", decision_trade_date=request.decision_trade_date) == second.program_runs[0]


def test_same_program_date_with_changed_evidence_fails_closed() -> None:
    runner, _repository = _runner(evidence_by_program={"program_single": _evidence(score=1.0)})
    request = _request("program_single")
    runner.run(request)

    adapter = runner._evidence_adapter
    assert isinstance(adapter, _EvidenceAdapter)
    adapter.evidence_by_program["program_single"] = _evidence(score=2.0, source_hash="9" * 64)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        runner.run(request)
    assert exc_info.value.context["reason_code"] == REASON_RESEARCH_RUN_CONFLICT


def test_terminal_batch_receipt_is_returned_unchanged_on_exact_retry() -> None:
    runner, _repository = _runner(evidence_by_program={"program_single": _evidence(score=1.0)})
    request = _request("program_single")

    first = runner.run(request)
    second = runner.run(request)

    assert second.receipt_id == first.receipt_id
    assert second.receipt_hash == first.receipt_hash
    assert second.created_at == first.created_at


def test_one_program_failure_does_not_erase_independent_complete_result() -> None:
    runner, _repository = _runner(
        evidence_by_program={"program_good": _evidence(score=1.0)},
        broken_program_ids={"program_bad"},
    )

    receipt = runner.run(_request("program_bad", "program_good"))

    assert receipt.status is HistoricalResearchRunStatus.FAILED
    by_program = {row.program_id: row for row in receipt.program_runs}
    assert by_program["program_bad"].status is HistoricalResearchRunStatus.FAILED
    assert by_program["program_good"].status is HistoricalResearchRunStatus.COMPLETE
    assert by_program["program_good"].evidence_id is not None


def test_invalid_persisted_evidence_is_failed_not_waiting_input() -> None:
    class _InvalidEvidenceAdapter:
        def load(self, *, context: HistoricalResearchProgramContext, decision_trade_date: date, cursor=None):
            raise RuntimeConfigInvalidError(
                "evidence contract mismatch",
                context={"reason_code": REASON_PROGRAM_EVIDENCE_INVALID},
            )

    runner = HistoricalAdvisoryResearchRunner(
        repository=InMemoryHistoricalResearchRepository(),
        trading_date_resolver=_Calendar(),
        program_resolver=_Resolver(),
        evidence_adapter=_InvalidEvidenceAdapter(),
        now_provider=lambda: datetime(2026, 7, 12, 10, tzinfo=UTC),
    )

    receipt = runner.run(_request("program_single"))

    assert receipt.status is HistoricalResearchRunStatus.FAILED
    assert receipt.program_runs[0].status is HistoricalResearchRunStatus.FAILED
    assert receipt.program_runs[0].reason_codes == [REASON_PROGRAM_EVIDENCE_INVALID]


@pytest.mark.parametrize(
    ("payload", "reason_code"),
    [
        ({"data_source": "MINIQMT_REALTIME"}, "ADVISORY_PHASE0A2D_HISTORICAL_DATA_REQUIRED"),
        ({"origin": "SCHEDULED"}, "ADVISORY_PHASE0A2D_MANUAL_ORIGIN_REQUIRED"),
        ({"execution_prohibited": False}, "ADVISORY_PHASE0A2D_HISTORICAL_DATA_REQUIRED"),
    ],
)
def test_request_contract_rejects_non_research_inputs(payload: dict[str, object], reason_code: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HistoricalResearchBatchRequest(
            decision_trade_date=date(2026, 7, 10),
            program_ids=["program_single"],
            requested_at=datetime(2026, 7, 12, 10, tzinfo=UTC),
            **payload,
        )
    assert reason_code in str(exc_info.value)


def test_request_rejects_current_or_future_trading_date() -> None:
    runner, _repository = _runner(evidence_by_program={"program_single": _evidence()})

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        runner.run(_request("program_single", decision_trade_date=date(2026, 7, 12)))
    assert exc_info.value.context["reason_code"] == REASON_HISTORICAL_DATE_REQUIRED


def test_runner_ignores_client_supplied_future_requested_at_for_date_gate() -> None:
    runner, _repository = _runner(evidence_by_program={"program_single": _evidence()})
    spoofed = HistoricalResearchBatchRequest(
        decision_trade_date=date(2026, 7, 12),
        program_ids=["program_single"],
        requested_at=datetime(2027, 7, 12, 10, tzinfo=UTC),
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        runner.run(spoofed)
    assert exc_info.value.context["reason_code"] == REASON_HISTORICAL_DATE_REQUIRED


def test_runner_has_no_forbidden_runtime_imports() -> None:
    service_root = Path(__file__).resolve().parents[2] / "services" / "advisory_phase0a"
    imported_modules = set()
    for source_name in ("historical_research.py", "historical_research_postgres.py"):
        tree = ast.parse((service_root / source_name).read_text(encoding="utf-8"))
        imported_modules.update(
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        )
    forbidden_prefixes = (
        "backend.services.paper_trading",
        "backend.services.simulation_runtime",
        "backend.infra.qmt",
        "backend.services.miniqmt_execution_runtime",
        "backend.services.broker",
        "backend.data_service.realtime",
    )
    assert not any(module.startswith(prefix) for module in imported_modules for prefix in forbidden_prefixes)
    assert HISTORICAL_RESEARCH_DATA_SOURCE == "DB_HISTORICAL"
    assert HISTORICAL_RESEARCH_ORIGIN == "MANUAL_HISTORICAL_RESEARCH"
    assert HISTORICAL_RESEARCH_SCOPE == "HISTORICAL_RESEARCH_ONLY"


def test_persisted_dse_adapter_accepts_only_the_exact_dated_program_context() -> None:
    context, manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    evidence = ProspectiveSelectionEvidenceAssembler().assemble(
        context=context,
        manifest=manifest,
        selection_run_id=context.selection_run_id,
        artifact=artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=selected,
        excluded=[],
        created_by="unit",
    )
    payload = evidence.evidence_payload_json
    package_lineage = payload["phase0a_package_lineage"]
    effective_config = payload["phase0a_effective_config_chain"]
    program_context = HistoricalResearchProgramContext(
        program_id="program_single",
        binding_version_id=package_lineage["binding_ref"]["binding_id"],
        binding_payload_hash=package_lineage["binding_ref"]["binding_hash"],
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        policy_hash="a" * 64,
        effective_runtime_config_hash=effective_config["package_effective_config_hash"],
    )

    class _Cursor:
        def execute(self, *_args, **_kwargs) -> None:
            return None

        def fetchall(self):
            return [
                {
                    "evidence_id": evidence.evidence_id,
                    "artifact_hash": evidence.artifact_hash,
                    "evidence_payload_json": payload,
                }
            ]

    result = PersistedHistoricalSelectionEvidenceAdapter().load(
        context=program_context,
        decision_trade_date=evidence.cutoff_date,
        cursor=_Cursor(),
    )

    assert result.evidence_id == evidence.evidence_id
    assert result.candidates[0].symbol == selected[0].symbol
    assert result.candidates[0].model_dump().keys() == {"symbol", "rank", "score", "stock_name", "component_scores"}


def test_persisted_dse_adapter_rejects_context_mismatch_as_failed_contract() -> None:
    context, manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    evidence = ProspectiveSelectionEvidenceAssembler().assemble(
        context=context,
        manifest=manifest,
        selection_run_id=context.selection_run_id,
        artifact=artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=selected,
        excluded=[],
        created_by="unit",
    )

    class _Cursor:
        def execute(self, *_args, **_kwargs) -> None:
            return None

        def fetchall(self):
            return [
                {
                    "evidence_id": evidence.evidence_id,
                    "artifact_hash": evidence.artifact_hash,
                    "evidence_payload_json": evidence.evidence_payload_json,
                }
            ]

    mismatched_context = HistoricalResearchProgramContext(
        program_id="program_single",
        binding_version_id="different_binding",
        binding_payload_hash="d" * 64,
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        policy_hash="a" * 64,
        effective_runtime_config_hash=evidence.evidence_payload_json["phase0a_effective_config_chain"]["package_effective_config_hash"],
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        PersistedHistoricalSelectionEvidenceAdapter().load(
            context=mismatched_context,
            decision_trade_date=evidence.cutoff_date,
            cursor=_Cursor(),
        )
    assert exc_info.value.context["reason_code"] == REASON_PROGRAM_EVIDENCE_INVALID


def test_persisted_dse_adapter_rejects_source_observed_after_frozen_cutoff() -> None:
    context, manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    evidence = ProspectiveSelectionEvidenceAssembler().assemble(
        context=context,
        manifest=manifest,
        selection_run_id=context.selection_run_id,
        artifact=artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=selected,
        excluded=[],
        created_by="unit",
    )
    payload = evidence.evidence_payload_json
    package_lineage = payload["phase0a_package_lineage"]
    effective_config = payload["phase0a_effective_config_chain"]
    delayed_payload = {
        **payload,
        "phase0a_source_evidence": [
            {
                **payload["phase0a_source_evidence"][0],
                "first_observed_at": "2026-07-10T15:30:00+08:00",
            },
            *payload["phase0a_source_evidence"][1:],
        ],
    }
    program_context = HistoricalResearchProgramContext(
        program_id="program_single",
        binding_version_id=package_lineage["binding_ref"]["binding_id"],
        binding_payload_hash=package_lineage["binding_ref"]["binding_hash"],
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        policy_hash="a" * 64,
        effective_runtime_config_hash=effective_config["package_effective_config_hash"],
    )

    class _Cursor:
        def execute(self, *_args, **_kwargs) -> None:
            return None

        def fetchall(self):
            return [
                {
                    "evidence_id": evidence.evidence_id,
                    "artifact_hash": evidence.artifact_hash,
                    "evidence_payload_json": delayed_payload,
                }
            ]

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        PersistedHistoricalSelectionEvidenceAdapter().load(
            context=program_context,
            decision_trade_date=evidence.cutoff_date,
            cursor=_Cursor(),
        )
    assert exc_info.value.context["reason_code"] == REASON_PROGRAM_EVIDENCE_INVALID


def test_historical_research_migration_is_additive_and_execution_free() -> None:
    migration = (
        Path(__file__).resolve().parents[2]
        / "db"
        / "migrations"
        / "add_advisory_historical_research_runner_20260712.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS app.advisory_research_batch" in migration
    assert "CREATE TABLE IF NOT EXISTS app.advisory_research_program_run" in migration
    assert "CREATE TABLE IF NOT EXISTS app.advisory_research_batch_receipt" in migration
    assert "DB_HISTORICAL" in migration
    assert "MANUAL_HISTORICAL_RESEARCH" in migration
    assert "execution_prohibited IS TRUE" in migration
    assert not any(token in migration.upper() for token in (" DROP ", " INSERT ", " UPDATE ", " DELETE ", " TRUNCATE "))

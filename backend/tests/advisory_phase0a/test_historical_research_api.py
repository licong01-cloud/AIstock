from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import advisory as advisory_router
from backend.services.advisory_phase0a.historical_research import (
    HistoricalAdvisoryResearchRunner,
    HistoricalResearchCandidate,
    HistoricalResearchInputUnavailable,
    HistoricalResearchProgramContext,
    HistoricalSelectionEvidence,
    InMemoryHistoricalResearchRepository,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


class _Calendar:
    def require_completed_historical_trading_date(self, *, decision_trade_date: date, requested_at: datetime) -> None:
        if decision_trade_date >= requested_at.date():
            raise RuntimeConfigInvalidError(
                "decision date must precede the request date",
                context={"reason_code": "ADVISORY_PHASE0A2D_HISTORICAL_DATE_REQUIRED"},
            )


class _Resolver:
    def resolve(self, *, program_id: str, decision_trade_date: date, cursor=None) -> HistoricalResearchProgramContext:
        return HistoricalResearchProgramContext(
            program_id=program_id,
            binding_version_id=f"bind_{program_id}",
            binding_payload_hash="a" * 64,
            package_id=f"pkg_{program_id}",
            manifest_sha256="b" * 64,
            policy_hash="c" * 64,
            effective_runtime_config_hash="d" * 64,
        )


class _Adapter:
    def load(self, *, context: HistoricalResearchProgramContext, decision_trade_date: date, cursor=None) -> HistoricalSelectionEvidence:
        if context.program_id == "waiting":
            raise HistoricalResearchInputUnavailable("DSE is not available")
        return HistoricalSelectionEvidence(
            evidence_id="dse_unit",
            evidence_hash="e" * 64,
            artifact_id="ssa_unit",
            artifact_payload_hash="f" * 64,
            source_watermark_hash="1" * 64,
            candidate_outcome="CANDIDATES_PRESENT",
            candidates=[
                HistoricalResearchCandidate(
                    symbol="000001.SZ",
                    rank=1,
                    score=0.91,
                    stock_name="Unit Stock",
                    component_scores={"alpha": 0.91},
                )
            ],
        )


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(advisory_router.router, prefix="/api/v1")
    runner = HistoricalAdvisoryResearchRunner(
        repository=InMemoryHistoricalResearchRepository(),
        trading_date_resolver=_Calendar(),
        program_resolver=_Resolver(),
        evidence_adapter=_Adapter(),
    )
    app.dependency_overrides[advisory_router.get_historical_research_runner] = lambda: runner
    return TestClient(app)


def test_historical_research_api_is_manual_research_only() -> None:
    client = _client()
    request = {
        "decision_trade_date": "2026-07-10",
        "program_ids": ["program_native_multi", "program_single"],
        "data_source": "DB_HISTORICAL",
        "origin": "MANUAL_HISTORICAL_RESEARCH",
        "requested_at": datetime(2026, 7, 12, 10, tzinfo=UTC).isoformat(),
        "research_scope": "HISTORICAL_RESEARCH_ONLY",
        "execution_prohibited": True,
    }

    created = client.post("/api/v1/advisory/research-batches", json=request)

    assert created.status_code == 200
    payload = created.json()
    assert payload["batch"]["data_source"] == "DB_HISTORICAL"
    assert payload["batch"]["origin"] == "MANUAL_HISTORICAL_RESEARCH"
    assert payload["batch"]["execution_prohibited"] is True
    assert payload["receipt"]["status"] == "COMPLETE"
    candidate = payload["receipt"]["program_runs"][0]["research_candidates"][0]
    assert set(candidate) == {"symbol", "rank", "score", "stock_name", "component_scores"}
    assert not {"price", "target", "order", "account", "cash", "position", "broker"} & set(payload["receipt"])

    batch_id = payload["batch"]["batch_id"]
    loaded = client.get(f"/api/v1/advisory/research-batches/{batch_id}")
    assert loaded.status_code == 200
    assert loaded.json()["receipt"]["receipt_hash"] == payload["receipt"]["receipt_hash"]

    program = client.get(f"/api/v1/advisory/research-batches/{batch_id}/programs/program_single")
    assert program.status_code == 200
    assert program.json()["program_run"]["research_scope"] == "HISTORICAL_RESEARCH_ONLY"
    assert program.json()["program_run"]["execution_prohibited"] is True


def test_historical_research_api_rejects_nonhistorical_source() -> None:
    client = _client()

    response = client.post(
        "/api/v1/advisory/research-batches",
        json={
            "decision_trade_date": "2026-07-10",
            "program_ids": ["program_single"],
            "data_source": "MINIQMT_REALTIME",
            "origin": "MANUAL_HISTORICAL_RESEARCH",
            "requested_at": datetime(2026, 7, 12, 10, tzinfo=UTC).isoformat(),
        },
    )

    assert response.status_code == 422
    assert "HISTORICAL_DATA_REQUIRED" in str(response.json())

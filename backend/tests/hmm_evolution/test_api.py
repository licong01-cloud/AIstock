from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import hmm_evolution
from backend.services.hmm_evolution.errors import SchemaUnavailableError


VALID_SPEC = {
    "schema_version": "hmm_evaluation_spec_v2",
    "base_loop_ref": "qe_20260706_013235_bbd4/Loop8",
    "window_start": "2025-01-02",
    "window_end": "2025-12-31",
    "as_of": {"policy": "latest_common_completed", "requested_date": None},
    "label_horizon_days": 20,
    "universe": {"type": "source_loop_stock_pool_st_pit"},
    "topk": 50,
    "date_coverage_policy": "batch_common_intersection_with_evidence",
    "missing_sector_policy": "neutral_with_evidence",
    "market_forward_return": {"mode": "required", "horizon_trading_days": 10},
    "sort_policy": "score_desc_symbol_asc_v1",
    "metric_version": "hmm_replacement_metrics_v2",
    "recommendation_version": "hmm_recommendation_v1",
}


class _Service:
    def __init__(self) -> None:
        self.captured: dict[str, Any] = {}

    def list_candidates(self, **_kwargs: Any) -> list[Any]:
        return []

    def submit_batch(self, **kwargs: Any) -> tuple[dict[str, Any], bool]:
        self.captured = kwargs
        return (
            {
                "batch_id": "hmmb_test",
                "status": "preparation_queued",
                "candidate_count": len(kwargs["candidate_ids"]),
            },
            True,
        )


class _FailingService(_Service):
    def list_candidates(self, **_kwargs: Any) -> list[Any]:
        raise SchemaUnavailableError("schema missing")


class _UnexpectedValueErrorService(_Service):
    def list_candidates(self, **_kwargs: Any) -> list[Any]:
        raise ValueError("internal conversion failed")


class _Value:
    def __init__(self, value: str) -> None:
        self.value = value


class _AssetReader:
    def __init__(self, *, content_type: str, data: bytes) -> None:
        self.content_type = content_type
        self.data = data
        self.read_calls = 0

    async def stat_asset(self, _task_id: str, _loop_name: str, relative_path: str):
        return SimpleNamespace(
            relative_path=relative_path,
            size_bytes=len(self.data),
            sha256="a" * 64,
            content_type=self.content_type,
            trust_level=_Value("verified_hash"),
            access_mode=_Value("inspection_only"),
        )

    async def read_asset(self, *_args: Any, **_kwargs: Any):
        self.read_calls += 1
        return SimpleNamespace(
            data=self.data,
            receipt=SimpleNamespace(
                sha256="a" * 64,
                trust_level=_Value("verified_hash"),
                access_mode=_Value("inspection_only"),
            ),
        )


def _client(service: Any) -> TestClient:
    app = FastAPI()
    app.include_router(hmm_evolution.router, prefix="/api/v1")
    app.dependency_overrides[hmm_evolution.get_runtime] = lambda: SimpleNamespace(service=service)
    return TestClient(app)


def _asset_client(reader: _AssetReader) -> TestClient:
    app = FastAPI()
    app.include_router(hmm_evolution.router, prefix="/api/v1")
    app.dependency_overrides[hmm_evolution.get_runtime] = lambda: SimpleNamespace(
        qe_asset_reader=reader,
        qe_read_client=SimpleNamespace(),
    )
    return TestClient(app)


def test_list_candidates_uses_success_envelope() -> None:
    response = _client(_Service()).get("/api/v1/hmm-evolution/candidates")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["data"] == []
    assert payload["trace_id"]


def test_batch_creation_uses_fixed_non_gate_recommendation_contract() -> None:
    service = _Service()
    response = _client(service).post(
        "/api/v1/hmm-evolution/batch",
        json={"candidate_ids": ["hmmc_a"], "evaluation_spec": VALID_SPEC},
        headers={"Idempotency-Key": "test-batch-key"},
    )
    assert response.status_code == 202
    assert response.json()["data"]["batch"]["batch_id"] == "hmmb_test"
    assert service.captured["idempotency_key"] == "test-batch-key"
    assert service.captured["recommendation_spec"]["thresholds"] is None
    assert service.captured["recommendation_spec"]["qe_final_review_required"] is True


def test_hmm_error_is_not_hidden_or_nested_under_detail() -> None:
    response = _client(_FailingService()).get("/api/v1/hmm-evolution/candidates")
    assert response.status_code == 503
    payload = response.json()
    assert payload["reason_code"] == "hmm_evolution_schema_unavailable"
    assert "detail" not in payload
    assert payload["trace_id"]


def test_unexpected_value_error_is_not_misclassified_as_user_input() -> None:
    response = _client(_UnexpectedValueErrorService()).get("/api/v1/hmm-evolution/candidates")
    assert response.status_code == 500
    assert response.json()["reason_code"] == "hmm_evolution_unknown_error"


def test_request_validation_has_stable_hmm_reason_code() -> None:
    response = _client(_Service()).post("/api/v1/hmm-evolution/batch", json={})
    assert response.status_code == 400
    payload = response.json()
    assert payload["reason_code"] == "hmm_evolution_invalid_spec"
    assert payload["message"] == "HMM evolution request validation failed"


def test_runtime_is_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("HMM_EVOLUTION_RUNTIME_MODE", raising=False)
    try:
        hmm_evolution.require_api_runtime()
    except Exception as exc:
        assert getattr(exc, "reason_code", None) == "hmm_evolution_runtime_disabled"
    else:  # pragma: no cover - fail-closed contract.
        raise AssertionError("disabled runtime unexpectedly accepted API traffic")


def test_disabled_runtime_returns_stable_503_from_dependency(monkeypatch) -> None:
    monkeypatch.setenv("HMM_EVOLUTION_RUNTIME_MODE", "disabled")
    app = FastAPI()
    app.include_router(hmm_evolution.router, prefix="/api/v1")
    response = TestClient(app).get("/api/v1/hmm-evolution/candidates")
    assert response.status_code == 503
    assert response.json()["reason_code"] == "hmm_evolution_runtime_disabled"


def test_large_asset_range_is_explicit_and_bounded() -> None:
    assert hmm_evolution._parse_range("bytes=100-199", size_bytes=1_000, max_bytes=200) == (  # noqa: SLF001
        100,
        199,
    )
    try:
        hmm_evolution._parse_range("bytes=0-500", size_bytes=1_000, max_bytes=200)  # noqa: SLF001
    except Exception as exc:
        assert getattr(exc, "reason_code", None) == "hmm_evolution_qe_asset_too_large"
    else:  # pragma: no cover
        raise AssertionError("oversized Range unexpectedly passed")


def test_text_asset_content_redacts_secrets_and_local_paths() -> None:
    reader = _AssetReader(
        content_type="text/plain",
        data=b"password=top-secret\nworkspace=F:\\Dev\\AIstock\\private\\run.log",
    )
    response = _asset_client(reader).get(
        "/api/v1/hmm-evolution/qe-assets/qe_task/Loop8/content",
        params={"path": "logs/run.log"},
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert "top-secret" not in payload["text"]
    assert "F:\\Dev\\AIstock" not in payload["text"]
    assert payload["redaction_count"] >= 2
    assert reader.read_calls == 1


def test_binary_asset_content_is_rejected_before_raw_bytes_are_returned() -> None:
    reader = _AssetReader(content_type="application/octet-stream", data=b"secret-binary")
    response = _asset_client(reader).get(
        "/api/v1/hmm-evolution/qe-assets/qe_task/Loop8/content",
        params={"path": "artifacts/pred.pkl"},
    )

    assert response.status_code == 415
    assert response.json()["reason_code"] == "hmm_evolution_qe_asset_content_unsupported"
    assert reader.read_calls == 0

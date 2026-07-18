from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from backend.services.hmm_evolution.candidate_artifact import CandidateArtifactResolver
from backend.services.hmm_evolution.errors import ArtifactHashMismatchError
from backend.services.hmm_evolution.input_adapter import HMMEvaluationInputAdapter
from backend.services.hmm_evolution.market_repository import MarketReturnRead, MarketWatermark
from backend.services.hmm_evolution.models import (
    CandidateLifecycle,
    CandidateRecord,
    EvaluationSpec,
)


class _Source:
    def __init__(
        self,
        predictions,
        labels,
        *,
        pred_sha="b" * 64,
        pred_artifact_rows=None,
        label_artifact_rows=None,
    ):
        self.predictions = predictions
        self.labels = labels
        self.pred_sha = pred_sha
        self.pred_artifact_rows = pred_artifact_rows or len(predictions)
        self.label_artifact_rows = label_artifact_rows or len(labels)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def get_predictions(self, start_date, end_date):
        return self.predictions.copy()

    async def get_labels(self, start_date, end_date, horizon_days=10):
        return self.labels.copy()

    def get_artifact_source_info(self):
        return {
            "pred.pkl": {
                "source": "prediction_store",
                "uri": "cas://qe/pred.pkl",
                "sha256": self.pred_sha,
                "size_bytes": 100,
                "row_count": self.pred_artifact_rows,
                "zero_copy": True,
            },
            "label.pkl": {
                "source": "prediction_store",
                "uri": "cas://qe/label.pkl",
                "sha256": "c" * 64,
                "size_bytes": 100,
                "row_count": self.label_artifact_rows,
                "zero_copy": True,
            },
        }


class _MarketRepository:
    def __init__(self):
        self.watermark_calls = 0
        self.return_calls = 0

    def resolve_watermark(self, *, policy, requested_date):
        self.watermark_calls += 1
        return MarketWatermark(
            requested_policy=policy,
            requested_date=requested_date,
            resolved_as_of_date=date(2026, 1, 30),
            dataset_max_dates={
                "market.trading_calendar": date(2026, 1, 31),
                "market.kline_daily_raw": date(2026, 1, 30),
            },
            calendar_start_date=date(2020, 1, 1),
            calendar_end_date=date(2026, 1, 31),
            pit_mapping_symbol_count=2,
            pit_market_symbol_count=2,
            read_only_transaction={"transaction_read_only": True},
        )

    def read_forward_returns(
        self,
        *,
        symbols,
        trade_dates,
        horizon_trading_days,
        as_of_date,
    ):
        self.return_calls += 1
        frame = pd.DataFrame(
            [(item, symbol, 10, 0.1, date(2026, 1, 20)) for item in trade_dates for symbol in symbols],
            columns=["trade_date", "symbol", "horizon_days", "future_return", "label_date"],
        )
        return MarketReturnRead(
            returns=frame,
            price_row_count=len(frame) * 2,
            requested_symbol_count=len(symbols),
            requested_date_count=len(trade_dates),
            horizon_trading_days=horizon_trading_days,
            as_of_date=as_of_date,
            read_only_transaction={"transaction_read_only": True},
        )


def test_input_adapter_loads_shared_phase0_inputs_once_and_freezes_plan(tmp_path) -> None:
    trade_date = date(2026, 1, 5)
    predictions = pd.DataFrame(
        [(trade_date, "A", 2.0), (trade_date, "B", 1.0)],
        columns=["trade_date", "symbol", "score"],
    )
    labels = pd.DataFrame(
        [(trade_date, "A", 10, 0.1), (trade_date, "B", 10, 0.2)],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    root = tmp_path / "coefficients"
    root.mkdir()
    artifact = root / "candidate.json"
    artifact.write_text(
        json.dumps(
            {
                "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
                "stock_sector_map": {"A": "S", "B": "S"},
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    resolver = CandidateArtifactResolver(artifact_roots={"research": root})
    preview = resolver.preview_configured_local(
        root_alias="research",
        relative_path="candidate.json",
    )
    now = datetime.now(timezone.utc)
    candidate = CandidateRecord(
        candidate_id=preview.candidate_id,
        manifest_hash=preview.manifest_hash,
        display_name="candidate",
        source_type=preview.manifest.source_type,
        source_ref=preview.manifest.source_ref,
        artifact_manifest=preview.manifest,
        algorithm_version=preview.manifest.algorithm_version,
        lifecycle_status=CandidateLifecycle.RESEARCH_ONLY,
        created_by="tester",
        row_version=1,
        created_at=now,
        updated_at=now,
    )
    spec = EvaluationSpec(
        base_loop_ref="qe_task/Loop8",
        window_start=trade_date,
        window_end=trade_date,
        as_of={"policy": "latest_common_completed", "requested_date": None},
        label_horizon_days=10,
        topk=1,
        market_forward_return={"mode": "required", "horizon_trading_days": 10},
    )
    source = _Source(
        predictions,
        labels,
        pred_artifact_rows=20,
        label_artifact_rows=18,
    )
    preferences = []
    market_repository = _MarketRepository()
    adapter = HMMEvaluationInputAdapter(
        candidate_resolver=resolver,
        market_repository=market_repository,
        source_factory=lambda _spec, preference: preferences.append(preference) or source,
    )

    prepared = asyncio.run(adapter.prepare_batch(candidates=[candidate], evaluation_spec=spec))

    assert len(prepared.plans) == 1
    plan = prepared.plans[0]
    assert plan.resolved_as_of_date == date(2026, 1, 30)
    assert plan.source_manifest["artifacts"][0]["zero_copy"] is True
    assert [item["row_count"] for item in plan.source_manifest["artifacts"]] == [20, 18]
    assert [item["selected_row_count"] for item in plan.source_manifest["artifacts"]] == [2, 2]
    assert plan.source_manifest["market_forward_return"]["price_row_count"] == 4
    assert prepared.market_returns is not None
    assert len(prepared.market_returns) == 2
    assert preferences == ["prediction_store_first"]

    checkpoints = []
    replayed = asyncio.run(
        adapter.load_evaluation(
            evaluation={
                "eval_id": "hmme_single",
                "evaluation_spec": spec.model_dump(mode="json"),
                "source_manifest": plan.source_manifest,
            },
            candidate=candidate,
            checkpoint=checkpoints.append,
        )
    )
    assert preferences == ["prediction_store_first", "prediction_store_only"]
    assert replayed.evaluation_dates == (trade_date,)
    assert replayed.market_returns is not None
    assert checkpoints == [
        "before_shared_source_inputs",
        "after_shared_source_inputs",
        "before_shared_market_returns",
        "after_shared_market_returns",
        "before_candidate_artifact_hmme_single",
        "after_candidate_artifact_hmme_single",
    ]
    assert market_repository.watermark_calls == 1
    assert market_repository.return_calls == 2

    batch_replayed = asyncio.run(
        adapter.load_batch_evaluations(
            evaluations=(
                (
                    {
                        "eval_id": "hmme_batch_1",
                        "evaluation_spec": spec.model_dump(mode="json"),
                        "source_manifest": plan.source_manifest,
                    },
                    candidate,
                ),
                (
                    {
                        "eval_id": "hmme_batch_2",
                        "evaluation_spec": spec.model_dump(mode="json"),
                        "source_manifest": plan.source_manifest,
                    },
                    candidate,
                ),
            ),
            candidate_concurrency=2,
        )
    )
    assert set(batch_replayed.inputs_by_eval_id) == {"hmme_batch_1", "hmme_batch_2"}
    assert batch_replayed.errors_by_eval_id == {}
    assert preferences == [
        "prediction_store_first",
        "prediction_store_only",
        "prediction_store_only",
    ]
    assert market_repository.return_calls == 3


def test_input_adapter_disabled_market_mode_never_queries_market_repository(tmp_path) -> None:
    trade_date = date(2026, 1, 5)
    predictions = pd.DataFrame([(trade_date, "A", 1.0)], columns=["trade_date", "symbol", "score"])
    labels = pd.DataFrame(
        [(trade_date, "A", 10, 0.1)],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    root = tmp_path / "coefficients"
    root.mkdir()
    (root / "candidate.json").write_text(
        json.dumps(
            {
                "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
                "stock_sector_map": {"A": "S"},
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    resolver = CandidateArtifactResolver(artifact_roots={"research": root})
    preview = resolver.preview_configured_local(root_alias="research", relative_path="candidate.json")
    now = datetime.now(timezone.utc)
    candidate = CandidateRecord(
        candidate_id=preview.candidate_id,
        manifest_hash=preview.manifest_hash,
        display_name="candidate",
        source_type=preview.manifest.source_type,
        source_ref=preview.manifest.source_ref,
        artifact_manifest=preview.manifest,
        algorithm_version=preview.manifest.algorithm_version,
        lifecycle_status=CandidateLifecycle.RESEARCH_ONLY,
        created_by="tester",
        row_version=1,
        created_at=now,
        updated_at=now,
    )
    spec = EvaluationSpec(
        base_loop_ref="qe_task/Loop8",
        window_start=trade_date,
        window_end=trade_date,
        as_of={"policy": "explicit", "requested_date": trade_date.isoformat()},
        label_horizon_days=10,
        topk=1,
        market_forward_return={"mode": "disabled", "horizon_trading_days": 10},
    )
    market_repository = _MarketRepository()
    adapter = HMMEvaluationInputAdapter(
        candidate_resolver=resolver,
        market_repository=market_repository,
        source_factory=lambda _spec, _preference: _Source(predictions, labels),
    )

    prepared = asyncio.run(adapter.prepare_batch(candidates=[candidate], evaluation_spec=spec))

    assert prepared.market_returns is None
    assert prepared.market_watermark is None
    assert prepared.plans[0].source_manifest["market_forward_return"] == {
        "mode": "disabled",
        "horizon_trading_days": 10,
        "requested_policy": "explicit",
        "requested_date": trade_date.isoformat(),
        "resolved_as_of_date": trade_date.isoformat(),
        "query_executed": False,
    }
    assert market_repository.watermark_calls == 0
    assert market_repository.return_calls == 0


def test_replay_rejects_phase0_artifact_receipt_drift(tmp_path) -> None:
    trade_date = date(2026, 1, 5)
    predictions = pd.DataFrame([(trade_date, "A", 1.0)], columns=["trade_date", "symbol", "score"])
    labels = pd.DataFrame(
        [(trade_date, "A", 10, 0.1)],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    root = tmp_path / "coefficients"
    root.mkdir()
    (root / "candidate.json").write_text(
        json.dumps(
            {
                "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
                "stock_sector_map": {"A": "S"},
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    resolver = CandidateArtifactResolver(artifact_roots={"research": root})
    preview = resolver.preview_configured_local(root_alias="research", relative_path="candidate.json")
    now = datetime.now(timezone.utc)
    candidate = CandidateRecord(
        candidate_id=preview.candidate_id,
        manifest_hash=preview.manifest_hash,
        display_name="candidate",
        source_type=preview.manifest.source_type,
        source_ref=preview.manifest.source_ref,
        artifact_manifest=preview.manifest,
        algorithm_version=preview.manifest.algorithm_version,
        lifecycle_status=CandidateLifecycle.RESEARCH_ONLY,
        created_by="tester",
        row_version=1,
        created_at=now,
        updated_at=now,
    )
    spec = EvaluationSpec(
        base_loop_ref="qe_task/Loop8",
        window_start=trade_date,
        window_end=trade_date,
        as_of={"policy": "explicit", "requested_date": trade_date.isoformat()},
        label_horizon_days=10,
        topk=1,
        market_forward_return={"mode": "disabled", "horizon_trading_days": 10},
    )
    sources = [_Source(predictions, labels), _Source(predictions, labels, pred_sha="d" * 64)]
    adapter = HMMEvaluationInputAdapter(
        candidate_resolver=resolver,
        market_repository=_MarketRepository(),
        source_factory=lambda _spec, _preference: sources.pop(0),
    )
    prepared = asyncio.run(adapter.prepare_batch(candidates=[candidate], evaluation_spec=spec))

    with pytest.raises(ArtifactHashMismatchError, match="receipt changed"):
        asyncio.run(
                adapter.load_evaluation(
                    evaluation={
                        "eval_id": "hmme_receipt_drift",
                        "evaluation_spec": spec.model_dump(mode="json"),
                    "source_manifest": prepared.plans[0].source_manifest,
                },
                candidate=candidate,
            )
        )

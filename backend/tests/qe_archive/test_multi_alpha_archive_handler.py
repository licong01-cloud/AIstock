from __future__ import annotations

from datetime import datetime, timezone

from backend.services.qe_archive.handlers.multi_alpha_combine_archive_handler import (
    MULTI_ALPHA_COMBINE_EVENT_TYPE,
    MULTI_ALPHA_COMBINE_SCHEMA_VERSION,
    MultiAlphaCombineArchiveHandler,
)
from backend.services.qe_archive.models import ClaimedOutboxEvent
from backend.services.qe_archive.multi_alpha_provenance import MultiAlphaProvenanceResolver
from backend.services.qe_archive.worker_service import SUPPORTED_WORKER_EVENT_TYPES, QEArchiveWorkerService


class FakeMultiAlphaArchiveRepository:
    def __init__(self) -> None:
        self.writes: list[dict] = []
        self.multi_alpha_run_ids = ["macb_1", "macb_2"]

    def fetch_archive_run_for_seed(self, seed_run_id: str):
        if seed_run_id != "qear_run_seed_a":
            return None
        return {
            "run_id": seed_run_id,
            "experiment_id": "exp_a",
            "task_id": "task_a",
            "loop_id": "Loop1",
            "loop_index": 1,
            "run_type": "evolution_loop",
            "model_type": "LGBModel",
            "model_family": "tree",
            "factor_set_hash": "hash_a",
            "factor_names": ["factor_a", "factor_b"],
            "factor_count": 2,
            "freq": "day",
            "label_horizon": 5,
            "archived_at": datetime(2026, 6, 28, tzinfo=timezone.utc),
        }

    def resolve_evolution_loop_seed(self, *, task_id: str, loop_index: int):
        if (task_id, loop_index) != ("qe_task", 2):
            return None
        return {
            "run_id": "qear_run_seed_b",
            "experiment_id": "exp_b",
            "task_id": task_id,
            "loop_id": "Loop2",
            "loop_index": loop_index,
            "run_type": "evolution_loop",
            "model_type": "LGBModel",
            "model_family": "tree",
            "factor_set_hash": "hash_a",
            "factor_names": ["factor_a", "factor_b"],
            "factor_count": 2,
            "freq": "day",
            "label_horizon": 5,
            "archived_at": datetime(2026, 6, 28, tzinfo=timezone.utc),
        }

    def fetch_multi_alpha_combine_run(self, run_id: str):
        if run_id == "missing":
            return None
        return {
            "run": {
                "id": run_id,
                "roster_hash": "roster_abc",
                "roster_json": [
                    {
                        "leg_id": "leg_a",
                        "seed_run_ids": ["qear_run_seed_a", "qe_task_L2"],
                        "metadata": {
                            "factor_names": ["factor_a", "factor_b"],
                            "factor_set_hash": "hash_a",
                            "model_type": "LGBModel",
                            "model_family": "tree",
                            "freq": "day",
                            "label_horizon": 5,
                        },
                    },
                    {"leg_id": "leg_b", "seed_run_ids": ["bad_seed"], "metadata": {}},
                ],
                "oos_start": "2026-01-01",
                "oos_end": "2026-06-01",
                "normalize_method": "zscore",
                "walk_forward_json": {"enabled": True, "window": 60},
                "baseline_leg_id": "leg_a",
                "status": "failed",
                "reason": {"logical_status": "partial_failed", "reason_code": "child_failed"},
                "created_at": datetime(2026, 6, 28, tzinfo=timezone.utc),
                "updated_at": datetime(2026, 6, 28, tzinfo=timezone.utc),
            },
            "scheme_results": [
                {
                    "weighting_scheme": "equal",
                    "weights_json": {"leg_a": 0.5, "leg_b": 0.5},
                    "per_window_weights_json": [{"apply_date": "2026-01-02", "weights": {"leg_a": 0.5, "leg_b": 0.5}}],
                    "cagr": 0.1,
                    "max_drawdown": -0.05,
                    "sharpe": 1.2,
                    "calmar": 2.0,
                    "topk_return_20": 0.03,
                    "topk_hit_rate_20": 0.55,
                    "turnover": 12.0,
                    "vs_baseline_sharpe_delta": 0.2,
                    "vs_baseline_calmar_delta": 0.4,
                    "pred_persisted": True,
                    "skipped": False,
                    "skipped_reason": None,
                },
                {
                    "weighting_scheme": "risk_parity",
                    "weights_json": {"leg_a": 0.7, "leg_b": 0.3},
                    "per_window_weights_json": [],
                    "sharpe": 0.9,
                    "calmar": 1.0,
                    "pred_persisted": False,
                    "skipped": True,
                    "skipped_reason": "noncomputable",
                },
            ],
            "loo": [
                {
                    "weighting_scheme": "equal",
                    "dropped_leg_id": "leg_b",
                    "marginal_cagr": 0.02,
                    "marginal_sharpe": 0.1,
                    "marginal_calmar": 0.2,
                }
            ],
        }

    def archive_multi_alpha_bundle(self, **kwargs):
        self.writes.append(kwargs)
        return {
            "run_id": kwargs["run_header"]["run_id"],
            "run_rows": 1,
            "leg_rows": len(kwargs["legs"]),
            "leg_source_rows": len(kwargs["leg_sources"]),
            "scheme_rows": len(kwargs["schemes"]),
            "loo_rows": len(kwargs["loo"]),
        }

    def list_multi_alpha_combine_run_ids(self, **_kwargs):
        return list(self.multi_alpha_run_ids)


def test_multi_alpha_provenance_resolves_archive_run_id_and_evolution_loop_id() -> None:
    resolver = MultiAlphaProvenanceResolver(FakeMultiAlphaArchiveRepository())  # type: ignore[arg-type]

    direct = resolver.resolve_seed("qear_run_seed_a")
    loop = resolver.resolve_seed("qe_task_L2")
    missing = resolver.resolve_seed("unknown_seed")

    assert direct.resolved is True
    assert direct.source_experiment_id == "exp_a"
    assert direct.source_loop_id == "Loop1"
    assert direct.source_run_type == "evolution_loop"
    assert loop.resolved is True
    assert loop.source_task_id == "qe_task"
    assert loop.source_loop_index == 2
    assert missing.resolved is False
    assert missing.seed_ref_kind == "unknown"
    assert missing.resolve_note


def test_multi_alpha_provenance_requires_complete_exp_loop_run_coordinates() -> None:
    class IncompleteRepository(FakeMultiAlphaArchiveRepository):
        def fetch_archive_run_for_seed(self, seed_run_id: str):
            row = super().fetch_archive_run_for_seed(seed_run_id)
            row = dict(row or {})
            row["loop_index"] = None
            return row

    resolver = MultiAlphaProvenanceResolver(IncompleteRepository())  # type: ignore[arg-type]

    result = resolver.resolve_seed("qear_run_seed_a")

    assert result.resolved is False
    assert result.seed_ref_kind == "archive_run_id"
    assert result.source_experiment_id == "exp_a"
    assert result.source_loop_id == "Loop1"
    assert "loop_index" in str(result.resolve_note)


def test_multi_alpha_provenance_records_unresolved_failure_modes() -> None:
    class FailureRepository(FakeMultiAlphaArchiveRepository):
        def fetch_archive_run_for_seed(self, seed_run_id: str):
            if seed_run_id == "qear_run_missing":
                return None
            if seed_run_id == "qear_run_bad":
                return {
                    "run_id": seed_run_id,
                    "experiment_id": "",
                    "task_id": "task_bad",
                    "loop_id": "",
                    "loop_index": "not-an-int",
                    "run_type": "",
                    "factor_names": "not-a-list",
                }
            return super().fetch_archive_run_for_seed(seed_run_id)

        def resolve_evolution_loop_seed(self, *, task_id: str, loop_index: int):
            if task_id == "qe_missing":
                return None
            if task_id == "qe_noarchive":
                return {
                    "task_id": task_id,
                    "loop_id": "Loop3",
                    "loop_index": loop_index,
                    "experiment_id": "exp_noarchive",
                    "run_id": None,
                    "run_type": None,
                }
            if task_id == "qe_incomplete":
                return {
                    "task_id": task_id,
                    "loop_id": "",
                    "loop_index": loop_index,
                    "experiment_id": None,
                    "run_id": "qear_run_incomplete",
                    "run_type": "evolution_loop",
                }
            return super().resolve_evolution_loop_seed(task_id=task_id, loop_index=loop_index)

    resolver = MultiAlphaProvenanceResolver(FailureRepository())  # type: ignore[arg-type]

    empty = resolver.resolve_seed("")
    missing_archive = resolver.resolve_seed("qear_run_missing")
    bad_archive = resolver.resolve_seed("qear_run_bad")
    missing_loop = resolver.resolve_seed("qe_missing_L3")
    no_archive = resolver.resolve_seed("qe_noarchive_L4")
    incomplete_loop = resolver.resolve_seed("qe_incomplete_L5")

    assert empty.resolved is False
    assert empty.resolve_method == "empty_seed_ref"
    assert missing_archive.resolved is False
    assert "not found" in str(missing_archive.resolve_note)
    assert bad_archive.resolved is False
    assert bad_archive.source_factor_names is None
    assert "experiment_id" in str(bad_archive.resolve_note)
    assert "loop_id" in str(bad_archive.resolve_note)
    assert "loop_index" in str(bad_archive.resolve_note)
    assert "run_type" in str(bad_archive.resolve_note)
    assert missing_loop.resolved is False
    assert "qe_evolution_loops row not found" in str(missing_loop.resolve_note)
    assert no_archive.resolved is False
    assert no_archive.source_experiment_id == "exp_noarchive"
    assert "matching qe_archive.run row is missing" in str(no_archive.resolve_note)
    assert incomplete_loop.resolved is False
    assert "experiment_id" in str(incomplete_loop.resolve_note)
    assert "loop_id" in str(incomplete_loop.resolve_note)


def test_multi_alpha_archive_handler_materializes_bundle_and_marks_incomplete_provenance() -> None:
    repo = FakeMultiAlphaArchiveRepository()
    handler = MultiAlphaCombineArchiveHandler(repository=repo, clock=lambda: datetime(2026, 6, 28, tzinfo=timezone.utc))  # type: ignore[arg-type]

    result = handler.archive_run("macb_1")

    assert result["written"] is True
    assert result["rows_upserted"] == 9
    assert result["status"] == "partial_failed"
    assert result["resolved_source_count"] == 2
    assert result["unresolved_source_count"] == 1
    assert repo.writes
    write = repo.writes[-1]
    assert write["archive_run"]["run_type"] == "multi_alpha_combine"
    assert write["archive_run"]["source_system"] == "multi_alpha"
    assert write["run_header"]["status"] == "partial_failed"
    assert write["run_header"]["reason_json"]["reason_code"] == "child_failed"
    leg_a = next(row for row in write["legs"] if row["leg_id"] == "leg_a")
    leg_b = next(row for row in write["legs"] if row["leg_id"] == "leg_b")
    assert leg_a["provenance_complete"] is True
    assert leg_a["factor_names"] == ["factor_a", "factor_b"]
    assert leg_b["provenance_complete"] is False
    unresolved = next(row for row in write["leg_sources"] if row["seed_ref"] == "bad_seed")
    assert unresolved["resolved"] is False
    assert unresolved["resolve_note"]
    scheme = next(row for row in write["schemes"] if row["weighting_scheme"] == "equal")
    assert scheme["scheme_algorithm"] == "equal"
    assert scheme["weights_json"] == {"leg_a": 0.5, "leg_b": 0.5}
    assert scheme["per_window_weights_json"][0]["weights"]["leg_a"] == 0.5
    loo = write["loo"][0]
    assert loo["marginal_sharpe"] == 0.1


def test_multi_alpha_archive_handler_dry_run_and_missing_source_are_explicit() -> None:
    repo = FakeMultiAlphaArchiveRepository()
    handler = MultiAlphaCombineArchiveHandler(repository=repo)  # type: ignore[arg-type]

    dry = handler.archive_run("macb_1", dry_run=True)
    missing = handler.archive_run("missing")

    assert dry["written"] is False
    assert not repo.writes
    assert missing["skipped_reason"] == "multi_alpha_source_run_missing"


def test_multi_alpha_archive_worker_registration_and_dispatch() -> None:
    assert MULTI_ALPHA_COMBINE_EVENT_TYPE in SUPPORTED_WORKER_EVENT_TYPES

    event = ClaimedOutboxEvent(
        event_id="evt_macb",
        event_type=MULTI_ALPHA_COMBINE_EVENT_TYPE,
        source_system="multi_alpha",
        source_id="macb_1",
        source_sub_id="macb_1",
        payload={"schema_version": MULTI_ALPHA_COMBINE_SCHEMA_VERSION, "routing_class": "archive", "run_id": "macb_1"},
    )

    class FakeWorkerRepo(FakeMultiAlphaArchiveRepository):
        def __init__(self) -> None:
            super().__init__()
            self.completed_events: list[str] = []
            self.completed_jobs: list[str] = []
            self.claim_event_types: tuple[str, ...] | None = None

        def claim_outbox_events(self, **kwargs):
            self.claim_event_types = tuple(kwargs.get("event_types") or ())
            return [event]

        def create_archive_job(self, job):
            return "job_macb"

        def complete_archive_job(self, job_id, *, run_id=None, stats=None):
            self.completed_jobs.append(job_id)

        def complete_outbox_event(self, event_id):
            self.completed_events.append(event_id)

        def fail_archive_job(self, *args, **kwargs):
            raise AssertionError("should not fail macb archive job")

        def fail_outbox_event(self, *args, **kwargs):
            raise AssertionError("should not fail macb outbox event")

    repo = FakeWorkerRepo()
    result = QEArchiveWorkerService(repository=repo, enabled=True).run_once(limit=1)  # type: ignore[arg-type]

    assert MULTI_ALPHA_COMBINE_EVENT_TYPE in (repo.claim_event_types or ())
    assert result["completed"] == 1
    assert result["failed"] == 0
    assert repo.completed_events == ["evt_macb"]
    assert repo.completed_jobs == ["job_macb"]


def test_multi_alpha_archive_worker_missing_source_is_audited_skip() -> None:
    event = ClaimedOutboxEvent(
        event_id="evt_missing_macb",
        event_type=MULTI_ALPHA_COMBINE_EVENT_TYPE,
        source_system="multi_alpha",
        source_id="missing",
        source_sub_id="missing",
        payload={"schema_version": MULTI_ALPHA_COMBINE_SCHEMA_VERSION, "routing_class": "archive", "run_id": "missing"},
    )

    class FakeWorkerRepo(FakeMultiAlphaArchiveRepository):
        def __init__(self) -> None:
            super().__init__()
            self.completed_events: list[str] = []
            self.completed_jobs: list[tuple[str, dict | None]] = []
            self.skipped_events: list[tuple[str, str, str]] = []

        def claim_outbox_events(self, **kwargs):
            if kwargs.get("event_types"):
                return [event]
            return []

        def create_archive_job(self, job):
            return "job_missing_macb"

        def complete_archive_job(self, job_id, *, run_id=None, stats=None):
            assert run_id is None
            self.completed_jobs.append((job_id, dict(stats or {})))

        def complete_outbox_event(self, event_id):
            self.completed_events.append(event_id)

        def skip_outbox_event(self, event, *, reason_code, trigger_reason="realtime"):
            self.skipped_events.append((event.event_id, reason_code, trigger_reason))

        def fail_archive_job(self, *args, **kwargs):
            raise AssertionError("missing macb source should be skipped, not failed")

        def fail_outbox_event(self, *args, **kwargs):
            raise AssertionError("missing macb source should be skipped, not retried")

    repo = FakeWorkerRepo()
    result = QEArchiveWorkerService(repository=repo, enabled=True).run_once(limit=1)  # type: ignore[arg-type]

    assert result["completed"] == 0
    assert result["skipped"] == 1
    assert result["failed"] == 0
    assert repo.completed_events == []
    assert repo.skipped_events == [("evt_missing_macb", "multi_alpha_source_run_missing", "realtime")]
    assert repo.completed_jobs[0][0] == "job_missing_macb"
    assert repo.completed_jobs[0][1]["terminal_outbox_status"] == "skipped"

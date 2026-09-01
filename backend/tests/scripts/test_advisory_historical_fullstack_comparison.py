from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import pytest


SCRIPT_PATH = Path("scripts/advisory_historical_fullstack_comparison.py")
SPEC = importlib.util.spec_from_file_location("advisory_historical_fullstack_comparison", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class _Connection:
    def __init__(self) -> None:
        self.closed = False
        self.entered = False
        self.exited = False
        self.committed = False
        self.rolled_back = False
        self.reset_called = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *_args) -> None:
        self.exited = True

    def close(self) -> None:
        self.closed = True

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def reset(self) -> None:
        self.reset_called = True


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.returned = []

    def getconn(self):
        return self.connection

    def putconn(self, connection, *, close=False) -> None:
        self.returned.append((connection, close))


def _install_pool(monkeypatch, connection: _Connection) -> _Pool:
    def base_factory():
        raise AssertionError("base factory is used only as the DSN authority")

    setattr(
        base_factory,
        "_aistock_process_worker_dsn",
        "host=127.0.0.1 port=5432 dbname=aistock user=test connect_timeout=5",
    )
    pool = _Pool(connection)
    monkeypatch.setattr(MODULE, "explicit_historical_range_connection_factory", lambda: base_factory)
    monkeypatch.setattr(MODULE, "ThreadedConnectionPool", lambda **_kwargs: pool)
    return pool


def test_comparison_connection_factory_commits_resets_and_reuses(monkeypatch) -> None:
    connection = _Connection()
    pool = _install_pool(monkeypatch, connection)

    factory = MODULE._comparison_connection_factory()
    with factory() as opened:
        assert opened is connection
        assert connection.closed is False

    assert connection.committed is True
    assert connection.reset_called is True
    assert connection.closed is False
    assert pool.returned == [(connection, False)]
    worker_dsn = getattr(factory, "_aistock_process_worker_dsn")
    assert "max_parallel_workers_per_gather=0" in worker_dsn
    assert "work_mem=64MB" in worker_dsn


def test_comparison_connection_factory_rolls_back_before_reuse(monkeypatch) -> None:
    connection = _Connection()
    pool = _install_pool(monkeypatch, connection)

    factory = MODULE._comparison_connection_factory()
    with pytest.raises(RuntimeError, match="failure injection"):
        with factory():
            raise RuntimeError("failure injection")

    assert connection.committed is False
    assert connection.rolled_back is True
    assert connection.reset_called is True
    assert pool.returned == [(connection, False)]


class _OutcomeService:
    def __init__(self, *, previous_status: str) -> None:
        self.previous_status = previous_status
        self.request = None

    def get_batch(self, _batch_id):
        return {"status": "COMPLETED", "row_version": 133}

    def list_runs(self, _batch_id, *, limit):
        assert limit == 20
        return {
            "items": [
                {"range_run_id": "run-a", "status": "COMPLETED"},
                {"range_run_id": "run-b", "status": "COMPLETED"},
            ]
        }

    def get_operation(self, operation_id):
        if operation_id == "old-operation":
            return {"status": self.previous_status, "result_status": self.previous_status}
        assert operation_id == "new-operation"
        return {"status": "COMPLETED", "result_status": "COMPLETED"}

    def refresh_outcomes(self, _batch_id, request, *, background_tasks):
        self.request = request
        assert background_tasks is not None
        return {"data": {"operation": {"operation_id": "new-operation", "status": "QUEUED"}}}


def _write_outcome_state(root: Path, *, operation_status: str = "RETRYABLE_FAILED") -> dict:
    state = {
        "schema_version": "advisory_fullstack_comparison_long_task_state_v1",
        "contract_hash": "c" * 64,
        "batch_id": "batch",
        "outcome_operation_id": "old-operation",
        "outcome_operation_status": operation_status,
        "outcome_retry_generation": 1,
    }
    (root / "long_task_state_v6.json").write_text(json.dumps(state), encoding="utf-8")
    return state


def test_outcome_retry_uses_a_new_idempotency_generation(monkeypatch, tmp_path: Path) -> None:
    _write_outcome_state(tmp_path)
    service = _OutcomeService(previous_status="RETRYABLE_FAILED")
    monkeypatch.setattr(MODULE, "_label_as_of_trade_date", lambda: date(2026, 8, 14))

    MODULE._refresh_outcomes(
        state_root=tmp_path,
        service=service,
        contract={"contract_hash": "c" * 64},
    )

    assert service.request is not None
    assert service.request.operation_idempotency_key.endswith("-r2")
    persisted = json.loads((tmp_path / "long_task_state_v6.json").read_text(encoding="utf-8"))
    assert persisted["outcome_retry_generation"] == 2
    assert persisted["outcome_operation_id"] == "new-operation"
    assert persisted["outcome_operation_status"] == "COMPLETED"


def test_outcome_retry_rejects_an_active_parent(tmp_path: Path) -> None:
    _write_outcome_state(tmp_path, operation_status="RUNNING")
    service = _OutcomeService(previous_status="RUNNING")

    with pytest.raises(RuntimeError, match="OUTCOME_IN_PROGRESS"):
        MODULE._refresh_outcomes(
            state_root=tmp_path,
            service=service,
            contract={"contract_hash": "c" * 64},
        )

    assert service.request is None


def test_outcome_correction_rejects_an_active_base_operation(tmp_path: Path) -> None:
    _write_outcome_state(tmp_path, operation_status="RUNNING")
    service = _OutcomeService(previous_status="RUNNING")

    with pytest.raises(RuntimeError, match="OUTCOME_IN_PROGRESS"):
        MODULE._correct_outcomes(
            state_root=tmp_path,
            service=service,
            contract={"contract_hash": "c" * 64},
        )

    assert service.request is None


def test_report_renderer_labels_enter_samples_and_gross_returns() -> None:
    projection = {
        group: {
            "1": {
                "sample_count": 2,
                "missing_sample_count": 0,
                "win_rate": win_rate,
                "mean_return": mean_return,
                "median_return": 0.01,
                "monthly": {
                    "2026-05": {
                        "sample_count": 2,
                        "win_rate": win_rate,
                        "mean_return": mean_return,
                    }
                },
            }
        }
        for group, win_rate, mean_return in (
            ("A5", 0.4, 0.01),
            ("B5", 0.5, 0.02),
            ("C5", 0.5, 0.01),
        )
    }
    result = {
        "result_hash": "a" * 64,
        "contract_hash": "b" * 64,
        "a_range_run_id": "run-a",
        "b_range_run_id": "run-b",
        "c_parent_range_run_id": "run-a",
        "bundle_id": "c" * 64,
        "challenger_evidence": {
            "implementation_hash": "e" * 64,
            "completed_day_count": 1,
            "artifact_refs_by_date": {},
        },
        "rank_summary": {
            "day_count": 1,
            "hmm_changed_day_count": 1,
            "risk_changed_day_count": 0,
            "selection_changed_day_count": 0,
            "model_changed_day_count": 1,
            "total_b_excluded_count": 2,
            "mean_b_excluded_count": 2.0,
            "mean_a5_b5_overlap": 4.0,
            "mean_a5_c5_overlap": 2.0,
        },
        "performance_by_projection": {
            "RETURN_NET_ABSOLUTE": projection,
            "RETURN_GROSS": projection,
            "RETURN_NET_EXCESS": projection,
        },
        "matched_deltas_net_absolute": {
            group: {
                "1": {
                    "paired_day_count": 1,
                    "win_rate_difference": 0.1,
                    "win_rate_difference_ci95": (0.05, 0.15),
                    "mean_return_difference": 0.01,
                    "mean_return_difference_ci95": (0.005, 0.015),
                }
            }
            for group in ("B5", "C5")
        },
        "lifecycle": {
            "A20": {
                "actions": {"ENTER": 1},
                "daily": [{"decision_trade_date": "2026-05-15"}],
                "episode_performance_by_projection": {
                    "RETURN_NET_ABSOLUTE": {"sample_count": 1, "win_rate": 1.0}
                },
                "active_at_end_count": 5,
                "active_snapshot_count": 806,
                "exit_reason_counts": {},
            },
            "C5": {
                "action_counts": {"ENTER": 2, "EXIT": 1},
                "daily": [{"decision_trade_date": "2026-05-15"}],
                "completed_episode_count": 1,
                "episode_win_rate": 0.0,
                "active_at_end_count": 1,
                "episode_return_basis": "GROSS_DECISION_MARK",
                "exit_reason_counts": {"TEST_EXIT": 1},
            }
        },
        "market_context": {
            "full_window": {
                "csi300_compounded_return": -0.02,
                "trade_date_count": 1,
                "regime": "DOWN",
            }
        },
        "direct_mark_source": {
            "record_count": 2,
            "records_content_hash": "d" * 64,
        },
    }

    report = MODULE._render_comparison_report(result)

    assert "净绝对收益（主口径）" in report
    assert "毛收益（审计对照）" in report
    assert "固定期限分母仅包含名单生命周期产生的真实 `ENTER` 动作" in report
    assert "50.00%" in report
    assert "EXECUTABLE_NET_ABSOLUTE" in report
    assert "806" not in report
    assert "B5-A5" in report
    assert "[5.00%, 15.00%]" in report
    assert "`TEST_EXIT`" in report
    assert "d" * 64 in report


def test_direct_mark_source_evidence_is_deterministic_and_sensitive() -> None:
    first = MODULE._direct_mark_source_evidence(
        {
            "2026-05-18": {"000002.SZ": 12.5},
            "2026-05-15": {"000001.SZ": 10.0},
        }
    )
    reordered = MODULE._direct_mark_source_evidence(
        {
            "2026-05-15": {"000001.SZ": 10.0},
            "2026-05-18": {"000002.SZ": 12.5},
        }
    )
    changed = MODULE._direct_mark_source_evidence(
        {
            "2026-05-15": {"000001.SZ": 10.0},
            "2026-05-18": {"000002.SZ": 12.6},
        }
    )

    assert first == reordered
    assert first["record_count"] == 2
    assert first["day_record_counts"] == {"2026-05-15": 1, "2026-05-18": 1}
    assert first["records_content_hash"] != changed["records_content_hash"]

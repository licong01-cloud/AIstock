from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.quantevolver.qe_log_store import (
    QE_LIVE_LOG_FILE_COUNT,
    QE_LIVE_LOG_STATE_ROOT_INVALID,
    QE_LIVE_LOG_STATE_ROOT_MISSING,
    QELiveLogConfigurationError,
    QELiveLogStore,
    default_qe_live_log_root,
)


def test_live_log_store_uses_exactly_five_bounded_slots(tmp_path: Path) -> None:
    store = QELiveLogStore(tmp_path, max_file_bytes=320)
    for index in range(40):
        store.append(
            {
                "task_id": "task-1",
                "node_id": "node-1",
                "source_cursor": f"source-{index}",
                "broker_seq": index,
                "payload": {"logs": [f"line-{index}"]},
            }
        )

    paths = store.slot_paths()
    assert len(paths) == QE_LIVE_LOG_FILE_COUNT
    assert [path.name for path in paths] == [f"qe-live-{index}.jsonl" for index in range(5)]
    assert all(path.exists() for path in paths)
    assert all(path.stat().st_size <= 320 for path in paths)
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted(path.name for path in paths)


def test_live_log_store_tail_is_task_filtered_and_read_only_when_empty(tmp_path: Path) -> None:
    store = QELiveLogStore(tmp_path, max_file_bytes=4096)
    assert store.read_task_tail("missing", tail=10)["logs"] == []
    assert list(tmp_path.iterdir()) == []

    store.append({"task_id": "other", "payload": {"logs": ["ignore"]}})
    store.append({"task_id": "task-1", "payload": {"logs": ["one", "two"]}})
    store.append({"task_id": "task-1", "payload": {"logs": ["three"]}})

    result = store.read_task_tail("task-1", tail=2)
    assert result["logs"] == ["two", "three"]
    assert result["source"] == "qe_live_log_ring"


def test_live_log_store_rejects_sixth_slot_or_oversized_record(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly five"):
        QELiveLogStore(tmp_path, file_count=6)

    store = QELiveLogStore(tmp_path, max_file_bytes=64)
    with pytest.raises(ValueError, match="exceeds"):
        store.append({"task_id": "task-1", "payload": {"logs": ["x" * 100]}})


def test_default_live_log_root_requires_explicit_external_state(monkeypatch) -> None:
    monkeypatch.delenv("QE_LIVE_LOG_DIR", raising=False)
    monkeypatch.delenv("RDAGENT_STATE_ROOT", raising=False)

    with pytest.raises(QELiveLogConfigurationError) as error:
        default_qe_live_log_root()

    assert error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_MISSING


def test_default_live_log_root_derives_from_external_rdagent_state_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "rdagent-state"
    monkeypatch.delenv("QE_LIVE_LOG_DIR", raising=False)
    monkeypatch.setenv("RDAGENT_STATE_ROOT", str(state_root))

    assert default_qe_live_log_root() == state_root.resolve() / "qe_live_logs"


def test_rdagent_state_root_must_be_absolute_and_repo_external(monkeypatch) -> None:
    monkeypatch.delenv("QE_LIVE_LOG_DIR", raising=False)
    monkeypatch.setenv("RDAGENT_STATE_ROOT", "relative/rdagent-state")
    with pytest.raises(QELiveLogConfigurationError) as relative_error:
        default_qe_live_log_root()
    assert relative_error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID

    repository_root = Path(__file__).resolve().parents[3]
    monkeypatch.setenv("RDAGENT_STATE_ROOT", str(repository_root / "rdagent-state"))
    with pytest.raises(QELiveLogConfigurationError) as repository_error:
        default_qe_live_log_root()
    assert repository_error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID


def test_explicit_live_log_root_must_be_absolute_and_repo_external(monkeypatch) -> None:
    monkeypatch.setenv("QE_LIVE_LOG_DIR", "relative/qe-live")
    with pytest.raises(QELiveLogConfigurationError) as relative_error:
        default_qe_live_log_root()
    assert relative_error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID

    repository_root = Path(__file__).resolve().parents[3]
    monkeypatch.setenv("QE_LIVE_LOG_DIR", str(repository_root / "rdagent_assets" / "qe_live_logs"))
    with pytest.raises(QELiveLogConfigurationError) as repository_error:
        default_qe_live_log_root()
    assert repository_error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID


def test_explicit_external_live_log_root_is_accepted(tmp_path: Path, monkeypatch) -> None:
    explicit = tmp_path / "qe-live"
    monkeypatch.setenv("QE_LIVE_LOG_DIR", str(explicit))
    monkeypatch.delenv("RDAGENT_STATE_ROOT", raising=False)

    assert default_qe_live_log_root() == explicit.resolve()


def test_live_log_root_rejects_another_git_checkout(tmp_path: Path, monkeypatch) -> None:
    checkout = tmp_path / "other-checkout"
    (checkout / ".git").mkdir(parents=True)
    monkeypatch.setenv("QE_LIVE_LOG_DIR", str(checkout / "rdagent-state" / "qe-live"))

    with pytest.raises(QELiveLogConfigurationError) as error:
        default_qe_live_log_root()

    assert error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID


def test_live_log_root_rejects_linked_worktree_git_file(tmp_path: Path, monkeypatch) -> None:
    checkout = tmp_path / "linked-worktree"
    checkout.mkdir()
    (checkout / ".git").write_text("gitdir: somewhere", encoding="utf-8")
    monkeypatch.setenv("QE_LIVE_LOG_DIR", str(checkout / "state" / "qe-live"))

    with pytest.raises(QELiveLogConfigurationError) as error:
        default_qe_live_log_root()

    assert error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID


def test_invalid_explicit_root_never_falls_back_to_valid_state_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("QE_LIVE_LOG_DIR", "relative/qe-live")
    monkeypatch.setenv("RDAGENT_STATE_ROOT", str(tmp_path / "valid-state"))

    with pytest.raises(QELiveLogConfigurationError) as error:
        default_qe_live_log_root()

    assert error.value.reason_code == QE_LIVE_LOG_STATE_ROOT_INVALID

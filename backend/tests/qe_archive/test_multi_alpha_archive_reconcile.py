from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from backend.services.multi_alpha.combine_backtest import REQUEST_SNAPSHOT_KEY
from scripts.reconcile_qe_multi_alpha_archive import reconcile_archive


def fixture_services():
    source = {"run": {"id": "r", "backtest_config_json": {
        REQUEST_SNAPSHOT_KEY: {"prediction_task_selection": {"baseline_only": True}}},
        "execution_identity_json": {"dataset": {"dataset_manifest_sha256": "frozen"}}}}
    state = {"archive_status": "archive_skipped", "archive_run": None,
             "archive_outbox": {"event_id": "e", "status": "skipped",
                                "error_message": "qe_lifecycle_no_evaluable_result"}}
    calls = []

    def archive(_rid, *, dry_run):
        calls.append(("archive", dry_run))
        if not dry_run:
            state.update(archive_status="archived", archive_run={"status": "succeeded", "research_valid": True})
        return {"run_id": "r", "dry_run": dry_run, "written": not dry_run}

    def complete(event, *, run_id):
        assert run_id == "r"
        calls.append(("complete", event))
        state["archive_outbox"].update(status="completed", error_message=None)
        return True

    repository = SimpleNamespace(fetch_multi_alpha_combine_run=lambda _: deepcopy(source),
                                 complete_skipped_multi_alpha_archive_event=complete)
    handler = SimpleNamespace(archive_run=archive)
    def reader(_):
        return deepcopy(state)
    return source, state, calls, repository, handler, reader


def test_preview_never_writes_or_closes_old_delivery():
    _, state, calls, repository, handler, reader = fixture_services()
    result = reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler, status_reader=reader)
    assert calls == [("archive", True)]
    assert result["apply"] is False and state["archive_status"] == "archive_skipped"


def test_restores_original_delivery_idempotently_and_preserves_skip_history():
    source, _, calls, repository, handler, reader = fixture_services()
    before = deepcopy(source)
    first = reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler, status_reader=reader, apply=True)
    second = reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler, status_reader=reader, apply=True)
    assert first["outbox_before"]["error_message"] == "qe_lifecycle_no_evaluable_result"
    assert first["status"] == second["status"] == "ARCHIVED_VERIFIED_ORIGINAL_RUN_UNCHANGED"
    assert calls.count(("complete", "e")) == 1
    assert source == before


def test_explicit_dataset_mismatch_makes_no_write():
    _, _, calls, repository, handler, reader = fixture_services()
    with pytest.raises(ValueError, match="dataset_identity_mismatch"):
        reconcile_archive("r", expected_manifest="other", repository=repository, handler=handler, status_reader=reader, apply=True)
    assert calls == []


def test_known_skip_is_a_visible_block_not_an_infinite_wait():
    _, _, calls, repository, handler, reader = fixture_services()
    handler.archive_run = lambda _, dry_run: {"run_id": "r", "dry_run": dry_run, "written": False, "skipped_reason": "missing"}
    with pytest.raises(ValueError, match="archive_preview_blocked:missing"):
        reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler, status_reader=reader, apply=True)
    assert calls == []


def test_active_outbox_is_not_overridden():
    _, state, calls, repository, handler, reader = fixture_services()
    state["archive_outbox"]["status"] = "processing"
    with pytest.raises(ValueError, match="outbox_not_terminal_known_skip"):
        reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler, status_reader=reader, apply=True)
    assert calls == []


def test_failed_archive_never_completes_delivery():
    _, _, calls, repository, handler, reader = fixture_services()
    original = handler.archive_run
    handler.archive_run = lambda rid, dry_run: original(rid, dry_run=True)
    with pytest.raises(ValueError, match="write_not_completed"):
        reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler, status_reader=reader, apply=True)
    assert not any(c[0] == "complete" for c in calls)


def test_concurrent_event_change_is_not_reported_as_completed():
    _, state, _, repository, handler, reader = fixture_services()
    def moved_event(_event, *, run_id):
        state["archive_outbox"]["status"] = "processing"
        return False
    repository.complete_skipped_multi_alpha_archive_event = moved_event
    with pytest.raises(ValueError, match="delivery_incomplete"):
        reconcile_archive("r", expected_manifest="frozen", repository=repository, handler=handler,
                          status_reader=reader, apply=True)


@pytest.mark.parametrize("bad", [None, "pending_delivery", "wrong_run", "invalid_research", "http_only"])
def test_runtime_smoke_requires_a_real_accepted_account(bad):
    from scripts.aistock_issue_workflow import _business_smoke_semantic_contract
    contract, validate = _business_smoke_semantic_contract('/api/v1/multi-alpha/combine-backtest/runs/r/archive-status')
    assert contract == "qe_multi_alpha_archive_status"
    payload = {"status": "success", "data": {"run_id": "r", "archive_status": "archived",
        "archive_run": {"run_id": "r", "status": "succeeded", "research_valid": True},
        "archive_outbox": {"event_id": "e", "status": "completed", "error_message": None}}}
    if bad == "pending_delivery":
        payload["data"]["archive_outbox"]["status"] = "pending"
    elif bad == "wrong_run":
        payload["data"]["archive_run"]["run_id"] = "other"
    elif bad == "invalid_research":
        payload["data"]["archive_run"]["research_valid"] = False
    elif bad == "http_only":
        payload = {"status": "success"}
    assert validate(payload)[0] == ("passed" if bad is None else "failed")

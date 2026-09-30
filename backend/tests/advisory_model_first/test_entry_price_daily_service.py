import pytest

from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def test_deadline_applies_remaining_budget_to_each_statement_and_reuses_readonly_connection():
    now = [0.0]
    budget = EntryWorkBudget(monotonic=lambda: now[0])
    calls, connections = [], []

    class Cursor:
        def execute(self, sql, params):
            calls.append((sql, params))
        def close(self):
            pass

    class Connection:
        closed = False
        rollbacks = 0
        def cursor(self, **kwargs):
            return Cursor()
        def set_session(self, **kwargs):
            assert kwargs["readonly"] and not kwargs["autocommit"]
        def rollback(self):
            self.rollbacks += 1
        def close(self):
            self.closed = True

    def connector(**kwargs):
        assert 2 <= kwargs["connect_timeout"] <= 5
        result = Connection()
        connections.append(result)
        return result

    session = BoundedEntryReadSession(budget, connector=connector)
    with session.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1", ())
            now[0] = 28
            cursor.execute("SET LOCAL statement_timeout = %s", (300_000,))
            cursor.execute("SELECT 2", ())
            now[0] = 30
            with pytest.raises(AdvisoryModelFirstError, match="budget"):
                cursor.execute("SELECT 3", ())
    assert [params[0] for sql, params in calls if "statement_timeout" in sql] == [30_000, 2_000, 2_000]
    assert all(sql != "SELECT 3" for sql, _ in calls)
    assert len(connections) == 1 and connections[0].rollbacks == 1
    session.close()
    assert connections[0].closed


def test_no_connection_is_opened_when_remaining_budget_cannot_cover_connect_timeout():
    now = [0.0]
    budget = EntryWorkBudget(monotonic=lambda: now[0])
    def forbidden(**kwargs):
        raise AssertionError("connection must not start without budget")
    session = BoundedEntryReadSession(budget, connector=forbidden)
    now[0] = 29
    with pytest.raises(AdvisoryModelFirstError, match="deferred"):
        with session.connection():
            pytest.fail("not reachable")


def test_qe_guard_checks_all_pages_and_resolves_finished_evolution_template():
    from backend.services.advisory_model_first.entry_price_daily_service import QEEntryResourceGuard
    calls = []
    def get(path, params):
        calls.append((path, params))
        if "/evolution/tasks/" in path:
            return dict(status="success", data={"task_id": "qe_task", "status": "completed"})
        # QE's expanded-child view paginates parents, not returned rows. Use
        # the flat public view, which includes both parents and child runs.
        assert params["include_children"] == "false"
        if params["offset"] == 0:
            row = dict(experiment_id="template", status="created", canonical_status="planned", qe_task_id="qe_task",
                       progress_summary={"kind": "evolution", "task_id": "qe_task", "status": "completed"})
            return dict(ok=True, total=2, offset=0, items=[row], has_more=True)
        return dict(ok=True, total=2, offset=1, items=[dict(experiment_id="finished", status="completed", canonical_status="completed")], has_more=False)
    result = QEEntryResourceGuard(get_json=get).check(EntryWorkBudget())
    assert result == {"status": "READ_SNAPSHOT_IDLE", "checked_experiments": 2, "exclusive_lease": False}
    assert len(calls) == 3


@pytest.mark.parametrize("violation", ["pending", "unknown", "missing_page", "parent_running", "paused", "legacy_unknown"])
def test_qe_guard_never_turns_uncertain_activity_into_idle(violation):
    from backend.services.advisory_model_first.entry_price_daily_service import QEEntryResourceGuard
    def get(path, params):
        if "/evolution/tasks/" in path:
            return dict(status="success", data={"task_id": "qe_task", "status": "running"})
        row = dict(experiment_id="test", canonical_status="completed", status="completed")
        if violation == "pending":
            row["canonical_status"] = "pending"
        if violation == "unknown":
            row["canonical_status"] = "unrecognized"
        if violation == "paused":
            row["progress_summary"] = {"status": "paused"}
        if violation == "legacy_unknown":
            row["canonical_status"] = None
        if violation == "parent_running":
            row.update(status="created", canonical_status="planned", qe_task_id="qe_task",
                       progress_summary={"kind": "evolution", "task_id": "qe_task", "status": "completed"})
        return dict(ok=True, total=2 if violation == "missing_page" else 1, offset=0, items=[row], has_more=False)
    result = QEEntryResourceGuard(get_json=get).check(EntryWorkBudget())
    assert result["status"] == "WAITING_RESOURCE" and not result["exclusive_lease"]


def daily_fixture(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from backend.services.advisory_model_first.entry_price_daily_service import AdvisoryEntryPriceDailyService, _open
    from backend.services.advisory_model_first.entry_price_role_binding import build_entry_price_role
    from backend.services.advisory_model_first.entry_price_confirmation_contracts import EntryPriceConfirmationSettlementDay
    from backend.services.advisory_model_first.research_control import evidence_reference_for_file
    from backend.tests.advisory_model_first.test_entry_price_service import integrated_service, T
    from backend.tests.advisory_model_first.test_entry_price_role_binding import store_and_role
    store, original, _ = store_and_role(tmp_path, monkeypatch)
    entry, _, args = integrated_service()
    clock = [_open(T) - timedelta(minutes=10)]
    role = build_entry_price_role(**{**original.functional_payload(), "created_at": clock[0] - timedelta(hours=1),
                                    "effective_from_target_date": T})
    request = SimpleNamespace(scope=role.scope, request_sha256=role.confirmation_request_sha256,
                              control=SimpleNamespace(q10=-0.04, q50=0, q90=0.04))
    store._now = lambda: clock[0]
    store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="unit-only")
    original_versions = entry._programs.recommendation_list_versions
    entry._programs.recommendation_list_versions = lambda *a, **kw: [dict(row, version_status="PUBLISHED") for row in original_versions(*a, **kw)]
    original_detail = entry._programs.recommendation_list_version_detail
    def detail(identity):
        result = original_detail(identity)
        result["list_version"].update(program_id=role.program_id, version_status="PUBLISHED", created_at=(clock[0] - timedelta(hours=2)).isoformat())
        return result
    entry._programs.recommendation_list_version_detail = detail
    calls = []
    real = entry.evaluate_day
    def evaluate(**kwargs):
        calls.append("predict")
        return real(**kwargs)
    entry.evaluate_day = evaluate
    class Outcomes:
        missing = False
        def load(self, *, symbols, target_trade_date):
            calls.append("settle")
            return EntryPriceConfirmationSettlementDay(target_trade_date=target_trade_date,
                outcomes=[dict(symbol=symbol, market_status="NOT_APPLICABLE" if i else "UNAVAILABLE" if self.missing else "AVAILABLE",
                    raw_open=None if i or self.missing else 10.0,
                    reason_code="AUTHORITATIVE_SUSPENSION" if i else "UNKNOWN" if self.missing else None)
                    for i, symbol in enumerate(symbols)], source_sha256="d" * 64)
    class Resource:
        blocked = False
        def check(self, budget):
            budget.check()
            return {"status": "WAITING_RESOURCE" if self.blocked else "READ_SNAPSHOT_IDLE"}
    outcome, resource = Outcomes(), Resource()
    service = AdvisoryEntryPriceDailyService(model_root=tmp_path, program_service=entry._programs, day_service=entry,
        outcome_source=outcome, role_store=store, resource_guard=resource,
        confirmation_reader=lambda *_a, **_kw: (request, {}), now_provider=lambda: clock[0])
    assert evidence_reference_for_file(role.confirmation.artifact_uri, role=role.confirmation.role) == role.confirmation
    assert clock[0] < datetime.now(timezone.utc)  # This is a historical fake-clock test, not natural evidence.
    return service, role, clock, calls, outcome, resource


def test_capture_readonly_settle_exact_retry_uses_frozen_predictions(tmp_path, monkeypatch):
    from datetime import timedelta
    from backend.services.advisory_model_first.entry_price_daily_service import _mature
    service, role, clock, calls, _, _ = daily_fixture(tmp_path, monkeypatch)
    args = dict(program_id=role.program_id, target_trade_date=role.effective_from_target_date)
    before = sorted(str(p) for p in tmp_path.rglob("*"))
    assert service.read_price(**args)["reason_code"] == "ENTRY_PRICE_WAITING_CAPTURE"
    assert before == sorted(str(p) for p in tmp_path.rglob("*"))
    capture = service.capture(**args)
    assert capture["status"] == "PUBLISHED"
    assert service.capture(**args) == capture
    assert calls == ["predict"]
    assert service.read_price(**args) == capture["prediction"]["envelope"]
    assert service.settle(**args)["status"] == "WAITING_MATURITY"
    clock[0] = _mature(role.effective_from_target_date) + timedelta(days=2)
    result = service.settle(**args)
    assert result["status"] == "SETTLED" and result["suspended"] == 1 and result["valid_rows"] == 1
    assert service.settle(**args) == result
    assert calls == ["predict", "settle"]
    assert service.status(program_id=role.program_id)["quality"]["valid_rows"] == 1


def test_resource_wait_and_missed_capture_never_create_prediction(tmp_path, monkeypatch):
    from backend.services.advisory_model_first.entry_price_daily_service import _open
    service, role, clock, calls, _, resource = daily_fixture(tmp_path, monkeypatch)
    args = dict(program_id=role.program_id, target_trade_date=role.effective_from_target_date)
    resource.blocked = True
    assert service.capture(**args)["status"] == "WAITING_RESOURCE"
    resource.blocked = False
    clock[0] = _open(role.effective_from_target_date)
    assert service.capture(**args)["status"] == "MISSED_CAPTURE"
    assert service.read_price(**args)["reason_code"] == "ENTRY_PRICE_NOT_CAPTURED"
    assert calls == [] and not (tmp_path / "entry_price_daily_predictions").exists()


def test_unknown_outcome_retained_as_failed_input_and_artifact_corruption_rejected(tmp_path, monkeypatch):
    import json
    from backend.services.advisory_model_first.entry_price_daily_service import _mature
    service, role, clock, _, outcome, _ = daily_fixture(tmp_path, monkeypatch)
    target = role.effective_from_target_date
    args = dict(program_id=role.program_id, target_trade_date=target)
    service.capture(**args)
    clock[0] = _mature(target)
    outcome.missing = True
    result = service.settle(**args)
    assert result["status"] == "FAILED_INPUT" and result["market_unknown"] == 1
    assert len(result["settlement"]["outcomes"]) == 2
    path = service._path(role, target, "predictions")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["prediction"]["envelope"]["available_count"] = 0
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="hash"):
        service.read_price(**args)


@pytest.mark.parametrize("field", ["package_id", "universe_identity_sha256", "review_policy_sha256", "training_lineage"])
def test_rehashed_prediction_cannot_change_confirmed_role_scope(tmp_path, monkeypatch, field):
    import json
    from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256
    service, role, _, _, _, _ = daily_fixture(tmp_path, monkeypatch)
    target = role.effective_from_target_date
    args = dict(program_id=role.program_id, target_trade_date=target)
    service.capture(**args)
    path = service._path(role, target, "predictions")
    payload = json.loads(path.read_text(encoding="utf-8"))
    envelope = payload["prediction"]["envelope"]
    if field == "training_lineage":
        envelope[field]["parent_bundle_id"] = "0" * 64
    else:
        envelope[field] = "other-package" if field == "package_id" else "0" * 64
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = canonical_json_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="identity"):
        service.read_price(**args)


def test_busy_publication_lock_defers_instead_of_waiting(tmp_path):
    from backend.services.advisory_model_first.entry_price_daily_service import _daily_lock
    path = tmp_path / "artifact.json"
    with _daily_lock(path, EntryWorkBudget()):
        with pytest.raises(AdvisoryModelFirstError) as error:
            with _daily_lock(path, EntryWorkBudget()):
                pytest.fail("a concurrent writer must not enter")
        assert error.value.reason_code == "ADVISORY_ENTRY_DAILY_LOCK_BUSY"


def test_disabled_role_still_settles_prior_capture_without_rerunning_model(tmp_path, monkeypatch):
    from backend.services.advisory_model_first.entry_price_daily_service import _mature
    service, role, clock, calls, _, _ = daily_fixture(tmp_path, monkeypatch)
    args = dict(program_id=role.program_id, target_trade_date=role.effective_from_target_date)
    captured = service.capture(**args)
    service._roles.rollback(model_root=tmp_path, program_id=role.program_id, binding_version_id=role.binding_version_id,
                            expected_current_role_sha256=role.role_sha256, authorization_ref="unit-disable")
    service._programs.list_programs = lambda **_kw: []
    clock[0] = _mature(role.effective_from_target_date)
    assert service.read_price(**args, list_version_id=captured["input_identity"]["list_version_id"]) == captured["prediction"]["envelope"]
    assert service.read_price(**args)["reason_code"] == "ENTRY_PRICE_NOT_CONFIGURED"
    result = service.run_once()
    assert [row["status"] for row in result["results"]] == ["SETTLED"]
    assert calls == ["predict", "settle"]
    assert service.run_once()["results"] == []


@pytest.mark.parametrize("change", ["clock", "pointer", "resource"])
def test_capture_rechecks_critical_state_before_publishing(tmp_path, monkeypatch, change):
    from backend.services.advisory_model_first.entry_price_daily_service import _open
    service, role, clock, _, _, resource = daily_fixture(tmp_path, monkeypatch)
    original = service._entry.evaluate_day
    def evaluate(**kwargs):
        result = original(**kwargs)
        if change == "clock":
            clock[0] = _open(role.effective_from_target_date)
        elif change == "pointer":
            service._roles.rollback(model_root=tmp_path, program_id=role.program_id, binding_version_id=role.binding_version_id,
                                    expected_current_role_sha256=role.role_sha256, authorization_ref="unit-disable")
        else:
            resource.blocked = True
        return result
    service._entry.evaluate_day = evaluate
    args = dict(program_id=role.program_id, target_trade_date=role.effective_from_target_date)
    if change == "pointer":
        with pytest.raises(AdvisoryModelFirstError, match="pointer changed"):
            service.capture(**args)
    else:
        assert service.capture(**args)["status"] == ("MISSED_CAPTURE" if change == "clock" else "WAITING_RESOURCE")
    assert service._read(role, role.effective_from_target_date, "predictions") is None


def test_newer_draft_does_not_hide_published_capture_input(tmp_path, monkeypatch):
    service, role, _, calls, _, _ = daily_fixture(tmp_path, monkeypatch)
    original = service._programs.recommendation_list_versions
    def versions(*a, **kw):
        rows = original(*a, **kw)
        return [dict(rows[0], list_version_id="draft-only", version_status="DRAFT"), *rows]
    service._programs.recommendation_list_versions = versions
    # The legacy fixture detail resolves its first row; pin its persisted detail independently of the list page.
    old_detail = service._programs.recommendation_list_version_detail
    def detail(identity):
        assert identity != "draft-only"
        value = old_detail(identity)
        value["list_version"].update(list_version_id=identity, version_status="PUBLISHED")
        return value
    service._programs.recommendation_list_version_detail = detail
    result = service.capture(program_id=role.program_id, target_trade_date=role.effective_from_target_date)
    assert result["status"] == "PUBLISHED" and calls == ["predict"]


def test_budget_exhausting_capture_cannot_starve_pending_settlement(tmp_path, monkeypatch):
    from datetime import timedelta
    from backend.services.advisory_model_first.entry_price_daily_service import _mature
    service, role, clock, _, _, _ = daily_fixture(tmp_path, monkeypatch)
    target = role.effective_from_target_date
    service.capture(program_id=role.program_id, target_trade_date=target)
    clock[0] = _mature(target)
    service._programs.program.status = "ENABLED"
    service._programs.program.review_schedule = {"frequency": "daily_after_close"}
    service._programs.list_programs = lambda **_kw: [service._programs.program]
    service._programs.calendar_provider.next_trading_day = lambda *_a, **_kw: target + timedelta(days=1)
    monotonic, events = [0.0], []
    service._budget = lambda: EntryWorkBudget(monotonic=lambda: monotonic[0])
    def capture(*_args):
        events.append("capture")
        monotonic[0] += 31
        return {"status": "DEFERRED"}
    def settle(*_args):
        events.append("settlement")
        return {"status": "SETTLED"}
    service._capture, service._settle = capture, settle
    service.run_once()
    assert events == ["capture"]
    service.run_once()
    assert events == ["capture", "settlement", "capture"]

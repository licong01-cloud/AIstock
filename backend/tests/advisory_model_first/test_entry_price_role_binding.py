from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.entry_price_role_binding import EntryPriceRoleStore, build_entry_price_role
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_entry_price_service import integrated_service


def store_and_role(tmp_path, monkeypatch):
    from backend.services.advisory_model_first import model_bundle, price_range_runtime_bundle
    service, _, values = integrated_service()
    scope = values["scope"]
    confirmation_file = tmp_path / "evaluation.json"
    confirmation_file.write_text('{"fixture":"confirmation"}', encoding="utf-8")
    confirmation = SimpleNamespace(scope=scope, request_sha256="a" * 64, projection_producer_version="advisory_entry_price_core_v1")

    class Preflight:
        blocked = False
        def preflight(self, **kwargs):
            return dict(overall_status="BLOCKED" if self.blocked else "READY_BASELINE_ONLY",
                        blockers=["PACKAGE_RETIRED"] if self.blocked else [],
                        package={"manifest_sha256": scope.package_manifest_sha256})

    preflight = Preflight()
    monkeypatch.setattr(price_range_runtime_bundle, "load_frozen_price_range_bundle", lambda **kwargs: None)
    monkeypatch.setattr(model_bundle, "load_frozen_research_bundle", service._parent_loader)
    store = EntryPriceRoleStore(
        program_service=service._programs, preflight=preflight, confirmation_reader=lambda *_a, **_kw: (confirmation, {}),
        now_provider=lambda: datetime(2026, 9, 28, 10, tzinfo=timezone.utc),
    )
    role = build_entry_price_role(
        program_id=values["program_id"], binding_version_id=values["binding_version_id"], scope=scope,
        confirmation=evidence_reference_for_file(confirmation_file, role="ENTRY_PRICE_CONFIRMATION"),
        confirmation_request_sha256=confirmation.request_sha256, effective_from_target_date=date(2026, 9, 29),
        created_at=datetime(2026, 9, 28, 9, tzinfo=timezone.utc),
    )
    return store, role, preflight


def test_role_scope_isolated_cas_and_disable_retry(tmp_path, monkeypatch):
    store, role, _ = store_and_role(tmp_path, monkeypatch)
    args = dict(model_root=tmp_path, program_id=role.program_id, binding_version_id=role.binding_version_id)
    assert store.read(**args) is None
    assert not (tmp_path / "entry_price_roles").exists()
    result = store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="user-fixture")
    assert result["status"] == "APPLIED"
    assert store.read(**args)[0] == role
    assert not (tmp_path / "program_bindings").exists()
    assert not (tmp_path / "price_range_bindings").exists()
    assert store.read(**dict(args, program_id="advp_other")) is None
    assert store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="user-fixture")["status"] == "EXACT_RETRY"
    with pytest.raises(AdvisoryModelFirstError, match="predecessor"):
        store.publish(role, model_root=tmp_path, expected_current_role_sha256="b" * 64, authorization_ref="user-fixture")
    with pytest.raises(AdvisoryModelFirstError, match="compare-and-swap"):
        store.rollback(**args, expected_current_role_sha256="b" * 64, authorization_ref="user-fixture")
    store.rollback(**args, expected_current_role_sha256=role.role_sha256, authorization_ref="user-fixture")
    assert not store.read(**args)[1]["enabled"]
    assert store.rollback(**args, expected_current_role_sha256=role.role_sha256, authorization_ref="user-fixture")["status"] == "EXACT_RETRY"


def test_retired_package_and_mutated_confirmation_cannot_activate(tmp_path, monkeypatch):
    store, role, preflight = store_and_role(tmp_path, monkeypatch)
    preflight.blocked = True
    with pytest.raises(AdvisoryModelFirstError, match="retired"):
        store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="user-fixture")
    preflight.blocked = False
    from pathlib import Path
    Path(role.confirmation.artifact_uri).write_text('{"changed":true}', encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="changed"):
        store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="user-fixture")
    assert not (store.directory(model_root=tmp_path, program_id=role.program_id, binding_version_id=role.binding_version_id) / "active.json").exists()


def test_changed_binding_and_path_traversal_fail_closed(tmp_path, monkeypatch):
    store, role, _ = store_and_role(tmp_path, monkeypatch)
    store._programs.binding["binding_version_id"] = "advb_other"
    with pytest.raises(AdvisoryModelFirstError, match="Program"):
        store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="user-fixture")
    with pytest.raises(AdvisoryModelFirstError):
        store.read(model_root=tmp_path, program_id="../other", binding_version_id=role.binding_version_id)


def test_new_binding_cannot_backfill_past_open(tmp_path, monkeypatch):
    store, role, _ = store_and_role(tmp_path, monkeypatch)
    store._now = lambda: datetime(2026, 9, 29, 2, tzinfo=timezone.utc)
    with pytest.raises(AdvisoryModelFirstError, match="capture window"):
        store.publish(role, model_root=tmp_path, expected_current_role_sha256=None, authorization_ref="user-fixture")

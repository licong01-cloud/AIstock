from __future__ import annotations

from datetime import date, datetime, time, timezone
import json
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field, TypeAdapter, model_validator

from .entry_price_contracts import EntryPriceScope, Nonempty, Sha256, _Contract
from .entry_price_confirmation import read_confirmed_entry_price_artifact
from .errors import AdvisoryModelFirstError
from .model_binding_resolution import (
    AdvisoryModelBindingResolver, _atomic_write_descriptor, _exclusive_descriptor_lock,
)
from .price_range_contracts import canonical_json_sha256
from .research_control import evidence_reference_for_file
from .research_control_contracts import EvidenceReferenceV1


class EntryPriceRoleBindingV1(_Contract):
    schema_version: Literal["advisory_entry_price_role_binding_v1"] = "advisory_entry_price_role_binding_v1"
    role: Literal["ENTRY_PRICE"] = "ENTRY_PRICE"
    projection_producer_version: Literal[
        "advisory_entry_price_core_v1", "advisory_entry_price_core_v2"
    ] = "advisory_entry_price_core_v1"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    activation_mode: Literal["PRICE_DISTRIBUTION_SHADOW_ONLY"] = "PRICE_DISTRIBUTION_SHADOW_ONLY"
    program_id: str = Field(pattern=r"^advp_[A-Za-z0-9_-]{1,123}$")
    binding_version_id: str = Field(pattern=r"^advb_[A-Za-z0-9_-]{1,123}$")
    scope: EntryPriceScope
    confirmation: EvidenceReferenceV1
    confirmation_request_sha256: Sha256
    effective_from_target_date: date
    created_at: datetime
    previous_role_sha256: Sha256 | None = None
    role_sha256: Sha256

    @model_validator(mode="after")
    def check_identity(self):
        if self.created_at.utcoffset() is None:
            raise ValueError("entry role creation timestamp must have a timezone")
        if self.role_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("entry role hash mismatch")
        return self

    def functional_payload(self):
        return self.model_dump(mode="json", exclude={"role_sha256"})


def build_entry_price_role(**values) -> EntryPriceRoleBindingV1:
    values = dict(values)
    values.setdefault("created_at", datetime.now(timezone.utc))
    payload = {}
    for name, field in EntryPriceRoleBindingV1.model_fields.items():
        if name == "role_sha256":
            continue
        adapter = TypeAdapter(field.rebuild_annotation())
        value = values[name] if name in values else field.get_default(call_default_factory=True)
        payload[name] = adapter.dump_python(adapter.validate_python(value), mode="json")
    if set(values) - set(payload):
        raise ValueError("unknown entry role fields")
    return EntryPriceRoleBindingV1(**payload, role_sha256=canonical_json_sha256(payload))


class EntryPriceRoleStore:
    """One immutable ENTRY_PRICE history per Program/binding; independent of Ranking pointers."""

    def __init__(self, *, program_service=None, preflight=None, confirmation_reader=None, now_provider=None):
        self._programs = program_service
        self._preflight = preflight
        self._confirmation_reader = confirmation_reader or read_confirmed_entry_price_artifact
        self._now = now_provider or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def directory(*, model_root, program_id, binding_version_id) -> Path:
        # Existing ID validation performs no writes; the separate role subtree gets the same escape check.
        AdvisoryModelBindingResolver().descriptor_path(
            model_root=model_root, program_id=program_id, binding_version_id=binding_version_id,
        )
        root = Path(model_root).resolve()
        target = root / "entry_price_roles" / program_id / binding_version_id
        if not target.resolve().is_relative_to(root) or target.resolve() != target.absolute():
            _role_error("entry role path escapes model root")
        return target

    def read(self, *, model_root, program_id, binding_version_id):
        directory = self.directory(model_root=model_root, program_id=program_id, binding_version_id=binding_version_id)
        active = directory / "active.json"
        if active.resolve() != active.absolute():
            _role_error("active entry pointer cannot be redirected by a symlink")
        if not active.exists():
            return None
        pointer = _read_json(active)
        digest = pointer.pop("pointer_sha256", None)
        if digest != canonical_json_sha256(pointer) or set(pointer) != {"role_sha256", "enabled", "activated_at", "authorization_ref"}:
            _role_error("active entry pointer content is invalid")
        if not isinstance(pointer["enabled"], bool) or not isinstance(pointer["authorization_ref"], str) or not pointer["authorization_ref"].strip():
            _role_error("active entry pointer state or authority is invalid")
        activated_at = datetime.fromisoformat(pointer["activated_at"])
        if activated_at.utcoffset() is None:
            _role_error("entry activation timestamp lacks timezone")
        role = self._read_version(directory, pointer["role_sha256"], program_id, binding_version_id)
        return role, {**pointer, "pointer_sha256": digest}

    def prepare(self, *, model_root, program_id, binding_version_id, scope, confirmation_path,
                effective_from_target_date, expected_current_role_sha256=None):
        scope = EntryPriceScope.model_validate(scope)
        confirmation, _ = self._confirmation_reader(confirmation_path, recompute=True)
        if confirmation.scope != scope:
            _role_error("confirmation scope differs from proposed entry role")
        role = build_entry_price_role(
            program_id=program_id, binding_version_id=binding_version_id, scope=scope,
            confirmation=evidence_reference_for_file(confirmation_path, role="ENTRY_PRICE_CONFIRMATION"),
            confirmation_request_sha256=confirmation.request_sha256,
            projection_producer_version=confirmation.projection_producer_version,
            effective_from_target_date=effective_from_target_date, previous_role_sha256=expected_current_role_sha256,
            created_at=self._now(),
        )
        self.check_scope(role, model_root=model_root)
        current = self.read(model_root=model_root, program_id=program_id, binding_version_id=binding_version_id)
        if (current[0].role_sha256 if current else None) != expected_current_role_sha256:
            _role_error("entry role changed before preparation", "ADVISORY_ENTRY_ROLE_CAS_CONFLICT")
        return role

    def publish(self, role: EntryPriceRoleBindingV1, *, model_root, expected_current_role_sha256, authorization_ref: str):
        role = EntryPriceRoleBindingV1.model_validate(role)
        _authority(authorization_ref)
        directory = self.directory(model_root=model_root, program_id=role.program_id, binding_version_id=role.binding_version_id)
        with _exclusive_descriptor_lock(directory / "active.json"):
            current = self.read(model_root=model_root, program_id=role.program_id, binding_version_id=role.binding_version_id)
            if current and current[0] == role and current[1]["enabled"]:
                if expected_current_role_sha256 != role.previous_role_sha256:
                    _role_error("entry retry must carry the original expected predecessor", "ADVISORY_ENTRY_ROLE_CAS_CONFLICT")
                self.check_scope(role, model_root=model_root)
                self._check_confirmation(role)
                return {"status": "EXACT_RETRY", "role_sha256": role.role_sha256, "enabled": True}
            if (current[0].role_sha256 if current else None) != expected_current_role_sha256 or role.previous_role_sha256 != expected_current_role_sha256:
                _role_error("entry role compare-and-swap failed", "ADVISORY_ENTRY_ROLE_CAS_CONFLICT")
            now = self._now()
            if now.utcoffset() is None or role.created_at > now or now >= _target_open(role.effective_from_target_date):
                _role_error("new entry role must begin in a future valid capture window")
            self.check_scope(role, model_root=model_root)
            self._check_confirmation(role)
            path = directory / "versions" / f"{role.role_sha256}.json"
            if path.resolve() != path.absolute():
                _role_error("entry version path cannot be redirected by a symlink")
            payload = _encode(role.model_dump(mode="json"))
            if path.exists():
                if path.read_bytes() != payload:
                    _role_error("immutable entry role version changed")
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                _atomic_write_descriptor(target=path, encoded=payload)
            if self._now() >= _target_open(role.effective_from_target_date):
                _role_error("entry preparation crossed the valid capture window")
            self._write_pointer(directory, role.role_sha256, enabled=True, authorization_ref=authorization_ref)
            return {"status": "APPLIED", "role_sha256": role.role_sha256, "enabled": True}

    def rollback(self, *, model_root, program_id, binding_version_id, expected_current_role_sha256,
                 target_role_sha256=None, authorization_ref: str):
        _authority(authorization_ref)
        directory = self.directory(model_root=model_root, program_id=program_id, binding_version_id=binding_version_id)
        with _exclusive_descriptor_lock(directory / "active.json"):
            current = self.read(model_root=model_root, program_id=program_id, binding_version_id=binding_version_id)
            if not current or current[0].role_sha256 != expected_current_role_sha256:
                _role_error("entry rollback compare-and-swap failed", "ADVISORY_ENTRY_ROLE_CAS_CONFLICT")
            if target_role_sha256 is None:
                role, enabled = current[0], False
                if not current[1]["enabled"]:
                    return {"status": "EXACT_RETRY", "role_sha256": role.role_sha256, "enabled": False}
            else:
                role = self._read_version(directory, target_role_sha256, program_id, binding_version_id)
                self.check_scope(role, model_root=model_root)
                self._check_confirmation(role)
                enabled = True
            self._write_pointer(directory, role.role_sha256, enabled=enabled, authorization_ref=authorization_ref)
            return {"status": "ROLLED_BACK" if enabled else "DISABLED", "role_sha256": role.role_sha256, "enabled": enabled}

    def check_scope(self, role, *, model_root):
        from backend.services.advisory_program import AdvisoryProgramService
        from backend.services.advisory_delivery_preflight import AdvisoryDeliveryPreflightService
        from .price_range_runtime_bundle import load_frozen_price_range_bundle
        from .model_bundle import load_frozen_research_bundle

        programs = self._programs or AdvisoryProgramService()
        program = programs.get_program(role.program_id)
        binding = programs.active_binding(role.program_id)
        scope = role.scope
        universe = scope.universe_selection.model_dump(mode="json")
        if (binding.get("binding_version_id") != role.binding_version_id
                or tuple(binding.get("package_ids") or ()) != (scope.package_id,)
                or tuple(program.package_ids) != (scope.package_id,)
                or binding.get("universe_selection") != universe
                or program.review_policy_sha256 != scope.review_policy_sha256
                or int(program.target_count) != scope.target_count):
            _role_error("current Program differs from confirmed entry scope", "ADVISORY_ENTRY_ROLE_SCOPE_MISMATCH")
        service = self._preflight or AdvisoryDeliveryPreflightService(program_service=programs)
        readiness = service.preflight(package_id=scope.package_id, universe_selection=universe,
                                      target_count=scope.target_count, program_id=role.program_id)
        if (readiness.get("blockers") or readiness.get("overall_status") not in {"READY_WITH_MODEL", "READY_BASELINE_ONLY"}
                or readiness.get("package", {}).get("manifest_sha256") != scope.package_manifest_sha256):
            _role_error("entry package asset is retired, incompatible or unavailable", "ADVISORY_ENTRY_ROLE_INPUT_UNAVAILABLE")
        load_frozen_price_range_bundle(
            model_root=model_root, price_range_bundle_id=scope.price_range_bundle_id,
            price_range_bundle_manifest_sha256=scope.price_range_bundle_manifest_sha256,
            expected_package_id=scope.package_id, expected_manifest_sha256=scope.package_manifest_sha256,
            expected_style_profile_hash=scope.style_profile_hash, expected_parent_bundle_id=scope.parent_bundle_id,
            expected_outcome_bundle_id=scope.outcome_bundle_id, booster_factory=lambda _path: None,
        )
        parent = load_frozen_research_bundle(
            model_root=model_root, bundle_id=scope.parent_bundle_id, expected_package_id=scope.package_id,
            expected_manifest_sha256=scope.package_manifest_sha256,
            expected_selection_runtime_semantics_hash=scope.selection_runtime_semantics_hash,
            booster_factory=lambda _path: None,
        )
        if (parent.manifest_file_sha256 != scope.parent_bundle_manifest_sha256
                or parent.manifest.get("style_profile_hash") != scope.style_profile_hash
                or parent.manifest.get("feature_schema_hash") != scope.feature_schema_sha256):
            _role_error("entry parent feature lineage is unavailable or changed")

    def _check_confirmation(self, role):
        actual = evidence_reference_for_file(role.confirmation.artifact_uri, role=role.confirmation.role)
        if actual != role.confirmation:
            _role_error("confirmation artifact changed after binding preparation")
        request, _ = self._confirmation_reader(role.confirmation.artifact_uri, recompute=True)
        if (request.request_sha256 != role.confirmation_request_sha256 or request.scope != role.scope
                or request.projection_producer_version != role.projection_producer_version):
            _role_error("entry confirmation lineage or operator identity changed")

    @staticmethod
    def _read_version(directory, digest, program_id, binding_version_id):
        TypeAdapter(Sha256).validate_python(digest)
        path = directory / "versions" / f"{digest}.json"
        if not path.resolve().is_relative_to(directory.resolve()):
            _role_error("entry version path escapes role directory")
        role = EntryPriceRoleBindingV1.model_validate(_read_json(path))
        if role.role_sha256 != digest or role.program_id != program_id or role.binding_version_id != binding_version_id:
            _role_error("entry role version belongs to another Program/binding")
        return role

    def _write_pointer(self, directory, digest, *, enabled, authorization_ref):
        payload = dict(role_sha256=digest, enabled=enabled, activated_at=self._now().isoformat(), authorization_ref=authorization_ref)
        payload["pointer_sha256"] = canonical_json_sha256(payload)
        target = directory / "active.json"
        encoded = _encode(payload)
        _atomic_write_descriptor(target=target, encoded=encoded)
        if target.read_bytes() != encoded:
            _role_error("entry active pointer readback failed")


def _encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _read_json(path):
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        _role_error("entry role file must contain an object")
    return value


def _authority(reference):
    TypeAdapter(Nonempty).validate_python(reference)


def _target_open(target):
    return datetime.combine(target, time(9, 30), ZoneInfo("Asia/Shanghai"))


def _role_error(message, reason_code="ADVISORY_ENTRY_ROLE_IDENTITY_MISMATCH"):
    raise AdvisoryModelFirstError(message, reason_code=reason_code)

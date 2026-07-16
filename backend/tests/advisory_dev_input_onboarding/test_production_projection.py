from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from types import SimpleNamespace

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    InventoryClassification,
    RealDevOnboardingError,
    SourceFactEligibility,
)
from backend.services.advisory_dev_input_onboarding.production_projection import (
    SQL,
    RealDevOnboardingInventoryService,
    readonly_onboarding_connection,
)
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import DatabaseConnectionConfig
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.models import StrategyPackageManifest
import backend.services.advisory_dev_input_onboarding.production_projection as projection_module
from backend.tests.advisory_dev_input_onboarding.conftest import onboarding_request as base_onboarding_request


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _identity(label: TargetLabel, *, database: str, address: str) -> DatabaseIdentity:
    return DatabaseIdentity(
        target_label=label,
        current_database=database,
        server_address=address,
        server_port=5432,
        server_version_num=160000,
        current_user_hash=SHA_A,
        environment_contract_hash=SHA_B if label is TargetLabel.PRODUCTION else SHA_C,
    )


def _manifest(package_id: str, digest: str, *, multi: bool) -> dict[str, Any]:
    components = [
        {
            "alpha_id": "lstm" if multi else "trend",
            "alpha_name": "LSTM" if multi else "Trend",
            "component_weight": 0.6 if multi else 1.0,
            "model_id": "model_1",
            "holding_period": "5d",
            "rebalance_frequency": "1d",
            "factor_ids": ["factor_1"],
            "score_direction": "higher_better",
            "score_normalization": "rank",
            "lineage": {},
        }
    ]
    if multi:
        components.append(
            {
                "alpha_id": "fundamental",
                "alpha_name": "Fundamental",
                "component_weight": 0.4,
                "model_id": "model_2",
                "holding_period": "10d",
                "rebalance_frequency": "5d",
                "factor_ids": ["factor_2"],
                "score_direction": "higher_better",
                "score_normalization": "rank",
                "lineage": {},
            }
        )
    payload = {
        "package_id": package_id,
        "package_name": package_id,
        "package_version": "1.0.0",
        "source": {
            "source_type": "multi_alpha_combine_run" if multi else "candidate_strategy_package",
            "source_id": f"source_{package_id}",
            "created_at": "2026-07-01T00:00:00Z",
        },
        "manifest_sha256": digest,
        "alpha_mode": "multi_alpha" if multi else "single_alpha",
        "package_status": "SELECTION_ENABLED",
        "alpha_components": components,
        "alpha_combination_policy": {
            "method": "weighted_sum" if multi else "identity",
            "weights": {item["alpha_id"]: item["component_weight"] for item in components},
        },
        "factor_set": [
            {"factor_id": f"factor_{index}", "factor_name": f"Factor {index}"}
            for index in range(1, len(components) + 1)
        ],
        "model_asset": [
            {
                "model_id": item["model_id"],
                "model_type": "test",
                "asset_ref": f"aistock-package-asset://blobs/{SHA_C}?model_id={item['model_id']}",
                "sha256": SHA_C,
            }
            for item in components
        ] if multi else {
            "model_id": "model_1",
            "model_type": "test",
            "asset_ref": f"aistock-package-asset://blobs/{SHA_C}?model_id=model_1",
            "sha256": SHA_C,
        },
        "runtime_assets": {
            "contract_version": "strategy_package_runtime_assets_v2",
            "alpha158": {"enabled": False},
        },
        "source_evidence": {
            "authority": "admitted_strategy_package",
            **({"multi_alpha": {"legs": [{"leg_id": item["alpha_id"]} for item in components]}} if multi else {}),
        },
        "backtest_summary": {"ic": 0.01},
    }
    return StrategyPackageManifest.model_validate(payload).model_dump(mode="json")


SHA_A = compute_manifest_json_sha256(_manifest("pkg_single", "0" * 64, multi=False))
SHA_B = compute_manifest_json_sha256(_manifest("pkg_multi", "0" * 64, multi=True))


@pytest.fixture
def onboarding_request():
    base = base_onboarding_request.__wrapped__()
    payload = base.model_dump(mode="python", exclude={"request_hash"})
    payload["expected_package_manifest_sha256s"] = {"pkg_single": SHA_A, "pkg_multi": SHA_B}
    return type(base).model_validate(payload)


class StubProjection:
    def __init__(self, identity: DatabaseIdentity, rows: dict[str, list[dict[str, Any]]]) -> None:
        self._identity = identity
        self._rows = rows
        self.query_count = 0
        self.write_query_count = 0

    def identity(self) -> DatabaseIdentity:
        self.query_count += 1
        return self._identity

    def all(self, name: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        self.query_count += 1
        return list(self._rows.get(name, ()))


def _source_rows() -> dict[str, list[dict[str, Any]]]:
    return {
        "packages": [
            {
                "package_id": "pkg_single",
                "source_id": "s1",
                "package_status": "SELECTION_ENABLED",
                "manifest_json": _manifest("pkg_single", SHA_A, multi=False),
                "manifest_sha256": SHA_A,
                "alpha_mode": "single_alpha",
                "data_vintage": {},
            },
            {
                "package_id": "pkg_multi",
                "source_id": "s2",
                "package_status": "SELECTION_ENABLED",
                "manifest_json": _manifest("pkg_multi", SHA_B, multi=True),
                "manifest_sha256": SHA_B,
                "alpha_mode": "multi_alpha",
                "data_vintage": {},
            },
        ],
        "package_asset_counts": [
            {"package_id": "pkg_single", "asset_count": 59},
            {"package_id": "pkg_multi", "asset_count": 37},
        ],
        "source_programs": [
            {"program_id": "prod_single", "target_count": 5, "review_policy": {}},
            {"program_id": "prod_multi", "target_count": 5, "review_policy": {}},
        ],
        "source_bindings": [
            {
                "program_id": "prod_single",
                "package_mode": "single",
                "package_ids": ["pkg_single"],
                "effective_from_trade_date": None,
                "effective_to_trade_date": None,
                "activation_status": "ACTIVE",
            },
            {
                "program_id": "prod_multi",
                "package_mode": "single",
                "package_ids": ["pkg_multi"],
                "effective_from_trade_date": None,
                "effective_to_trade_date": None,
                "activation_status": "ACTIVE",
            },
        ],
        "dse_summary": [
            {
                "package_id": "pkg_single",
                "schema_version": "daily_selection_evidence_v1",
                "evidence_count": 3,
                "trade_dates": [date(2026, 7, 15)],
            },
            {
                "package_id": "pkg_multi",
                "schema_version": "daily_selection_evidence_v1",
                "evidence_count": 3,
                "trade_dates": [date(2026, 7, 15)],
            },
        ],
    }


def test_inventory_keeps_legacy_source_facts_diagnostic_without_blocking_package_closure(
    onboarding_request, onboarding_request_ref
) -> None:
    source = StubProjection(
        _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
        _source_rows(),
    )
    target = StubProjection(
        _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
        {},
    )
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=source,  # type: ignore[arg-type]
        target=target,  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.classification is InventoryClassification.DUAL_TRACK_AVAILABLE
    assert receipt.reason_codes == ()
    assert receipt.common_completed_trade_dates == ()
    assert {item.binding_fact_eligibility for item in receipt.program_candidates} == {
        SourceFactEligibility.LEGACY_BINDING_INELIGIBLE
    }
    assert {item.dse_fact_eligibility for item in receipt.program_candidates} == {
        SourceFactEligibility.DSE_V1_INELIGIBLE
    }
    multi = next(item for item in receipt.program_candidates if item.package_id == "pkg_multi")
    assert {item.holding_period for item in multi.components} == {"5d", "10d"}
    assert {item.window_evidence_status for item in multi.components} == {"PROSPECTIVE_DSE_V2_REQUIRED"}
    assert source.write_query_count == target.write_query_count == 0


def test_inventory_reports_target_conflict_with_zero_writes(onboarding_request, onboarding_request_ref) -> None:
    source = StubProjection(
        _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
        _source_rows(),
    )
    target = StubProjection(
        _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
        {"target_packages": [{"package_id": "pkg_single", "manifest_sha256": SHA_C, "alpha_mode": "single_alpha"}]},
    )
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=source,  # type: ignore[arg-type]
        target=target,  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.classification is InventoryClassification.TARGET_CONFLICT
    assert receipt.reason_codes == ("ADVISORY_REAL_DEV_TARGET_CONFLICT",)
    assert source.write_query_count == target.write_query_count == 0


def test_inventory_marks_dated_binding_and_v2_dse_eligible(onboarding_request, onboarding_request_ref) -> None:
    rows = _source_rows()
    for binding in rows["source_bindings"]:
        binding["effective_from_trade_date"] = date(2026, 7, 20)
    rows["dse_summary"] = [
        {
            "package_id": package_id,
            "schema_version": "daily_selection_evidence_v2",
            "evidence_count": 1,
            "trade_dates": [date(2026, 7, 21)],
        }
        for package_id in ("pkg_single", "pkg_multi")
    ]
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=StubProjection(
            _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
            rows,
        ),  # type: ignore[arg-type]
        target=StubProjection(
            _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
            {},
        ),  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.common_completed_trade_dates == (date(2026, 7, 21),)
    assert {item.binding_fact_eligibility for item in receipt.program_candidates} == {SourceFactEligibility.ELIGIBLE}
    assert {item.dse_fact_eligibility for item in receipt.program_candidates} == {SourceFactEligibility.ELIGIBLE}


def test_inventory_reports_missing_explicit_program_and_package(onboarding_request, onboarding_request_ref) -> None:
    rows = _source_rows()
    rows["packages"] = [row for row in rows["packages"] if row["package_id"] != "pkg_multi"]
    rows["source_programs"] = [row for row in rows["source_programs"] if row["program_id"] != "prod_multi"]
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=StubProjection(
            _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
            rows,
        ),  # type: ignore[arg-type]
        target=StubProjection(
            _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
            {},
        ),  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.classification is InventoryClassification.INPUT_INCOMPLETE
    assert "ADVISORY_REAL_DEV_PACKAGE_MISSING" in receipt.reason_codes
    assert "ADVISORY_REAL_DEV_MULTI_TRACK_MISSING" in receipt.reason_codes
    assert "ADVISORY_REAL_DEV_SOURCE_PROGRAM_MISSING" in receipt.reason_codes


def test_inventory_accepts_exact_existing_target_program_and_binding(onboarding_request, onboarding_request_ref) -> None:
    target_rows = {
        "target_packages": [
            {"package_id": "pkg_single", "manifest_sha256": SHA_A, "alpha_mode": "single_alpha"},
            {"package_id": "pkg_multi", "manifest_sha256": SHA_B, "alpha_mode": "multi_alpha"},
        ],
        "target_programs": [
            {
                "program_id": spec.program_id,
                "target_count": spec.target_count,
                "review_policy": spec.review_policy,
            }
            for spec in onboarding_request.target_dev_program_specs
        ],
        "target_bindings": [
            {
                "program_id": spec.program_id,
                "package_mode": "single_package",
                "package_ids": [spec.package_id],
                "effective_from_trade_date": date(2026, 7, 20),
                "effective_to_trade_date": None,
                "activation_status": "ACTIVE",
            }
            for spec in onboarding_request.target_dev_program_specs
        ],
    }
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=StubProjection(
            _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
            _source_rows(),
        ),  # type: ignore[arg-type]
        target=StubProjection(
            _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
            target_rows,
        ),  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.classification is InventoryClassification.DUAL_TRACK_AVAILABLE


def test_candidate_inventory_discovers_exact_manifest_hashes_without_request(
    onboarding_inventory_query, onboarding_inventory_query_ref
) -> None:
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_inventory_query,
        selected_input_ref=onboarding_inventory_query_ref,
        source=StubProjection(
            _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
            _source_rows(),
        ),  # type: ignore[arg-type]
        target=StubProjection(
            _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
            {},
        ),  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.classification is InventoryClassification.DUAL_TRACK_AVAILABLE
    assert receipt.selected_request_hash is None
    assert receipt.selected_inventory_query_hash == onboarding_inventory_query.inventory_query_hash
    assert {item.package_id: item.manifest_sha256 for item in receipt.program_candidates} == {
        "pkg_single": SHA_A,
        "pkg_multi": SHA_B,
    }
    assert receipt.dependency_closure_hash is None


def test_target_active_legacy_null_binding_is_a_conflict(onboarding_request, onboarding_request_ref) -> None:
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=StubProjection(
            _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
            _source_rows(),
        ),  # type: ignore[arg-type]
        target=StubProjection(
            _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
            {
                "target_bindings": [
                    {
                        "program_id": "dev_single",
                        "package_mode": "single_package",
                        "package_ids": ["pkg_single"],
                        "effective_from_trade_date": None,
                        "effective_to_trade_date": None,
                        "activation_status": "ACTIVE",
                    }
                ]
            },
        ),  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert receipt.classification is InventoryClassification.TARGET_CONFLICT
    assert receipt.reason_codes == ("ADVISORY_REAL_DEV_TARGET_CONFLICT",)


def test_non_retired_package_status_remains_eligible_for_explicit_o2_closure(
    onboarding_request, onboarding_request_ref
) -> None:
    rows = _source_rows()
    rows["packages"][0] = {**rows["packages"][0], "package_status": "PAPER_FAILED"}
    receipt = RealDevOnboardingInventoryService().project(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        source=StubProjection(
            _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1"),
            rows,
        ),  # type: ignore[arg-type]
        target=StubProjection(
            _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2"),
            {},
        ),  # type: ignore[arg-type]
        release_catalog_fingerprint=SHA_C,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    candidate = next(item for item in receipt.program_candidates if item.package_id == "pkg_single")
    assert candidate.package_eligible is True
    assert candidate.package_status == "PAPER_FAILED"
    assert candidate.has_source_evidence is True
    assert candidate.closure_status.value == "O2_EXPORT_VERIFICATION_REQUIRED"
    assert receipt.dependency_closure_hash is None


def test_inventory_rejects_same_physical_database(onboarding_request, onboarding_request_ref) -> None:
    identity_source = _identity(TargetLabel.PRODUCTION, database="same", address="10.0.0.1")
    identity_target = _identity(TargetLabel.DEV, database="same", address="10.0.0.1")
    with pytest.raises(RealDevOnboardingError, match="same physical database"):
        RealDevOnboardingInventoryService().project(
            input_contract=onboarding_request,
            selected_input_ref=onboarding_request_ref,
            source=StubProjection(identity_source, _source_rows()),  # type: ignore[arg-type]
            target=StubProjection(identity_target, {}),  # type: ignore[arg-type]
            release_catalog_fingerprint=SHA_C,
            observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        )


def test_fixed_query_registry_contains_select_only() -> None:
    assert SQL
    for statement in SQL.values():
        normalized = " ".join(statement.split()).upper()
        assert normalized.startswith("SELECT ")
        assert " FOR UPDATE" not in normalized
        assert " FOR SHARE" not in normalized


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, statement: str) -> None:
        assert statement == "SHOW transaction_read_only"

    def fetchone(self):
        return ("on",)


class _Connection:
    def __init__(self) -> None:
        self.session: dict[str, Any] = {}
        self.rollback_count = 0
        self.closed = False

    def set_session(self, **kwargs: Any) -> None:
        self.session = kwargs

    def cursor(self):
        return _Cursor()

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


def test_connection_enforces_readonly_options_and_rolls_back() -> None:
    connection = _Connection()
    captured: dict[str, Any] = {}

    def connector(**kwargs: Any):
        captured.update(kwargs)
        return connection

    config = DatabaseConnectionConfig(
        target_label=TargetLabel.PRODUCTION,
        host="db.example.invalid",
        port=5432,
        database="aistock",
        user="readonly",
        password="fixture",
        environment_contract_hash=SHA_A,
    )
    with readonly_onboarding_connection(config, connector=connector) as actual:
        assert actual is connection
    assert "default_transaction_read_only=on" in captured["options"]
    assert connection.session["readonly"] is True
    assert connection.session["isolation_level"] == "REPEATABLE READ"
    assert connection.rollback_count == 1
    assert connection.closed


def test_connection_failure_redacts_driver_message_and_credentials() -> None:
    config = DatabaseConnectionConfig(
        target_label=TargetLabel.PRODUCTION,
        host="db.example.invalid",
        port=5432,
        database="aistock",
        user="readonly",
        password="fixture",
        environment_contract_hash=SHA_A,
    )

    def connector(**_kwargs: Any):
        raise RuntimeError("driver included do-not-leak and absolute/path")

    with pytest.raises(RealDevOnboardingError) as captured:
        with readonly_onboarding_connection(config, connector=connector):
            pass
    rendered = str(captured.value)
    assert "do-not-leak" not in rendered
    assert "absolute/path" not in rendered
    assert captured.value.context == {"error_type": "RuntimeError", "target_label": "PRODUCTION"}


def test_inventory_uses_exact_env_targets_and_fresh_release_identity(
    monkeypatch, tmp_path, onboarding_request, onboarding_request_ref
) -> None:
    source_identity = _identity(TargetLabel.PRODUCTION, database="aistock", address="10.0.0.1")
    target_identity = _identity(TargetLabel.DEV, database="aistock_dev", address="10.0.0.2")
    source_projection = StubProjection(source_identity, _source_rows())
    target_projection = StubProjection(target_identity, {})
    resolved_labels: list[TargetLabel] = []

    def resolve(*, target_label: TargetLabel, env_file):
        resolved_labels.append(target_label)
        return DatabaseConnectionConfig(
            target_label=target_label,
            host="prod.invalid" if target_label is TargetLabel.PRODUCTION else "dev.invalid",
            port=5432,
            database="aistock" if target_label is TargetLabel.PRODUCTION else "aistock_dev",
            user="readonly",
            password="fixture",
            environment_contract_hash=source_identity.environment_contract_hash
            if target_label is TargetLabel.PRODUCTION
            else target_identity.environment_contract_hash,
        )

    connections: list[_Connection] = []

    def connector(**_kwargs: Any):
        connection = _Connection()
        connections.append(connection)
        return connection

    def projection_factory(_connection, config):
        return source_projection if config.target_label is TargetLabel.PRODUCTION else target_projection

    class SchemaGuard:
        def verify(self, **_kwargs):
            return SimpleNamespace(catalog_fingerprint=SHA_C, database_identity=target_identity)

    monkeypatch.setattr(projection_module, "resolve_database_connection", resolve)
    monkeypatch.setattr(projection_module, "load_exact_release_receipt", lambda **_kwargs: object())
    monkeypatch.setattr(projection_module, "FixedReadOnlyProjection", projection_factory)
    receipt = RealDevOnboardingInventoryService(connector=connector, schema_guard=SchemaGuard()).inventory(
        input_contract=onboarding_request,
        selected_input_ref=onboarding_request_ref,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert resolved_labels == [TargetLabel.PRODUCTION, TargetLabel.DEV]
    assert receipt.classification is InventoryClassification.DUAL_TRACK_AVAILABLE
    assert receipt.release_catalog_fingerprint == SHA_C
    assert len(connections) == 2
    assert all(connection.rollback_count == 1 and connection.closed for connection in connections)

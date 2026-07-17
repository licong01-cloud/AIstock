from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg2
import pytest

import backend.services.advisory_dev_input_onboarding.dev_importer as importer_module
import backend.services.advisory_dev_input_onboarding.production_projection as projection_module
from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    ImportCommitOutcome,
    ImportPlanStatus,
    InventoryClassification,
    PackageClosureStatus,
    PackageInventoryCandidate,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingError,
    SourceFactEligibility,
    database_identity_hash,
    deserialize_postgres_value,
    serialize_postgres_value,
)
from backend.services.advisory_dev_input_onboarding.dev_importer import RealDevPackageImporter
from backend.services.advisory_dev_input_onboarding.production_projection import (
    FixedReadOnlyProjection,
    RealDevProductionPackageExporter,
    readonly_onboarding_connection,
)
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import DatabaseConnectionConfig
from backend.tests.advisory_dev_input_onboarding.o2_fixtures import (
    DATABASE_BUNDLE_BLOB_RAW,
    DATABASE_BUNDLE_BLOB_SHA256,
    build_database_bundle,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_MIGRATION_CHAIN = (
    REPO_ROOT / "backend/migrations/data_sync_targets_20260519.sql",
    REPO_ROOT / "backend/migrations/trading_core_v2_schema.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_package_asset_20260509.sql",
    REPO_ROOT / "backend/migrations/qe_phase4_master_seed_contract_20260509.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_candidate_strategy_package_20260513.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_multi_alpha_base_20260619.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_multi_alpha_combine_source_type_20260629.sql",
)

TEST_TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION strategy_pkg.reject_test_asset() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.metadata ->> 'reject' = 'true' THEN
        RAISE EXCEPTION 'test trigger rejection';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER reject_test_asset BEFORE INSERT ON strategy_pkg.package_asset
FOR EACH ROW EXECUTE FUNCTION strategy_pkg.reject_test_asset();
"""


def _apply_production_migration_chain(connection) -> None:
    with connection.cursor() as cursor:
        for migration in PRODUCTION_MIGRATION_CHAIN:
            cursor.execute(migration.read_text(encoding="utf-8-sig"))
        cursor.execute(TEST_TRIGGER_SQL)
    connection.commit()


@pytest.fixture
def postgres_dsn() -> str:
    dsn = os.getenv("ADVISORY_O2_TEST_DSN")
    if not dsn:
        pytest.skip("ADVISORY_O2_TEST_DSN is not configured for disposable PostgreSQL")
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            for schema in ("strategy_pkg", "selection", "paper_v2", "trading_core", "market"):
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        connection.commit()
        _apply_production_migration_chain(connection)
    finally:
        connection.close()
    yield dsn
    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for schema in ("strategy_pkg", "selection", "paper_v2", "trading_core", "market"):
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    finally:
        connection.close()


def _config(dsn: str, *, target_label: TargetLabel = TargetLabel.DEV) -> DatabaseConnectionConfig:
    values = psycopg2.extensions.parse_dsn(dsn)
    return DatabaseConnectionConfig(
        target_label=target_label,
        host=values.get("host", "127.0.0.1"),
        port=int(values.get("port", 5432)),
        database=values["dbname"],
        user=values["user"],
        password=values.get("password", "fixture"),
        environment_contract_hash=("c" if target_label is TargetLabel.DEV else "b") * 64,
    )


def _connector(dsn: str):
    def connect(**kwargs: Any):
        return psycopg2.connect(dsn, options=kwargs.get("options"))

    return connect


def _importer(monkeypatch, dsn: str) -> RealDevPackageImporter:
    config = _config(dsn)
    connector = _connector(dsn)
    connection = connector()
    try:
        identity = importer_module._database_identity(connection, config, require_readonly=False)
    finally:
        connection.close()

    class SchemaGuard:
        def verify(self, **_kwargs):
            return SimpleNamespace(database_identity=identity)

    monkeypatch.setattr(importer_module, "resolve_database_connection", lambda **_kwargs: config)
    monkeypatch.setattr(importer_module, "load_exact_release_receipt", lambda **_kwargs: object())
    return RealDevPackageImporter(connector=connector, schema_guard=SchemaGuard())


def _prepared(monkeypatch, tmp_path: Path, dsn: str, request, *, metadata=None):
    bundle, _ = build_database_bundle(request, asset_metadata=metadata)
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    store.publish_blob(raw=DATABASE_BUNDLE_BLOB_RAW, expected_sha256=DATABASE_BUNDLE_BLOB_SHA256)
    bundle_ref = store.publish(bundle).ref
    source_root = tmp_path / "source-assets"
    source_root.mkdir()
    target_root = tmp_path / "target-assets"
    importer = _importer(monkeypatch, dsn)
    plan = importer.plan(
        bundle=bundle,
        bundle_ref=bundle_ref,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
    )
    return importer, bundle, bundle_ref, store, plan, source_root, target_root


def _counts(dsn: str) -> tuple[int, int]:
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM strategy_pkg.package")
            packages = int(cursor.fetchone()[0])
            cursor.execute("SELECT count(*) FROM strategy_pkg.package_asset")
            assets = int(cursor.fetchone()[0])
        return packages, assets
    finally:
        connection.close()


def test_production_exporter_reads_actual_migration_chain_database(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    request_payload = onboarding_request.model_dump(mode="python", exclude={"request_hash"})
    request_payload["source_program_refs"] = ()
    request_without_programs = type(onboarding_request).model_validate(request_payload)
    source_bundle, _ = build_database_bundle(request_without_programs)
    connection = psycopg2.connect(postgres_dsn)
    try:
        with connection.cursor() as cursor:
            for row_set in source_bundle.relation_row_sets:
                columns = importer_module.RELATION_SEMANTIC_COLUMNS[row_set.relation_name]
                for row in row_set.sorted_rows:
                    cursor.execute(
                        importer_module.TARGET_INSERT_SQL[row_set.relation_name],
                        tuple(importer_module._adapt_database_value(name, row[name]) for name in columns),
                    )
        connection.commit()
    finally:
        connection.close()

    source_config = _config(postgres_dsn, target_label=TargetLabel.PRODUCTION)
    with readonly_onboarding_connection(source_config, connector=_connector(postgres_dsn)) as connection:
        source_identity = FixedReadOnlyProjection(connection, source_config).identity()
    target_identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev_target",
        server_address="target.invalid",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="a" * 64,
        environment_contract_hash="c" * 64,
    )
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request_ref = store.publish(source_bundle.request).ref
    inventory = RealDevOnboardingInventoryReceipt(
        inventory_invocation_id="migration_chain_export",
        source_database_identity=source_identity,
        target_database_identity=target_identity,
        release_receipt_ref=source_bundle.request.release_receipt_ref,
        release_catalog_fingerprint="c" * 64,
        program_candidates=(
            PackageInventoryCandidate(
                package_id="pkg_single",
                manifest_sha256=source_bundle.request.expected_package_manifest_sha256s["pkg_single"],
                alpha_mode=AlphaMode.SINGLE,
                package_status="SELECTION_ENABLED",
                components=(
                    AlphaComponentEvidence(
                        alpha_id="single",
                        alpha_name="single",
                        component_weight=1.0,
                        holding_period="5d",
                        rebalance_frequency="1d",
                        score_direction="higher_better",
                        score_normalization="rank",
                        factor_ids=("factor_single",),
                    ),
                ),
                package_asset_count=1,
                has_runtime_assets=True,
                has_source_evidence=True,
                closure_status=PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED,
                binding_fact_eligibility=SourceFactEligibility.MISSING,
                dse_fact_eligibility=SourceFactEligibility.MISSING,
                package_eligible=True,
            ),
            PackageInventoryCandidate(
                package_id="pkg_multi",
                manifest_sha256=source_bundle.request.expected_package_manifest_sha256s["pkg_multi"],
                alpha_mode=AlphaMode.MULTI,
                package_status="SELECTION_ENABLED",
                components=tuple(
                    item.component
                    for item in source_bundle.native_multi_component_refs
                    if item.parent_package_id == "pkg_multi"
                ),
                package_asset_count=1,
                has_runtime_assets=True,
                has_source_evidence=True,
                closure_status=PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED,
                binding_fact_eligibility=SourceFactEligibility.MISSING,
                dse_fact_eligibility=SourceFactEligibility.MISSING,
                package_eligible=True,
            ),
        ),
        selected_input_ref=request_ref,
        selected_request_hash=source_bundle.request.request_hash,
        relation_row_counts={"source.strategy_pkg.package": 2},
        classification=InventoryClassification.DUAL_TRACK_AVAILABLE,
        observed_at=datetime.now(timezone.utc),
    )
    source_root = tmp_path / "source-assets"
    source_blob = source_root / "blobs" / DATABASE_BUNDLE_BLOB_SHA256[:2] / DATABASE_BUNDLE_BLOB_SHA256
    source_blob.parent.mkdir(parents=True)
    source_blob.write_bytes(DATABASE_BUNDLE_BLOB_RAW)
    monkeypatch.setattr(projection_module, "resolve_database_connection", lambda **_kwargs: source_config)
    exported = RealDevProductionPackageExporter(connector=_connector(postgres_dsn)).export(
        request=source_bundle.request,
        request_ref=request_ref,
        inventory=inventory,
        env_file=tmp_path / ".env",
        evidence_store=store,
        source_package_asset_root=source_root,
        target_package_asset_root=tmp_path / "target-assets",
    )
    assert exported.bundle.source_database_identity_hash == database_identity_hash(source_identity)
    assert len(exported.bundle.artifact_blob_refs) == 2


def test_first_import_exact_rerun_and_fresh_verify(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    importer, bundle, ref, store, plan, source_root, target_root = _prepared(
        monkeypatch, tmp_path, postgres_dsn, onboarding_request
    )
    assert plan.status is ImportPlanStatus.EXECUTABLE
    receipt = importer.import_dev(
        bundle=bundle,
        bundle_ref=ref,
        supplied_plan=plan,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
        source_package_asset_root=source_root,
        target_package_asset_root=target_root,
    )
    assert receipt.commit_outcome is ImportCommitOutcome.COMMITTED
    assert _counts(postgres_dsn) == (2, 2)

    rerun_plan = importer.plan(
        bundle=bundle,
        bundle_ref=ref,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
    )
    assert rerun_plan.status is ImportPlanStatus.ALREADY_PRESENT
    rerun = importer.import_dev(
        bundle=bundle,
        bundle_ref=ref,
        supplied_plan=rerun_plan,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
        source_package_asset_root=source_root,
        target_package_asset_root=target_root,
    )
    assert rerun.commit_outcome is ImportCommitOutcome.ALREADY_PRESENT
    assert rerun.receipt_hash != receipt.receipt_hash
    assert _counts(postgres_dsn) == (2, 2)
    importer.verify_import(
        bundle=bundle,
        bundle_ref=ref,
        receipt=receipt,
        supplied_plan=plan,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
        source_package_asset_root=source_root,
        target_package_asset_root=target_root,
    )


def test_rollback_only_executes_full_import_and_proves_zero_database_residue(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    importer, bundle, ref, store, plan, source_root, target_root = _prepared(
        monkeypatch, tmp_path, postgres_dsn, onboarding_request
    )
    assert plan.status is ImportPlanStatus.EXECUTABLE
    receipt = importer.validate_rollback(
        bundle=bundle,
        bundle_ref=ref,
        supplied_plan=plan,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
        source_package_asset_root=source_root,
        target_package_asset_root=target_root,
    )
    assert receipt.commit_outcome is ImportCommitOutcome.ROLLED_BACK
    assert receipt.physical_commit_count == 0
    assert sum(receipt.inserted_row_counts.values()) == 4
    assert receipt.post_readback_row_hashes == {
        "strategy_pkg.package": (),
        "strategy_pkg.package_asset": (),
    }
    assert _counts(postgres_dsn) == (0, 0)
    post_plan = importer.plan(
        bundle=bundle,
        bundle_ref=ref,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
    )
    assert post_plan.status is ImportPlanStatus.EXECUTABLE
    assert post_plan.plan_hash == plan.plan_hash


def test_conflict_plan_has_zero_dml_even_with_other_insert_rows(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    importer, bundle, ref, store, plan, source_root, target_root = _prepared(
        monkeypatch, tmp_path, postgres_dsn, onboarding_request
    )
    package_set = next(item for item in bundle.relation_row_sets if item.relation_name == "strategy_pkg.package")
    single = next(row for row in package_set.sorted_rows if row["package_id"] == "pkg_single")
    values = {name: deserialize_postgres_value(single[name]) for name in package_set.semantic_column_names}
    values["package_status"] = "RETIRED"
    connection = psycopg2.connect(postgres_dsn)
    try:
        with connection.cursor() as cursor:
            columns = importer_module.RELATION_SEMANTIC_COLUMNS["strategy_pkg.package"]
            cursor.execute(
                f"INSERT INTO strategy_pkg.package ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                tuple(
                    importer_module._adapt_database_value(name, serialize_postgres_value(values[name]))
                    for name in columns
                ),
            )
        connection.commit()
    finally:
        connection.close()
    conflict = importer.plan(
        bundle=bundle,
        bundle_ref=ref,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
    )
    assert conflict.status is ImportPlanStatus.CONFLICT
    assert conflict.insert_rows_by_relation
    assert conflict.ordered_write_operations == ()
    with pytest.raises(RealDevOnboardingError):
        importer.import_dev(
            bundle=bundle,
            bundle_ref=ref,
            supplied_plan=conflict,
            evidence_store=store,
            env_file=tmp_path / ".env",
            release_receipt_root=tmp_path / "release",
            source_package_asset_root=source_root,
            target_package_asset_root=target_root,
        )
    assert _counts(postgres_dsn) == (1, 0)
def test_trigger_error_rolls_back_parent_rows(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    importer, bundle, ref, store, plan, source_root, target_root = _prepared(
        monkeypatch, tmp_path, postgres_dsn, onboarding_request, metadata={"reject": True}
    )
    with pytest.raises(RealDevOnboardingError):
        importer.import_dev(
            bundle=bundle,
            bundle_ref=ref,
            supplied_plan=plan,
            evidence_store=store,
            env_file=tmp_path / ".env",
            release_receipt_root=tmp_path / "release",
            source_package_asset_root=source_root,
            target_package_asset_root=target_root,
        )
    assert _counts(postgres_dsn) == (0, 0)


def test_concurrent_same_bundle_converges_without_global_lock(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    importer, bundle, ref, store, plan, source_root, target_root = _prepared(
        monkeypatch, tmp_path, postgres_dsn, onboarding_request
    )

    def run():
        return importer.import_dev(
            bundle=bundle,
            bundle_ref=ref,
            supplied_plan=plan,
            evidence_store=store,
            env_file=tmp_path / ".env",
            release_receipt_root=tmp_path / "release",
            source_package_asset_root=source_root,
            target_package_asset_root=target_root,
        ).commit_outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _index: run(), range(2)))
    assert set(outcomes).issubset({ImportCommitOutcome.COMMITTED, ImportCommitOutcome.ALREADY_PRESENT})
    assert ImportCommitOutcome.COMMITTED in outcomes
    assert _counts(postgres_dsn) == (2, 2)


def test_concurrent_different_bundle_never_last_write_wins(
    monkeypatch,
    tmp_path: Path,
    postgres_dsn: str,
    onboarding_request,
) -> None:
    importer, first, first_ref, store, first_plan, source_root, target_root = _prepared(
        monkeypatch, tmp_path, postgres_dsn, onboarding_request
    )
    second, _ = build_database_bundle(onboarding_request, multi_package_status="PAPER_ENABLED")
    second_ref = store.publish(second).ref
    second_plan = importer.plan(
        bundle=second,
        bundle_ref=second_ref,
        evidence_store=store,
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
    )

    def run(bundle, ref, plan):
        try:
            return importer.import_dev(
                bundle=bundle,
                bundle_ref=ref,
                supplied_plan=plan,
                evidence_store=store,
                env_file=tmp_path / ".env",
                release_receipt_root=tmp_path / "release",
                source_package_asset_root=source_root,
                target_package_asset_root=target_root,
            ).commit_outcome
        except RealDevOnboardingError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda args: run(*args),
                ((first, first_ref, first_plan), (second, second_ref, second_plan)),
            )
        )
    assert sum(value is ImportCommitOutcome.COMMITTED for value in results) == 1
    assert sum(isinstance(value, RealDevOnboardingError) for value in results) == 1
    assert _counts(postgres_dsn) == (2, 2)

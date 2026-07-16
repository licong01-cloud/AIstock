from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime

import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.control_binding import (
    PostgresControlBindingRepository,
)
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY,
    Phase1GExecutionBatchRequest,
)
from backend.services.advisory_phase1.phase1g_dev_evidence import Phase1GDevEvidenceService
from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    ExecutionMode,
    InventoryStatus,
    L3SourceClassification,
    PersistentStatus,
    Phase1GDevExecutionManifest,
    RollbackStatus,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_store import (
    Phase1GDevEvidenceStore,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_postgres import (
    capture_current_transaction_residue_probes,
    run_control_binding_concurrency_probe,
    verify_zero_residue,
)
from backend.services.advisory_phase1.phase1g_dev_rollback import (
    Phase1GDevRollbackCoordinator,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
)
from backend.services.advisory_phase1.phase1g_result_store import Phase1GResultStore
from backend.services.advisory_phase1.readiness_plan import Phase1EExecutionPlan
from backend.services.advisory_phase1.phase1g_service import (
    Phase1GInvocationBatchStatus,
    Phase1GService,
)
from backend.tests.advisory_phase1.test_phase1g_g3_transactional_writer_postgres import (
    _raw_factory,
)
from backend.tests.advisory_phase1.test_phase1g_historical_trace_projection import (
    _rehash_artifact_and_dse,
    historical_multi_alpha_case,
    historical_raw_empty_case,
)
from backend.tests.advisory_phase1.test_phase1g_historical_trace_projection_postgres import (
    BASELINE_G2_SQL,
)
from backend.tests.advisory_phase1.test_phase1g_service_postgres import (
    _loaded_case,
)
from backend.tests.advisory_phase1.test_release_schema_dev_db import _fresh_apply
from backend.tests.advisory_phase1.test_phase1g_source_replay import g2_source_case
from backend.tests.advisory_phase1.phase1g_test_support import (
    write_phase1e_plan_artifact,
)
from backend.services.strategy_package.manifest import compute_manifest_json_sha256


pytest_plugins = ("backend.tests.advisory_phase1.test_release_schema_dev_db",)


def _write_disposable_dev_env(*, path, config) -> None:  # type: ignore[no-untyped-def]
    values = {
        "TDX_DB_DEV_HOST": config.host,
        "TDX_DB_DEV_PORT": config.port,
        "TDX_DB_DEV_NAME": config.database,
        "TDX_DB_DEV_USER": config.user,
        "TDX_DB_DEV_PASSWORD": config.password,
    }
    path.write_text(
        "".join(f"{key}={json.dumps(str(value))}\n" for key, value in values.items()),
        encoding="utf-8",
    )


def _exact_disposable_dev_config(config) -> DatabaseConnectionConfig:  # type: ignore[no-untyped-def]
    return DatabaseConnectionConfig(
        target_label=config.target_label,
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        environment_contract_hash=canonical_json_sha256(
            {
                "target_label": config.target_label.value,
                "host": config.host,
                "port": config.port,
                "database": config.database,
                "user": config.user,
            }
        ),
    )


def _write_release_receipt(*, root, receipt) -> None:  # type: ignore[no-untyped-def]
    raw = json.dumps(
        canonicalize(receipt.model_dump(mode="json")),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    path = root / "receipts" / f"{receipt.receipt_content_hash}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


def _with_g5_artifact_store_policy(
    plan, *, capture_plan
) -> Phase1EExecutionPlan:  # type: ignore[no-untyped-def]
    payload = plan.model_dump(mode="python", exclude={"plan_hash", "plan_id"})
    policy_hash = str(PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash)
    for operation in payload["planned_operations"]:
        request_field = (
            "complete_request_payload"
            if operation["complete_request_payload"] is not None
            else "request_template_payload"
        )
        hash_field = (
            "complete_request_hash"
            if request_field == "complete_request_payload"
            else "request_template_hash"
        )
        request = operation[request_field]
        scope = request["scope_context"]
        scope["batch_contract"] = {"artifact_store_policy_hash": policy_hash}
        if request_field == "request_template_payload":
            request["capture_plan"] = capture_plan.model_dump(mode="json")
            output_slots = tuple(
                {
                    "slot": value,
                    "source_type": "phase1g_runtime_value",
                    "slot_schema_version": "advisory_phase1e_output_slot_v1",
                    "producer_operation": "phase1g_observation_capture",
                    "hash_validation": "exact_value",
                }
                for value in (
                    "control_binding_event_hash",
                    "capture_batch_id",
                    "capture_fencing_token",
                )
            )
            operation["required_output_slots"] = output_slots
            operation["unresolved_input_refs"] = output_slots
        operation[hash_field] = canonical_json_sha256(request)
    return Phase1EExecutionPlan.model_validate(payload)


def _seed_g2_projection_inputs(*, config, cases) -> None:  # type: ignore[no-untyped-def]
    connection = psycopg2.connect(**config.connect_kwargs())
    connection.autocommit = True
    try:
        with connection.cursor() as cur:
            cur.execute(BASELINE_G2_SQL)
            for case in cases:
                dse = case["dse_row"]
                cur.execute(
                    """
                    INSERT INTO selection.daily_selection_evidence
                        (evidence_id, target_trade_date, cutoff_date, package_id,
                         manifest_sha256, runtime_profile_version_id,
                         runtime_profile_hash, source_type, data_source,
                         candidate_count, excluded_count, artifact_hash,
                         evidence_payload_json, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        dse["evidence_id"],
                        dse["target_trade_date"],
                        dse["cutoff_date"],
                        dse["package_id"],
                        dse["manifest_sha256"],
                        dse["runtime_profile_version_id"],
                        dse["runtime_profile_hash"],
                        dse["source_type"],
                        dse["data_source"],
                        dse["candidate_count"],
                        dse["excluded_count"],
                        dse["artifact_hash"],
                        psycopg2.extras.Json(dse["evidence_payload_json"]),
                        dse["created_at"],
                    ),
                )
                artifact = case["artifact_row"]
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.selection_score_artifact
                        (artifact_id, package_id, manifest_sha256, trade_date,
                         data_source, runtime_config_hash, scores_json,
                         artifact_sha256, score_count, universe_count,
                         top_score_symbol, status, metadata,
                         artifact_contract_version, artifact_payload_sha256,
                         artifact_input_context_hash, source_revision_set_hash,
                         asset_closure_hash, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        artifact["artifact_id"],
                        artifact["package_id"],
                        artifact["manifest_sha256"],
                        artifact["trade_date"],
                        artifact["data_source"],
                        artifact["runtime_config_hash"],
                        psycopg2.extras.Json(artifact["scores_json"]),
                        artifact["artifact_sha256"],
                        artifact["score_count"],
                        artifact["universe_count"],
                        artifact["top_score_symbol"],
                        artifact["status"],
                        psycopg2.extras.Json(canonicalize(artifact["metadata"])),
                        artifact["artifact_contract_version"],
                        artifact["artifact_payload_sha256"],
                        artifact["artifact_input_context_hash"],
                        artifact["source_revision_set_hash"],
                        artifact["asset_closure_hash"],
                        artifact["created_at"],
                    ),
                )
                package = case["package_row"]
                cur.execute(
                    "INSERT INTO strategy_pkg.package VALUES (%s, %s, %s, %s)",
                    (
                        package["package_id"],
                        psycopg2.extras.Json(package["manifest_json"]),
                        package["manifest_sha256"],
                        package["alpha_mode"],
                    ),
                )
                binding = case["binding_row"]
                cur.execute(
                    """
                    INSERT INTO app.advisory_strategy_binding_version
                        (binding_version_id, program_id, package_mode, package_ids,
                         runtime_config_json, effective_from_trade_date,
                         effective_to_trade_date, activation_status,
                         binding_payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        binding["binding_version_id"],
                        binding["program_id"],
                        binding["package_mode"],
                        psycopg2.extras.Json(binding["package_ids"]),
                        psycopg2.extras.Json(binding["runtime_config_json"]),
                        binding["effective_from_trade_date"],
                        binding["effective_to_trade_date"],
                        binding["activation_status"],
                        psycopg2.extras.Json(binding["binding_payload_json"]),
                    ),
                )
    finally:
        connection.close()


def _namespaced_multi_case() -> dict:  # type: ignore[type-arg]
    case = deepcopy(historical_multi_alpha_case())
    package_id = "package-g5-multi"
    artifact_id = "artifact-g5-multi"
    binding_version_id = "binding-g5-multi"
    manifest = case["package_row"]["manifest_json"]
    manifest["package_id"] = package_id
    manifest.pop("manifest_sha256", None)
    manifest_sha256 = compute_manifest_json_sha256(manifest)
    manifest["manifest_sha256"] = manifest_sha256

    artifact = case["artifact_row"]
    artifact.update(
        artifact_id=artifact_id,
        package_id=package_id,
        manifest_sha256=manifest_sha256,
    )
    artifact_parity = artifact["metadata"]["multi_alpha_parent_parity"]
    artifact_parity.update(
        parent_package_id=package_id,
        parent_manifest_sha256=manifest_sha256,
    )
    artifact["metadata"]["multi_alpha_parent_parity_hash"] = canonical_json_sha256(
        artifact_parity
    )

    payload = case["dse_row"]["evidence_payload_json"]
    payload["phase0a_candidate_lineage"].update(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        selection_score_artifact_id=artifact_id,
    )
    lineage = payload["phase0a_package_lineage"]
    lineage.update(package_id=package_id, manifest_sha256=manifest_sha256)
    lineage_parity = lineage["multi_alpha"]["multi_alpha_parent_parity"]
    lineage_parity.update(
        parent_package_id=package_id,
        parent_manifest_sha256=manifest_sha256,
    )
    lineage["multi_alpha"]["multi_alpha_parent_parity_hash"] = (
        canonical_json_sha256(lineage_parity)
    )
    case["dse_row"].update(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
    )
    case["package_row"].update(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
    )
    case["binding_row"].update(
        binding_version_id=binding_version_id,
        package_ids=[package_id],
    )
    plan, target, event = g2_source_case(
        manifest_sha256=manifest_sha256,
        alpha_mode="multi_alpha",
        component_ids=("leg_a", "leg_b"),
        package_id=package_id,
        binding_version_id=binding_version_id,
    )
    case.update(plan=plan, target=target, event=event)
    return _rehash_artifact_and_dse(case)


def test_disposable_postgres_full_g4_graph_rolls_back_with_zero_residue(
    database_factory, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    config = _exact_disposable_dev_config(database_factory())
    _contract, release_receipt = _fresh_apply(config)
    bootstrap = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=object(),
        result_store=Phase1GResultStore(root=tmp_path / "bootstrap-results"),
        schema_guard=object(),
    )
    loaded = _loaded_case(
        bootstrap,
        config,
        release_receipt,
        suffix="g5-rollback",
    )
    coordinator = Phase1GDevRollbackCoordinator(
        connection_factory=_raw_factory(config),
        application_name="aistock:g5:l3:postgres-test",
        statement_timeout_ms=DEFAULT_CAPTURE_POLICY_REGISTRY.statement_timeout_ms,
        lock_timeout_ms=DEFAULT_CAPTURE_POLICY_REGISTRY.lock_timeout_ms,
    )
    probes = ()
    control_request = None
    with coordinator:
        service = Phase1GService(
            connection_config=config,
            transaction_connection_factory=coordinator.transaction_connection_factory,
            readonly_connection_factory=coordinator.readonly_connection_factory,
            artifact_resolver=object(),
            result_store=Phase1GResultStore(root=tmp_path / "rollback-results"),
            schema_guard=object(),
        )
        service._load_target = lambda _target: loaded  # type: ignore[method-assign]
        plan = service.plan_batch(
            Phase1GExecutionBatchRequest(targets=(loaded.target_request,))
        )
        outcome = service.capture_batch(plan)
        assert outcome.batch_status is Phase1GInvocationBatchStatus.SUCCESS
        assert outcome.target_outcomes[0].dml_executed is True
        result = service._result_store.load(outcome.target_outcomes[0].capture_result_ref)
        with coordinator.owner_cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            event = PostgresControlBindingRepository.read_exact_in_transaction(
                cur, result.control_binding_event_hash
            )
            control_request = event.request
            probes = capture_current_transaction_residue_probes(cursor=cur)
    checks = verify_zero_residue(
        connection_factory=_raw_factory(config),
        probes=probes,
    )
    assert coordinator.physical_rollback_count == 1
    assert coordinator.recorder.summary().observed_transactional_dml is True
    assert all(item.residue_count == 0 for item in checks)
    assert sum(item.checked_identity_count for item in checks) > 0
    assert control_request is not None
    concurrency_hash = run_control_binding_concurrency_probe(
        connection_factory=_raw_factory(config),
        request=control_request,
        lock_timeout_ms=100,
    )
    assert len(concurrency_hash) == 64


def test_disposable_postgres_g5_persistent_dual_track_first_run_and_exact_rerun(
    database_factory, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    config = _exact_disposable_dev_config(database_factory())
    _contract, release_receipt = _fresh_apply(config)
    single_case = historical_raw_empty_case()
    multi_case = _namespaced_multi_case()
    _seed_g2_projection_inputs(config=config, cases=(single_case, multi_case))
    bootstrap = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=object(),
        result_store=Phase1GResultStore(root=tmp_path / "bootstrap-dual-results"),
        schema_guard=object(),
    )
    single = _loaded_case(
        bootstrap,
        config,
        release_receipt,
        case_factory=lambda: single_case,
        suffix="g5-persistent-single",
    )
    multi = _loaded_case(
        bootstrap,
        config,
        release_receipt,
        case_factory=lambda: multi_case,
        suffix="g5-persistent-multi",
    )
    env_file = tmp_path / "disposable.env"
    release_root = tmp_path / "release"
    phase1e_root = tmp_path / "phase1e"
    result_root = tmp_path / "persistent-results"
    evidence_root = tmp_path / "g5-evidence"
    release_root.mkdir()
    phase1e_root.mkdir()
    result_root.mkdir()
    evidence_root.mkdir()
    _write_disposable_dev_env(path=env_file, config=config)
    _write_release_receipt(root=release_root, receipt=release_receipt)
    for loaded in (single, multi):
        write_phase1e_plan_artifact(
            root=phase1e_root,
            plan=_with_g5_artifact_store_policy(
                loaded.phase1e_plan,
                capture_plan=loaded.capture_plans[0],
            ),
            store_policy_hash=str(
                PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
            ),
        )

    evidence_store = Phase1GDevEvidenceStore(root=evidence_root)
    service = Phase1GDevEvidenceService(
        env_file=env_file,
        release_receipt_root=release_root,
        phase1e_artifact_root=phase1e_root,
        phase1g_result_root=result_root,
        evidence_store=evidence_store,
        now_provider=lambda: datetime(2026, 7, 16, 1, 0, tzinfo=UTC),
    )
    inventory_stored = service.inventory()
    inventory = evidence_store.load(inventory_stored.ref)
    for candidate in inventory.l4_target_candidates:
        assert candidate.target_request is not None
        service.context.service._load_target(candidate.target_request)
    assert inventory.inventory_status is InventoryStatus.L4_DUAL_TRACK_READY
    assert inventory.l3_source_eligible_count >= 1
    assert inventory.l4_single_executable_count == 1
    assert inventory.l4_native_multi_executable_count == 1
    inventory_ref = inventory_stored.ref
    rollback_manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
        source_candidate_hashes=tuple(
            item.source_candidate_hash
            for item in inventory.l3_source_candidates
            if item.classification
            in {
                L3SourceClassification.ELIGIBLE_SINGLE,
                L3SourceClassification.ELIGIBLE_NATIVE_MULTI,
            }
        ),
    )
    rollback_stored, _rollback_summary = service.validate_rollback(
        inventory_ref=inventory_ref,
        manifest=rollback_manifest,
    )
    rollback = evidence_store.load(rollback_stored.ref)
    assert rollback.rollback_status is RollbackStatus.COMPLETE_ZERO_RESIDUE
    assert rollback.observed_transactional_dml is True
    assert rollback.physical_rollback_count == 1
    assert rollback.fresh_connection_residue_checks
    assert all(item.residue_count == 0 for item in rollback.fresh_connection_residue_checks)
    rollback_ref = rollback_stored.ref
    candidates = tuple(item for item in inventory.l4_target_candidates if item.executable)
    manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.PERSISTENT_DUAL_TRACK,
        target_request_hashes=tuple(
            str(item.target_request.request_hash) for item in candidates
        ),
        single_target_count=1,
        native_multi_target_count=1,
    )
    persistent_stored, summary_stored = service.capture_persistent(
        inventory_ref=inventory_ref,
        rollback_ref=rollback_ref,
        manifest=manifest,
    )
    persistent = evidence_store.load(persistent_stored.ref)
    summary = evidence_store.load(summary_stored.ref)
    assert persistent.persistent_status is PersistentStatus.COMPLETE_DUAL_TRACK
    assert persistent.single_target_count == 1
    assert persistent.native_multi_target_count == 1
    assert persistent.rerun_dml_target_count == 0
    assert all(
        item.first_operation_status == "SUCCESS"
        and item.rerun_operation_status == "SUCCESS"
        and item.rerun_dml_executed is False
        for item in persistent.target_outcomes
    )
    assert summary.persistent_receipt_ref == persistent_stored.ref
    verified = service.verify_evidence(persistent_stored.ref, db_readback=True)
    assert verified["db_readback"] is True
    assert (
        verified["referenced_readback_hash"]
        == persistent.referenced_readback_hash
    )

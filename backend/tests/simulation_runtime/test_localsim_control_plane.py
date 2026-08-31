from __future__ import annotations

from datetime import UTC, date, datetime
from copy import deepcopy

import pytest

from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.models import SimulationReleaseBinding, canonical_json_sha256
from backend.services.simulation_runtime.successor_models import (
    LegacyLocalSimAccountInventoryV1,
    LegacyLocalSimLineageStatus,
    LocalSimSafeBoundaryDecisionV1,
    SimulationAccountStatus,
)
from backend.services.simulation_runtime.successor_repository import InMemoryLocalSimSuccessorRepository
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.services.trading_core.errors import InvalidStateTransitionError


NOW = datetime(2026, 8, 31, 1, 0, tzinfo=UTC)


def _service() -> tuple[LocalSimControlPlaneService, InMemoryLocalSimSuccessorRepository]:
    repository = InMemoryLocalSimSuccessorRepository()
    return LocalSimControlPlaneService(repository=repository, clock=lambda: NOW), repository


def _create_account(service: LocalSimControlPlaneService, *, account_name: str = "six-month replay"):
    policy = local_sim_twap_only_policy_snapshot()
    return service.create_account(
        account_name=account_name,
        package_id="pkg_current_alpha",
        manifest_sha256="a" * 64,
        admission_receipt_id="admission_current_alpha",
        initial_capital=1_000_000.0,
        runtime_profile_id="localsim_runtime",
        runtime_profile_version_id="runtime_v1",
        runtime_profile_sha256="b" * 64,
        daily_strategy_profile_version_id="daily_v1",
        execution_policy_version_id=policy["policy_version_id"],
        execution_policy_sha256=policy["policy_sha256"],
        execution_policy_json=policy["policy_json"],
        tail_policy_version_id="tail_v1",
        tail_policy_sha256="c" * 64,
        requested_execution_policy_audit={"requested": "V25_TWO_STAGE"},
        effective_from=date(2026, 9, 1),
        created_by="test",
    )


def _seed_legacy_binding(
    repository: InMemoryLocalSimSuccessorRepository,
    binding: SimulationReleaseBinding,
    *,
    legacy_account_id: str,
) -> SimulationReleaseBinding:
    config = deepcopy(binding.binding_config_json)
    config["strategy_id"] = legacy_account_id
    config["broker_account_id"] = legacy_account_id
    config.pop("account_group_id", None)
    config.pop("strategy_slot_id", None)
    config["metadata"] = {"legacy_inventory": True}
    binding_hash = canonical_json_sha256(config)
    values = binding.model_dump()
    values.update(
        {
            "binding_id": f"simbind_{binding_hash[:16]}",
            "strategy_id": legacy_account_id,
            "broker_account_id": legacy_account_id,
            "account_group_id": None,
            "strategy_slot_id": None,
            "binding_config_json": config,
            "binding_hash": binding_hash,
        }
    )
    legacy_binding = SimulationReleaseBinding.model_validate(values)
    repository.bindings[legacy_binding.binding_id] = legacy_binding
    repository.binding_hash_index[legacy_binding.binding_hash or ""] = legacy_binding.binding_id
    return legacy_binding


def test_create_account_atomically_freezes_account_release_binding_and_twap_policy() -> None:
    service, repository = _service()

    account, release, binding = _create_account(service)

    assert account.status is SimulationAccountStatus.ACTIVE
    assert binding.broker_account_id == account.account_id
    assert binding.binding_config_json["metadata"]["localsim_account_id"] == account.account_id
    assert release.release_config_json["execution_policy"]["policy_json"]["algo_code"] == "TWAP"
    assert release.release_config_json["metadata"]["requested_execution_policy_audit"] == {
        "requested": "V25_TWO_STAGE",
        "consulted_for_execution": False,
    }
    assert set(repository.accounts) == {account.account_id}
    assert set(repository.releases) == {release.release_id}
    assert set(repository.bindings) == {binding.binding_id}

    duplicate = _create_account(service)
    assert duplicate == (account, release, binding)
    assert len(repository.accounts) == len(repository.releases) == len(repository.bindings) == 1


def test_create_account_identity_failure_leaves_zero_orphan_rows() -> None:
    service, repository = _service()
    account, release, binding = _create_account(service)
    repository.accounts.clear()
    repository.account_hash_index.clear()
    repository.releases.clear()
    repository.release_hash_index.clear()
    repository.bindings.clear()
    repository.binding_hash_index.clear()
    tampered_binding = binding.model_copy(update={"broker_account_id": "another-account"})

    with pytest.raises(InvalidStateTransitionError, match="identities are inconsistent"):
        repository.create_account_bundle(account=account, release=release, binding=tampered_binding)

    assert repository.accounts == {}
    assert repository.releases == {}
    assert repository.bindings == {}


def test_account_lifecycle_is_explicit_cas_without_session_state_machine() -> None:
    service, _repository = _service()
    account, _release, _binding = _create_account(service)

    paused = service.pause_account(account_id=account.account_id, expected_version=1)
    assert paused.status is SimulationAccountStatus.PAUSED
    assert paused.version == 2

    with pytest.raises(InvalidStateTransitionError, match="CAS failed"):
        service.resume_account(account_id=account.account_id, expected_version=1)

    resumed = service.resume_account(account_id=account.account_id, expected_version=2)
    retired = service.retire_account(account_id=account.account_id, expected_version=resumed.version)
    assert retired.status is SimulationAccountStatus.RETIRED
    with pytest.raises(InvalidStateTransitionError, match="transition is invalid"):
        service.pause_account(account_id=account.account_id, expected_version=retired.version)


def test_successor_release_closes_old_binding_window_in_the_same_cas_transaction() -> None:
    service, repository = _service()
    account, base_release, base_binding = _create_account(service)
    policy = local_sim_twap_only_policy_snapshot()

    release, binding = service.create_successor_release(
        account_id=account.account_id,
        base_release_id=base_release.release_id,
        base_binding_id=base_binding.binding_id,
        runtime_profile_id="localsim_runtime",
        runtime_profile_version_id="runtime_v2",
        runtime_profile_sha256="d" * 64,
        daily_strategy_profile_version_id="daily_v2",
        execution_policy_version_id=policy["policy_version_id"],
        execution_policy_sha256=policy["policy_sha256"],
        execution_policy_json=policy["policy_json"],
        tail_policy_version_id="tail_v2",
        tail_policy_sha256="e" * 64,
        effective_from=date(2026, 9, 2),
        created_by="test",
    )

    assert release.base_release_id == base_release.release_id
    assert repository.get_binding(base_binding.binding_id).effective_to == date(2026, 9, 1)
    assert binding.effective_from == date(2026, 9, 2)
    assert binding.effective_to is None

    with pytest.raises(InvalidStateTransitionError, match="source binding authority changed"):
        repository.create_successor_binding(
            account=account,
            source_binding_id=base_binding.binding_id,
            expected_source_binding_hash="0" * 64,
            source_effective_to=date(2026, 9, 1),
            release=release,
            binding=binding,
        )


def test_legacy_lineage_is_unique_replayable_and_preserves_economic_identity() -> None:
    service, repository = _service()
    _account, release, binding = _create_account(service, account_name="seed authority")
    binding = _seed_legacy_binding(repository, binding, legacy_account_id="legacy_portfolio_keep")
    inventory = LegacyLocalSimAccountInventoryV1(
        legacy_account_id="legacy_portfolio_keep",
        account_name="retained LocalSIM",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        admission_receipt_id="admission_current_alpha",
        initial_capital=1_000_000.0,
        release_id=release.release_id,
        release_hash=release.release_hash or "",
        binding_id=binding.binding_id,
        binding_hash=binding.binding_hash or "",
        ledger_scope_id="legacy_portfolio_keep",
        economic_facts_sha256="d" * 64,
        current_status=SimulationAccountStatus.ACTIVE,
        runtime_owned=True,
        retained_by_user=True,
        in_flight_economic_transactions=0,
    )

    account, lineage = service.prepare_legacy_lineage(inventory, created_by="test")
    replayed_account, replayed_lineage = service.prepare_legacy_lineage(inventory, created_by="test")

    assert replayed_account == account
    assert replayed_lineage == lineage
    assert account.lineage_source_legacy_account_id == inventory.legacy_account_id
    assert lineage.ledger_scope_id == inventory.ledger_scope_id
    assert lineage.economic_facts_sha256 == inventory.economic_facts_sha256
    assert len(repository.lineages) == 1

    conflict = inventory.model_copy(update={"economic_facts_sha256": "e" * 64})
    with pytest.raises(InvalidStateTransitionError, match="different durable lineage"):
        service.prepare_legacy_lineage(conflict, created_by="test")

    with pytest.raises(InvalidStateTransitionError, match="pending state"):
        service.mark_lineage_activation_pending(
            legacy_account_id=inventory.legacy_account_id,
            expected_version=lineage.version,
            current_economic_facts_sha256="0" * 64,
        )
    pending = service.mark_lineage_activation_pending(
        legacy_account_id=inventory.legacy_account_id,
        expected_version=lineage.version,
        current_economic_facts_sha256=lineage.economic_facts_sha256,
    )
    decision = LocalSimSafeBoundaryDecisionV1(
        eligible=True,
        evaluated_at=NOW,
        current_trading_date=date(2026, 8, 31),
        activation_trade_date=date(2026, 8, 31),
        market_phase="PRE_OPEN",
        in_flight_economic_transactions=0,
        writer_claim_available=True,
        historical_provider_closed=True,
        reason_code="LOCALSIM_LINEAGE_SAFE_BOUNDARY",
    )
    active = service.activate_legacy_lineage(
        legacy_account_id=inventory.legacy_account_id,
        expected_version=pending.version,
        current_economic_facts_sha256=lineage.economic_facts_sha256,
        decision=decision,
    )
    assert active.status is LegacyLocalSimLineageStatus.ACTIVE


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"retained_by_user": False}, "not eligible"),
        ({"runtime_owned": False}, "not eligible"),
        ({"in_flight_economic_transactions": 1}, "zero in-flight"),
        ({"current_status": SimulationAccountStatus.RETIRED}, "cannot be reactivated"),
    ],
)
def test_legacy_lineage_fails_closed_for_unretained_terminal_or_inflight_inventory(
    update: dict[str, object], message: str
) -> None:
    service, repository = _service()
    _account, release, binding = _create_account(service, account_name="seed authority")
    binding = _seed_legacy_binding(repository, binding, legacy_account_id="legacy_bad")
    inventory = LegacyLocalSimAccountInventoryV1(
        legacy_account_id="legacy_bad",
        account_name="legacy bad",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        admission_receipt_id="admission_current_alpha",
        initial_capital=1_000_000.0,
        release_id=release.release_id,
        release_hash=release.release_hash or "",
        binding_id=binding.binding_id,
        binding_hash=binding.binding_hash or "",
        ledger_scope_id="legacy_bad",
        economic_facts_sha256="f" * 64,
        current_status=SimulationAccountStatus.ACTIVE,
        runtime_owned=True,
        retained_by_user=True,
        in_flight_economic_transactions=0,
    ).model_copy(update=update)

    with pytest.raises(InvalidStateTransitionError, match=message):
        service.prepare_legacy_lineage(inventory, created_by="test")

    assert repository.get_lineage_by_legacy_account("legacy_bad") is None

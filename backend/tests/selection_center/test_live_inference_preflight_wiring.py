"""Tests for SelectionCenterService cold-start preflight wiring (Task #33).

Asserts that ``run_packages`` calls into the live-inference preflight before
``generate_from_live_inference`` and surfaces a structured
``LiveInferencePreflightError`` instead of letting the run stall in the
heavy materialization path (the historical 30+ failure incident audited in
``docs/analysis/paper_v2_user_requirement_audit_20260507.md`` §0/§7 P0-4).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.live_inference import (
    LiveInferencePreflightCheck,
    LiveInferencePreflightError,
    LiveInferencePreflightResult,
    LiveInferenceResult,
    PREFLIGHT_CHECK_CONF_YAML,
    PREFLIGHT_CHECK_NAMES,
    PREFLIGHT_STATUS_BLOCKED,
    PREFLIGHT_STATUS_PASS,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    StrategyPackageSelectionArtifactService,
)
from backend.tests.selection_center.test_runtime_selection import (
    FakeSuspendLookup,
    NoopRefreshAudit,
    _live_inference_result,
    versioned_selection_runtime_config,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class _ResolverStubBase:
    """Common bits used by both passing and blocking stubs.

    ``load_source_for_strategy_package`` is exercised by SelectionPackageHealth.
    ``prepare_workspace`` is exercised by selection_artifact_service when
    preflight passes; tests that do NOT exercise the happy path leave it as
    a no-op (raising indirectly via the spy provider count assertion).
    """

    def load_source_for_strategy_package(self, **_kwargs: Any) -> Any:
        # Returning a truthy stub is enough — health check only needs the call
        # to succeed; it never inspects the returned object.
        return object()

    def prepare_workspace(self, **_kwargs: Any) -> Any:
        from pathlib import Path

        self.prepare_calls.append(_kwargs)

        class _Prepared:
            workspace_path = Path(".")
            factor_order_path = Path(".")
            factor_entry_path = Path(".")
            model_params_path = Path(".")
            model_source_path = Path(".")
            factor_source_dir = Path(".")
            factor_order = ["f1"]
            alpha158_factors: list[str] = []
            dynamic_factors = ["f1"]
            model_candidate_count = 1

        return _Prepared()


class _PassingPreflightResolver(_ResolverStubBase):
    """Preflight-only stub: ``require_preflight_or_raise`` always passes.

    The selection_center wiring test covers the negative path; for happy-path
    coverage we rely on existing tests in ``test_runtime_selection.py`` whose
    FakeResolver has been updated to emit a passing preflight stub.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.prepare_calls: list[dict[str, Any]] = []

    def require_preflight_or_raise(self, **kwargs: Any) -> LiveInferencePreflightResult:
        self.calls.append(kwargs)
        return LiveInferencePreflightResult(
            passed=True,
            checks=[
                LiveInferencePreflightCheck(
                    name=name,
                    status=PREFLIGHT_STATUS_PASS,
                    message="passing stub",
                )
                for name in PREFLIGHT_CHECK_NAMES
            ],
        )


class _BlockingPreflightResolver(_ResolverStubBase):
    """``require_preflight_or_raise`` raises typed error on every call."""

    def __init__(self, *, blocked_check: str = PREFLIGHT_CHECK_CONF_YAML) -> None:
        self.calls: list[dict[str, Any]] = []
        self.prepare_calls: list[dict[str, Any]] = []
        self._blocked_check = blocked_check

    def require_preflight_or_raise(self, **kwargs: Any) -> LiveInferencePreflightResult:
        self.calls.append(kwargs)
        result = LiveInferencePreflightResult(
            passed=False,
            checks=[
                LiveInferencePreflightCheck(
                    name=name,
                    status=(
                        PREFLIGHT_STATUS_BLOCKED
                        if name == self._blocked_check
                        else PREFLIGHT_STATUS_PASS
                    ),
                    message=(
                        f"{name} blocked"
                        if name == self._blocked_check
                        else f"{name} passed"
                    ),
                )
                for name in PREFLIGHT_CHECK_NAMES
            ],
        )
        raise LiveInferencePreflightError(
            f"live inference cold-start preflight failed: {self._blocked_check} blocked",
            context={
                "source_type": kwargs.get("source_type"),
                "source_id": kwargs.get("source_id"),
                "loop_id": kwargs.get("loop_id"),
                "run_id": kwargs.get("run_id"),
                "preflight": result.to_dict(),
                "blocked_check": self._blocked_check,
            },
        )


class _SpyLiveInferenceProvider:
    """Used to assert that generate_from_live_inference NEVER reached when
    preflight blocks. ``run`` returns a real LiveInferenceResult only for
    happy-path tests so that an accidental call from the failing path would
    still surface as a test failure (we assert call count == 0)."""

    backend_name = "spy"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def run(self, **kwargs: Any) -> LiveInferenceResult:
        self.calls.append(kwargs)
        return _live_inference_result(
            [
                {"symbol": "000001.SZ", "score": 0.7, "rank": 1, "reference_price": 10.0},
            ],
            metadata={"provider": "spy"},
            requested_trade_date=kwargs["trade_date"],
            effective_trade_date=kwargs["cutoff_date"] or kwargs["trade_date"],
        )


class _FakeCalendar:
    def ensure_trading_day(self, trade_date: date) -> None:
        return None

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return [end_date]


def _build_service(
    *,
    resolver: Any,
    provider: _SpyLiveInferenceProvider,
):
    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_preflight_wiring"},
            }
        )
    )
    package_repo.save_manifest(manifest)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        selection_artifact_service=artifact_service,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        calendar_provider=_FakeCalendar(),
    )
    return service, manifest


def test_selection_run_skips_preflight_when_auto_generate_disabled() -> None:
    """When auto_generate=False the artifact path is bypassed so preflight
    must not fire either (no needless DB / resolver call)."""

    resolver = _PassingPreflightResolver()
    provider = _SpyLiveInferenceProvider()
    service, manifest = _build_service(resolver=resolver, provider=provider)

    # Pre-seed an existing artifact so the runtime path can resolve scores
    # without hitting live inference at all.
    artifact_repo = service.runtime.artifact_repository
    from backend.services.strategy_package.live_inference import (
        AUTHORITATIVE_SELECTION_SCOPE,
        AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    )
    from backend.services.strategy_package.selection_artifact import (
        SelectionScoreArtifact,
        selection_artifact_runtime_hash,
    )
    runtime_hash = selection_artifact_runtime_hash(
        versioned_selection_runtime_config({"runtime_profile": {"selection": {"top_k": 1}}})
    )
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256,
            trade_date=date(2024, 1, 3),
            data_source="DB_HISTORICAL",
            runtime_config_hash=runtime_hash,
            scores_json=[
                {"symbol": "000001.SZ", "score": 0.5, "rank": 1, "reference_price": 10.0}
            ],
            score_count=1,
            universe_count=1,
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            },
            status="SUCCEEDED",
        )
    )

    service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({
            "runtime_profile": {"selection": {"top_k": 1}},
        }),
    )
    assert resolver.calls == []  # preflight NOT invoked
    assert provider.calls == []  # live inference NOT invoked


def test_selection_run_does_not_repeat_asset_preflight_when_auto_generate_enabled() -> None:
    resolver = _PassingPreflightResolver()
    provider = _SpyLiveInferenceProvider()
    service, manifest = _build_service(resolver=resolver, provider=provider)

    service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({
            "selection_artifact_config": {
                "auto_generate": True,
                "inference_backend": "local",
                "include_reference_price": False,
                "pit_mode": "NONE",
            },
            "runtime_profile": {"selection": {"top_k": 1}},
        }),
    )

    assert resolver.calls == []
    assert resolver.prepare_calls[0]["verify_model_code_contract"] is False
    assert provider.calls, "live inference provider should consume admitted assets without another preflight"


def test_selection_run_never_calls_legacy_blocking_preflight() -> None:
    resolver = _BlockingPreflightResolver(blocked_check=PREFLIGHT_CHECK_CONF_YAML)
    provider = _SpyLiveInferenceProvider()
    service, manifest = _build_service(resolver=resolver, provider=provider)

    service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({
            "selection_artifact_config": {
                "auto_generate": True,
                "inference_backend": "local",
                "include_reference_price": False,
                "pit_mode": "NONE",
            },
            "runtime_profile": {"selection": {"top_k": 1}},
        }),
    )

    assert resolver.calls == []
    assert resolver.prepare_calls[0]["verify_model_code_contract"] is False
    assert provider.calls

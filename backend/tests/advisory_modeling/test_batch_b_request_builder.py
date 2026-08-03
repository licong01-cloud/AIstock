from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.models import (
    HistoricalRangeAdmittedComponentV1,
    HistoricalRangeAdmittedPackageProjectionV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeFrozenProgramV1,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from scripts.advisory_short_rebound_batch_b import (
    BatchBRequestBuilder,
    _load_environment,
    build_parser,
    publish_batch_b_request,
)
from backend.services.advisory_modeling.style_profile import SHORT_REBOUND_TARGET_PACKAGE_ID
from backend.services.advisory_phase1.label_policy import TradingCalendar
from backend.services.advisory_program import (
    AdvisoryProgram,
    AdvisoryStrategyBindingVersion,
)


_COMMIT = "1" * 40


def _program() -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id="advp_short_rebound",
        program_name="Short rebound",
        status="ENABLED",
        target_count=20,
        package_mode="single_package",
        package_ids=[SHORT_REBOUND_TARGET_PACKAGE_ID],
        package_weights={SHORT_REBOUND_TARGET_PACKAGE_ID: 1.0},
        fusion_method=None,
        package_set_hash="2" * 64,
        fusion_policy_sha256=None,
        review_policy={"rank_enter_threshold": 20},
        review_policy_sha256="3" * 64,
        entry_price_basis="NEXT_OPEN",
        exit_price_basis="NEXT_OPEN",
        review_schedule={},
        version=7,
        enabled_since=datetime(2026, 6, 10, tzinfo=UTC),
    )


def _binding(*, binding_id: str = "advbind_short_rebound_v7") -> AdvisoryStrategyBindingVersion:
    return AdvisoryStrategyBindingVersion(
        binding_version_id=binding_id,
        program_id="advp_short_rebound",
        program_version=7,
        package_mode="single_package",
        package_ids=[SHORT_REBOUND_TARGET_PACKAGE_ID],
        package_weights={SHORT_REBOUND_TARGET_PACKAGE_ID: 1.0},
        fusion_method=None,
        package_set_hash="2" * 64,
        fusion_policy_sha256=None,
        effective_from_trade_date=date(2026, 6, 15),
        activated_at=datetime(2026, 6, 16, tzinfo=UTC),
    )


def _frozen_program() -> HistoricalRangeFrozenProgramV1:
    components = (
        HistoricalRangeAdmittedComponentV1(
            component_id="leg-a",
            weight=Decimal("0.6"),
            factor_order=("factor-a",),
            required_window=60,
            buffer_trading_days=5,
            runtime_input_identity_hash="4" * 64,
            lookback_contract_hash="5" * 64,
        ),
        HistoricalRangeAdmittedComponentV1(
            component_id="leg-b",
            weight=Decimal("0.4"),
            factor_order=("factor-b",),
            required_window=20,
            buffer_trading_days=5,
            runtime_input_identity_hash="6" * 64,
            lookback_contract_hash="7" * 64,
        ),
    )
    projection = HistoricalRangeAdmittedPackageProjectionV1(
        package_id=SHORT_REBOUND_TARGET_PACKAGE_ID,
        package_version="1",
        manifest_sha256="8" * 64,
        alpha_mode=HistoricalRangeAlphaMode.MULTI_ALPHA,
        components=components,
    )
    return HistoricalRangeFrozenProgramV1(
        research_program_id="advp_short_rebound",
        source_program_id="advp_short_rebound",
        source_program_version=7,
        source_binding_version_id="advbind_short_rebound_v7",
        package_id=SHORT_REBOUND_TARGET_PACKAGE_ID,
        package_version="1",
        manifest_sha256="8" * 64,
        alpha_mode=HistoricalRangeAlphaMode.MULTI_ALPHA,
        program_config={"program_id": "advp_short_rebound"},
        program_config_hash=canonical_json_sha256({"program_id": "advp_short_rebound"}),
        runtime_config={},
        runtime_config_hash=canonical_json_sha256({}),
        review_policy={"rank_enter_threshold": 20},
        review_policy_hash=canonical_json_sha256({"rank_enter_threshold": 20}),
        code_release_id="git_1111111111111111",
        code_release_hash="9" * 64,
        selection_semantics_version="strategy_package_selection_semantics_v1",
        selection_semantics_hash="a" * 64,
        list_semantics_version="advisory_historical_range_list_semantics_v2",
        list_semantics_hash="b" * 64,
        target_package_asset_root_hash="c" * 64,
        input_warmup_contract_hash="d" * 64,
        admitted_package_projection_hash=canonical_json_sha256(
            projection.model_dump(mode="json")
        ),
        admitted_package_projection=projection,
    )


class _ProgramReader:
    def __init__(self, bindings=None) -> None:
        self.bindings = list(bindings or [_binding()])

    @staticmethod
    def get_program(_program_id: str) -> AdvisoryProgram:
        return _program()

    def list_binding_versions(self, _program_id: str):
        return list(self.bindings)


def _calendar(start: date, end: date) -> TradingCalendar:
    dates = tuple(start + timedelta(days=offset) for offset in range((end - start).days + 31))
    return TradingCalendar(calendar_version="market.trading_calendar:test", trading_dates=dates)


def test_request_builder_resolves_complete_authoritative_identity() -> None:
    frozen_calls = []

    def freeze(spec, start, end):
        frozen_calls.append((spec, start, end))
        return _frozen_program()

    builder = BatchBRequestBuilder(
        program_reader=_ProgramReader(),
        frozen_program_provider=freeze,
        package_created_at_provider=lambda _package_id: datetime(2026, 6, 1, tzinfo=UTC),
        calendar_provider=_calendar,
        repository_commit=_COMMIT,
    )
    request = builder.build(
        program_id="advp_short_rebound",
        decision_date_start=date(2020, 1, 1),
        decision_date_end=date(2026, 7, 1),
        final_fit_as_of=datetime(2026, 7, 21, 23, 59, tzinfo=UTC),
    )

    assert request.existing_program.expected_program_version == 7
    assert request.existing_program.expected_binding_version_id == "advbind_short_rebound_v7"
    assert request.style_profile.effective_package_oos_cutoff == date(2026, 6, 16)
    assert request.dataset_intent.package_manifest_sha256 == "8" * 64
    assert request.dataset_intent.package_asset_closure_hash == "c" * 64
    assert request.dataset_intent.selection_runtime_semantics_hash == "a" * 64
    assert request.dataset_intent.repository_commit == _COMMIT
    assert request.dataset_intent.calendar_hash == _calendar(
        date(2020, 1, 1), date(2026, 7, 1)
    ).calendar_hash
    expected_components = tuple(
        item.model_dump(mode="json")
        for item in _frozen_program().admitted_package_projection.components
    )
    assert request.dataset_intent.multi_alpha_component_identity_set_hash == canonical_json_sha256(
        expected_components
    )
    assert len(frozen_calls) == 1


def test_request_builder_rejects_ambiguous_active_binding() -> None:
    reader = _ProgramReader(bindings=[_binding(), _binding(binding_id="other")])
    builder = BatchBRequestBuilder(
        program_reader=reader,
        frozen_program_provider=lambda *_args: _frozen_program(),
        package_created_at_provider=lambda _package_id: datetime(2026, 6, 1, tzinfo=UTC),
        calendar_provider=_calendar,
        repository_commit=_COMMIT,
    )

    with pytest.raises(ValueError, match="exactly one active binding"):
        builder.build(
            program_id="advp_short_rebound",
            decision_date_start=date(2020, 1, 1),
            decision_date_end=date(2026, 7, 1),
            final_fit_as_of=datetime(2026, 7, 21, 23, 59, tzinfo=UTC),
        )


def test_request_publish_is_content_addressed_and_repo_external(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    artifact = tmp_path / "artifact"
    repository.mkdir()
    artifact.mkdir()
    builder = BatchBRequestBuilder(
        program_reader=_ProgramReader(),
        frozen_program_provider=lambda *_args: _frozen_program(),
        package_created_at_provider=lambda _package_id: datetime(2026, 6, 1, tzinfo=UTC),
        calendar_provider=_calendar,
        repository_commit=_COMMIT,
    )
    request = builder.build(
        program_id="advp_short_rebound",
        decision_date_start=date(2020, 1, 1),
        decision_date_end=date(2026, 7, 1),
        final_fit_as_of=datetime(2026, 7, 21, 23, 59, tzinfo=UTC),
    )

    first = publish_batch_b_request(
        request=request,
        artifact_root=artifact,
        repository_root=repository,
    )
    second = publish_batch_b_request(
        request=request,
        artifact_root=artifact,
        repository_root=repository,
    )

    assert first == second
    assert first.name == f"{request.request_hash}.json"
    with pytest.raises(ValueError, match="outside repository_root"):
        publish_batch_b_request(
            request=request,
            artifact_root=repository,
            repository_root=repository,
        )


def test_cli_prepare_mode_does_not_require_a_prebuilt_request_or_spool() -> None:
    args = build_parser().parse_args(
        [
            "--prepare-program-id",
            "advp_short_rebound",
            "--decision-date-start",
            "2020-01-02",
            "--decision-date-end",
            "2026-07-01",
            "--final-fit-as-of",
            "2026-07-21T23:59:00+00:00",
            "--env-file",
            "F:/configured/.env",
            "--repository-root",
            "F:/Dev/AIstock",
            "--artifact-root",
            "F:/Dev/AIstock_artifacts/advisory_modeling",
        ]
    )

    assert args.request is None
    assert args.spool_root is None
    assert args.prepare_program_id == "advp_short_rebound"
    assert args.decision_date_start == date(2020, 1, 2)


def test_prepare_environment_requires_db_identity_but_not_materialization_roots(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "TDX_DB_HOST=localhost",
                "TDX_DB_PORT=5432",
                "TDX_DB_NAME=aistock",
                "TDX_DB_USER=research",
                "TDX_DB_PASSWORD=secret",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    values, resolved = _load_environment(env_file, require_runtime=False)

    assert resolved == env_file.resolve()
    assert values["TDX_DB_NAME"] == "aistock"

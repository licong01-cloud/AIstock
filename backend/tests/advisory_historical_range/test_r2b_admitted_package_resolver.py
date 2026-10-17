from __future__ import annotations

import pytest

from backend.services.advisory_historical_range.request_resolver import HistoricalRangeAdmittedPackageResolver
from backend.services.strategy_package.advisory_input_projection import project_advisory_inputs
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.tests.strategy_package.test_multi_alpha_live_selection import _make_parent


class _PackageReader:
    def __init__(self, delegate) -> None:  # noqa: ANN001
        self.delegate = delegate
        self.get_calls: list[str] = []

    def get(self, package_id: str):  # noqa: ANN201
        self.get_calls.append(package_id)
        return self.delegate.get(package_id)

    def validate(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
        raise AssertionError("historical resolver must not repeat package validation")

    def health(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
        raise AssertionError("historical resolver must not query package health")


def test_admitted_multi_alpha_resolution_uses_historical_projection_without_second_admission() -> None:
    repository, parent = _make_parent(live_weight_policy=False)
    reader = _PackageReader(repository)
    resolver = HistoricalRangeAdmittedPackageResolver(package_reader=reader)
    live_before = project_advisory_inputs(parent.current_manifest()).projection_hash

    resolved = resolver.resolve(parent.package_id)

    assert reader.get_calls == [parent.package_id]
    assert resolved.historical_projection.pit_universe_policy == "REQUIRE_EXISTING_READ_ONLY"
    assert resolved.historical_projection.pit_universe_ensure is False
    assert resolved.admitted_projection.alpha_mode.value == "multi_alpha"
    assert len(resolved.admitted_projection.components) == 2
    assert sum(item.weight for item in resolved.admitted_projection.components) == 1
    assert project_advisory_inputs(parent.current_manifest()).projection_hash == live_before
    assert resolved.historical_projection.query_contract_hash != project_advisory_inputs(
        parent.current_manifest()
    ).selection_query_contract_hash


@pytest.mark.parametrize("sequence_length", [1, 20])
def test_frozen_warmup_includes_the_actual_sequence_history(sequence_length: int) -> None:
    repository, parent = _make_parent(live_weight_policy=False)
    component_ids = [component.alpha_id for component in parent.current_manifest().alpha_components]
    baseline = HistoricalRangeAdmittedPackageResolver(package_reader=_PackageReader(repository)).resolve(parent.package_id)
    reader = _PackageReader(repository)
    resolver = HistoricalRangeAdmittedPackageResolver(
        package_reader=reader,
        sequence_length_reader=lambda _manifest: dict.fromkeys(component_ids, sequence_length),
    )

    actual = resolver.resolve(parent.package_id).admitted_projection

    assert reader.get_calls == [parent.package_id]
    for old, new in zip(baseline.admitted_projection.components, actual.components, strict=True):
        assert new.required_window == old.required_window + sequence_length - 1
        assert new.buffer_trading_days == old.buffer_trading_days == 5
        assert new.factor_order == old.factor_order
        assert (new.lookback_contract_hash == old.lookback_contract_hash) == (sequence_length == 1)


@pytest.mark.parametrize("invalid", [True, 0, 1.5, None])
def test_missing_or_invalid_sequence_metadata_cannot_silently_freeze_a_short_window(invalid) -> None:  # noqa: ANN001
    repository, parent = _make_parent(live_weight_policy=False)
    component_ids = [component.alpha_id for component in parent.current_manifest().alpha_components]
    resolver = HistoricalRangeAdmittedPackageResolver(
        package_reader=_PackageReader(repository),
        sequence_length_reader=lambda _manifest: dict.fromkeys(component_ids, invalid),
    )
    with pytest.raises(RuntimeConfigInvalidError, match="actual positive length"):
        resolver.resolve(parent.package_id)

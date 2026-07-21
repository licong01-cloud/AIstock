from __future__ import annotations

from backend.services.advisory_historical_range.request_resolver import HistoricalRangeAdmittedPackageResolver
from backend.services.strategy_package.advisory_input_projection import project_advisory_inputs
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

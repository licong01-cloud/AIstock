from dataclasses import replace
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_component_preparation import (
    ComponentPreparationError, component_dependencies, preparation_plan,
)
from backend.services.dataset_release.monthly_preparation_composition import preparation_eligible_domains
from backend.tests.dataset_release.test_monthly_preparation_sector import sector  # noqa: F401


def inputs(sector):  # noqa: F811 - imported pytest fixture
    _cas, snapshot, audit, profile, _output, _rows = sector
    dependencies = component_dependencies()
    required = set().union(*(set(row) for name, row in dependencies.items() if name != "factor"))
    snapshot = replace(snapshot, partitions=tuple(
        SimpleNamespace(spec=SimpleNamespace(dataset=dataset)) for dataset in sorted(required)
    ))
    audit["plan"] = preparation_plan(operation_id=snapshot.operation_id, cutoff=snapshot.official_cutoff,
                                     blocking_datasets=snapshot.omitted_datasets)
    return snapshot, audit, profile


def test_deferred_margin_does_not_block_seven_independent_domains(sector):  # noqa: F811
    snapshot, audit, profile = inputs(sector)
    result = preparation_eligible_domains(profile=profile, snapshot=snapshot, audit=audit)
    assert len(result) == 8
    assert result["factor"]["status"] == "DEFERRED"
    assert result["factor"]["blocking_datasets"] == ["margin_detail"]
    assert sum(row["status"] == "ELIGIBLE" for row in result.values()) == 7
    assert not any(row["status"] == "PASS" for row in result.values())


def test_real_sector_gate_failure_preserves_independent_price_eligibility(sector):  # noqa: F811
    snapshot, audit, profile = inputs(sector)
    gate = next(row for row in audit["gates"] if row["gate_id"] == "sector_authority")
    gate.update(observed_count=3, unexplained_missing_count=1)
    result = preparation_eligible_domains(profile=profile, snapshot=snapshot, audit=audit)
    assert result["sector_context"]["blocking_gates"] == ["sector_authority"]
    assert result["sector_context"]["status"] == "DEFERRED"
    assert result["day"]["status"] == result["minute"]["status"] == "ELIGIBLE"


@pytest.mark.parametrize("drift", ["plan", "missing_fact_domain", "count", "common_control"])
def test_malformed_or_missing_private_source_never_becomes_eligible(sector, drift):  # noqa: F811
    snapshot, audit, profile = inputs(sector)
    if drift == "plan":
        audit["plan"]["components"]["factor"]["status"] = "ELIGIBLE"
    elif drift == "missing_fact_domain":
        snapshot = replace(snapshot, partitions=tuple(row for row in snapshot.partitions
                                                      if row.spec.dataset != "sector_data"))
    elif drift == "count":
        audit["gates"][-1]["expected_count"] += 1
    else:
        gate = next(row for row in audit["gates"] if row["gate_id"] == "calendar_lifecycle")
        gate.update(observed_count=3, unexplained_missing_count=1)
    with pytest.raises(ComponentPreparationError):
        preparation_eligible_domains(profile=profile, snapshot=snapshot, audit=audit)

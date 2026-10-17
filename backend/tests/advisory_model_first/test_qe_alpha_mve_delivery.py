from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _deliver_bundle,
    _find_existing_bundle,
    _publish_bundle,
    inspect_qe_alpha_mve_bundle,
)
from backend.services.advisory_model_first.research_control import (
    evidence_reference_for_file,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_qe_alpha_mve_contracts import (
    make_qe_alpha_mve_request,
)
from scripts.advisory_qe_alpha_mve_run import main


def _published_bundle(tmp_path):
    static = tmp_path / "static_factors.parquet"
    pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2024-07-04")],
            "instrument": ["000001.SZ"],
            "value": [1.0],
        }
    ).to_parquet(static, index=False)
    n2b = tmp_path / "n2b"
    n2b.mkdir()
    outcomes_path = n2b / "arm_signal_outcomes.parquet"
    pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
            "instrument": ["000001.SZ"],
        }
    ).to_parquet(outcomes_path, index=False)
    request = make_qe_alpha_mve_request(
        output_root=(tmp_path / "output").as_posix(),
        registry_path=(tmp_path / "trial_registry.jsonl").as_posix(),
        route_path=(tmp_path / "current_route.md").as_posix(),
        n2b_bundle_path=n2b.as_posix(),
        outcomes_path=outcomes_path.as_posix(),
        outcomes_ref=evidence_reference_for_file(
            outcomes_path,
            role="n3_current_parent_signal_outcomes",
        ),
        static_factor_ref=evidence_reference_for_file(static, role="n3_static_factors_parquet"),
    )
    score_panel = pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
            "instrument": ["000001.SZ"],
            "score": [0.1],
            "economic_net_excess_bps": [10.0],
            "outcome_known": [True],
            **{proposal.proposal_id: [0.2] for proposal in request.proposals},
        }
    )
    daily = pd.DataFrame(
        {
            "proposal_id": [request.proposals[0].proposal_id],
            "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
            "rank_ic": [0.1],
        }
    )
    summary = {
        "schema_version": "advisory_qe_alpha_mve_proposal_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": 24,
        "proposals": [],
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier = {
        "schema_version": "advisory_qe_alpha_mve_frontier_v1",
        "request_sha256": request.request_sha256,
        "selection_rule": "TEST_FROZEN_NONE",
        "eligible_proposal_ids": [],
        "selected_proposal_id": None,
        "selected_trial_count": 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    outcomes = pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2024-07-04")],
            "instrument": ["000001.SZ"],
        }
    )
    sources = {
        "outcomes": outcomes,
        "benchmark": "000300.SH",
        "n2b_request": {"dataset_identity": "a" * 64},
        "n2b_manifest": {"policy_identity": "b" * 64},
    }
    bundle = _publish_bundle(
        request=request,
        sources=sources,
        score_panel=score_panel,
        daily_metrics=daily,
        proposal_summary=summary,
        frontier=frontier,
        elapsed_seconds=1.0,
    )
    return request, bundle


def test_publish_inspect_delivery_and_exact_registry_noop(tmp_path) -> None:
    request, bundle = _published_bundle(tmp_path)

    inspected = inspect_qe_alpha_mve_bundle(bundle)
    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)

    assert inspected["status"] == "VALID"
    assert inspected["selected_trial_count"] == 0
    assert inspected["factor_catalog_written"] is False
    assert _find_existing_bundle(request) == bundle
    assert first["registry"]["appended_count"] == 1
    assert second["registry"]["duplicate_noop_count"] == 1
    assert first["route"]["route_sha256"] == second["route"]["route_sha256"]


def test_bundle_mutation_fails_closed(tmp_path) -> None:
    _, bundle = _published_bundle(tmp_path)
    pd.DataFrame({"corrupted": [True]}).to_parquet(bundle / "daily_metrics.parquet", index=False)

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        inspect_qe_alpha_mve_bundle(bundle)
    assert exc_info.value.reason_code == "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID"


def test_cli_argument_failure_is_typed(capsys) -> None:
    assert main(["prepare"]) == 1
    assert "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID" in capsys.readouterr().out

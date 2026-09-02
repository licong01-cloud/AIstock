from __future__ import annotations

import json

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.qe_alpha_generator_contracts import (
    GENERATOR_ALLOWED_FIELDS,
    build_generator_proposal,
)
from backend.services.advisory_model_first.qe_alpha_generator_pipeline import (
    build_catalog_snapshot,
    build_family_prompt,
    evaluate_generated_overlays,
    expression_fingerprint,
    generate_alpha_candidates,
    inspect_generation_bundle,
    weighted_jaccard,
)
from backend.services.advisory_model_first.qe_alpha_mve_contracts import MVE_FAMILIES, build_default_proposals
from backend.tests.advisory_model_first.test_qe_alpha_generator_contracts import make_generator_request


def test_catalog_snapshot_excludes_code_performance_and_secret_values() -> None:
    snapshot = build_catalog_snapshot(
        [
            {
                "factor_name": "alpha_x",
                "source": "qe",
                "variables": {"db_pe_ttm": "T-visible"},
                "code_text": "def alpha_x(): return 1",
                "ic": 0.9,
                "performance_metrics": {"return": 99},
                "api_key": "must-not-survive",
            }
        ]
    )
    encoded = json.dumps(snapshot, ensure_ascii=False)
    assert snapshot["row_count"] == 1
    assert snapshot["read_only_transaction"] is True
    assert "def alpha_x" not in encoded
    assert "must-not-survive" not in encoded
    assert "performance_metrics" not in encoded
    assert snapshot["records"][0]["code_text_sha256"]


def test_weighted_structural_fingerprint_distinguishes_new_fields() -> None:
    left = expression_fingerprint(
        {"op": "ADD", "args": [{"op": "FIELD", "field": "db_pe_ttm"}, {"op": "FIELD", "field": "mf_lg_buy_amt"}]}
    )
    exact = expression_fingerprint(
        {"op": "ADD", "args": [{"op": "FIELD", "field": "db_pe_ttm"}, {"op": "FIELD", "field": "mf_lg_buy_amt"}]}
    )
    different = expression_fingerprint(
        {
            "op": "SAFE_DIVIDE",
            "args": [{"op": "FIELD", "field": "cp_cost_15pct"}, {"op": "FIELD", "field": "cp_cost_85pct"}],
        }
    )
    assert weighted_jaccard(left, exact) == 1.0
    assert weighted_jaccard(left, different) < 0.5


def test_generation_is_six_calls_target_free_and_exact_retry(tmp_path) -> None:
    request = make_generator_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    old = set(request.old_source_fields)
    new_fields = sorted(set(GENERATOR_ALLOWED_FIELDS) - old - {"market_regime"})
    calls: list[str] = []

    def fake_call(system_prompt, user_prompt, _request):
        payload = json.loads(user_prompt)
        family = payload["family"]
        family_index = MVE_FAMILIES.index(family)
        calls.append(family)
        proposals = []
        for index in range(4):
            left = new_fields[family_index * 8 + index * 2]
            right = new_fields[family_index * 8 + index * 2 + 1]
            proposals.append(
                {
                    "economic_hypothesis": f"{family} target-free mechanism {index}",
                    "mechanism": f"combine two previously unused T-visible fields for mechanism {index}",
                    "known_effect_exposures": ["LIQUIDITY", "VALUE"],
                    "expression": {
                        "op": "ADD" if index % 2 == 0 else "SAFE_DIVIDE",
                        "args": [
                            {"op": "FIELD", "field": left},
                            {
                                "op": "ADD",
                                "args": [{"op": "FIELD", "field": right}, {"op": "CONST", "value": index + 1.0}],
                            },
                        ],
                    },
                }
            )
        return json.dumps({"proposals": proposals}), {"total_tokens": 100}

    first = generate_alpha_candidates(request_path, llm_call=fake_call)
    assert calls == list(MVE_FAMILIES)
    assert first["accepted_expression_count"] == 24
    assert first["raw_generation_attempt_count"] == 24
    assert first["target_or_economic_metric_exposed"] is False
    assert inspect_generation_bundle(first["bundle_path"])["status"] == "VALID"

    def forbidden_retry(*_args):
        raise AssertionError("exact retry must not call the LLM")

    second = generate_alpha_candidates(request_path, llm_call=forbidden_retry)
    assert second["exact_retry"] is True
    assert second["bundle_id"] == first["bundle_id"]

    prompt = build_family_prompt(request, MVE_FAMILIES[0], build_default_proposals(), build_catalog_snapshot([]))
    assert "economic_net_excess_bps" not in "".join(prompt)
    assert "top5_lift" not in "".join(prompt)


def test_fixed_overlay_selects_only_consistently_incremental_candidate(tmp_path) -> None:
    request = make_generator_request(tmp_path)
    proposal = build_generator_proposal(
        proposal_id="N3G_PRICE_VOLUME_BEHAVIOR_01",
        family="PRICE_VOLUME_BEHAVIOR",
        economic_hypothesis="A persistent target-free ordering resolves a frozen parent boundary tie",
        mechanism="A new T-visible field separates two otherwise tied parent candidates at the Top5 boundary",
        known_effect_exposures=("MOMENTUM",),
        expression={"op": "FIELD", "field": "db_pe_ttm"},
    )
    dates = pd.bdate_range(request.signal_start, periods=382)
    rows = []
    for day in dates:
        for index in range(30):
            parent = float(index)
            if index in (24, 25):
                parent = 24.0
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{index:06d}.SZ",
                    "score": parent,
                    "economic_net_excess_bps": float(index * 10),
                    "outcome_known": True,
                    proposal.proposal_id: float(index),
                }
            )
    originality = [
        {
            "proposal_id": proposal.proposal_id,
            "parent_score_spearman_mean": 0.70,
            "max_abs_old_score_spearman_mean": 0.50,
            "accepted": True,
            "reason_codes": [],
        }
    ]
    _, daily, summary, stability, frontier = evaluate_generated_overlays(
        panel=pd.DataFrame(rows), proposals=(proposal,), request=request, originality=originality
    )
    assert len(daily) == 382
    assert daily["intervention"].all()
    result = summary["proposals"][0]
    assert result["rank_ic_delta_mean"] > 0
    assert result["top5_lift_mean_bps"] > 0
    assert result["positive_joint_time_block_count"] == 4
    assert result["eligible"] is True
    assert frontier["selected_proposal_id"] == proposal.proposal_id
    assert len(stability["rows"]) == 4


def test_unknown_top5_outcome_is_preserved_and_metric_is_unavailable(tmp_path) -> None:
    request = make_generator_request(tmp_path)
    proposal = build_generator_proposal(
        proposal_id="N3G_PRICE_VOLUME_BEHAVIOR_01",
        family="PRICE_VOLUME_BEHAVIOR",
        economic_hypothesis="A target-free ordering keeps unknown outcomes visible",
        mechanism="The score is complete while one top candidate outcome is intentionally not mature",
        known_effect_exposures=("MOMENTUM",),
        expression={"op": "FIELD", "field": "db_pe_ttm"},
    )
    rows = []
    for day in pd.bdate_range(request.signal_start, periods=2):
        for index in range(6):
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{index:06d}.SZ",
                    "score": float(index),
                    "economic_net_excess_bps": np.nan if index == 5 else float(index),
                    "outcome_known": index != 5,
                    proposal.proposal_id: float(index),
                }
            )
    originality = [
        {
            "proposal_id": proposal.proposal_id,
            "parent_score_spearman_mean": 0.70,
            "max_abs_old_score_spearman_mean": 0.50,
            "accepted": True,
            "reason_codes": [],
        }
    ]
    _, daily, _, _, _ = evaluate_generated_overlays(
        panel=pd.DataFrame(rows), proposals=(proposal,), request=request, originality=originality
    )
    assert daily["parent_top5_net_excess_bps"].isna().all()
    assert daily["overlay_top5_net_excess_bps"].isna().all()
    assert daily["top5_lift_bps"].isna().all()

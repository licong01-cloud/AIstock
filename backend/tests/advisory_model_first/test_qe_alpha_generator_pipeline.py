from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import backend.services.advisory_model_first.qe_alpha_generator_pipeline as generator_pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.qe_alpha_generator_contracts import (
    GENERATOR_ALLOWED_FIELDS,
    GENERATOR_PROMPT_SCHEMA_V1,
    build_generator_proposal,
)
from backend.services.advisory_model_first.qe_alpha_generator_pipeline import (
    _allowed_fields_from_parent_source,
    _parse_family_proposals,
    build_catalog_snapshot,
    build_family_prompt,
    evaluate_generated_overlays,
    expression_fingerprint,
    generate_alpha_candidates,
    inspect_generation_bundle,
    run_generator_mve,
    weighted_jaccard,
)
from backend.services.advisory_model_first.qe_alpha_mve_contracts import MVE_FAMILIES, build_default_proposals
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import _static_schema_sha256
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
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


def test_parent_source_schema_excludes_fields_absent_from_concrete_parquet(tmp_path) -> None:
    static_path = tmp_path / "static_factors.parquet"
    pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2026-01-05")],
            "instrument": ["000001.SZ"],
            "db_turnover_rate": [1.0],
        }
    ).to_parquet(static_path, index=False)
    static_ref = evidence_reference_for_file(static_path, role="n3_static_factors_parquet")
    parent_request = {
        "static_factor_ref": static_ref.model_dump(mode="json"),
        "static_schema_sha256": _static_schema_sha256(static_path),
    }

    allowed = _allowed_fields_from_parent_source(
        parent_request,
        old_source_fields=("close", "db_turnover_rate"),
    )

    assert "db_turnover_rate" in allowed
    assert "md_rqmcl" not in allowed
    assert "md_rzye" not in allowed


def test_generation_parser_rejects_field_outside_frozen_request_schema(tmp_path) -> None:
    request = make_generator_request(
        tmp_path,
        allowed_fields=tuple(sorted(set(GENERATOR_ALLOWED_FIELDS) - {"md_rqmcl", "sw2_open"})),
    )
    rows = [
        {
            "economic_hypothesis": "Margin short-sale pressure provides a distinct behavioral signal",
            "mechanism": "Use a T-visible margin field only when the frozen source physically provides it",
            "known_effect_exposures": ["LIQUIDITY"],
            "expression": {"op": "FIELD", "field": "md_rqmcl"},
        }
    ]

    with pytest.raises(ValueError, match="outside the frozen request source schema"):
        _parse_family_proposals(
            MVE_FAMILIES[0],
            rows,
            allowed_fields=request.allowed_fields,
        )

    catalog = build_catalog_snapshot(
        [
            {
                "factor_name": "unsupported_sector_open",
                "source": "qe",
                "variables": {"sw2_open": "T-visible"},
                "formula_hint": "rank(sw2_open)",
                "data_source": "sw2_open",
            }
        ]
    )
    _, user_prompt = build_family_prompt(
        request,
        "SECTOR_RELATIVE",
        build_default_proposals(),
        catalog,
    )
    assert "sw2_open" not in user_prompt


def test_prompt_v2_freezes_operator_parameters_and_json_response_mode(tmp_path, monkeypatch) -> None:
    request = make_generator_request(tmp_path)
    _, user_prompt = build_family_prompt(
        request,
        "REGIME_CONDITIONED",
        build_default_proposals(),
        build_catalog_snapshot([]),
    )
    payload = json.loads(user_prompt)
    declared_operators = [operator for item in payload["operator_contract"] for operator in item["operators"]]
    assert len(declared_operators) == len(set(declared_operators))
    assert set(declared_operators) == set(request.allowed_operators)
    trailing_contract = next(item for item in payload["operator_contract"] if "TRAILING_MEAN" in item["operators"])
    lag_contract = next(item for item in payload["operator_contract"] if "LAG" in item["operators"])
    assert trailing_contract["required_keys"] == ["op", "args", "window"]
    assert trailing_contract["parameter_rules"]["window"] == "integer 2..252 inclusive"
    assert lag_contract["parameter_rules"]["periods"] == "integer 1..252 inclusive"
    assert payload["response_contract"] == {
        "transport": "json_object",
        "top_level_keys": ["proposals"],
        "proposals_count": 4,
        "prose_or_markdown_allowed": False,
    }

    captured: dict[str, object] = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"proposals":[]}'))],
            usage=SimpleNamespace(prompt_tokens=1, completion_tokens=2, total_tokens=3),
        )

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=fake_completion))
    monkeypatch.setattr(
        generator_pipeline,
        "get_llm_kwargs",
        lambda _agent: {"model": request.model_identity.model},
    )
    _, telemetry = generator_pipeline._default_llm_call("system", user_prompt, request)
    assert captured["response_format"] == {"type": "json_object"}
    assert telemetry["response_format"] == "json_object"

    monkeypatch.setattr(
        generator_pipeline,
        "get_llm_kwargs",
        lambda _agent: {
            "model": request.model_identity.model,
            "response_format": {"type": "text"},
        },
    )
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        generator_pipeline._default_llm_call("system", user_prompt, request)
    assert exc_info.value.reason_code == "ADVISORY_QE_ALPHA_GENERATOR_MODEL_IDENTITY_MISMATCH"

    legacy_root = tmp_path / "legacy"
    legacy_root.mkdir()
    legacy = make_generator_request(legacy_root, prompt_schema_version=GENERATOR_PROMPT_SCHEMA_V1)
    captured.clear()
    monkeypatch.setattr(
        generator_pipeline,
        "get_llm_kwargs",
        lambda _agent: {"model": legacy.model_identity.model},
    )
    _, legacy_telemetry = generator_pipeline._default_llm_call("system", "legacy", legacy)
    assert "response_format" not in captured
    assert legacy_telemetry["response_format"] == "provider_default"


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
    new_fields = sorted(set(request.allowed_fields) - old - {"market_regime"})
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


def test_provider_failure_is_not_expression_rejection_and_recovers_only_failed_family(tmp_path) -> None:
    request = make_generator_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    old = set(request.old_source_fields)
    new_fields = sorted(set(request.allowed_fields) - old - {"market_regime"})
    first_calls: list[str] = []
    credential_marker = "_".join(("api", "key")) + "=must-not-survive"

    def response_for(family: str) -> str:
        family_index = MVE_FAMILIES.index(family)
        proposals = []
        for index in range(4):
            left = new_fields[family_index * 8 + index * 2]
            right = new_fields[family_index * 8 + index * 2 + 1]
            proposals.append(
                {
                    "economic_hypothesis": f"{family} transport-safe mechanism {index}",
                    "mechanism": f"combine two frozen T-visible inputs after transport recovery {index}",
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
        return json.dumps({"proposals": proposals})

    def first_call(_system, user, _request):
        family = json.loads(user)["family"]
        first_calls.append(family)
        if family == MVE_FAMILIES[0]:
            raise ConnectionError(f"provider TLS failed before response {credential_marker}")
        return response_for(family), {"total_tokens": 100}

    first = generate_alpha_candidates(request_path, llm_call=first_call)
    assert first_calls == list(MVE_FAMILIES)
    assert first["generation_status"] == "INFRASTRUCTURE_FAILURE"
    assert first["generation_call_count"] == 6
    assert first["raw_generation_attempt_count"] == 20
    assert first["accepted_expression_count"] == 20
    assert first["rejected_expression_count"] == 0
    assert first["failed_call_count"] == 1
    assert first["unresolved_failed_families"] == [MVE_FAMILIES[0]]
    assert credential_marker not in (Path(first["bundle_path"]) / "attempts.json").read_text(encoding="utf-8")
    first_manifest = (Path(first["bundle_path"]) / "manifest.json").read_bytes()
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        run_generator_mve(request_path, first["bundle_path"])
    assert exc_info.value.reason_code == "ADVISORY_QE_ALPHA_GENERATOR_LLM_CALL_FAILED"
    assert not Path(request.registry_path).exists()
    assert not Path(request.route_path).exists()

    recovery_calls: list[str] = []

    def recovery_call(_system, user, _request):
        family = json.loads(user)["family"]
        recovery_calls.append(family)
        return response_for(family), {"total_tokens": 100}

    recovered = generate_alpha_candidates(request_path, llm_call=recovery_call)
    assert recovery_calls == [MVE_FAMILIES[0]]
    assert recovered["recovery_attempted"] is True
    assert recovered["generation_status"] == "COMPLETE"
    assert recovered["generation_call_count"] == 7
    assert recovered["raw_generation_attempt_count"] == 24
    assert recovered["accepted_expression_count"] == 24
    assert recovered["failed_call_count"] == 1
    assert recovered["unresolved_failed_family_count"] == 0
    assert recovered["recovery_parent_bundle_id"] == first["bundle_id"]
    assert (Path(first["bundle_path"]) / "manifest.json").read_bytes() == first_manifest

    exact = generate_alpha_candidates(
        request_path,
        llm_call=lambda *_args: (_ for _ in ()).throw(AssertionError("successful families must not resample")),
    )
    assert exact["exact_retry"] is True
    assert exact["bundle_id"] == recovered["bundle_id"]


def test_schema_failure_requires_response_and_counts_four_expression_attempts(tmp_path) -> None:
    request = make_generator_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    old = set(request.old_source_fields)
    new_fields = sorted(set(request.allowed_fields) - old - {"market_regime"})
    family_counts = {family: 0 for family in MVE_FAMILIES}

    def schema_then_valid(_system, user, _request):
        prompt_payload = json.loads(user)
        family = prompt_payload["family"]
        family_counts[family] += 1
        if family_counts[family] == 1:
            return "not-json", {}
        assert prompt_payload["schema_only_retry"]["economic_feedback_included"] is False
        assert prompt_payload["schema_only_retry"]["violations"]
        family_index = MVE_FAMILIES.index(family)
        proposals = []
        for index in range(4):
            proposals.append(
                {
                    "economic_hypothesis": f"{family} schema recovery mechanism {index}",
                    "mechanism": f"combine two T-visible inputs after a schema-only retry {index}",
                    "known_effect_exposures": ["VALUE"],
                    "expression": {
                        "op": "ADD",
                        "args": [
                            {"op": "FIELD", "field": new_fields[family_index * 8 + index * 2]},
                            {"op": "FIELD", "field": new_fields[family_index * 8 + index * 2 + 1]},
                        ],
                    },
                }
            )
        return json.dumps({"proposals": proposals}), {}

    result = generate_alpha_candidates(request_path, llm_call=schema_then_valid)
    assert result["generation_status"] == "COMPLETE"
    assert result["generation_call_count"] == 12
    assert result["raw_generation_attempt_count"] == 48
    assert result["accepted_expression_count"] == 24
    assert result["rejected_expression_count"] == 24
    assert result["failed_call_count"] == 0


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

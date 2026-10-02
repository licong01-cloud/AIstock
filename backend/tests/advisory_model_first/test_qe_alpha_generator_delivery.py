from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.qe_alpha_generator_pipeline import (
    generate_alpha_candidates,
    inspect_generation_bundle,
)
from backend.services.advisory_model_first.qe_alpha_mve_contracts import MVE_FAMILIES
from backend.tests.advisory_model_first.test_qe_alpha_generator_contracts import make_generator_request
from scripts.advisory_qe_alpha_generator_mve_run import main


def _generation_bundle(tmp_path: Path) -> Path:
    request = make_generator_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    new_fields = sorted(set(request.allowed_fields) - set(request.old_source_fields) - {"market_regime"})

    def fake_call(_system, user, _request):
        family = json.loads(user)["family"]
        offset = MVE_FAMILIES.index(family) * 8
        proposals = []
        for index in range(4):
            proposals.append(
                {
                    "economic_hypothesis": f"{family} immutable proposal {index}",
                    "mechanism": f"two new T-visible inputs provide mechanism {index}",
                    "known_effect_exposures": ["VALUE"],
                    "expression": {
                        "op": "MULTIPLY" if index % 2 == 0 else "SUBTRACT",
                        "args": [
                            {"op": "FIELD", "field": new_fields[offset + index * 2]},
                            {"op": "FIELD", "field": new_fields[offset + index * 2 + 1]},
                        ],
                    },
                }
            )
        return json.dumps({"proposals": proposals}), {}

    result = generate_alpha_candidates(request_path, llm_call=fake_call)
    return Path(result["bundle_path"])


def test_generation_bundle_mutation_and_extra_member_fail_closed(tmp_path) -> None:
    bundle = _generation_bundle(tmp_path)
    assert inspect_generation_bundle(bundle)["status"] == "VALID"
    (bundle / "unexpected.txt").write_text("drift", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        inspect_generation_bundle(bundle)
    assert exc_info.value.reason_code == "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"


def test_cli_argument_failure_is_typed_and_secret_redacted(capsys) -> None:
    assert main(["prepare"]) == 1
    output = capsys.readouterr().out
    assert "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID" in output
    forbidden_credential_marker = "_".join(("api", "key")) + "="
    assert forbidden_credential_marker not in output


def test_insufficient_generation_is_persisted_and_exact_retry_does_not_resample(tmp_path) -> None:
    request = make_generator_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    calls = 0

    def duplicate_call(_system, user, _request):
        nonlocal calls
        calls += 1
        family = json.loads(user)["family"]
        proposals = [
            {
                "economic_hypothesis": f"{family} old-field-only duplicate {index}",
                "mechanism": "Uses only the already consumed close field and must not pass novelty support",
                "known_effect_exposures": ["MOMENTUM"],
                "expression": {
                    "op": "ADD",
                    "args": [
                        {"op": "FIELD", "field": "close"},
                        {"op": "CONST", "value": float(index + 1)},
                    ],
                },
            }
            for index in range(4)
        ]
        return json.dumps({"proposals": proposals}), {}

    first = generate_alpha_candidates(request_path, llm_call=duplicate_call)
    assert calls == 6
    assert first["generation_status"] == "INCOMPLETE_SUPPORT"
    assert first["accepted_expression_count"] == 0
    assert first["support_reason_codes"]

    second = generate_alpha_candidates(
        request_path,
        llm_call=lambda *_args: (_ for _ in ()).throw(AssertionError("must not resample")),
    )
    assert second["exact_retry"] is True
    assert second["bundle_id"] == first["bundle_id"]

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from backend.services.miniqmt_execution_runtime.kernel_product_source_capability import (
    K6DRouteSourceCapabilityV1,
    K6DSourceCapabilityError,
    _zero_product_call_proof,
    build_k6d_route_source_capability_v1,
)


ROOT = Path(__file__).resolve().parents[3]


def test_k6d_source_capability_closes_exact_retirement_inventory_and_zero_product_calls() -> None:
    capability = build_k6d_route_source_capability_v1(ROOT)
    strict = K6DRouteSourceCapabilityV1.model_validate_json(capability.model_dump_json(), strict=True)

    assert strict == capability
    assert strict.schema_version == "miniqmt_k6d_route_contract_v1"
    assert strict.product_coordinator_symbol == "MiniQMTKernelV2ProductCoordinator"
    assert len(strict.ordered_legacy_product_zero_proofs) == 5
    assert tuple(sorted(strict.ordered_legacy_product_zero_proofs)) == strict.ordered_legacy_product_zero_proofs


def test_k6d_source_capability_is_fresh_process_deterministic() -> None:
    local = build_k6d_route_source_capability_v1(ROOT).model_dump(mode="json")
    script = (
        "import json; "
        "from backend.services.miniqmt_execution_runtime.kernel_product_source_capability "
        "import build_k6d_route_source_capability_v1 as build; "
        "print(json.dumps(build().model_dump(mode='json'),sort_keys=True,separators=(',',':')))"
    )
    completed = subprocess.run(
        (sys.executable, "-c", script),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == local


def test_k6d_source_capability_fails_loud_when_product_root_calls_retired_route(tmp_path: Path) -> None:
    source = tmp_path / "product_root.py"
    source.write_text(
        "class ProductRoot:\n    def start(self):\n        return self.submit_event_loop_plan()\n",
        encoding="utf-8",
    )

    with pytest.raises(K6DSourceCapabilityError) as caught:
        _zero_product_call_proof(tmp_path, "product_root.py", "ProductRoot.start")

    assert caught.value.reason_code == "MINIQMT_K6D_LEGACY_PRODUCT_CALL_REACHABLE"
    assert caught.value.context["ordered_matches"] == [{"name": "submit_event_loop_plan", "line": 3, "column": 15}]

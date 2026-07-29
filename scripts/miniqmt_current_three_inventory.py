"""Emit K3-B legacy inventories without mutating runtime or business data."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only MiniQMT current-three legacy inventory")
    parser.add_argument("--output", required=True, help="Explicit inventory JSON output path")
    args = parser.parse_args(argv)
    output = str(args.output or "").strip()
    if not output:
        parser.error("--output must be an explicit non-empty path")
    args.output = output
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if os.environ.get("AISTOCK_MINIQMT_CURRENT_THREE_INVENTORY_WRITE_MODE"):
        raise RuntimeError("MINIQMT_K3_INVENTORY_WRITE_MODE_FORBIDDEN")

    from backend.services.miniqmt_execution_runtime.kernel_current_three_inventory import (
        build_current_three_legacy_inventory_set_v1,
    )
    from backend.services.miniqmt_execution_runtime.repository import PostgresMiniQMTExecutionRuntimeRepository

    repository = PostgresMiniQMTExecutionRuntimeRepository()
    runtime_ids = tuple(sorted(item.runtime_id for item in repository.list_runtimes()))
    inventories = []
    dependent = []
    for runtime_id in runtime_ids:
        read = repository.read_current_three_shadow_snapshot(runtime_id, include_archived=True)
        inventory_set, dependent_items = build_current_three_legacy_inventory_set_v1(read)
        inventories.append(inventory_set.model_dump(mode="json"))
        dependent.extend(item.model_dump(mode="json") for item in dependent_items)
    payload = {
        "schema_version": "miniqmt_current_three_inventory_artifact_v1",
        "read_only": True,
        "runtime_effect_applied": False,
        "ordered_inventory_sets": inventories,
        "ordered_dependent_buy_inventories": sorted(
            dependent, key=lambda item: (item["runtime_id"], item["buy_algo_instance_id"])
        ),
    }
    output = Path(args.output).expanduser().resolve(strict=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())

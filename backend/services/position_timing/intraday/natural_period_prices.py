"""Natural-period metadata adapter to the one existing shared price builder."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import Field

from . import natural_period_context as context
from . import price_plan_campaign as campaign
from .contracts import payload
from .period_context import read, require
from .replay import FileRef
from .research_inputs import CandidateReader


def implementation_identity():
    return context.implementation_identity() | {"intraday/natural_period_prices.py": hashlib.sha256(
        Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


class NaturalPriceSpec(campaign.CampaignPriceSpec):
    schema_version: Literal["position_timing_natural_period_price_spec_v1"] = "position_timing_natural_period_price_spec_v1"
    source_delivery: FileRef
    adapter_implementation: dict[str, str] = Field(default_factory=implementation_identity)


def build(spec: NaturalPriceSpec, timing_root, *, verify_only=False):
    require(spec.adapter_implementation == implementation_identity(), "NATURAL_PRICE_IMPLEMENTATION")
    c = context.load_context(spec.source_delivery)
    p, r = c["preflight"], c["request"]
    require(payload(spec.preflight) == c["delivery"]["preflight"], "NATURAL_PRICE_PREFLIGHT")
    refs = context.pins(c["refs"], spec.templates_schema, spec.event_schema)
    # In-memory argument projection, NOT an old-13 schema or stored preflight.
    view = {"inputs": {"package": c["package"], "candidate_manifest": r["candidate_manifest"]},
        "start_trade_date": p["trade_dates"][0], "package_id": r["package_id"],
        "manifest_sha256": r["manifest_sha256"], "selection_inventory": r["selection_inventory"]}
    period = {**p, "historical_sell_symbols": c["historical_sell_symbols"]}
    reader = CandidateReader(FileRef.model_validate(r["candidate_manifest"]))
    try:
        args = campaign._selection_context(spec, view, refs, read(r["selection_inventory"]), reader, period)
        return campaign._build(spec, timing_root, verify_only, args)
    finally:
        reader.close()


def inspect(manifest_ref: FileRef):
    m = read(manifest_ref, "position_timing_shared_price_plan_manifest_v1")
    require(m["implementation"] == campaign.implementation_identity(), "NATURAL_PRICE_ENGINE_IMPLEMENTATION")
    result = build(NaturalPriceSpec.model_validate(m["spec"]), Path(manifest_ref.path).parents[3], verify_only=True)
    require(result["manifest"] == payload(manifest_ref), "NATURAL_PRICE_MANIFEST")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "inspect"))
    parser.add_argument("--input", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--timing-root")
    args = parser.parse_args()
    ref = FileRef(path=args.input, sha256=args.sha256)
    if args.command == "build":
        require(args.timing_root is not None, "NATURAL_PRICE_OUTPUT_REQUIRED")
        result = build(NaturalPriceSpec.model_validate(read(ref)), args.timing_root)
    else:
        result = inspect(ref)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

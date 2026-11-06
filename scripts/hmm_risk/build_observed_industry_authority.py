"""Rebuild HMM forward classification using proven observation, never activate it."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def main() -> int:
    from backend.services.hmm_risk import observed_industry_authority as observed
    from backend.services.hmm_risk import risk_l2_database_inference as database
    from backend.services.hmm_risk import risk_l2_date_inference as core
    from backend.services.hmm_risk.formal_state_executor import read_json, validate_output_location, write_once
    from backend.services.hmm_risk.rotation_l1_input_bundle import _industry_adapter
    from backend.services.hmm_risk.stock_fact_repository import PostgresStockFactReader, StockFactSourceSpec

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, required=True)
    parser.add_argument("--mapping-sha256", required=True)
    parser.add_argument("--readback", type=Path, required=True)
    parser.add_argument("--readback-sha256", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    # All output and source validation precedes a database connection. Do not
    # overwrite user files or write a release component even on an error path.
    try:
        output = validate_output_location(args.output_root)
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    observed.require(not output.exists(), "authority output already exists")
    request = read_json(args.request)
    database.validate_request(request)
    observed.require(request["source"]["knowledge_mode"] == "forward_current_read_snapshot", "forward request required")
    authority = request["source"]["industry_authority"]
    validate_output_location(output, dataset_root=Path(authority["artifact_root"]))
    _industry_adapter(authority, forbidden_roots=(ROOT,))
    observation = observed.load_observation(
        args.mapping, args.mapping_sha256, args.readback, args.readback_sha256, authority
    )

    def git(*args):
        return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()

    observed.require(not git("status", "--porcelain"), "formal authority build requires a clean committed source")
    commit, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    with database.connect(request) as connection:
        plan = database._read_plan(request, connection)
        source = request["source"]
        reader = PostgresStockFactReader(
            connection,
            StockFactSourceSpec(
                source["universe_key"],
                source["universe_rule_version"],
                core._day(plan["start"]),
                core._day(plan["end"]),
                database.SOURCE_START,
            ),
            security_identity_manifest=database.load_security_source_identity_manifest(
                Path(source["security_identity_manifest"]), expected_sha256=source["security_identity_sha256"]
            ),
            provider_absence_manifest=database.load_provider_absence_manifest(
                Path(source["provider_absence_manifest"]), expected_sha256=source["provider_absence_sha256"]
            ),
        )
        reader.validate_source()
        denominator = reader.load_industry_pit_denominator(
            window_start=core._day(plan["start"]), window_end=core._day(plan["end"])
        )
    observed.require(
        (commit, tree) == (git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}"))
        and not git("status", "--porcelain"),
        "source changed during authority build",
    )
    result = observed.build_authority(
        base_authority=authority,
        observation=observation,
        denominator=denominator,
        output_root=output,
        source_commit=commit,
        source_tree=tree,
        forbidden_roots=(ROOT,),
    )
    rebound = {**request, "source": {**request["source"], "industry_authority": read_json(output / "authority.json")}}
    rebound["receipt_sha256"] = observed.canonical_sha256({k: v for k, v in rebound.items() if k != "receipt_sha256"})
    database.validate_request(rebound)
    write_once(output / "database_forward_request.json", rebound)
    result["request_path"] = str(output / "database_forward_request.json")
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "schema_version",
                    "authority_path",
                    "request_path",
                    "identity",
                    "data_date",
                    "target_symbols",
                    "resolved_symbols",
                    "target_unavailable",
                    "causal_daily_from",
                    "database_write",
                    "dataset_write",
                )
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

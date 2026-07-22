"""HMM Phase 1 acceptance benchmark orchestrator (DEV only).

Drives the real DEV backend (127.0.0.1:8011) through the authorized benchmark
sequence and reads receipts from the DEV database (127.0.0.1:5433/aistock_dev).

Hard rules:
- NEVER touches production DB (5432) or production ports (8001/3000/19080).
- API base is fixed to 127.0.0.1:8011; DB is asserted to be 5433 + 'dev'.
- benchmark submissions require a succeeded normal evaluation (service-side
  contract); this script submits the normal batch first and waits for it.

Subcommands:
  register --count 1|10        preview + register candidates via real API
  submit-normal --count 1|10   POST /batch purpose=evaluation, wait terminal
  submit-benchmark --kind zero-copy|cold|warm --count 1|10
                               POST /batch purpose=benchmark, wait terminal
  receipts [--benchmark-id X]  dump + verify performance receipts from DEV DB
  report                       full acceptance summary from DEV DB
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import psycopg2

API_BASE = "http://127.0.0.1:8011/api/v1/hmm-evolution"
DEV_DB_ENV_KEYS = (
    "TDX_DB_DEV_HOST",
    "TDX_DB_DEV_PORT",
    "TDX_DB_DEV_NAME",
    "TDX_DB_DEV_USER",
    "TDX_DB_DEV_PASSWORD",
)
TERMINAL_BATCH_STATUSES = {"completed", "partial_failed", "failed", "cancelled", "timed_out"}

CANONICAL_SPEC: dict[str, Any] = {
    "topk": 46,
    "as_of": {"policy": "latest_common_completed", "requested_date": None},
    "universe": {"type": "source_loop_stock_pool_st_pit"},
    "window_end": "2025-12-31",
    "sort_policy": "score_desc_symbol_asc_v1",
    "window_start": "2025-01-02",
    "base_loop_ref": "qe_20260705_004409_4437/Loop10",
    "metric_version": "hmm_replacement_metrics_v2",
    "schema_version": "hmm_evaluation_spec_v2",
    "label_horizon_days": 20,
    "date_coverage_policy": "batch_common_intersection_with_evidence",
    "market_forward_return": {"mode": "required", "horizon_trading_days": 10},
    "missing_sector_policy": "neutral_with_evidence",
    "recommendation_version": "hmm_recommendation_v1",
}

# Canonical 10-candidate batch hmmb_63d536bca43c480293de45d7d6952dba (production),
# in submission order.  Candidate IDs are content-addressed; DEV registration
# through the real resolver must reproduce them and is verified explicitly.
CANDIDATES: tuple[tuple[str, str, str], ...] = (
    # (expected candidate_id, snapshot_id, display_name)
    ("hmmc_573b2dd8892f8736e624dcf5", "38d8cd16-8fac-4d88-9293-4c42dcb50218",
     "HMM validation AUTOCYCLE R10 deepen B60 P0955"),
    ("hmmc_51c740b59086c181706442a3", "a48ab231-e43e-41a8-9855-0e908b7913d1",
     "HMM validation AUTOCYCLE R10 confirm B65 P1"),
    ("hmmc_f13f7cb4f507a4907dbae049", "60f055b8-03f2-4ee4-8130-6c5f0c6ad595",
     "HMM validation AUTOCYCLE R10 confirm B55 P1"),
    ("hmmc_7ff01b89a2cc97e101e163ac", "19c918f3-f27e-413b-8d46-0bb71f8eb548",
     "HMM validation AUTOCYCLE R10 bottom B25 P0985"),
    ("hmmc_a69ae30f0992c819cb894f8a", "bf4eda9d-d252-46f8-a063-fb3f95f49a1e",
     "HMM validation REGLINEAR T20 B15 boost01 pen005"),
    ("hmmc_6614b6938e0c85a6beeee32d", "8834983a-7a44-4073-8108-d509faa92a31",
     "HMM validation REGTOPBOT T20 B20 boost005 pen03"),
    ("hmmc_fa47b5fa387cdc9862ffe01d", "d2da20b1-f3c5-410b-aee9-9d71dff4e846",
     "HMM validation REGLINEAR T20 B15 boost005 pen005"),
    ("hmmc_646b89f809a65e1f1939f0d2", "41e5cea2-a8be-47ee-a3ca-831c9609be16",
     "HMM validation REGTOPBOT T20 B15 boost005 pen005"),
    ("hmmc_51125769a3e34f2a8dee4888", "bbec3863-fb67-445f-938e-66f092d18696",
     "HMM baseline b99c907b external acceptance"),
    ("hmmc_947fdd0c87bfd59e5c9d1fab", "6ea64754-003d-48d8-ad9e-d0e7857716c8",
     "HMM best ce4952c1 external acceptance"),
)
STANDARD_CANDIDATE_ID = "hmmc_51125769a3e34f2a8dee4888"
COEFFICIENT_ARTIFACT = "coefficients_preset_A_2024-07-01_2026-04-27.json"
CREATED_BY = "hmm_phase1_acceptance_20260721"

# 2026-07-22 user-approved ruling: the workspace-fallback cold/warm benchmark
# runs on the only approved + verifiable fallback loop qe_20260502_131502_9b54/Loop1.
# Identical to CANONICAL_SPEC except:
#   - base_loop_ref      -> the approved fallback loop (legacy-allowlisted on node1)
#   - label_horizon_days -> 10, matching that loop's real LABEL0 semantics
#     (mlruns param label_horizon=10, sha256-verified against the immutable
#     legacy receipt; interpreting it as h20 would be semantic fraud).
FALLBACK_SPEC: dict[str, Any] = {
    **CANONICAL_SPEC,
    "base_loop_ref": "qe_20260502_131502_9b54/Loop1",
    "label_horizon_days": 10,
}
SPECS = {"canonical": CANONICAL_SPEC, "fallback": FALLBACK_SPEC}

EXPECTED_CACHE_STATES = {
    "zero-copy": {"zero_copy_bypass"},
    "cold": {"fallback_download", "cold_miss"},
    "warm": {"warm_hit"},
}


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def api(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    with httpx.Client(base_url=API_BASE, timeout=120.0) as client:
        resp = client.request(method, path, **kwargs)
    if resp.status_code >= 400:
        raise SystemExit(f"API {method} {path} -> {resp.status_code}: {resp.text[:2000]}")
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("status") == "ok" and "data" in payload:
        return payload["data"]
    return payload


def _dev_dsn_from_env() -> dict[str, Any]:
    missing = [key for key in DEV_DB_ENV_KEYS if not os.getenv(key)]
    if missing:
        raise SystemExit(f"missing required DEV database config: {', '.join(missing)}")
    dsn: dict[str, Any] = {
        "host": os.environ["TDX_DB_DEV_HOST"],
        "port": int(os.environ["TDX_DB_DEV_PORT"]),
        "dbname": os.environ["TDX_DB_DEV_NAME"],
        "user": os.environ["TDX_DB_DEV_USER"],
        "password": os.environ["TDX_DB_DEV_PASSWORD"],
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in dsn["dbname"].lower():
        raise SystemExit(
            "refusing unsafe DEV database target: expected literal 127.0.0.1:5433 and a DEV database name"
        )
    return dsn


def dev_conn() -> Any:
    dsn = _dev_dsn_from_env()
    # NOTE: current_setting('port') cannot be used as the guard — DEV postgres runs
    # in a container publishing 5433->5432, so the server-reported port is 5432.
    # The authoritative invariant is the database identity itself.
    conn = psycopg2.connect(options="-c default_transaction_read_only=on -c timezone=UTC", **dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        (dbname,) = cur.fetchone()
        if dbname != dsn["dbname"]:
            raise SystemExit(f"refusing non-DEV target: db={dbname}")
    return conn


def selected_candidates(count: int) -> list[tuple[str, str, str]]:
    if count == 1:
        return [c for c in CANDIDATES if c[0] == STANDARD_CANDIDATE_ID]
    if count == 10:
        return list(CANDIDATES)
    raise SystemExit("--count must be 1 or 10")


def cmd_register(count: int) -> None:
    for expected_id, snapshot_id, display_name in selected_candidates(count):
        source = {
            "source_type": "existing_snapshot_coefficients",
            "snapshot_id": snapshot_id,
            "artifact_name": COEFFICIENT_ARTIFACT,
        }
        preview = api("POST", "/candidates/preview", json=source)
        log(f"preview {snapshot_id[:8]}: {json.dumps(preview, default=str)[:300]}")
        result = api(
            "POST",
            "/candidates",
            json={**source, "display_name": display_name, "created_by": CREATED_BY},
        )
        candidate = result.get("candidate", result)
        candidate_id = candidate.get("candidate_id")
        created = result.get("created", candidate.get("created"))
        if candidate_id != expected_id:
            raise SystemExit(
                f"content-addressed ID drift: expected {expected_id}, got {candidate_id} "
                f"(snapshot {snapshot_id})"
            )
        log(f"registered {candidate_id} created={created}")


def _submit(count: int, *, purpose: str, benchmark_id: str | None, spec_name: str = "canonical") -> str:
    candidate_ids = [c[0] for c in selected_candidates(count)]
    body: dict[str, Any] = {
        "candidate_ids": candidate_ids,
        "evaluation_spec": SPECS[spec_name],
        "created_by": CREATED_BY,
        "execution_purpose": purpose,
    }
    if benchmark_id is not None:
        body["benchmark_id"] = benchmark_id
    result = api("POST", "/batch", json=body)
    batch = result.get("batch", result)
    batch_id = batch.get("batch_id")
    log(
        f"submitted purpose={purpose} benchmark_id={benchmark_id} spec={spec_name} "
        f"batch_id={batch_id} status={batch.get('status')} created={result.get('created')}"
    )
    return str(batch_id)


def wait_terminal(batch_id: str, *, timeout_s: int = 3600, poll_s: int = 15) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_status = None
    while time.monotonic() < deadline:
        batch = api("GET", f"/batches/{batch_id}")
        row = batch.get("batch", batch)
        status = row.get("status")
        if status != last_status:
            log(f"batch {batch_id} status={status}")
            last_status = status
        if status in TERMINAL_BATCH_STATUSES:
            return row
        time.sleep(poll_s)
    raise SystemExit(f"batch {batch_id} did not reach terminal within {timeout_s}s")


def cmd_submit_normal(count: int, spec_name: str) -> None:
    batch_id = _submit(count, purpose="evaluation", benchmark_id=None, spec_name=spec_name)
    row = wait_terminal(batch_id)
    if row.get("status") != "completed":
        raise SystemExit(f"normal batch {batch_id} ended status={row.get('status')}: {json.dumps(row, default=str)[:1500]}")
    log(f"normal batch {batch_id} completed")


def cmd_submit_benchmark(kind: str, count: int, spec_name: str) -> None:
    benchmark_id = f"bench_{kind.replace('-', '')}_{count}c_20260721"
    batch_id = _submit(count, purpose="benchmark", benchmark_id=benchmark_id, spec_name=spec_name)
    row = wait_terminal(batch_id)
    if row.get("status") != "completed":
        raise SystemExit(f"benchmark batch {batch_id} ended status={row.get('status')}: {json.dumps(row, default=str)[:1500]}")
    log(f"benchmark batch {batch_id} completed benchmark_id={benchmark_id}")


STAGE_FIELDS = (
    "api_receipt_persist",
    "preparation_queue_wait",
    "qe_source_load",
    "universe_resolve",
    "market_freeze",
    "evaluation_queue_wait",
    "compute",
    "result_persist",
    "request_to_terminal",
)


def cmd_receipts(benchmark_id: str | None) -> None:
    conn = dev_conn()
    with conn.cursor() as cur:
        where = "WHERE r.benchmark_id = %s" if benchmark_id else ""
        params: tuple[Any, ...] = (benchmark_id,) if benchmark_id else ()
        cur.execute(
            f"""
            SELECT r.receipt_id, r.receipt_level, r.batch_id, r.eval_id, r.benchmark_id,
                   r.receipt_status, r.stage_timings, r.cache_state, r.cache_evidence,
                   r.peak_rss_bytes, r.request_to_terminal_ms, r.result_hash,
                   r.created_at, r.finalized_at, b.status AS batch_status
            FROM hmm_evolution.performance_receipt r
            JOIN hmm_evolution.batch_test_run b ON b.batch_id = r.batch_id
            {where}
            ORDER BY r.created_at
            """,
            params,
        )
        rows = cur.fetchall()
    conn.close()
    if not rows:
        raise SystemExit(f"no receipts found for benchmark_id={benchmark_id}")
    # By design the receipt is split into two levels:
    #   batch level      -> API + preparation stages (no compute, no single result hash,
    #                       no process-RSS identity of one evaluation)
    #   evaluation level -> queue/compute/persist stages + peak RSS + result hash
    batch_stages = {
        "api_receipt_persist",
        "preparation_queue_wait",
        "qe_source_load",
        "universe_resolve",
        "market_freeze",
    }
    eval_stages = {"evaluation_queue_wait", "compute", "result_persist"}
    expected_top_state = {"zero-copy": "unknown", "cold": "cold", "warm": "warm"}
    # Evaluation phase runs AFTER this run's preparation stage.  In a cold run the
    # preparation downloads the artifacts into the task-scoped cache root, so by the
    # time the evaluation loads them the application cache IS warm: per-stage truth.
    # The cold/warm identity of the benchmark is carried by the batch-level receipt
    # (qe_source_load with real download vs cache hit).  A cold evaluation receipt
    # may therefore legitimately show fallback_download or warm_hit, never
    # zero_copy_bypass / cold_miss-without-download.
    eval_level_allowed = {
        "zero-copy": {"zero_copy_bypass"},
        "cold": {"fallback_download", "warm_hit"},
        "warm": {"warm_hit"},
    }
    failures: list[str] = []
    orphans: list[str] = []
    verified = 0
    for row in rows:
        (receipt_id, level, batch_id, eval_id, bid, status, stages, top_state, cache,
         peak_rss, rtt_ms, result_hash, created_at, finalized_at, batch_status) = row
        stages = stages or {}
        artifacts = cache or []
        if not isinstance(artifacts, list):
            failures.append(f"{receipt_id}: cache_evidence is not a list")
            artifacts = []
        if batch_status != "completed":
            # Failed/cancelled batches keep partial receipts by design ("保留 partial
            # receipt 与已完成的最后阶段").  They are evidence of the failure, not of
            # the successful benchmark, so they are reported but not asserted.
            orphans.append(
                f"{receipt_id} level={level} batch={batch_id} batch_status={batch_status} "
                f"receipt_status={status} stages={sorted(stages.keys())}"
            )
            continue
        verified += 1
        required_stages = batch_stages if level == "batch" else eval_stages
        missing_stages = sorted(s for s in required_stages if s not in stages)
        log(
            f"receipt {receipt_id} level={level} batch={batch_id} eval={eval_id} "
            f"status={status} rtt_ms={rtt_ms} peak_rss={peak_rss} cache_state={top_state}"
        )
        log(f"  stages: {sorted(stages.keys())}")
        for entry in artifacts:
            log(f"  artifact {entry.get('artifact')}: {entry.get('state')} source={entry.get('source')}")
        if status != "final":
            failures.append(f"{receipt_id}: status={status} (expected final)")
        if missing_stages:
            failures.append(f"{receipt_id}: missing {level}-level stages {missing_stages}")
        if rtt_ms is None:
            failures.append(f"{receipt_id}: request_to_terminal_ms is null")
        if level == "evaluation":
            if peak_rss is None:
                failures.append(f"{receipt_id}: evaluation-level peak_rss_bytes is null")
            if not result_hash:
                failures.append(f"{receipt_id}: evaluation-level result_hash missing")
        if benchmark_id and bid != benchmark_id:
            failures.append(f"{receipt_id}: benchmark_id mismatch {bid}")
        # cache-state verification against the benchmark kind
        if bid:
            match = re.fullmatch(r"bench_(zerocopy|cold|warm)_(?:1c|10c)_20260721", bid)
            kind = {"zerocopy": "zero-copy"}.get(match.group(1), match.group(1)) if match else None
            observed = {str(a.get("state")) for a in artifacts}
            if not observed:
                failures.append(f"{receipt_id}: no per-artifact cache evidence")
            elif level == "batch":
                expected = EXPECTED_CACHE_STATES.get(str(kind))
                if expected:
                    if kind == "zero-copy" and observed != expected:
                        failures.append(
                            f"{receipt_id}: zero-copy benchmark observed states {sorted(observed)}, "
                            f"expected exactly {sorted(expected)}"
                        )
                    elif kind in ("cold", "warm") and not observed <= expected:
                        failures.append(
                            f"{receipt_id}: {kind} benchmark observed states {sorted(observed)} "
                            f"outside expected {sorted(expected)}"
                        )
                expected_top = expected_top_state.get(str(kind))
                if expected_top is not None and top_state != expected_top:
                    failures.append(
                        f"{receipt_id}: batch-level cache_state={top_state}, expected {expected_top}"
                    )
            elif level == "evaluation":
                allowed = eval_level_allowed.get(str(kind))
                if allowed and not observed <= allowed:
                    failures.append(
                        f"{receipt_id}: {kind} evaluation-level states {sorted(observed)} "
                        f"outside allowed {sorted(allowed)}"
                    )
    for orphan in orphans:
        log(f"orphan (failed-batch partial receipt, by design, not asserted): {orphan}")
    if verified == 0:
        raise SystemExit("no receipts from completed batches found")
    if failures:
        print(json.dumps({"failures": failures}, indent=2))
        raise SystemExit(f"receipt verification failed: {len(failures)} issue(s)")
    log(f"receipt verification OK ({verified} receipts from completed batches, {len(orphans)} orphan partials)")


def cmd_report() -> None:
    conn = dev_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.batch_id, b.execution_purpose, b.benchmark_id, b.status,
                   b.candidate_count, b.created_at, b.started_at, b.completed_at
            FROM hmm_evolution.batch_test_run b
            ORDER BY b.created_at
            """
        )
        batches = cur.fetchall()
        cur.execute(
            """
            SELECT e.execution_purpose, e.benchmark_id, e.status, count(*)
            FROM hmm_evolution.offline_evaluation e
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
            """
        )
        evals = cur.fetchall()
        cur.execute(
            """
            SELECT benchmark_id, receipt_level, receipt_status, count(*),
                   min(request_to_terminal_ms), max(request_to_terminal_ms),
                   max(peak_rss_bytes)
            FROM hmm_evolution.performance_receipt
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
            """
        )
        receipts = cur.fetchall()
    conn.close()
    print(json.dumps({
        "batches": [
            {
                "batch_id": b[0], "purpose": b[1], "benchmark_id": b[2], "status": b[3],
                "candidates": b[4], "created_at": str(b[5]), "started_at": str(b[6]),
                "completed_at": str(b[7]),
                "duration_s": (b[7] - b[6]).total_seconds() if b[6] and b[7] else None,
            }
            for b in batches
        ],
        "evaluations": [
            {"purpose": e[0], "benchmark_id": e[1], "status": e[2], "count": e[3]}
            for e in evals
        ],
        "receipts": [
            {
                "benchmark_id": r[0], "level": r[1], "status": r[2], "count": r[3],
                "min_rtt_ms": r[4], "max_rtt_ms": r[5], "max_peak_rss": r[6],
            }
            for r in receipts
        ],
    }, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("register")
    p.add_argument("--count", type=int, required=True, choices=[1, 10])
    p = sub.add_parser("submit-normal")
    p.add_argument("--count", type=int, required=True, choices=[1, 10])
    p.add_argument("--spec", default="canonical", choices=sorted(SPECS))
    p = sub.add_parser("submit-benchmark")
    p.add_argument("--kind", required=True, choices=["zero-copy", "cold", "warm"])
    p.add_argument("--count", type=int, required=True, choices=[1, 10])
    p.add_argument("--spec", default="canonical", choices=sorted(SPECS))
    p = sub.add_parser("receipts")
    p.add_argument("--benchmark-id", default=None)
    sub.add_parser("report")
    args = parser.parse_args()

    if args.cmd == "register":
        cmd_register(args.count)
    elif args.cmd == "submit-normal":
        cmd_submit_normal(args.count, args.spec)
    elif args.cmd == "submit-benchmark":
        cmd_submit_benchmark(args.kind, args.count, args.spec)
    elif args.cmd == "receipts":
        cmd_receipts(args.benchmark_id)
    elif args.cmd == "report":
        cmd_report()
    return 0


if __name__ == "__main__":
    sys.exit(main())

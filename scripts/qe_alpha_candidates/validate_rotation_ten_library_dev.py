"""Validate ten-factor catalog/metrics/classification writes in existing DEV only.

Production application is a separate aftercare action. No schema changes, price
data writes, old factor changes, or availability decisions are made here.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def evaluated_sources(evaluation_dir: Path, generated: dict[str, str]) -> dict[str, str]:
    """Preserve evaluated bytes; Python versions may unparse tuple targets differently."""
    result = {}
    for name, expected in generated.items():
        actual = (evaluation_dir / f"{name}.py").read_text(encoding="utf-8")
        if ast.dump(ast.parse(actual)) != ast.dump(ast.parse(expected)):
            raise ValueError(f"evaluated source differs: {name}")
        result[name] = actual
    return result


def new_pair_records(payload: dict, names: set[str]) -> list[dict]:
    """DEV has only the new catalog entries; validate their complete 45 pairs."""
    records = []
    seen = set()
    for row in payload["pairs"]:
        a, b = row["factor_a"], row["factor_b"]
        if a not in names or b not in names:
            continue
        key = tuple(sorted((a, b)))
        if a == b or key in seen:
            raise ValueError("duplicate/self correlation pair")
        seen.add(key)
        records.append({**row, "method": payload["method"],
                        "data_period": f"as_of_{payload['snapshot_date']}"})
    if len(records) != len(names) * (len(names) - 1) // 2:
        raise ValueError("incomplete new-factor correlation pairs")
    return records


def main() -> int:
    from dotenv import dotenv_values
    import psycopg2

    from scripts.qe_alpha_candidates.run_rotation_ten_research import sources

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--evaluation-dir", type=Path, required=True)
    parser.add_argument("--resume-receipt", type=Path,
                        help="Explicit previous DEV receipt; update only its exact ten IDs after source equality proof")
    parser.add_argument("--correlations-only", action="store_true",
                        help="Validate the 45 new/new pairs in DEV; no catalog source or LLM changes")
    parser.add_argument("--llm-model", help="Explicit per-call model, e.g. the production binding; no global configuration writes")
    args = parser.parse_args()
    env = dotenv_values(args.env_file)
    for key, value in env.items():
        if value is not None:
            os.environ[key] = value
    for key in ("HOST", "PORT", "NAME", "USER", "PASSWORD"):
        os.environ[f"TDX_DB_{key}"] = env[f"TDX_DB_DEV_{key}"]
    if "dev" not in os.environ["TDX_DB_NAME"].lower():
        raise ValueError("existing DEV target required")
    if (os.environ["TDX_DB_HOST"], os.environ["TDX_DB_PORT"], os.environ["TDX_DB_NAME"]) == (
        env["TDX_DB_HOST"], env["TDX_DB_PORT"], env["TDX_DB_NAME"]
    ):
        raise ValueError("DEV must not equal production")
    os.environ.pop("PGOPTIONS", None)
    kwargs = {name: os.environ[f"TDX_DB_{key}"] for name, key in [
        ("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"), ("user", "USER"), ("password", "PASSWORD")
    ]}
    payload = json.loads((args.evaluation_dir / "metrics.json").read_text(encoding="utf-8"))
    source_map = evaluated_sources(args.evaluation_dir, sources())
    names = sorted(source_map)
    if len(names) != 10 or {r["factor_name"] for r in payload["metrics"]} != set(names):
        raise ValueError("exact ten-factor results required")
    windows = {"full", "out_sample", "recent_6m", "recent_3m", "recent_1m"}
    for name in names:
        records = [r for r in payload["metrics"] if r["factor_name"] == name]
        if len(records) != 5 or {r["eval_window"] for r in records} != windows:
            raise ValueError(f"incomplete official windows: {name}")
    ids = {}
    previous = None
    if args.resume_receipt:
        previous = json.loads(args.resume_receipt.read_text(encoding="utf-8"))
        if previous.get("target") != "DEV" or set(previous.get("catalog_ids", {})) != set(names):
            raise ValueError("resume receipt must own exactly these ten DEV names")
        ids = previous["catalog_ids"]
        if len(set(ids.values())) != 10:
            raise ValueError("resume IDs must be distinct")
    if args.correlations_only:
        if previous is None:
            raise ValueError("correlation validation requires this task's DEV receipt")
        output = args.evaluation_dir / "dev-correlation-validation.json"
        if output.exists():
            raise ValueError("DEV correlation receipt already exists")
        with psycopg2.connect(**kwargs, options="-c default_transaction_read_only=on") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT factor_name,id,code_text FROM aistock_factor_catalog WHERE factor_name=ANY(%s)", (names,))
                rows = cur.fetchall()
                if {row[0]: row[1] for row in rows} != ids or any(row[2] != source_map[row[0]] for row in rows):
                    raise ValueError("DEV catalog does not match evaluated task sources")
        correlation = json.loads((args.evaluation_dir / "correlations.json").read_text(encoding="utf-8"))
        records = new_pair_records(correlation, set(names))
        from backend.services.quantevolver.correlation_compute_service import _persist_correlations_batch
        written = _persist_correlations_batch(records, correlation["universe_metadata"])
        with psycopg2.connect(**kwargs, options="-c default_transaction_read_only=on") as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT factor_a_id,factor_b_id,correlation,as_of_date FROM qe_factor_correlations
                    WHERE factor_a_id=ANY(%s) AND factor_b_id=ANY(%s)""", (list(ids.values()), list(ids.values())))
                actual = {(a, b): (value, str(day)) for a, b, value, day in cur.fetchall()}
        expected = {tuple(sorted((ids[r["factor_a"]], ids[r["factor_b"]]))):
                    (r["correlation"], correlation["snapshot_date"]) for r in records}
        # PostgreSQL stores a double; compare the same serialized scalar, not a rounded report.
        complete = written == 45 and actual == expected
        receipt = {"target": "DEV", "complete": complete, "written_pairs": written,
                   "readback_pairs": len(actual), "scope": "new/new 45; not full production matrix",
                   "production_writes": 0, "ddl": "noop"}
        with output.open("x", encoding="utf-8") as handle:
            json.dump(receipt, handle, indent=2)
        print(json.dumps(receipt), flush=True)
        return 0 if complete else 1
    receipt_path = args.evaluation_dir / ("dev-library-specialized-validation.json" if previous else "dev-library-validation.json")
    if receipt_path.exists():
        raise ValueError("output receipt already exists; do not overwrite previous validation")
    with psycopg2.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT factor_name,id,code_text,asset_path FROM aistock_factor_catalog WHERE factor_name=ANY(%s) FOR UPDATE", (names,))
            existing = cur.fetchall()
            if existing and previous is None:
                raise ValueError("existing names require explicit resume; no overwrite")
            if previous:
                if {row[0]: row[1] for row in existing} != ids:
                    raise ValueError("DEV catalog no longer matches this task's IDs")
                for name, _, code, asset in existing:
                    reference = (args.evaluation_dir / f"{name}.reference.py").read_text(encoding="utf-8")
                    target = ROOT / "rdagent_assets/manual_factors" / f"{name}.py"
                    if asset != target.relative_to(ROOT).as_posix() or code not in (reference, source_map[name]):
                        raise ValueError(f"DEV source/asset changed outside task: {name}")
                    if target.is_symlink() or target.read_text(encoding="utf-8") != code:
                        raise ValueError(f"DEV source asset does not match catalog: {name}")
            for name in names:
                relative = Path("rdagent_assets/manual_factors") / f"{name}.py"
                target = ROOT / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if previous:
                    cur.execute("UPDATE aistock_factor_catalog SET code_text=%s WHERE id=%s AND factor_name=%s",
                                (source_map[name], ids[name], name))
                    # Only this task's checked source asset is replaced. Factor
                    # values, metrics and all other assets remain unchanged.
                    target.write_text(source_map[name], encoding="utf-8")
                    continue
                with target.open("x", encoding="utf-8") as handle:
                    handle.write(source_map[name])
                description = ("2026-09-07十因子研发；完整公式见源码。原始方向，不按测试结果翻转。"
                               + ("依赖index_factor_context.h5，非300/500/1000成员为NaN。" if name.startswith("m_pit_") else "仅依赖daily_pv.h5。"))
                cur.execute("""INSERT INTO aistock_factor_catalog
                    (factor_name,source,catalog_version,generated_at_utc,catalog_source,code_text,asset_path,description_cn)
                    VALUES (%s,'manual','manual_v1',%s,'manual',%s,%s,%s) RETURNING id""",
                            (name, datetime.now(timezone.utc), source_map[name], relative.as_posix(), description))
                ids[name] = cur.fetchone()[0]
    from backend.services.quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService
    from backend.services.quantevolver.factor_analyst import FactorAnalyst

    writer = FactorOfficialEvaluationService()
    saved = writer._save_metrics(payload, payload["snapshot_date"], ids)
    if saved["errors"] or saved["inserted"] != 50:
        raise RuntimeError(f"DEV metric writes incomplete: {saved}")
    monthly = 0
    for record in payload["metrics"]:
        if record["eval_window"] == "full":
            monthly += writer._save_monthly_ic(record["factor_name"], payload["snapshot_date"], record.get("monthly_ic_series", []))
    classification = []
    for name in names:
        result = FactorAnalyst().analyze_single_factor(name, "manual", use_llm=True, llm_model=args.llm_model)
        classification.append({"factor_name": name, "category": result.get("category"), "ok": result.get("ok"),
                               "error": result.get("error"), "reason": result.get("classification_reason"),
                               "llm_completed": not result.get("classification_reason", "rule classification:").startswith("rule classification:")})
        print(json.dumps(classification[-1], ensure_ascii=False), flush=True)
    with psycopg2.connect(**kwargs, options="-c default_transaction_read_only=on") as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT c.factor_name,
                (SELECT count(*) FROM aistock_factor_metrics m WHERE m.factor_catalog_id=c.id) metrics,
                (SELECT count(*) FROM qe_factor_classification cl WHERE cl.factor_name=c.factor_name) classification
                FROM aistock_factor_catalog c WHERE c.id=ANY(%s) ORDER BY c.factor_name""", (list(ids.values()),))
            readback = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
    tables_complete = len(readback) == 10 and all(r["metrics"] == 5 and r["classification"] == 1 for r in readback)
    llm_complete = all(r["ok"] and r["llm_completed"] for r in classification)
    complete = tables_complete and llm_complete
    receipt = {"target": "DEV", "complete": complete,
               "tables_complete": tables_complete, "llm_complete": llm_complete,
               "explicit_llm_model": args.llm_model,
               "catalog_ids": ids, "metrics": saved,
               "monthly_rows": monthly, "classification": classification, "readback": readback,
               "production_writes": 0, "ddl": "noop"}
    with receipt_path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, ensure_ascii=False, indent=2)
    print(json.dumps({"dev_complete": complete, "production_writes": 0, "metrics": saved["inserted"]}), flush=True)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())

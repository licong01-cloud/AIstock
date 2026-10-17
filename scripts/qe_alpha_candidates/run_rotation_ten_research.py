"""Compute ten candidates and existing official metrics without database writes."""
from __future__ import annotations

import argparse
import ast
import json
import math
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def specialize_source(source: str, factor_names: tuple[str, ...], selected_name: str) -> str:
    """Keep only the selected formula so catalog analysis sees the real signal."""
    class Specializer(ast.NodeTransformer):
        def decision(self, node):
            if not isinstance(node, ast.Compare) or len(node.ops) != 1:
                return None
            left, right = node.left, node.comparators[0]
            if not isinstance(left, ast.Name) or left.id not in {"name", "factor_name"}:
                return None
            if not isinstance(right, ast.Subscript) or not isinstance(right.value, ast.Name) or right.value.id != "FACTOR_NAMES":
                return None
            if not isinstance(right.slice, ast.Constant) or not isinstance(right.slice.value, int):
                return None
            same = selected_name == factor_names[right.slice.value]
            if isinstance(node.ops[0], ast.Eq):
                return same
            if isinstance(node.ops[0], ast.NotEq):
                return not same
            return None

        def visit_If(self, node):
            decision = self.decision(node.test)
            if decision is None:
                return self.generic_visit(node)
            selected = node.body if decision else node.orelse
            result = []
            for child in selected:
                changed = self.visit(child)
                result.extend(changed if isinstance(changed, list) else [changed])
            return result

        def visit_IfExp(self, node):
            decision = self.decision(node.test)
            return self.generic_visit(node) if decision is None else self.visit(node.body if decision else node.orelse)

        def visit_Assign(self, node):
            if any(isinstance(target, ast.Name) and target.id == "FACTOR_NAMES" for target in node.targets):
                node.value = ast.Tuple(elts=[ast.Constant(selected_name)], ctx=ast.Load())
            return self.generic_visit(node)

    tree = ast.fix_missing_locations(Specializer().visit(ast.parse(source)))
    return ast.unparse(tree) + "\n"


def sources() -> dict[str, str]:
    from scripts.qe_alpha_candidates import rotation_index_factors, rotation_liquidity_factors

    result = {}
    for module in [rotation_index_factors, rotation_liquidity_factors]:
        source = Path(module.__file__).read_text(encoding="utf-8")
        for name in module.FACTOR_NAMES:
            result[name] = specialize_source(source, module.FACTOR_NAMES, name) + f"\nFACTOR_NAME = {name!r}\nif __name__ == '__main__':\n    compute_factor(FACTOR_NAME)\n"
    return result


def json_safe(value):
    """Unavailable engine diagnostics use JSON null, never non-standard NaN."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def main() -> int:
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--factor-data-dir", type=Path, required=True)
    parser.add_argument("--context-dir", type=Path, required=True)
    parser.add_argument("--qlib-bin", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", default="2018-08-01")
    parser.add_argument("--end", default="2026-08-31")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    load_dotenv(args.env_file, override=True)
    # The metric engine reads existing canonical PIT/suspend rows. It must not
    # bootstrap or repair any production table during research computation.
    os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on"

    from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
    from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
    from backend.services.quantevolver.qe_eval_v2_metric_engine import compute_single_factor_metrics, prepare_shared_context

    args.output_dir.mkdir(parents=True, exist_ok=args.resume)
    cache = BacktestBaseDataMemoryCache.load_once(args.factor_data_dir, args.start, args.end,
                                                allowed_files=("daily_pv.h5",), supplemental_data_dir=args.context_dir)
    instruments = set(cache.get("daily_pv.h5").index.get_level_values("instrument"))
    ctx = prepare_shared_context(qlib_bin_path=args.qlib_bin, start_date=args.start, end_date=args.end,
                                 instrument_hint=instruments, universe_key="aistock_equity_pit_canonical_v2")
    eligibility = ctx["st_pit_eligible_mask"].stack()
    eligible_index = eligibility.loc[eligibility].index
    del eligibility
    executor = OfflineCodeTextFactorExecutor(cache)
    rows = []
    reports = {}
    batch_id = str(uuid.uuid4())
    for name, source in sources().items():
        source_path = args.output_dir / f"{name}.py"
        metric_path = args.output_dir / f"{name}.metrics.json"
        reused = args.resume and source_path.is_file() and metric_path.is_file()
        if reused:
            if source_path.read_text(encoding="utf-8") != source:
                raise ValueError(f"cannot reuse results from different factor source: {name}")
            metric = json.loads(metric_path.read_text(encoding="utf-8"))
            if metric["metrics"]["full"]["data_start"] != args.start or metric["metrics"]["full"]["data_end"] != args.end:
                raise ValueError("resume window differs")
            print(json.dumps({"reused": name}), flush=True)
        else:
            result = executor.compute_factor(name, source)
            if not result.success:
                raise RuntimeError(f"{name}: {result.error}")
            frame = result.dataframe.rename(columns={"value": name})
            frame.to_hdf(args.output_dir / f"{name}.h5", key="data", mode="w")
            source_path.write_text(source, encoding="utf-8")
            aligned = frame.reindex(eligible_index)
            metric = compute_single_factor_metrics(name, aligned, ctx)
            metric["full_canonical_opportunities"] = len(eligible_index)
            metric["full_canonical_finite_values"] = int(aligned.notna().sum().iloc[0])
            metric["full_canonical_coverage"] = metric["full_canonical_finite_values"] / len(eligible_index)
            metric = json_safe(metric)
            metric_path.write_text(json.dumps(metric, ensure_ascii=False, default=str, allow_nan=False), encoding="utf-8")
            del aligned, frame, result
        for window, values in metric["metrics"].items():
            rows.append({"factor_name": name, "eval_window": window, **values})
        reports[name] = metric["reports"]
        out = metric["metrics"].get("out_sample", {})
        print(json.dumps({"factor_name": name, "windows": len(metric["metrics"]),
                          "ic": out.get("ic_mean"), "rank_ic": out.get("rank_ic_mean"),
                          "h20_rank_ic": out.get("h20_rank_ic_mean"), "coverage": out.get("coverage"),
                          "metric_seconds": metric["duration"]}, allow_nan=False), flush=True)
        del metric
    payload = {"calc_batch_id": batch_id, "metrics": rows, "reports": reports,
               "universe_metadata": ctx["universe_metadata"], "snapshot_date": args.end,
               "calc_engine": "qe_eval_v2", "database_writes": 0}
    (args.output_dir / "metrics.json").write_text(json.dumps(payload, ensure_ascii=False, default=str, allow_nan=False), encoding="utf-8")
    print(json.dumps({"computed": 10, "metric_rows": len(rows), "database_writes": 0}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

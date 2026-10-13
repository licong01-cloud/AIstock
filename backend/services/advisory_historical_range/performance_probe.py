"""Isolated, read-only actual-package performance/equivalence comparison.

Only small code/config snapshots and score outputs are written under X. Model
and fitted dataset blobs are referenced in place; no training, business artifact,
database mutation, service control, or market-data copying is performed here.
"""

from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.request import urlopen

from dotenv import dotenv_values

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.strategy_package.live_inference import WslStrategyPackageInferenceProvider
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from scripts.strategy_package_live_inference import _atomic_json, _configure_temporary_root


def _source_identity(repo: Path) -> dict[str, str]:
    paths = (
        "backend/inference_engine.py", "scripts/strategy_package_live_inference.py",
        "backend/services/advisory_historical_range/__init__.py",
        "backend/services/advisory_historical_range/alpha158_batch.py",
        "backend/services/advisory_historical_range/inference_session.py",
        "backend/services/advisory_historical_range/factor_input_materialization.py",
        "backend/services/advisory_historical_range/performance_probe.py",
    )
    return {path: hashlib.sha256((repo / path).read_bytes()).hexdigest() for path in paths}


def _reference_blob(store: LocalPackageAssetStore, descriptor: dict, destination: Path, distro: str) -> None:
    # The existing asset store verifies admitted content. Resolve its documented
    # immutable layout; materialize_file's same-volume hard link cannot cross F/X.
    store.verify(descriptor["asset_ref"], sha256=descriptor["sha256"], size_bytes=descriptor["size_bytes"])
    source = store.root / "blobs" / descriptor["sha256"][:2] / descriptor["sha256"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["wsl", "-d", distro, "--", "ln", "-s", win_to_wsl_path(str(source)), win_to_wsl_path(str(destination))],
        capture_output=True, check=False, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    if result.returncode:
        raise RuntimeError("cannot reference an immutable package blob; no copy fallback")


def _snapshot_workspace(args: argparse.Namespace, root: Path) -> Path:
    source = Path(args.workspace).resolve(strict=True)
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    with urlopen(f"http://127.0.0.1:8001/api/v1/strategy-packages/{args.package_id}", timeout=20) as response:
        package = json.load(response)["package"]["manifest"]
    diagnostics = manifest["diagnostics"]
    if diagnostics["package_id"] != args.package_id or diagnostics["package_manifest_sha256"] != package["manifest_sha256"]:
        raise ValueError("probe workspace does not describe the requested current package")
    workspace = root / "workspace"
    workspace.mkdir()
    store = LocalPackageAssetStore(root=Path(args.asset_root))
    primary = manifest["primary_assets"]
    model = package["model_asset"]
    _reference_blob(store, model, workspace / primary["model_weight_relpath"], args.distro)
    _reference_blob(store, model["preprocessor_asset"], workspace / primary["dataset_processor_relpath"], args.distro)
    for code in model["model_code_assets"]:
        _reference_blob(store, code, workspace / "model" / code["relative_path"], args.distro)
    factor_entry = (source / primary["factor_entry_relpath"]).read_text(encoding="utf-8")
    assignment = next(
        node for node in ast.parse(factor_entry).body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "_FACTOR_FILES" for target in node.targets)
    )
    references = {}
    for number, (name, original) in enumerate(ast.literal_eval(assignment.value).items()):
        if original.startswith("/mnt/") and len(original) > 7 and original[6] == "/":
            original_path = Path(original[5].upper() + ":/" + original[7:])
        else:
            original_path = Path(original)
        target = workspace / "factors" / str(number) / "factor.py"
        target.parent.mkdir(parents=True)
        target.write_text(original_path.read_text(encoding="utf-8"), encoding="utf-8")
        references[name] = win_to_wsl_path(str(target))
    lines = factor_entry.splitlines(keepends=True)
    lines[assignment.lineno - 1:assignment.end_lineno] = ["_FACTOR_FILES = " + repr(references) + "\n"]
    (workspace / primary["factor_entry_relpath"]).write_text("".join(lines), encoding="utf-8")
    (workspace / "factor_order.json").write_text((source / "factor_order.json").read_text(encoding="utf-8"), encoding="utf-8")
    _atomic_json(workspace / "manifest.json", manifest)
    return workspace


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--workspace", required=True, help="Existing native prepared config, read only")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--env-file", default="F:/Dev/AIstock/.env", help="Credential location only, never recorded")
    parser.add_argument("--asset-root", default="F:/Dev/AIstock/rdagent_assets/package_assets")
    parser.add_argument("--distro", default="Ubuntu")
    parser.add_argument("--plan", required=True, help="JSON list of exact trade_date/cutoff_date requests")
    parser.add_argument("--modes", nargs="+", choices=["optimized", "reference"], default=["optimized", "reference"])
    args = parser.parse_args()
    root = Path(args.output_root).resolve()
    if root.exists():
        raise FileExistsError("probe output root already exists; no previous run is overwritten")
    _configure_temporary_root(str(root))
    allowed_keys = {"TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD", "AISTOCK_PG_STATEMENT_TIMEOUT_MS"}
    for key, value in dotenv_values(args.env_file).items():
        if key in allowed_keys and value is not None:
            os.environ.setdefault(key, value)
    workspace = _snapshot_workspace(args, root)
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    _atomic_json(root / "plan.json", plan)
    provider = WslStrategyPackageInferenceProvider(
        repo_root=Path(__file__).resolve().parents[3], distro=args.distro,
        safe_artifact_roots=(workspace, Path(args.asset_root)),
    )
    sources = _source_identity(provider.repo_root)
    summary = {"status": "RUNNING", "package_id": args.package_id, "modes": {}, "score_equivalence": "PENDING",
               "source_identity": sources, "source_binding": "PENDING"}
    _atomic_json(root / "progress.json", summary)
    for mode in args.modes:
        mode_root = root / mode
        mode_root.mkdir()
        output = mode_root / "batch.json"
        arguments = ["scripts/strategy_package_live_inference.py", "--runtime-workspace", win_to_wsl_path(str(workspace)),
                     "--historical-plan", win_to_wsl_path(str(root / "plan.json")), "--historical-read-only",
                     "--temp-root", win_to_wsl_path(str(mode_root)), "--output-path", win_to_wsl_path(str(output))]
        if mode == "reference":
            arguments.append("--performance-reference")
        command = (f"source {provider.conda_sh} && conda activate {provider.conda_env} && cd {provider._quote(win_to_wsl_path(str(provider.repo_root)))} && "
                   + provider._build_env_exports(historical_read_only=True) + " python " + " ".join(provider._quote(arg) for arg in arguments))
        summary["active_mode"] = mode
        summary["started_at"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(root / "progress.json", summary)
        started = time.perf_counter()
        with (mode_root / "execution.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen(["wsl", "-d", args.distro, "bash", "-lc", command], stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
                                       creationflags=subprocess.CREATE_NO_WINDOW)
            assert process.stdout is not None
            for line in process.stdout:
                password = os.getenv("TDX_DB_PASSWORD")
                if password:
                    line = line.replace(password, "[REDACTED]")
                log.write(line)
                log.flush()
            returncode = process.wait()
        summary["modes"][mode] = {"wall_seconds": time.perf_counter() - started, "returncode": returncode}
        if returncode:
            summary["status"] = "FAILED"
            _atomic_json(root / "progress.json", summary)
            return returncode
        _atomic_json(root / "progress.json", summary)
    if set(summary["modes"]) == {"optimized", "reference"}:
        equal = True
        for request in plan:
            day = request["trade_date"]
            results = [json.loads((root / mode / "batch_dates" / day / "scores.json").read_text(encoding="utf-8")) for mode in ("optimized", "reference")]
            identities = [[{key: value for key, value in row.items() if key != "first_observed_at"}
                           for row in result["source_read_receipts"]] for result in results]
            equal = equal and results[0]["scores"] == results[1]["scores"] and identities[0] == identities[1] and results[0]["input_context"] == results[1]["input_context"]
        summary["score_equivalence"] = "EXACT_PASS" if equal else "NOT_PROVED"
    summary["status"] = "COMPLETE"
    summary["source_binding"] = "EXACT_PASS" if sources == _source_identity(provider.repo_root) else "CHANGED_DURING_PROBE"
    _atomic_json(root / "progress.json", summary)
    print(json.dumps(summary, ensure_ascii=False))
    return 1 if summary["score_equivalence"] == "NOT_PROVED" or summary["source_binding"] != "EXACT_PASS" else 0


if __name__ == "__main__":
    raise SystemExit(main())

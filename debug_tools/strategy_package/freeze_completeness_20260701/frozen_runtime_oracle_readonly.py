from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        raise FileNotFoundError(f"env file not found: {env_file}")
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _force_pg_readonly() -> None:
    existing = os.environ.get("PGOPTIONS", "").strip()
    readonly = "-c default_transaction_read_only=on"
    if readonly not in existing:
        os.environ["PGOPTIONS"] = f"{existing} {readonly}".strip()


def _bootstrap_repo(repo_root: Path, env_file: Path | None) -> None:
    repo_root = repo_root.resolve()
    os.chdir(repo_root)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    _load_env_file(env_file or repo_root / ".env")
    _force_pg_readonly()


def _write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

from datetime import date


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only frozen StrategyPackage runtime oracle.")
    parser.add_argument("package_id")
    parser.add_argument("trade_date", type=date.fromisoformat)
    parser.add_argument("--repo-root", type=Path, default=_default_repo_root())
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument("--json-output", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _bootstrap_repo(args.repo_root, args.env_file)

    from backend.services.strategy_package.live_inference import (
        QEExperimentRuntimeAssetResolver,
        WslStrategyPackageInferenceProvider,
        win_to_wsl_path,
    )
    from backend.services.strategy_package.repository import StrategyPackageRepository

    print(f"=== READ-ONLY frozen oracle: {args.package_id} @ {args.trade_date} ===")
    repo = StrategyPackageRepository()
    record = repo.get(args.package_id)
    manifest = record.current_manifest()
    print(f"status(source_type)={record.source_type} source_id={record.source_id} loop_id={record.loop_id}")
    print(f"manifest_sha256={manifest.manifest_sha256}")
    print(f"factor_set={len(manifest.factor_set)} alpha_mode={manifest.alpha_mode}")

    resolver = QEExperimentRuntimeAssetResolver()
    bogus_source_id = "qe_DELETED_SOURCE_readonly_oracle_0000"
    source = resolver.load_source_for_strategy_package(
        source_type=record.source_type,
        source_id=bogus_source_id,
        loop_id="loop_DELETED_READONLY_ORACLE",
        run_id=None,
        manifest=manifest,
        package_id=record.package_id,
    )
    print(f"[resolve] model_params_origin={source.model_params_origin!r}")
    print(f"[resolve] source_workspace_type={getattr(source, 'source_workspace_type', None)!r}")
    print(f"[resolve] asset_workspace_path={source.asset_workspace_path}")
    print(f"[resolve] factor_names={len(source.factor_names)}")
    if source.model_params_origin != "package_asset":
        raise RuntimeError(
            "NOT self-contained: "
            f"origin={source.model_params_origin}; expected package_asset with bogus QE source id"
        )

    prepared = resolver.prepare_workspace(
        package_id=args.package_id,
        manifest_sha256=manifest.manifest_sha256,
        source=source,
        runtime_config=None,
        path_converter=win_to_wsl_path,
    )
    print(f"[prepare] workspace={prepared.workspace_path}")
    print(
        "[prepare] factor_count="
        f"{len(prepared.factor_order)} alpha158={len(prepared.alpha158_factors)} "
        f"dynamic={len(prepared.dynamic_factors)}"
    )
    print(f"[prepare] model_params_path={prepared.model_params_path}")

    provider = WslStrategyPackageInferenceProvider(repo_root=args.repo_root.resolve())
    print(f"[wsl] distro={provider.distro} env={provider.conda_env}; running inference")
    try:
        result = provider.run(workspace=prepared, trade_date=args.trade_date, cutoff_date=None)
    except Exception as exc:
        context = getattr(exc, "context", None) or {}
        print(f"[wsl] ERROR {type(exc).__name__}: {exc}")
        print(f"[wsl] returncode={context.get('returncode')}")
        print("[wsl] ---- stderr_tail ----")
        print(context.get("stderr_tail") or "(none)")
        print("[wsl] ---- stdout_tail ----")
        print((context.get("stdout_tail") or "(none)")[-2000:])
        return 3

    scores = result.scores
    if not scores:
        raise RuntimeError("empty scores from frozen runtime oracle")
    import math

    if not all(math.isfinite(float(score["score"])) for score in scores):
        raise RuntimeError("non-finite scores from frozen runtime oracle")
    print(f"[wsl] score_count={len(scores)}")
    print(f"[wsl] top5={[ (s['symbol'], round(float(s['score']), 5)) for s in scores[:5] ]}")
    payload = {
        "ok": True,
        "package_id": args.package_id,
        "trade_date": str(args.trade_date),
        "origin": source.model_params_origin,
        "score_count": len(scores),
        "top10": scores[:10],
    }
    _write_json(args.json_output, payload)
    print(
        f"SELF-CONTAINED PROVEN: {args.package_id} produced {len(scores)} finite scores "
        "from package_asset without QE source fallback"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

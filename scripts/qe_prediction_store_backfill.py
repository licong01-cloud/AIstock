"""One-off helper to backfill QE recorder artifacts into prediction-store.

The script is intentionally file/API only: it scans existing QE workspaces for
``mlruns/**/artifacts/pred.pkl`` and reuses the runner upload client to POST
artifacts when explicitly requested. Missing pred.pkl is reported as an error,
never silently skipped.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
_REMOTE_WORKSPACE_ROOT = Path("/home/lc999/projects/RD-Agent-main/qe_workspace")
DEFAULT_WORKSPACE_ROOT = Path(
    os.getenv("QE_BACKFILL_WORKSPACE_ROOT")
    or (_REMOTE_WORKSPACE_ROOT if _REMOTE_WORKSPACE_ROOT.exists() else "F:/Dev/RD-Agent-main/qe_workspace")
)
LOOP_RE = re.compile(r"^(?P<task>.+?)(?:[/\\:](?P<loop_a>Loop\d+)|_L(?P<loop_b>\d+))$")


@dataclass(frozen=True)
class BackfillTarget:
    run_key: str
    task_id: str | None
    loop_id: str | None
    loop_index: int | None
    loop_dir: Path


@dataclass(frozen=True)
class BackfillPlan:
    target: BackfillTarget
    artifacts: dict[str, Path]
    status: str
    error: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "run_key": self.target.run_key,
            "task_id": self.target.task_id,
            "loop_id": self.target.loop_id,
            "loop_index": self.target.loop_index,
            "loop_dir": str(self.target.loop_dir),
            "status": self.status,
            "artifacts": {key: str(value) for key, value in sorted(self.artifacts.items())},
            "error": self.error,
        }


def parse_loop_range(raw: str) -> list[int]:
    result: list[int] = []
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            start = int(left)
            end = int(right)
            if start > end:
                raise ValueError(f"invalid loop range {part!r}: start > end")
            result.extend(range(start, end + 1))
        else:
            result.append(int(part))
    deduped = sorted(set(result))
    if not deduped:
        raise ValueError("at least one loop index is required")
    return deduped


def build_targets(
    selectors: list[str],
    *,
    workspace_root: Path,
    task_ids: list[str],
    loops: list[int],
    run_key_template: str,
) -> list[BackfillTarget]:
    targets: list[BackfillTarget] = []
    for task_id in task_ids:
        for loop_index in loops:
            loop_id = f"Loop{loop_index}"
            targets.append(
                BackfillTarget(
                    run_key=run_key_template.format(task_id=task_id, loop_id=loop_id, loop_index=loop_index),
                    task_id=task_id,
                    loop_id=loop_id,
                    loop_index=loop_index,
                    loop_dir=workspace_root / task_id / loop_id,
                )
            )

    for selector in selectors:
        targets.append(_target_from_selector(selector, workspace_root=workspace_root))

    if not targets:
        raise ValueError("provide --task-id or one or more selectors")
    return targets


def _target_from_selector(selector: str, *, workspace_root: Path) -> BackfillTarget:
    text = str(selector or "").strip()
    if not text:
        raise ValueError("empty selector is not allowed")
    path = Path(text)
    if path.exists():
        return _target_from_loop_dir(path, run_key=path.name)

    match = LOOP_RE.match(text)
    if match:
        task_id = match.group("task")
        loop_text = match.group("loop_a") or f"Loop{match.group('loop_b')}"
        loop_index = int(loop_text.removeprefix("Loop"))
        return BackfillTarget(
            run_key=text.replace("\\", "/").replace("/", "_").replace(":", "_"),
            task_id=task_id,
            loop_id=loop_text,
            loop_index=loop_index,
            loop_dir=workspace_root / task_id / loop_text,
        )

    candidate = workspace_root / text
    if candidate.exists():
        return _target_from_loop_dir(candidate, run_key=text)
    raise FileNotFoundError(
        f"cannot resolve selector {selector!r}; expected existing path, task/LoopN, task_LN, or workspace child"
    )


def _target_from_loop_dir(path: Path, *, run_key: str) -> BackfillTarget:
    loop_dir = path.resolve()
    loop_id = loop_dir.name if loop_dir.name.startswith("Loop") else None
    loop_index = int(loop_id.removeprefix("Loop")) if loop_id and loop_id.removeprefix("Loop").isdigit() else None
    task_id = loop_dir.parent.name if loop_id else None
    return BackfillTarget(
        run_key=run_key,
        task_id=task_id,
        loop_id=loop_id,
        loop_index=loop_index,
        loop_dir=loop_dir,
    )


def plan_target(target: BackfillTarget) -> BackfillPlan:
    if not target.loop_dir.exists():
        return BackfillPlan(target=target, artifacts={}, status="missing", error="loop directory does not exist")

    pred = _newest_artifact(target.loop_dir, "pred.pkl")
    if pred is None:
        return BackfillPlan(target=target, artifacts={}, status="missing", error="pred.pkl not found")

    artifact_dir = pred.parent
    artifacts = {"prediction": pred}
    params = _first_existing(artifact_dir, ("params.pkl", "params_pkl"))
    if params is not None:
        artifacts["model_params"] = params
    label = artifact_dir / "label.pkl"
    if label.exists():
        artifacts["label"] = label
    return BackfillPlan(target=target, artifacts=artifacts, status="ready")


def _newest_artifact(loop_dir: Path, name: str) -> Path | None:
    candidates = sorted(
        loop_dir.glob(f"mlruns/**/artifacts/{name}"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _first_existing(directory: Path, names: tuple[str, ...]) -> Path | None:
    for name in names:
        path = directory / name
        if path.exists():
            return path
    return None


def upload_plan(plan: BackfillPlan, *, base_url: str | None, dry_run: bool) -> dict[str, Any]:
    payload = plan.to_json()
    if plan.status != "ready":
        payload["upload_status"] = "skipped_not_ready"
        return payload
    if dry_run:
        payload["upload_status"] = "dry_run"
        return payload
    if base_url:
        os.environ["AISTOCK_PREDICTION_STORE_BASE_URL"] = base_url

    try:
        from scripts import qe_prediction_store_client as client
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import qe_prediction_store_client as client  # type: ignore

    metadata = {
        "producer": "qe_prediction_store_backfill",
        "task_id": plan.target.task_id,
        "loop_id": plan.target.loop_id,
        "loop_index": plan.target.loop_index,
        "backfill": True,
    }
    manifest = client._post_artifacts(  # noqa: SLF001 - reuse runner upload logic for this one-off operator script.
        run_key=plan.target.run_key,
        artifacts=plan.artifacts,
        metadata=metadata,
    )
    payload["upload_status"] = "uploaded"
    payload["manifest"] = manifest
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("selectors", nargs="*", help="Existing path, task/LoopN, task:LoopN, task_LN, or workspace child")
    parser.add_argument("--workspace-root", type=Path, default=DEFAULT_WORKSPACE_ROOT)
    parser.add_argument("--task-id", action="append", default=[], help="QE task id; combine with --loops")
    parser.add_argument("--run-id", action="append", default=[], help="Alias selector for an already-known run key/path")
    parser.add_argument("--experiment-id", action="append", default=[], help="Alias selector, e.g. qe_task_L3")
    parser.add_argument("--loops", default="1-14", help="Loop indexes, e.g. 1-14 or 1,3,5")
    parser.add_argument("--run-key-template", default="{task_id}_L{loop_index}")
    parser.add_argument("--base-url", default=os.getenv("AISTOCK_PREDICTION_STORE_BASE_URL", ""))
    parser.add_argument("--upload", action="store_true", help="Actually POST artifacts; default only probes and reports")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any selected pred.pkl is missing")
    args = parser.parse_args(argv)

    workspace_root = args.workspace_root.expanduser()
    loops = parse_loop_range(args.loops)
    targets = build_targets(
        [*args.selectors, *args.run_id, *args.experiment_id],
        workspace_root=workspace_root,
        task_ids=args.task_id,
        loops=loops,
        run_key_template=args.run_key_template,
    )
    plans = [plan_target(target) for target in targets]
    results = [
        upload_plan(plan, base_url=args.base_url or None, dry_run=not args.upload)
        for plan in plans
    ]
    summary = {
        "schema_version": "qe_prediction_store_backfill_report_v1",
        "workspace_root": str(workspace_root),
        "upload_requested": bool(args.upload),
        "total": len(results),
        "ready": sum(1 for item in results if item["status"] == "ready"),
        "missing": sum(1 for item in results if item["status"] == "missing"),
        "results": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    if args.strict and summary["missing"]:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

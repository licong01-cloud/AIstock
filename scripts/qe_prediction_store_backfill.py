"""Backfill QE recorder artifacts and QE Archive pointer associations.

The workspace path scans ``mlruns/**/artifacts/pred.pkl`` and reuses the runner
upload client to POST artifacts when explicitly requested. The archive-link
path uses the backend repository and existing central manifests to associate
historical ``qe_archive.run`` rows without re-uploading blobs or rewriting
experiment statistics. Both operations default to dry-run.
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

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
_REMOTE_WORKSPACE_ROOT = Path("/home/lc999/projects/RD-Agent-main/qe_workspace")
DEFAULT_WORKSPACE_ROOT = Path(
    os.getenv("QE_BACKFILL_WORKSPACE_ROOT")
    or (_REMOTE_WORKSPACE_ROOT if _REMOTE_WORKSPACE_ROOT.exists() else "F:/Dev/RD-Agent-main/qe_workspace")
)
ENV_FILE_ENV = "AISTOCK_ENV_FILE"
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
    configured_env_file = str(os.getenv(ENV_FILE_ENV) or "").strip()
    if configured_env_file:
        load_dotenv(Path(configured_env_file).expanduser(), override=False)
    load_dotenv(REPO_ROOT / ".env", override=False)
    load_dotenv(override=False)
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
    parser.add_argument(
        "--link-archive",
        action="store_true",
        help="Resolve stored manifests and backfill qe_archive.run_artifact associations",
    )
    parser.add_argument(
        "--apply-links",
        action="store_true",
        help="Write archive associations; requires --link-archive (default is dry-run)",
    )
    parser.add_argument("--archive-run-id", action="append", default=[], help="Limit archive linking to qear_run id")
    parser.add_argument("--archive-after-run-id", help="Resume linking after this lexicographic run id")
    parser.add_argument("--archive-limit", type=int, default=0, help="Maximum archive runs; 0 processes all in pages")
    parser.add_argument("--archive-page-size", type=int, default=200, help="Archive candidate page size (1-500)")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero for missing workspace artifacts or failed/corrupt archive-link results",
    )
    args = parser.parse_args(argv)

    if args.apply_links and not args.link_archive:
        parser.error("--apply-links requires --link-archive")
    if args.archive_limit < 0:
        parser.error("--archive-limit must be >= 0")

    workspace_root = args.workspace_root.expanduser()
    workspace_requested = bool(
        args.selectors
        or args.run_id
        or args.experiment_id
        or args.upload
        or (args.task_id and not args.link_archive)
    )
    workspace_summary: dict[str, Any] | None = None
    if workspace_requested:
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
        workspace_summary = {
            "schema_version": "qe_prediction_store_backfill_report_v1",
            "workspace_root": str(workspace_root),
            "upload_requested": bool(args.upload),
            "total": len(results),
            "ready": sum(1 for item in results if item["status"] == "ready"),
            "missing": sum(1 for item in results if item["status"] == "missing"),
            "results": results,
        }

    archive_summary: dict[str, Any] | None = None
    if args.link_archive:
        from backend.services.qe_archive.backfill_service import (
            QEArchiveArtifactLinkBackfillOptions,
            QEArchiveBackfillService,
        )

        archive_summary = QEArchiveBackfillService().backfill_prediction_artifact_links(
            QEArchiveArtifactLinkBackfillOptions(
                run_ids=args.archive_run_id,
                task_ids=args.task_id,
                after_run_id=args.archive_after_run_id,
                limit=args.archive_limit,
                page_size=args.archive_page_size,
                write=bool(args.apply_links),
                verify_sha256=True,
            )
        )

    if workspace_summary is not None and archive_summary is None:
        summary = workspace_summary
    elif archive_summary is not None and workspace_summary is None:
        summary = archive_summary
    else:
        if workspace_summary is None and archive_summary is None:
            parser.error("provide a workspace selector/--task-id or use --link-archive")
        summary = {
            "schema_version": "qe_prediction_store_and_archive_link_backfill_v2",
            "workspace": workspace_summary,
            "archive_links": archive_summary,
        }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    strict_failed = bool(workspace_summary and workspace_summary.get("missing")) or bool(
        archive_summary
        and (
            archive_summary.get("failed")
            or archive_summary.get("corrupt")
        )
    )
    if args.strict and strict_failed:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

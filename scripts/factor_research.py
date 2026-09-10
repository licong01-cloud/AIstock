"""因子研究：数据库记录/恢复、只读上下文、候选执行；不写正式因子库。"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.factor_research.models import (  # noqa: E402
    ResearchError, encode, read_json, request, response,
)


def configure(env_file: Path, target: str):
    """Select only existing TDX_DB keys before importing any DB consumer."""
    from dotenv import dotenv_values

    if not env_file.is_file():
        raise ResearchError("configuration_missing", "Explicit env-file not found")
    env = dotenv_values(env_file)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    keys = ("HOST", "PORT", "NAME", "USER", "PASSWORD")
    if any(env.get(prefix + key) is None for key in keys):
        raise ResearchError("configuration_missing", "Selected target has incomplete TDX_DB configuration")
    if target == "dev" and all(env[prefix + k] == env.get("TDX_DB_" + k) for k in ("HOST", "PORT", "NAME")):
        raise ResearchError("target_mismatch", "DEV resolves to production")
    for key in keys:
        os.environ["TDX_DB_" + key] = env[prefix + key]
    return {"target": target, "host": env[prefix + "HOST"], "port": env[prefix + "PORT"], "database": env[prefix + "NAME"]}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    for command in ("create", "list", "show", "record", "context", "run", "attach", "quality"):
        item = sub.add_parser(command)
        item.add_argument("--env-file", type=Path, required=True)
        item.add_argument("--target", choices=("dev", "production"), required=True,
                          help="Explicit connection target, not production-write authorization")
        item.add_argument("--format", choices=("summary", "json"), default="summary")
        if command in {"create", "record", "run", "attach"}:
            item.add_argument("--input", type=Path, required=True)
            item.add_argument("--dry-run", action="store_true", help="Show requested action without database/file writes")
        if command in {"list", "show", "context"}:
            item.add_argument("--limit", type=int, default=20)
        if command in {"list", "context"}:
            item.add_argument("--offset", type=int, default=0)
        if command == "list":
            item.add_argument("--status", choices=("active", "paused", "completed"))
            item.add_argument("--factor")
            item.add_argument("--query")
        elif command == "show":
            item.add_argument("--task-id", required=True)
            item.add_argument("--before-revision", type=int)
        elif command == "context":
            item.add_argument("--factor", action="append", required=True)
            item.add_argument("--start-date", required=True, help="Metrics calculated_at / correlation as_of_date lower bound")
            item.add_argument("--end-date", required=True)
        elif command == "quality":
            item.add_argument("--factor", action="append")
            item.add_argument("--as-of", required=True)
            item.add_argument("--history-start", required=True)
            item.add_argument("--recent-start", required=True)
    return result


def dispatch(args):
    target = configure(args.env_file, args.target)
    payload = read_json(args.input) if hasattr(args, "input") else None
    if getattr(args, "dry_run", False):
        if args.command in {"create", "record"}:
            request(payload, create=args.command == "create")
        elif args.command == "run":
            from backend.services.factor_research.runner import validate_spec
            validate_spec(payload)
        return response(result={"dry_run": True, "target": target, "action": args.command,
                                "task_id": payload.get("task_id"), "database_writes": False,
                                "file_writes": False})
    from backend.services.factor_research.repository import ResearchRepository
    from backend.services.factor_research.service import ResearchService
    repo = ResearchRepository()
    service = ResearchService(repo, target=target)
    if args.command == "create":
        return repo.create(payload)
    if args.command == "record":
        return repo.record(payload)
    if args.command == "run":
        return service.run(payload)
    if args.command == "attach":
        return service.attach(payload)
    if args.command == "list":
        return response(result=repo.list(status=args.status, factor=args.factor, query=args.query,
                                         limit=args.limit, offset=args.offset))
    if args.command == "show":
        data = repo.show(args.task_id, before_revision=args.before_revision, limit=args.limit)
        return response(result=data, task_id=args.task_id, revision=data["task"]["revision"])
    if args.command == "quality":
        from backend.services.factor_research.quality_repository import quality_report
        report = quality_report(repo, as_of=args.as_of, history_start=args.history_start,
                                recent_start=args.recent_start, names=args.factor)
        report["database_target"] = target
        if args.format == "summary":
            report = {key: value for key, value in report.items() if key != "factors"}
        return response(result=report)
    return response(result=service.context(args.factor, start_date=args.start_date, end_date=args.end_date,
                                           limit=args.limit, offset=args.offset))


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        output = dispatch(args)
        rendered = encode(output)
    except ResearchError as exc:
        output = response(error=exc.as_dict())
        rendered = encode(output)
    except Exception as exc:
        # Raw DB/connection exceptions can contain credentials. Do not print them.
        output = response(error={"code": "operation_failed", "exception_type": type(exc).__name__,
                                 "detail": "Check the explicit target, input contract and task records; no fallback performed"})
        rendered = encode(output)
    if args.format == "json":
        print(rendered)
    else:
        print(f"{'OK' if output['ok'] else 'FAILED'} {args.command} task={output['task_id']} revision={output['revision']}")
        print(rendered)
    return 0 if output["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

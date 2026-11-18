"""Follow an existing QE task using reads only; never submit or rerun work."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any
from urllib.parse import quote, urlencode
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SCHEMA = "qe_task_followup_v1"
ACCOUNT_FILES = ("report_normal_1day.pkl", "positions_normal_1day.pkl", "indicators_normal_1day_obj.pkl")
TERMINAL = {"completed", "failed", "cancelled", "canceled", "error", "stopped"}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ReadUnavailable(RuntimeError):
    """An observation failure, with no implication about execution."""


def error_info(error: Exception) -> dict:
    return {"error_type": type(error).__name__,
            "reason_code": str(error) if isinstance(error, ReadUnavailable) else type(error).__name__}


class MainAPI:
    def __init__(self, base_url: str, timeout: float = 30):
        import requests

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.trust_env = False

    def get(self, path: str) -> Any:
        try:
            response = self.session.get(self.base_url + path, timeout=self.timeout)
            response.raise_for_status()
            body = response.json()
        except Exception as error:
            # URLs/response bodies may contain secrets. Persist only the class.
            raise ReadUnavailable(type(error).__name__) from error
        if isinstance(body, dict) and (body.get("ok") is False or (
                body.get("status") == "error" and "task_id" not in body)):
            raise ReadUnavailable("API_ERROR_ENVELOPE")
        return body.get("data", body) if isinstance(body, dict) else body

    def close(self) -> None:
        self.session.close()


def client_for_node(node_id: str):
    from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient

    return QEWorkspaceClient.for_node(node_id)


def dataset_identity(loop: dict, batch_policy: dict) -> dict:
    """Read declared identities; a public summary is never a binding."""
    local = (loop.get("_qe_batch_execution_policy") or {}).get("dataset_identity")
    batch = batch_policy.get("dataset_identity")
    contract = (loop.get("absolute_target_replay") or {}).get("dataset")
    values = [v for v in (local, batch) if isinstance(v, dict) and v]
    issues: list[str] = []
    if len(values) == 2 and values[0] != values[1]:
        issues.append("BATCH_LOOP_IDENTITY_CONFLICT")
    selected = values[0] if values else {}
    if isinstance(contract, dict) and contract:
        if (selected.get("dataset_manifest_sha256") and contract.get("manifest_sha256")
                and selected["dataset_manifest_sha256"] != contract["manifest_sha256"]):
            issues.append("FROZEN_CONTRACT_IDENTITY_CONFLICT")
    has_manifest = bool(selected.get("dataset_manifest_sha256") or (
        isinstance(contract, dict) and contract.get("manifest_sha256")))
    return {
        "status": "CONFLICT" if issues else ("RECORDED" if has_manifest else "UNKNOWN"),
        "batch_identity": selected or None,
        "contract_identity": contract,
        "summary": loop.get("dataset_summary"),
        "issues": issues,
    }


def associate_archives(task_id: str, rows: list, loops: list[dict]) -> dict:
    """Associate exact sources, without guessing a latest attempt from ordering."""
    matches: list[dict] = []
    truncated = len(rows) >= 500
    for loop in loops:
        candidates = [r for r in rows if isinstance(r, dict) and r.get("task_id") == task_id
                      and r.get("loop_index") == loop["loop_index"]
                      and r.get("node_id") == loop.get("node_id")
                      and (not loop.get("experiment_id") or r.get("experiment_id") == loop["experiment_id"])]
        status = "MISSING"
        if loop["status"] == "completed" and not loop.get("experiment_id"):
            status = "SOURCE_IDENTITY_UNAVAILABLE"
        elif len(candidates) > 1:
            status = "AMBIGUOUS"
        elif candidates:
            row = candidates[0]
            status = "RECORDED_VALID" if (row.get("status") in {"completed", "succeeded"}
                                            and row.get("research_valid") is True) else "NOT_VALIDATED"
        matches.append({"loop_index": loop["loop_index"], "status": status,
                        "run_ids": [r.get("run_id") for r in candidates],
                        "latest_attempt_check": "NOT_EXPOSED_BY_LIST_API"})
    completed_indices = {r["loop_index"] for r in loops if r["status"] == "completed"}
    completed = [r for r in matches if r["loop_index"] in completed_indices]
    return {"status": "RECORDED_VALID" if completed and not truncated and all(
        r["status"] == "RECORDED_VALID" for r in completed) else "INCOMPLETE",
        "loops": matches, "catalog_truncated": truncated}


async def observe(task_id: str, api: Any, *, node_factory=client_for_node, previous: dict | None = None) -> dict:
    result: dict[str, Any] = {
        "schema_version": SCHEMA, "task_id": task_id, "observed_at": utc_now(),
        "execution_status": "UNKNOWN", "observer_status": "OK", "warnings": [], "loops": [],
        "warehouse": {"status": "UNAVAILABLE"}, "collection": {"status": "NOT_REQUESTED"},
        "analysis": {"status": "NOT_REQUESTED", "economic_acceptance": "NOT_EVALUATED"},
    }
    if previous:
        result["last_known"] = {"observed_at": previous.get("observed_at"),
                                "execution_status": previous.get("execution_status")}
    try:
        config = api.get("/quantevolver/evolution/tasks/" + quote(task_id, safe="") + "/custom-evo-config")
        if not isinstance(config, dict) or config.get("task_id") != task_id:
            raise ReadUnavailable("TASK_RESPONSE_IDENTITY")
        configured = config.get("loops")
        statuses = config.get("loop_statuses")
        if not isinstance(configured, list) or not configured or not isinstance(statuses, list):
            raise ReadUnavailable("TASK_RESPONSE_SHAPE")
        indices = [r["loop_index"] for r in configured]
        if len(set(indices)) != len(indices) or any(type(i) is not int or i <= 0 for i in indices):
            raise ReadUnavailable("LOOP_INDEX_SHAPE")
        status_map = {r["loop_index"]: r for r in statuses}
        if len(status_map) != len(statuses):
            raise ReadUnavailable("DUPLICATE_LOOP_STATUS")
        result["execution_status"] = config.get("status") or "UNKNOWN"
        result["config_sha256"] = sha(json.dumps(config, sort_keys=True, default=str).encode())
        for loop in configured:
            row = status_map.get(loop["loop_index"], {})
            identity = dataset_identity(loop, config.get("execution_policy") or {})
            result["loops"].append({
                "loop_index": loop["loop_index"], "label": loop.get("label"),
                "node_id": row.get("node_id") or loop.get("node_id"),
                "experiment_id": row.get("experiment_id"), "status": row.get("status") or "UNKNOWN",
                "dataset": identity, "native": {"status": "UNAVAILABLE"},
                "collection": {"status": "NOT_REQUESTED"},
            })
            if identity["status"] != "RECORDED":
                result["warnings"].append({"loop_index": loop["loop_index"], "code": "DATASET_" + identity["status"]})
    except Exception as error:
        result["observer_status"] = "UNAVAILABLE" if isinstance(error, ReadUnavailable) else "ERROR"
        result["warnings"].append({"code": "TASK_READ_UNAVAILABLE", **error_info(error)})
        return result
    try:
        identity = api.get("/runtime-identity")
        result["runtime_identity"] = identity.get("merge_commit") if isinstance(identity, dict) else None
    except Exception as error:
        result["warnings"].append({"code": "RUNTIME_IDENTITY_UNAVAILABLE", "error_type": type(error).__name__})

    async def native(row):
        try:
            if not row["node_id"]:
                raise ReadUnavailable("NODE_UNAVAILABLE")
            async with node_factory(row["node_id"]) as client:
                body = await client.get_loop_status(task_id, "Loop" + str(row["loop_index"]))
            if not isinstance(body, dict) or not body.get("status"):
                raise ReadUnavailable("NODE_RESPONSE_SHAPE")
            row["native"] = {k: body.get(k) for k in ("status", "receipt_status", "request_digest", "submission_intent_hash")}
            if row["status"] == "completed" and body["status"] in {"running", "failed", "error"}:
                result["warnings"].append({"code": "NODE_CONTROLLER_CONFLICT", "loop_index": row["loop_index"]})
        except Exception as error:
            row["native"] = {"status": "UNAVAILABLE", "error_type": type(error).__name__}
            result["warnings"].append({"code": "NODE_READ_UNAVAILABLE", "loop_index": row["loop_index"]})

    await asyncio.gather(*(native(row) for row in result["loops"]))
    try:
        rows = api.get("/qe-archive/runs?" + urlencode({"search": task_id, "limit": 500, "run_type": "evolution_loop"}))
        if not isinstance(rows, list):
            raise ReadUnavailable("ARCHIVE_RESPONSE_SHAPE")
        result["warehouse"] = associate_archives(task_id, rows, result["loops"])
    except Exception as error:
        result["warehouse"] = {"status": "UNAVAILABLE", "error_type": type(error).__name__}
        result["warnings"].append({"code": "WAREHOUSE_READ_UNAVAILABLE"})
    return result


class ArtifactStore:
    """Immutable per-file receipts let a stopped collection resume without a lock."""

    def __init__(self, root: Path):
        self.root = root.resolve()

    def find(self, identity: dict, *, expected_sha: str | None = None) -> dict | None:
        key = sha(json.dumps(identity, sort_keys=True).encode())
        directory = self.root / "artifacts" / key
        if not directory.exists():
            return None
        for receipt in sorted(directory.glob("*.json"), reverse=True):
            try:
                row = json.loads(receipt.read_bytes())
                path = directory / row["local_name"]
                if (row["identity"] == identity and (not expected_sha or row["sha256"] == expected_sha)
                        and path.parent == directory and path.is_file()
                        and not path.is_symlink() and sha(path.read_bytes()) == row["sha256"]):
                    return {**row, "local_path": str(path), "reused": True}
            except (OSError, ValueError, KeyError):
                continue  # Corrupt local receipts are not proof; re-read the source.
        return None

    def put(self, identity: dict, raw: bytes) -> dict:
        key = sha(json.dumps(identity, sort_keys=True).encode())
        directory = self.root / "artifacts" / key
        directory.mkdir(parents=True, exist_ok=True)
        name = uuid.uuid4().hex
        path = directory / (name + ".bin")
        with path.open("xb") as stream:
            stream.write(raw)
        row = {"identity": identity, "sha256": sha(raw), "size": len(raw), "local_name": path.name}
        with (directory / (name + ".json")).open("x", encoding="utf-8") as stream:
            json.dump(row, stream, ensure_ascii=False)
        return {**row, "local_path": str(path), "reused": False}


async def collect_accounts(snapshot: dict, config: dict, store: ArtifactStore, *, node_factory=client_for_node) -> dict:
    """Read original account bytes only; no pickle loading or economic calculation."""
    configs = {r["loop_index"]: r for r in config["loops"]}
    for row in snapshot["loops"]:
        if row["status"] != "completed":
            continue
        if row["native"].get("status") != "completed":
            row["collection"] = {"status": "UNAVAILABLE", "reason_code": "SOURCE_COMPLETION_UNCONFIRMED"}
            snapshot["warnings"].append({"code": "ACCOUNT_COLLECTION_UNAVAILABLE", "loop_index": row["loop_index"]})
            continue
        try:
            task, index, node = snapshot["task_id"], row["loop_index"], row["node_id"]
            loop_id = "Loop" + str(index)
            async with node_factory(node) as client:
                recorder = await client.get_workspace_file(task, loop_id, "qe_current_recorder.json")
                if not isinstance(recorder, dict) or not recorder.get("recorder_id"):
                    raise ReadUnavailable("CURRENT_RECORDER_UNAVAILABLE")
                catalog = await client.list_workspace_files(task, loop_id)
                if catalog.get("catalog_completeness") != "complete":
                    raise ReadUnavailable("ACCOUNT_CATALOG_INCOMPLETE")
                entries = catalog.get("files", catalog.get("assets", []))
                paths = {str(r.get("relative_path") or r.get("path") or r.get("filename")).replace("\\", "/"): r for r in entries}
                matches = [p for p in paths if p.endswith("/portfolio_analysis/" + ACCOUNT_FILES[0])
                           and "/" + recorder["recorder_id"] + "/" in p]
                if len(matches) != 1:
                    raise ReadUnavailable("CURRENT_ACCOUNT_PATH_AMBIGUOUS")
                prefix = matches[0].rsplit("/", 1)[0] + "/"
                wanted = [prefix + f for f in ACCOUNT_FILES]
                contract = configs[index].get("absolute_target_replay")
                if contract:
                    wanted += ["qe_absolute_target_actions.jsonl", "qe_absolute_target_result.json"]
                files: list[dict] = []
                for source in wanted:
                    if source not in paths:
                        raise ReadUnavailable("ACCOUNT_FILE_MISSING")
                    identity = {"task_id": task, "loop_index": index, "node_id": node,
                                "recorder_id": recorder["recorder_id"], "source_path": source}
                    expected = paths[source].get("sha256")
                    saved = store.find(identity, expected_sha=expected)
                    if saved is None:
                        raw = await client.download_workspace_file_bytes(task, loop_id, source)
                        if expected and sha(raw) != expected:
                            raise ReadUnavailable("SOURCE_ACCOUNT_HASH_MISMATCH")
                        saved = store.put(identity, raw)
                    saved["remote_hash_verification"] = "SHA256_MATCH" if expected else "UNAVAILABLE_SAME_RECORDER"
                    files.append(saved)
                if contract:
                    from backend.services.quantevolver.absolute_target_consumer import validate_absolute_result

                    raw_result = json.loads(Path(files[-1]["local_path"]).read_bytes())
                    validate_absolute_result(raw_result, contract)
                    if raw_result["actions_sha256"] != files[-2]["sha256"]:
                        raise ReadUnavailable("ACCOUNT_ACTIONS_HASH_MISMATCH")
                if recorder != await client.get_workspace_file(task, loop_id, "qe_current_recorder.json"):
                    raise ReadUnavailable("CURRENT_RECORDER_CHANGED")
                row["collection"] = {"status": "COLLECTED", "recorder": recorder, "files": files}
        except Exception as error:
            row["collection"] = {"status": "UNAVAILABLE", **error_info(error)}
            snapshot["warnings"].append({"code": "ACCOUNT_COLLECTION_UNAVAILABLE", "loop_index": row["loop_index"]})
    completed = [r for r in snapshot["loops"] if r["status"] == "completed"]
    snapshot["collection"] = {"status": "COLLECTED" if completed and all(
        r["collection"]["status"] == "COLLECTED" for r in completed) else "INCOMPLETE"}
    return snapshot


def attach_analysis(snapshot: dict, path: Path) -> None:
    try:
        raw = path.read_bytes()
        receipt = json.loads(raw)
        if not isinstance(receipt, dict) or receipt.get("task_id") != snapshot["task_id"]:
            raise ReadUnavailable("ANALYSIS_TASK_MISMATCH")
        snapshot["analysis"] = {"status": "RECEIPT_AVAILABLE", "path": str(path), "sha256": sha(raw),
                                "reported_status": receipt.get("status"), "economic_acceptance": "NOT_EVALUATED"}
    except Exception as error:
        snapshot["analysis"] = {"status": "UNAVAILABLE", "error_type": type(error).__name__,
                                "economic_acceptance": "NOT_EVALUATED"}
        snapshot["warnings"].append({"code": "ANALYSIS_RECEIPT_UNAVAILABLE"})


def write_snapshot(directory: Path, snapshot: dict) -> None:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / (uuid.uuid4().hex + ".json")).open("x", encoding="utf-8") as stream:
            json.dump(snapshot, stream, ensure_ascii=False, indent=2, allow_nan=False)
    except (OSError, TypeError, ValueError) as error:
        snapshot["warnings"].append({"code": "LOCAL_REPORT_WRITE_UNAVAILABLE", "error_type": type(error).__name__})


def followup_finished(snapshot: dict, *, collecting: bool) -> bool:
    if snapshot["observer_status"] != "OK":
        return False
    if snapshot["execution_status"] in TERMINAL - {"completed"}:
        return True
    return (snapshot["execution_status"] == "completed" and snapshot["warehouse"]["status"] == "RECORDED_VALID"
            and (not collecting or snapshot["collection"]["status"] == "COLLECTED"))


async def follow(args, api: Any, *, node_factory=client_for_node, emit=print, sleep=asyncio.sleep) -> dict:
    previous = None
    session_dir = args.output_dir / "sessions" / uuid.uuid4().hex if args.output_dir else None
    store = ArtifactStore(args.output_dir) if args.collect_accounts else None
    while True:
        snapshot = await observe(args.task_id, api, node_factory=node_factory, previous=previous)
        if args.collect_accounts and snapshot["observer_status"] == "OK":
            try:
                config = api.get("/quantevolver/evolution/tasks/" + quote(args.task_id, safe="") + "/custom-evo-config")
                await collect_accounts(snapshot, config, store, node_factory=node_factory)
            except Exception as error:
                snapshot["collection"] = {"status": "UNAVAILABLE", **error_info(error)}
                snapshot["warnings"].append({"code": "ACCOUNT_COLLECTION_UNAVAILABLE", **error_info(error)})
        if args.analysis_receipt:
            attach_analysis(snapshot, args.analysis_receipt)
        if session_dir:
            write_snapshot(session_dir, snapshot)
        emit(json.dumps(snapshot, ensure_ascii=False, allow_nan=False))
        if not args.watch or followup_finished(snapshot, collecting=args.collect_accounts):
            return snapshot
        if snapshot["execution_status"] != "UNKNOWN":
            previous = snapshot
        await sleep(args.poll_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8001/api/v1")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--collect-accounts", action="store_true")
    parser.add_argument("--analysis-receipt", type=Path)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=45)
    args = parser.parse_args(argv)
    if args.poll_seconds <= 0 or (args.collect_accounts and not args.output_dir):
        parser.error("positive polling interval and collection output directory are required")
    if args.env_file:
        from dotenv import dotenv_values

        for key, value in dotenv_values(args.env_file).items():
            if value is not None:
                os.environ[key] = value
    # for_node resolves node URLs with one SELECT; this process never writes DB.
    os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=15000"
    api = MainAPI(args.api_base_url)
    try:
        snapshot = asyncio.run(follow(args, api))
        return 0 if snapshot["observer_status"] == "OK" else 1
    except KeyboardInterrupt:
        return 130
    finally:
        api.close()


if __name__ == "__main__":
    raise SystemExit(main())

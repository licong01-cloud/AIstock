"""Remote synchronization for QE factor-value cache.

The local cache under rdagent_assets/factor_values is the authority.  Remote
nodes receive rsync copies for acceleration only; QE must still recompute on a
cache miss.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from psycopg2.extras import RealDictCursor

from ...db.pg_pool import get_conn


PROJECT_ROOT = Path(__file__).resolve().parents[3]
LOCAL_CACHE_ROOT = PROJECT_ROOT / "rdagent_assets" / "factor_values"
LOCAL_SINGLE_DIR = LOCAL_CACHE_ROOT / "single"
LOCAL_META_PATH = LOCAL_CACHE_ROOT / "_meta.json"
REMOTE_SYNC_DIR = LOCAL_CACHE_ROOT / "_remote_sync"
REMOTE_STATUS_PATH = REMOTE_SYNC_DIR / "status.json"
REMOTE_JOBS_PATH = REMOTE_SYNC_DIR / "jobs.ndjson"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _win_to_wsl(path: Path | str) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(default)
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _run_wsl_bash(script: str, timeout_s: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["wsl", "bash", "-lc", script],
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_s,
        check=False,
    )


def _remote_host(api_base_url: str) -> Optional[str]:
    parsed = urlparse(api_base_url or "")
    host = parsed.hostname
    if not host:
        return None
    return host


def _is_local_host(host: Optional[str]) -> bool:
    return str(host or "").lower() in {"", "127.0.0.1", "localhost", "::1"}


def _default_remote_cache_dir(ssh_user: str) -> str:
    return f"/home/{ssh_user}/aistock_cache/factor_values"


@dataclass(frozen=True)
class RemoteCacheNode:
    node_id: str
    display_name: Optional[str]
    api_base_url: str
    ssh_user: str
    host: str
    factor_cache_dir: Optional[str]
    status: Optional[str]

    @property
    def ssh_target(self) -> str:
        return f"{self.ssh_user}@{self.host}"

    @property
    def resolved_cache_dir(self) -> str:
        return self.factor_cache_dir or _default_remote_cache_dir(self.ssh_user)


class FactorCacheRemoteSyncService:
    """Synchronize local factor cache to remote QE nodes through rsync/ssh."""

    def __init__(self, local_cache_root: Path = LOCAL_CACHE_ROOT):
        self.local_cache_root = Path(local_cache_root)
        self.local_single_dir = self.local_cache_root / "single"
        self.local_meta_path = self.local_cache_root / "_meta.json"
        self.sync_dir = self.local_cache_root / "_remote_sync"
        self.status_path = self.sync_dir / "status.json"
        self.jobs_path = self.sync_dir / "jobs.ndjson"

    # ------------------------------------------------------------------
    # Node and metadata helpers
    # ------------------------------------------------------------------

    def list_remote_nodes(self) -> List[RemoteCacheNode]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT node_id, display_name, api_base_url, ssh_user,
                           factor_cache_dir, status
                    FROM infra.compute_nodes
                    ORDER BY node_id
                    """
                )
                rows = [dict(row) for row in cur.fetchall()]

        nodes: List[RemoteCacheNode] = []
        for row in rows:
            host = _remote_host(str(row.get("api_base_url") or ""))
            ssh_user = str(row.get("ssh_user") or "").strip()
            if _is_local_host(host) or not ssh_user:
                continue
            nodes.append(
                RemoteCacheNode(
                    node_id=str(row["node_id"]),
                    display_name=row.get("display_name"),
                    api_base_url=str(row.get("api_base_url") or ""),
                    ssh_user=ssh_user,
                    host=str(host),
                    factor_cache_dir=row.get("factor_cache_dir"),
                    status=row.get("status"),
                )
            )
        return nodes

    def get_node(self, node_id: str) -> RemoteCacheNode:
        node_id = str(node_id or "").strip()
        for node in self.list_remote_nodes():
            if node.node_id == node_id:
                return node
        raise ValueError(f"Remote factor-cache node not found or not sync-capable: {node_id}")

    def _configure_default_dir_if_needed(self, node: RemoteCacheNode) -> RemoteCacheNode:
        if node.factor_cache_dir:
            return node
        resolved = node.resolved_cache_dir
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE infra.compute_nodes
                    SET factor_cache_dir = %s
                    WHERE node_id = %s AND factor_cache_dir IS NULL
                    """,
                    (resolved, node.node_id),
                )
        return RemoteCacheNode(
            node_id=node.node_id,
            display_name=node.display_name,
            api_base_url=node.api_base_url,
            ssh_user=node.ssh_user,
            host=node.host,
            factor_cache_dir=resolved,
            status=node.status,
        )

    def load_local_meta(self) -> Dict[str, Any]:
        return _load_json(self.local_meta_path, {"factors": {}})

    def _local_factor_entries(self, factor_names: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
        wanted = {str(name) for name in factor_names or [] if str(name or "").strip()}
        factors = self.load_local_meta().get("factors") or {}
        result: Dict[str, Dict[str, Any]] = {}
        for name, entry in factors.items():
            if wanted and name not in wanted:
                continue
            if not isinstance(entry, dict) or str(entry.get("status") or "ok").lower() == "error":
                continue
            if not entry.get("date_range"):
                continue
            parquet = self.local_single_dir / f"{name}.parquet"
            if not parquet.exists():
                continue
            result[name] = entry
        return result

    def _remote_meta(self, node: RemoteCacheNode, timeout_s: int = 20) -> Dict[str, Any]:
        remote_meta = f"{node.resolved_cache_dir.rstrip('/')}/_meta.json"
        script = (
            "ssh -o BatchMode=yes -o ConnectTimeout=8 "
            f"{shlex.quote(node.ssh_target)} "
            f"{shlex.quote(f'test -f {shlex.quote(remote_meta)} && cat {shlex.quote(remote_meta)} || echo {{\\\"factors\\\":{{}}}}')}"
        )
        proc = _run_wsl_bash(script, timeout_s=timeout_s)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "ssh remote meta read failed").strip())
        text = (proc.stdout or "").strip()
        if not text:
            return {"factors": {}}
        return json.loads(text)

    def _remote_file_exists(self, node: RemoteCacheNode, remote_path: str, timeout_s: int = 10) -> bool:
        script = (
            "ssh -o BatchMode=yes -o ConnectTimeout=8 "
            f"{shlex.quote(node.ssh_target)} "
            f"{shlex.quote(f'test -f {shlex.quote(remote_path)}')}"
        )
        proc = _run_wsl_bash(script, timeout_s=timeout_s)
        return proc.returncode == 0

    # ------------------------------------------------------------------
    # Stats and plan
    # ------------------------------------------------------------------

    @staticmethod
    def _entry_matches(local_entry: Dict[str, Any], remote_entry: Dict[str, Any]) -> bool:
        keys = ("source_hash_raw", "date_range", "window_train_start", "window_backtest_end")
        for key in keys:
            local_val = local_entry.get(key)
            if local_val is not None and str(remote_entry.get(key) or "") != str(local_val):
                return False
        return True

    def plan_sync(
        self,
        node: RemoteCacheNode,
        factor_names: Optional[Iterable[str]] = None,
        *,
        force: bool = False,
    ) -> Dict[str, Any]:
        local_entries = self._local_factor_entries(factor_names)
        remote_meta = self._remote_meta(node)
        remote_factors = remote_meta.get("factors") or {}
        remote_root = node.resolved_cache_dir.rstrip("/")

        sync_items: List[Dict[str, Any]] = []
        skipped_items: List[Dict[str, Any]] = []
        local_missing: List[Dict[str, Any]] = []
        for name, local_entry in sorted(local_entries.items()):
            local_parquet = self.local_single_dir / f"{name}.parquet"
            if not local_parquet.exists():
                local_missing.append({"factor_name": name, "reason": "local_parquet_missing"})
                continue
            remote_entry = remote_factors.get(name) or {}
            remote_parquet = f"{remote_root}/single/{name}.parquet"
            meta_match = isinstance(remote_entry, dict) and self._entry_matches(local_entry, remote_entry)
            if not force and meta_match and self._remote_file_exists(node, remote_parquet):
                skipped_items.append(
                    {
                        "factor_name": name,
                        "status": "synced",
                        "remote_date_range": remote_entry.get("date_range"),
                    }
                )
                continue
            reason = "force" if force else "missing_or_stale"
            if isinstance(remote_entry, dict) and remote_entry:
                if not meta_match:
                    reason = "remote_meta_stale"
            else:
                reason = "remote_missing"
            sync_items.append(
                {
                    "factor_name": name,
                    "reason": reason,
                    "local_date_range": local_entry.get("date_range"),
                    "remote_date_range": remote_entry.get("date_range") if isinstance(remote_entry, dict) else None,
                    "size_bytes": local_parquet.stat().st_size,
                }
            )

        return {
            "node_id": node.node_id,
            "remote_cache_dir": node.resolved_cache_dir,
            "local_count": len(local_entries),
            "sync_items": sync_items,
            "skipped_items": skipped_items,
            "local_missing": local_missing,
            "remote_meta": remote_meta,
        }

    def get_stats(self, node_id: Optional[str] = None, include_factor_status: bool = True) -> Dict[str, Any]:
        local_entries = self._local_factor_entries()
        local_size = sum((self.local_single_dir / f"{name}.parquet").stat().st_size for name in local_entries)
        nodes = self.list_remote_nodes()
        selected_node_id = node_id or (nodes[0].node_id if nodes else None)
        remote_nodes: List[Dict[str, Any]] = []
        factor_status: Dict[str, Any] = {}

        for node in nodes:
            node_payload: Dict[str, Any] = {
                "node_id": node.node_id,
                "display_name": node.display_name,
                "status": node.status,
                "host": node.host,
                "ssh_user": node.ssh_user,
                "factor_cache_dir": node.factor_cache_dir,
                "resolved_factor_cache_dir": node.resolved_cache_dir,
                "configured": bool(node.factor_cache_dir),
            }
            try:
                remote_meta = self._remote_meta(node)
                remote_factors = remote_meta.get("factors") or {}
                synced = 0
                stale = 0
                missing = 0
                for name, local_entry in local_entries.items():
                    remote_entry = remote_factors.get(name)
                    if isinstance(remote_entry, dict) and self._entry_matches(local_entry, remote_entry):
                        synced += 1
                        status = "synced"
                    elif remote_entry:
                        stale += 1
                        status = "stale"
                    else:
                        missing += 1
                        status = "missing"
                    if include_factor_status and node.node_id == selected_node_id:
                        factor_status[name] = {
                            "status": status,
                            "local_date_range": local_entry.get("date_range"),
                            "remote_date_range": remote_entry.get("date_range") if isinstance(remote_entry, dict) else None,
                        }
                node_payload.update(
                    {
                        "reachable": True,
                        "remote_cached": len(remote_factors),
                        "synced": synced,
                        "missing": missing,
                        "stale": stale,
                        "top_level_as_of_date": remote_meta.get("as_of_date"),
                    }
                )
            except Exception as exc:
                node_payload.update(
                    {
                        "reachable": False,
                        "error": str(exc),
                        "remote_cached": 0,
                        "synced": 0,
                        "missing": len(local_entries),
                        "stale": 0,
                    }
                )
            remote_nodes.append(node_payload)

        status = _load_json(self.status_path, {})
        return {
            "ok": True,
            "local": {
                "cache_root": str(self.local_cache_root),
                "cached": len(local_entries),
                "size_mb": round(local_size / 1024 / 1024, 1),
                "meta_sha256": _sha256_file(self.local_meta_path) if self.local_meta_path.exists() else None,
            },
            "selected_node_id": selected_node_id,
            "remote_nodes": remote_nodes,
            "factor_status": factor_status,
            "last_sync": status.get("last_sync"),
        }

    # ------------------------------------------------------------------
    # Sync execution
    # ------------------------------------------------------------------

    def sync_to_node(
        self,
        node_id: str,
        factor_names: Optional[Iterable[str]] = None,
        *,
        force: bool = False,
        configure_default_dir: bool = True,
        timeout_s: int = 1800,
    ) -> Dict[str, Any]:
        node = self.get_node(node_id)
        if configure_default_dir:
            node = self._configure_default_dir_if_needed(node)

        plan = self.plan_sync(node, factor_names, force=force)
        sync_items = plan["sync_items"]
        job = {
            "job_id": f"fc_sync_{int(datetime.now().timestamp() * 1000)}",
            "node_id": node.node_id,
            "remote_cache_dir": node.resolved_cache_dir,
            "started_at": _now_iso(),
            "finished_at": None,
            "status": "running",
            "requested_count": plan["local_count"],
            "sync_count": len(sync_items),
            "skipped_count": len(plan["skipped_items"]),
            "failed_count": 0,
            "synced_factors": [item["factor_name"] for item in sync_items],
            "error": None,
        }
        try:
            if sync_items:
                self._run_rsync(node, sync_items, plan["remote_meta"], timeout_s=timeout_s)
            job["status"] = "completed"
        except Exception as exc:
            job["status"] = "failed"
            job["failed_count"] = len(sync_items)
            job["error"] = str(exc)
        finally:
            job["finished_at"] = _now_iso()
            self._record_job(job)
        return job

    def sync_to_all_remote_nodes(
        self,
        factor_names: Optional[Iterable[str]] = None,
        *,
        force: bool = False,
        configure_default_dir: bool = True,
    ) -> Dict[str, Any]:
        jobs = []
        for node in self.list_remote_nodes():
            jobs.append(
                self.sync_to_node(
                    node.node_id,
                    factor_names,
                    force=force,
                    configure_default_dir=configure_default_dir,
                )
            )
        ok = all(job.get("status") == "completed" for job in jobs)
        return {"ok": ok, "jobs": jobs}

    def _run_rsync(
        self,
        node: RemoteCacheNode,
        sync_items: List[Dict[str, Any]],
        remote_meta: Dict[str, Any],
        *,
        timeout_s: int,
    ) -> None:
        remote_root = node.resolved_cache_dir.rstrip("/")
        mkdir_cmd = (
            "ssh -o BatchMode=yes -o ConnectTimeout=8 "
            f"{shlex.quote(node.ssh_target)} "
            f"{shlex.quote(f'mkdir -p {shlex.quote(remote_root)}/single')}"
        )
        proc = _run_wsl_bash(mkdir_cmd, timeout_s=30)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "remote mkdir failed").strip())

        with tempfile.TemporaryDirectory(prefix="factor_cache_sync_") as tmpdir:
            tmp_path = Path(tmpdir)
            files_from = tmp_path / "files.txt"
            files_from.write_text(
                "".join(f"{item['factor_name']}.parquet\n" for item in sync_items),
                encoding="utf-8",
            )
            src_single_wsl = _win_to_wsl(self.local_single_dir) + "/"
            files_from_wsl = _win_to_wsl(files_from)
            rsync_cmd = (
                "rsync -az --partial --files-from="
                f"{shlex.quote(files_from_wsl)} "
                f"{shlex.quote(src_single_wsl)} "
                f"{shlex.quote(node.ssh_target)}:{shlex.quote(remote_root + '/single/')}"
            )
            proc = _run_wsl_bash(rsync_cmd, timeout_s=timeout_s)
            if proc.returncode != 0:
                raise RuntimeError((proc.stderr or proc.stdout or "rsync parquet failed").strip())

            local_meta = self.load_local_meta()
            merged_meta = dict(remote_meta or {})
            merged_factors = dict(merged_meta.get("factors") or {})
            local_factors = local_meta.get("factors") or {}
            for item in sync_items:
                name = item["factor_name"]
                merged_factors[name] = local_factors[name]
            merged_meta["factors"] = merged_factors
            merged_meta["as_of_date"] = local_meta.get("as_of_date") or merged_meta.get("as_of_date")
            merged_meta["_last_remote_sync"] = {
                "synced_at": _now_iso(),
                "source": "AIstock",
                "factor_count": len(sync_items),
                "local_meta_sha256": _sha256_file(self.local_meta_path) if self.local_meta_path.exists() else None,
            }
            meta_tmp = tmp_path / "_meta.json"
            meta_tmp.write_text(json.dumps(merged_meta, ensure_ascii=False, indent=2), encoding="utf-8")
            remote_tmp = f"{remote_root}/_meta.json.tmp"
            meta_rsync_cmd = (
                "rsync -az --partial "
                f"{shlex.quote(_win_to_wsl(meta_tmp))} "
                f"{shlex.quote(node.ssh_target)}:{shlex.quote(remote_tmp)}"
            )
            proc = _run_wsl_bash(meta_rsync_cmd, timeout_s=60)
            if proc.returncode != 0:
                raise RuntimeError((proc.stderr or proc.stdout or "rsync meta failed").strip())
            remote_meta_final = f"{remote_root}/_meta.json"
            mv_inner = f"mv {shlex.quote(remote_tmp)} {shlex.quote(remote_meta_final)}"
            mv_cmd = (
                "ssh -o BatchMode=yes -o ConnectTimeout=8 "
                f"{shlex.quote(node.ssh_target)} "
                f"{shlex.quote(mv_inner)}"
            )
            proc = _run_wsl_bash(mv_cmd, timeout_s=30)
            if proc.returncode != 0:
                raise RuntimeError((proc.stderr or proc.stdout or "remote meta mv failed").strip())

    def _record_job(self, job: Dict[str, Any]) -> None:
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        with self.jobs_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(job, ensure_ascii=False) + "\n")
        status = _load_json(self.status_path, {})
        status["last_sync"] = job
        per_node = status.setdefault("nodes", {})
        per_node[job["node_id"]] = job
        _save_json(self.status_path, status)

"""Remote synchronization for QE factor-value cache.

The local cache under rdagent_assets/factor_values is the authority.  Remote
nodes receive cache copies for acceleration only; QE must still recompute on a
cache miss.  Remote writes are performed through the execution-node API only:
Windows-side code must never shell out into worker directories.
"""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import quote

from psycopg2.extras import RealDictCursor
import requests

from ...db.pg_pool import get_conn
from ..strategy_package.workspace_policy import ensure_aistock_artifact_path
from ..trading_core.errors import StrategyPackageValidationError


PROJECT_ROOT = Path(__file__).resolve().parents[3]
LOCAL_CACHE_ROOT = PROJECT_ROOT / "rdagent_assets" / "factor_values"
LOCAL_SINGLE_DIR = LOCAL_CACHE_ROOT / "single"
LOCAL_META_PATH = LOCAL_CACHE_ROOT / "_meta.json"
REMOTE_SYNC_DIR = LOCAL_CACHE_ROOT / "_remote_sync"
REMOTE_STATUS_PATH = REMOTE_SYNC_DIR / "status.json"
REMOTE_JOBS_PATH = REMOTE_SYNC_DIR / "jobs.ndjson"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _bounded_workers(value: int, item_count: int) -> int:
    if item_count <= 0:
        return 1
    try:
        workers = int(value)
    except Exception:
        workers = 4
    return max(1, min(workers, 16, item_count))


def _safe_factor_file_name(factor_name: str) -> str:
    text = str(factor_name or "").strip()
    if (
        not text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
        or Path(text).name != text
    ):
        raise StrategyPackageValidationError(
            "factor cache name must be a single safe path segment",
            context={"factor_name": str(factor_name)},
        )
    return f"{text}.parquet"


class FactorCacheNodeApiUnavailable(RuntimeError):
    """Raised when a node does not expose the required factor-cache API."""


@dataclass(frozen=True)
class RemoteCacheNode:
    node_id: str
    display_name: Optional[str]
    api_base_url: str
    factor_cache_dir: Optional[str]
    status: Optional[str]

    @property
    def cache_dir_param(self) -> Optional[str]:
        return str(self.factor_cache_dir).strip() if self.factor_cache_dir else None

    @property
    def resolved_cache_dir(self) -> str:
        return self.cache_dir_param or "node-api-default"


class FactorCacheNodeApiClient:
    """HTTP-only client for execution-node factor-cache operations."""

    def __init__(self, api_base_url: str, timeout_s: float = 60.0):
        self.api_base_url = str(api_base_url or "").rstrip("/")
        self.timeout_s = float(timeout_s)
        if not self.api_base_url:
            raise FactorCacheNodeApiUnavailable("node api_base_url is empty")

    def _url(self, path: str) -> str:
        return f"{self.api_base_url}/api/v1/qe_workspace/factor-cache/{path.lstrip('/')}"

    @staticmethod
    def _params(cache_dir: Optional[str]) -> Dict[str, str]:
        return {"cache_dir": cache_dir} if cache_dir else {}

    @staticmethod
    def _raise_api_unavailable(exc: requests.exceptions.HTTPError, url: str) -> None:
        response = exc.response
        status = response.status_code if response is not None else "?"
        if status == 404:
            raise FactorCacheNodeApiUnavailable(
                "node factor-cache API is unavailable; direct worker directory access is forbidden"
            ) from exc
        body = ""
        try:
            body = response.text[:300] if response is not None else ""
        except Exception:
            body = ""
        raise RuntimeError(f"node factor-cache API HTTP {status}: {url} {body}") from exc

    def get_meta(self, *, cache_dir: Optional[str]) -> Dict[str, Any]:
        url = self._url("meta")
        try:
            resp = requests.get(url, params=self._params(cache_dir), timeout=self.timeout_s)
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.HTTPError as exc:
            self._raise_api_unavailable(exc, url)
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"node factor-cache meta request failed: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"node factor-cache meta response is invalid: {exc}") from exc

        if isinstance(payload, dict) and isinstance(payload.get("meta"), dict):
            return payload["meta"]
        if isinstance(payload, dict) and isinstance(payload.get("factors"), dict):
            return payload
        raise RuntimeError(f"node factor-cache meta response missing factors: {payload}")

    def factor_exists(self, *, factor_name: str, cache_dir: Optional[str]) -> bool:
        url = self._url(f"factors/{quote(str(factor_name), safe='')}/status")
        try:
            resp = requests.get(url, params=self._params(cache_dir), timeout=self.timeout_s)
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.HTTPError as exc:
            self._raise_api_unavailable(exc, url)
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"node factor-cache status request failed: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"node factor-cache status response is invalid: {exc}") from exc
        return bool(isinstance(payload, dict) and payload.get("exists") is True)

    def upload_factor_file(
        self,
        *,
        cache_dir: Optional[str],
        factor_name: str,
        path: Path,
        timeout_s: int,
    ) -> Dict[str, Any]:
        url = self._url(f"factors/{quote(str(factor_name), safe='')}/file")
        file_path = Path(path)
        size_bytes = file_path.stat().st_size
        params = self._params(cache_dir)
        params["expected_size"] = str(size_bytes)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size_bytes),
        }
        try:
            with file_path.open("rb") as f:
                resp = requests.put(url, params=params, data=f, headers=headers, timeout=timeout_s)
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.HTTPError as exc:
            self._raise_api_unavailable(exc, url)
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"node factor-cache streaming upload failed for {factor_name}: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"node factor-cache streaming upload response is invalid for {factor_name}: {exc}") from exc

        if not isinstance(payload, dict) or payload.get("ok") is False:
            raise RuntimeError(f"node factor-cache streaming upload returned invalid payload for {factor_name}: {payload}")
        return payload

    def update_meta(
        self,
        *,
        cache_dir: Optional[str],
        merged_meta: Dict[str, Any],
        timeout_s: int,
    ) -> Dict[str, Any]:
        url = self._url("meta")
        try:
            resp = requests.post(
                url,
                params=self._params(cache_dir),
                json={"meta": merged_meta},
                timeout=timeout_s,
            )
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.HTTPError as exc:
            self._raise_api_unavailable(exc, url)
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"node factor-cache meta update failed: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"node factor-cache meta update response is invalid: {exc}") from exc

        if not isinstance(payload, dict) or payload.get("ok") is False:
            raise RuntimeError(f"node factor-cache meta update returned invalid payload: {payload}")
        return payload

    def upload_sync_bundle(
        self,
        *,
        cache_dir: Optional[str],
        factor_files: Dict[str, Path],
        merged_meta: Dict[str, Any],
        timeout_s: int,
        max_workers: int = 4,
    ) -> Dict[str, Any]:
        workers = _bounded_workers(max_workers, len(factor_files))
        uploaded: List[Dict[str, Any]] = []

        def _upload_one(item: tuple[str, Path]) -> Dict[str, Any]:
            factor_name, path = item
            return self.upload_factor_file(
                cache_dir=cache_dir,
                factor_name=factor_name,
                path=path,
                timeout_s=timeout_s,
            )

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="factor-cache-upload") as pool:
            futures = {pool.submit(_upload_one, item): item[0] for item in factor_files.items()}
            for future in as_completed(futures):
                factor_name = futures[future]
                try:
                    uploaded.append(future.result())
                except Exception as exc:
                    raise RuntimeError(f"node factor-cache streaming upload failed: {factor_name}: {exc}") from exc

        meta_result = self.update_meta(
            cache_dir=cache_dir,
            merged_meta=merged_meta,
            timeout_s=timeout_s,
        )
        return {
            "ok": True,
            "transport": "node_api_streaming_put",
            "upload_workers": workers,
            "uploaded_count": len(uploaded),
            "uploaded": sorted(uploaded, key=lambda item: str(item.get("factor_name") or "")),
            "meta": meta_result,
        }


class FactorCacheRemoteSyncService:
    """Synchronize local factor cache to remote QE nodes through node APIs."""

    def __init__(
        self,
        local_cache_root: Path = LOCAL_CACHE_ROOT,
        node_api_client_factory: Optional[Callable[[RemoteCacheNode, int], FactorCacheNodeApiClient]] = None,
    ):
        self.local_cache_root = Path(local_cache_root)
        ensure_aistock_artifact_path(self.local_cache_root, purpose="QE factor-cache local root")
        self.local_single_dir = self.local_cache_root / "single"
        self.local_meta_path = self.local_cache_root / "_meta.json"
        self.sync_dir = self.local_cache_root / "_remote_sync"
        self.status_path = self.sync_dir / "status.json"
        self.jobs_path = self.sync_dir / "jobs.ndjson"
        ensure_aistock_artifact_path(self.sync_dir, purpose="QE factor-cache sync state directory")
        self._node_api_client_factory = node_api_client_factory or (
            lambda node, timeout_s: FactorCacheNodeApiClient(node.api_base_url, timeout_s=timeout_s)
        )

    # ------------------------------------------------------------------
    # Node and metadata helpers
    # ------------------------------------------------------------------

    def list_remote_nodes(self) -> List[RemoteCacheNode]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT node_id, display_name, api_base_url,
                           factor_cache_dir, status
                    FROM infra.compute_nodes
                    ORDER BY node_id
                    """
                )
                rows = [dict(row) for row in cur.fetchall()]

        nodes: List[RemoteCacheNode] = []
        for row in rows:
            api_base_url = str(row.get("api_base_url") or "").strip()
            if not api_base_url:
                continue
            nodes.append(
                RemoteCacheNode(
                    node_id=str(row["node_id"]),
                    display_name=row.get("display_name"),
                    api_base_url=api_base_url,
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
        # API-only sync does not invent or write remote filesystem paths from the
        # Windows side.  When factor_cache_dir is absent the node API chooses its
        # own server-side default.
        return RemoteCacheNode(
            node_id=node.node_id,
            display_name=node.display_name,
            api_base_url=node.api_base_url,
            factor_cache_dir=node.factor_cache_dir,
            status=node.status,
        )

    def load_local_meta(self) -> Dict[str, Any]:
        return _load_json(self.local_meta_path, {"factors": {}})

    def _local_disk_factor_names(self) -> List[str]:
        if not self.local_single_dir.is_dir():
            return []
        names: List[str] = []
        for path in self.local_single_dir.glob("*.parquet"):
            if path.name.startswith("_"):
                continue
            names.append(path.stem)
        return sorted(names)

    def _local_meta_factor_names(self) -> List[str]:
        factors = self.load_local_meta().get("factors") or {}
        if not isinstance(factors, dict):
            return []
        return sorted(str(name) for name in factors)

    def _local_factor_entries(self, factor_names: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
        wanted = {str(name) for name in factor_names or [] if str(name or "").strip()}
        factors = self.load_local_meta().get("factors") or {}
        if not isinstance(factors, dict):
            factors = {}
        for name in factors:
            if wanted and name not in wanted:
                continue
            _safe_factor_file_name(name)
        result: Dict[str, Dict[str, Any]] = {}
        for name in self._local_disk_factor_names():
            if wanted and name not in wanted:
                continue
            parquet = self._local_parquet_path(name)
            if not parquet.exists():
                continue
            entry = factors.get(name)
            if not isinstance(entry, dict):
                result[name] = {
                    "status": "missing_meta_reconcile_required",
                    "_has_meta": False,
                    "_metadata_status": "missing_meta_reconcile_required",
                    "_size_bytes": parquet.stat().st_size,
                }
                continue
            if str(entry.get("status") or "ok").lower() == "error":
                continue
            metadata_status = "ok" if entry.get("date_range") else "incomplete_meta_reconcile_required"
            enriched = dict(entry)
            enriched["_has_meta"] = True
            enriched["_metadata_status"] = metadata_status
            enriched["_size_bytes"] = parquet.stat().st_size
            result[name] = enriched
        return result

    def _local_parquet_path(self, factor_name: str) -> Path:
        path = self.local_single_dir / _safe_factor_file_name(factor_name)
        return ensure_aistock_artifact_path(path, purpose="QE factor-cache local parquet")

    def _node_api_client(self, node: RemoteCacheNode, timeout_s: int) -> FactorCacheNodeApiClient:
        return self._node_api_client_factory(node, timeout_s)

    def _remote_meta(self, node: RemoteCacheNode, timeout_s: int = 20) -> Dict[str, Any]:
        return self._node_api_client(node, timeout_s).get_meta(cache_dir=node.cache_dir_param)

    def _remote_file_exists(self, node: RemoteCacheNode, factor_name: str, timeout_s: int = 10) -> bool:
        return self._node_api_client(node, timeout_s).factor_exists(
            factor_name=factor_name,
            cache_dir=node.cache_dir_param,
        )

    # ------------------------------------------------------------------
    # Stats and plan
    # ------------------------------------------------------------------

    @staticmethod
    def _entry_matches(local_entry: Dict[str, Any], remote_entry: Dict[str, Any]) -> bool:
        if local_entry.get("_metadata_status") not in (None, "ok"):
            return False
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

        sync_items: List[Dict[str, Any]] = []
        skipped_items: List[Dict[str, Any]] = []
        local_missing: List[Dict[str, Any]] = []
        requested = {str(name) for name in factor_names or [] if str(name or "").strip()}
        for name in sorted(requested - set(local_entries)):
            local_missing.append({"factor_name": name, "reason": "local_parquet_missing"})
        for name, local_entry in sorted(local_entries.items()):
            local_parquet = self._local_parquet_path(name)
            if not local_parquet.exists():
                local_missing.append({"factor_name": name, "reason": "local_parquet_missing"})
                continue
            metadata_status = str(local_entry.get("_metadata_status") or "ok")
            if metadata_status != "ok":
                skipped_items.append(
                    {
                        "factor_name": name,
                        "status": "metadata_reconcile_required",
                        "reason": metadata_status,
                    }
                )
                continue
            remote_entry = remote_factors.get(name) or {}
            meta_match = isinstance(remote_entry, dict) and self._entry_matches(local_entry, remote_entry)
            if not force and meta_match and self._remote_file_exists(node, name):
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
        local_disk_names = set(local_entries)
        local_meta_names = set(self._local_meta_factor_names())
        local_size = sum(int(entry.get("_size_bytes") or 0) for entry in local_entries.values())
        local_meta_count = len(local_meta_names)
        local_orphan_count = len(local_disk_names - local_meta_names)
        local_orphan_meta_count = len(local_meta_names - local_disk_names)
        local_metadata_pending = sum(
            1
            for entry in local_entries.values()
            if str(entry.get("_metadata_status") or "ok") != "ok"
        )
        nodes = self.list_remote_nodes()
        selected_node_id = node_id or (nodes[0].node_id if nodes else None)
        remote_nodes: List[Dict[str, Any]] = []
        factor_status: Dict[str, Any] = {}

        for node in nodes:
            node_payload: Dict[str, Any] = {
                "node_id": node.node_id,
                "display_name": node.display_name,
                "status": node.status,
                "factor_cache_dir": node.factor_cache_dir,
                "resolved_factor_cache_dir": node.resolved_cache_dir,
                "configured": bool(node.factor_cache_dir),
                "sync_transport": "node_api_streaming_put",
            }
            try:
                remote_meta = self._remote_meta(node)
                remote_factors = remote_meta.get("factors") or {}
                synced = 0
                stale = 0
                missing = 0
                metadata_pending = 0
                for name, local_entry in local_entries.items():
                    remote_entry = remote_factors.get(name)
                    local_meta_status = str(local_entry.get("_metadata_status") or "ok")
                    if local_meta_status != "ok":
                        metadata_pending += 1
                        if remote_entry:
                            stale += 1
                            status = "metadata_pending"
                        else:
                            missing += 1
                            status = "missing"
                    elif isinstance(remote_entry, dict) and self._entry_matches(local_entry, remote_entry):
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
                            "local_meta_status": local_meta_status,
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
                        "metadata_pending": metadata_pending,
                        "local_disk_factor_count": len(local_entries),
                        "local_meta_factor_count": local_meta_count,
                        "local_orphan_parquet_count": local_orphan_count,
                        "local_orphan_meta_count": local_orphan_meta_count,
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
                        "metadata_pending": local_metadata_pending,
                        "local_disk_factor_count": len(local_entries),
                        "local_meta_factor_count": local_meta_count,
                        "local_orphan_parquet_count": local_orphan_count,
                        "local_orphan_meta_count": local_orphan_meta_count,
                    }
                )
            remote_nodes.append(node_payload)

        status = _load_json(self.status_path, {})
        return {
            "ok": True,
            "local": {
                "cache_root": str(self.local_cache_root),
                "cached": len(local_entries),
                "disk_cached": len(local_entries),
                "meta_cached": local_meta_count,
                "orphan_parquet_count": local_orphan_count,
                "orphan_meta_count": local_orphan_meta_count,
                "metadata_pending": local_metadata_pending,
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
        upload_workers: int = 4,
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
            "sync_transport": "node_api_streaming_put",
            "upload_workers": _bounded_workers(upload_workers, len(sync_items)),
        }
        try:
            if sync_items:
                self._sync_via_node_api(
                    node,
                    sync_items,
                    plan["remote_meta"],
                    timeout_s=timeout_s,
                    upload_workers=upload_workers,
                )
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
        upload_workers: int = 4,
    ) -> Dict[str, Any]:
        jobs = []
        for node in self.list_remote_nodes():
            jobs.append(
                self.sync_to_node(
                    node.node_id,
                    factor_names,
                    force=force,
                    configure_default_dir=configure_default_dir,
                    upload_workers=upload_workers,
                )
            )
        ok = all(job.get("status") == "completed" for job in jobs)
        return {"ok": ok, "jobs": jobs}

    def _sync_via_node_api(
        self,
        node: RemoteCacheNode,
        sync_items: List[Dict[str, Any]],
        remote_meta: Dict[str, Any],
        *,
        timeout_s: int,
        upload_workers: int,
    ) -> None:
        local_meta = self.load_local_meta()
        merged_meta = dict(remote_meta or {})
        merged_factors = dict(merged_meta.get("factors") or {})
        local_factors = local_meta.get("factors") or {}
        local_entries = self._local_factor_entries(item["factor_name"] for item in sync_items)
        factor_files: Dict[str, Path] = {}
        for item in sync_items:
            name = item["factor_name"]
            local_entry = local_entries.get(name) or {}
            if name not in local_factors or str(local_entry.get("_metadata_status") or "ok") != "ok":
                raise RuntimeError(f"local factor meta missing or incomplete, reconcile required: {name}")
            path = self._local_parquet_path(name)
            if not path.exists():
                raise RuntimeError(f"local factor parquet missing: {name}")
            merged_factors[name] = local_factors[name]
            factor_files[name] = path

        merged_meta["factors"] = merged_factors
        merged_meta["as_of_date"] = local_meta.get("as_of_date") or merged_meta.get("as_of_date")
        merged_meta["_last_remote_sync"] = {
            "synced_at": _now_iso(),
            "source": "AIstock",
            "factor_count": len(sync_items),
            "local_meta_sha256": _sha256_file(self.local_meta_path) if self.local_meta_path.exists() else None,
            "transport": "node_api_streaming_put",
            "upload_workers": _bounded_workers(upload_workers, len(sync_items)),
        }

        self._node_api_client(node, timeout_s).upload_sync_bundle(
            cache_dir=node.cache_dir_param,
            factor_files=factor_files,
            merged_meta=merged_meta,
            timeout_s=timeout_s,
            max_workers=_bounded_workers(upload_workers, len(sync_items)),
        )

    def _record_job(self, job: Dict[str, Any]) -> None:
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        with self.jobs_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(job, ensure_ascii=False) + "\n")
        status = _load_json(self.status_path, {})
        status["last_sync"] = job
        per_node = status.setdefault("nodes", {})
        per_node[job["node_id"]] = job
        _save_json(self.status_path, status)

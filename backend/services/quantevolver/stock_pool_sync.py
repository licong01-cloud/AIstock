"""Stock-pool delivery helpers for QE execution nodes.

The Windows FastAPI process must not create, probe, or copy files inside an
execution node filesystem.  Filtered pools are read only from the AIstock-owned
local ``stock_pools`` cache, then delivered as normal loop ``experiment_files``
through the QE workspace API.  The node-side loop command installs the file into
its own Qlib instruments directory before ``qrun`` starts.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import shlex
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from ...db.pg_pool import get_conn
    from ..strategy_package.workspace_policy import ensure_aistock_artifact_path
except ImportError:  # tests may import backend/services as a top-level package
    from backend.db.pg_pool import get_conn
    from backend.services.strategy_package.workspace_policy import ensure_aistock_artifact_path

logger = logging.getLogger("aistock.quantevolver.stock_pool_sync")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STOCK_POOL_ROOT = PROJECT_ROOT / "stock_pools"
_FILTERED_POOL_RE = re.compile(r"^filtered_pool_[A-Za-z0-9_.-]+(?:\.txt)?$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LINUX_ABS_PATH_RE = re.compile(r"^/[A-Za-z0-9_./:@%+=,-]+$")


def is_filtered_stock_pool(stock_pool_path: str | None) -> bool:
    return bool(stock_pool_path and "filtered_pool" in str(stock_pool_path))


def _stock_pool_root() -> Path:
    raw = os.getenv("STOCK_POOL_OUTPUT_DIR")
    return Path(raw) if raw else DEFAULT_STOCK_POOL_ROOT


def _node_host(api_base_url: str | None) -> str:
    if not api_base_url:
        return ""
    parsed = urlparse(api_base_url if "://" in api_base_url else f"http://{api_base_url}")
    return parsed.hostname or ""


def _node_path(node: dict[str, Any], key: str) -> str:
    value = str(node.get(key) or "").strip()
    if value:
        return value
    workspace_config = node.get("workspace_config")
    if isinstance(workspace_config, dict):
        return str(workspace_config.get(key) or "").strip()
    return ""


def _safe_stock_pool_filename(stock_pool_path: str) -> str:
    value = str(stock_pool_path or "").strip()
    if not value:
        raise RuntimeError("stock_pool path is empty")
    normalized = value.replace("\\", "/")
    filename = normalized.rsplit("/", 1)[-1]
    if not filename.endswith(".txt"):
        filename = f"{filename}.txt"
    if not _FILTERED_POOL_RE.fullmatch(filename) or Path(filename).name != filename:
        raise RuntimeError(
            "stock_pool must be a filtered_pool file name or metadata path ending in filtered_pool_*.txt"
        )
    return filename


def _resolve_local_stock_pool_path(stock_pool_path: str) -> Path:
    """Resolve caller metadata to an AIstock-owned local stock-pool cache file.

    Historical callers may pass a Linux instruments path.  That path is treated
    only as metadata: Windows never probes it; only the safe basename is used.
    """
    filename = _safe_stock_pool_filename(stock_pool_path)
    root = _stock_pool_root()
    raw = str(stock_pool_path or "").strip()
    normalized = raw.replace("\\", "/")

    # Accept an explicit local file only when it is under an AIstock artifact root.
    if (":" in raw or raw.startswith(".")) and normalized.rsplit("/", 1)[-1] == filename:
        explicit = Path(raw)
        if explicit.is_absolute() or explicit.exists():
            return ensure_aistock_artifact_path(
                explicit,
                purpose="QE filtered stock-pool local source",
                extra_roots=[root],
            )

    return ensure_aistock_artifact_path(
        root / filename,
        purpose="QE filtered stock-pool local source",
        extra_roots=[root],
    )


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_stock_pool_payload(stock_pool_path: str) -> tuple[Path, str, str, str]:
    local_path = _resolve_local_stock_pool_path(stock_pool_path)
    if not local_path.exists() or not local_path.is_file():
        raise RuntimeError(
            "local filtered stock_pool cache file is missing; generate the pool before submitting QE: "
            f"{local_path}"
        )
    payload = local_path.read_bytes()
    digest = _sha256_bytes(payload)
    if not _SHA256_RE.fullmatch(digest):
        raise RuntimeError(f"invalid local stock_pool sha256 for {local_path}: {digest}")
    try:
        content = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"stock_pool file must be UTF-8 text: {local_path}") from exc
    return local_path, local_path.name, content, digest


def _remote_instruments_dir(node: dict[str, Any]) -> str:
    node_id = str(node.get("node_id") or "<unknown>")
    qlib_data = _node_path(node, "qlib_data_path")
    if not qlib_data:
        raise RuntimeError(f"node {node_id} missing qlib_data_path; cannot install stock_pool")
    qlib_data = qlib_data.rstrip("/")
    if not qlib_data.startswith("/") or "\x00" in qlib_data or not _LINUX_ABS_PATH_RE.fullmatch(qlib_data):
        raise RuntimeError(
            f"node {node_id} has invalid qlib_data_path for stock_pool install: {qlib_data!r}"
        )
    return f"{qlib_data}/instruments"


def build_stock_pool_install_command(*, filename: str, remote_instruments_dir: str, expected_sha256: str) -> str:
    """Build the node-side shell fragment that installs a packaged pool file."""
    if not _FILTERED_POOL_RE.fullmatch(filename) or Path(filename).name != filename:
        raise RuntimeError(f"unsafe stock_pool filename for install command: {filename!r}")
    if not _SHA256_RE.fullmatch(expected_sha256):
        raise RuntimeError(f"invalid stock_pool sha256 for install command: {expected_sha256!r}")
    dest = f"{remote_instruments_dir.rstrip('/')}/{filename}"
    src_q = shlex.quote(filename)
    dir_q = shlex.quote(remote_instruments_dir.rstrip("/"))
    dest_q = shlex.quote(dest)
    sha_q = shlex.quote(expected_sha256)
    return " && ".join(
        [
            f"test -f {src_q}",
            f"actual_sha=\"$(sha256sum {src_q} | cut -d ' ' -f 1)\"",
            (
                f"[ \"$actual_sha\" = {sha_q} ] || "
                f"{{ echo 'stock_pool source checksum mismatch' >&2; exit 1; }}"
            ),
            f"mkdir -p {dir_q}",
            f"cp -f {src_q} {dest_q}",
            f"final_sha=\"$(sha256sum {dest_q} | cut -d ' ' -f 1)\"",
            (
                f"[ \"$final_sha\" = {sha_q} ] || "
                f"{{ echo 'stock_pool installed checksum mismatch' >&2; exit 1; }}"
            ),
        ]
    )


def inject_stock_pool_install_command(execution_command: str, install_command: str | None) -> str:
    """Insert node-side stock-pool installation after the initial ``cd`` command."""
    command = str(execution_command or "").strip()
    if not install_command:
        return command
    if not command:
        raise RuntimeError("cannot inject stock_pool install command into an empty execution command")
    first, sep, rest = command.partition(" && ")
    if sep and first.strip().startswith("cd ") and rest.strip():
        return f"{first} && {install_command} && {rest}"
    return f"{install_command} && {command}"


def prepare_stock_pool_loop_payload(stock_pool_path: str | None, node: dict[str, Any]) -> dict[str, Any] | None:
    """Prepare stock-pool file content and node-side install command for a loop."""
    if not is_filtered_stock_pool(stock_pool_path):
        return None
    node_id = str(node.get("node_id") or "<unknown>")
    if not str(node.get("api_base_url") or "").strip():
        raise RuntimeError(f"node {node_id} missing api_base_url; cannot package stock_pool")
    local_path, filename, content, digest = _read_stock_pool_payload(str(stock_pool_path))
    instruments_dir = _remote_instruments_dir(node)
    remote_path = f"{instruments_dir.rstrip('/')}/{filename}"
    install_command = build_stock_pool_install_command(
        filename=filename,
        remote_instruments_dir=instruments_dir,
        expected_sha256=digest,
    )
    logger.info(
        "prepared stock_pool %s for node %s via loop payload sha256=%s",
        filename,
        node_id,
        digest,
    )
    return {
        "status": "packaged",
        "sync_transport": "loop_payload_api",
        "node_id": node_id,
        "host": _node_host(node.get("api_base_url")),
        "instrument_name": filename[:-4],
        "filename": filename,
        "local_path": str(local_path),
        "remote_path": remote_path,
        "sha256": digest,
        "experiment_files": {filename: content},
        "install_command": install_command,
    }


def _compute_node_by_id(node_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT node_id, api_base_url, qlib_data_path
                FROM infra.compute_nodes
                WHERE node_id = %s
                """,
                (node_id,),
            )
            row = cur.fetchone()
    if not row:
        raise RuntimeError(f"compute node does not exist: {node_id}")
    return {
        "node_id": row[0],
        "api_base_url": row[1],
        "qlib_data_path": row[2],
    }


def prepare_stock_pool_loop_payload_for_compute_node_by_id(
    node_id: str | None,
    stock_pool_path: str | None,
) -> dict[str, Any] | None:
    if not node_id or not is_filtered_stock_pool(stock_pool_path):
        return None
    return prepare_stock_pool_loop_payload(str(stock_pool_path), _compute_node_by_id(str(node_id)))


def sync_stock_pool_to_remote_node(stock_pool_path: str, node: dict[str, Any]) -> dict[str, Any]:
    """Compatibility wrapper: validate/package metadata without remote file access."""
    payload = prepare_stock_pool_loop_payload(stock_pool_path, node)
    if payload is None:
        return {"status": "skipped", "reason": "not_filtered_pool"}
    return {key: value for key, value in payload.items() if key not in {"experiment_files", "install_command"}}


def sync_all_filtered_pools_to_remote_node(node: dict[str, Any]) -> dict[str, Any]:
    """Validate all local filtered pools for a node; delivery remains per loop payload."""
    root = ensure_aistock_artifact_path(
        _stock_pool_root(),
        purpose="QE filtered stock-pool local root",
        extra_roots=[_stock_pool_root()],
    )
    files = sorted(root.glob("filtered_pool_*.txt")) if root.exists() else []
    validated = [sync_stock_pool_to_remote_node(path.name, node) for path in files]
    return {
        "status": "validated",
        "sync_transport": "loop_payload_api",
        "node_id": str(node.get("node_id") or "<unknown>"),
        "count": len(validated),
        "items": validated,
    }


def sync_stock_pool_to_compute_node_by_id(node_id: str | None, stock_pool_path: str | None) -> dict[str, Any] | None:
    """Backward-compatible preflight entry point used before task creation."""
    if not node_id or not is_filtered_stock_pool(stock_pool_path):
        return None
    return sync_stock_pool_to_remote_node(str(stock_pool_path), _compute_node_by_id(str(node_id)))

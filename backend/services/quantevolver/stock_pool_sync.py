"""Remote stock-pool synchronization helpers for QE runs.

Filtered instrument pools are generated in the local WSL qlib data directory.
When a QE run is submitted to a remote RDAgent node, the same file must exist in
the remote node's qlib instruments directory before Qlib starts.  These helpers
fail fast instead of letting the remote run silently fall back or fail later.
"""
from __future__ import annotations

import logging
import os
import shlex
import subprocess
from typing import Any
from urllib.parse import urlparse

try:
    from ...db.pg_pool import get_conn
except ImportError:  # tests may import backend/services as a top-level package
    from backend.db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.stock_pool_sync")

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
def _wsl_distro() -> str:
    distro = os.getenv("AISTOCK_WSL_DISTRO") or os.getenv("QLIB_WSL_DISTRO") or ""
    distro = distro.strip()
    if not distro:
        raise RuntimeError("AISTOCK_WSL_DISTRO or QLIB_WSL_DISTRO is required for stock_pool WSL access")
    return distro


def _wsl_bash_command(script: str) -> list[str]:
    return ["wsl", "-d", _wsl_distro(), "--", "bash", "-lc", script]


def is_filtered_stock_pool(stock_pool_path: str | None) -> bool:
    return bool(stock_pool_path and "filtered_pool" in str(stock_pool_path))


def _resolve_local_stock_pool_path(stock_pool_path: str) -> str:
    """Resolve a Qlib instrument name to the authoritative local WSL file path."""
    value = str(stock_pool_path).strip()
    if not value:
        raise RuntimeError("stock_pool path is empty")
    if "/" in value or "\\" in value:
        return value.replace("\\", "/")
    filename = value if value.endswith(".txt") else f"{value}.txt"
    qlib_data_path = os.getenv("QLIB_DATA_PATH_WSL", "").strip().rstrip("/")
    if not qlib_data_path:
        raise RuntimeError("QLIB_DATA_PATH_WSL is required to resolve local stock_pool paths")
    return f"{qlib_data_path}/instruments/{filename}"


def _node_host(api_base_url: str | None) -> str:
    if not api_base_url:
        return ""
    parsed = urlparse(api_base_url if "://" in api_base_url else f"http://{api_base_url}")
    return parsed.hostname or ""


def _run_checked(cmd: list[str], *, timeout: int, error_prefix: str) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, timeout=timeout, check=False, capture_output=True)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
        stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
        detail = stderr.strip() or stdout.strip()
        raise RuntimeError(f"{error_prefix}: {detail}")
    return result


def _assert_wsl_file_exists(stock_pool_path: str) -> None:
    _run_checked(
        _wsl_bash_command(f"test -f {shlex.quote(stock_pool_path)}"),
        timeout=10,
        error_prefix=f"stock_pool file does not exist in WSL: {stock_pool_path}",
    )


def _wsl_sha256(stock_pool_path: str) -> str:
    result = _run_checked(
        _wsl_bash_command(f"sha256sum {shlex.quote(stock_pool_path)} | awk '{{print $1}}'"),
        timeout=10,
        error_prefix=f"failed to checksum WSL stock_pool file: {stock_pool_path}",
    )
    return result.stdout.decode("utf-8", errors="replace").strip()


def sync_stock_pool_to_remote_node(stock_pool_path: str, node: dict[str, Any]) -> dict[str, str]:
    """Copy one filtered_pool file to a remote node's qlib instruments directory."""
    if not is_filtered_stock_pool(stock_pool_path):
        return {"status": "skipped", "reason": "not_filtered_pool"}

    local_stock_pool_path = _resolve_local_stock_pool_path(stock_pool_path)
    node_id = node.get("node_id") or "<unknown>"
    host = _node_host(node.get("api_base_url"))
    if not host:
        raise RuntimeError(f"node {node_id} missing api_base_url; cannot sync stock_pool")
    if host in _LOCAL_HOSTS:
        _assert_wsl_file_exists(local_stock_pool_path)
        return {
            "status": "skipped",
            "reason": "local_node",
            "node_id": str(node_id),
            "host": host,
            "local_path": local_stock_pool_path,
            "sha256": _wsl_sha256(local_stock_pool_path),
        }

    remote_qlib_data = node.get("qlib_data_path") or ""
    if not remote_qlib_data:
        raise RuntimeError(f"node {node_id} missing qlib_data_path; cannot sync stock_pool")

    _assert_wsl_file_exists(local_stock_pool_path)
    local_sha256 = _wsl_sha256(local_stock_pool_path)

    ssh_user = node.get("ssh_user") or ""
    if not ssh_user:
        raise RuntimeError(f"node {node_id} missing ssh_user; cannot sync stock_pool")
    ssh_target = f"{ssh_user}@{host}"
    remote_instruments_dir = f"{remote_qlib_data.rstrip('/')}/instruments"
    remote_path = f"{remote_instruments_dir}/{os.path.basename(local_stock_pool_path)}"

    _run_checked(
        ["ssh", ssh_target, "mkdir", "-p", remote_instruments_dir],
        timeout=10,
        error_prefix=f"failed to create remote instruments dir {ssh_target}:{remote_instruments_dir}",
    )

    _run_checked(
        _wsl_bash_command(
            "scp -o ConnectTimeout=10 "
            f"{shlex.quote(local_stock_pool_path)} "
            f"{shlex.quote(f'{ssh_target}:{remote_instruments_dir}/')}"
        ),
        timeout=30,
        error_prefix=f"failed to sync stock_pool {local_stock_pool_path} -> {ssh_target}:{remote_instruments_dir}/",
    )
    remote_checksum = _run_checked(
        ["ssh", ssh_target, f"sha256sum {shlex.quote(remote_path)} | awk '{{print $1}}'"],
        timeout=10,
        error_prefix=f"failed to checksum remote stock_pool {ssh_target}:{remote_path}",
    ).stdout.decode("utf-8", errors="replace").strip()
    if remote_checksum != local_sha256:
        raise RuntimeError(
            f"stock_pool checksum mismatch after sync: local={local_sha256} "
            f"remote={remote_checksum} path={ssh_target}:{remote_path}"
        )
    logger.info(
        "synced stock_pool %s -> %s:%s/ sha256=%s",
        os.path.basename(local_stock_pool_path),
        ssh_target,
        remote_instruments_dir,
        local_sha256,
    )
    return {
        "status": "synced",
        "node_id": str(node_id),
        "host": host,
        "remote_path": remote_path,
        "local_path": local_stock_pool_path,
        "sha256": local_sha256,
    }


def sync_all_filtered_pools_to_remote_node(node: dict[str, Any]) -> dict[str, str]:
    """Copy all local filtered_pool_*.txt files to a remote node."""
    node_id = node.get("node_id") or "<unknown>"
    host = _node_host(node.get("api_base_url"))
    if not host:
        raise RuntimeError(f"node {node_id} missing api_base_url; cannot sync filtered pools")
    if host in _LOCAL_HOSTS:
        return {"status": "skipped", "reason": "local_node", "node_id": str(node_id), "host": host}

    remote_qlib_data = node.get("qlib_data_path") or ""
    if not remote_qlib_data:
        raise RuntimeError(f"node {node_id} missing qlib_data_path; cannot sync filtered pools")

    ssh_user = node.get("ssh_user") or ""
    if not ssh_user:
        raise RuntimeError(f"node {node_id} missing ssh_user; cannot sync filtered pools")
    ssh_target = f"{ssh_user}@{host}"
    remote_instruments_dir = f"{remote_qlib_data.rstrip('/')}/instruments"
    local_qlib_data = os.getenv("QLIB_DATA_PATH_WSL", "").strip().rstrip("/")
    if not local_qlib_data:
        raise RuntimeError("QLIB_DATA_PATH_WSL is required to sync filtered pools")
    local_instruments_dir = f"{local_qlib_data}/instruments"

    _run_checked(
        ["ssh", ssh_target, "mkdir", "-p", remote_instruments_dir],
        timeout=10,
        error_prefix=f"failed to create remote instruments dir {ssh_target}:{remote_instruments_dir}",
    )
    _run_checked(
        _wsl_bash_command(
            "scp -o ConnectTimeout=10 "
            f"{shlex.quote(local_instruments_dir)}/filtered_pool_*.txt "
            f"{shlex.quote(f'{ssh_target}:{remote_instruments_dir}/')}"
        ),
        timeout=60,
        error_prefix=f"failed to sync filtered_pool files -> {ssh_target}:{remote_instruments_dir}/",
    )
    logger.info("synced all filtered_pool files -> %s:%s/", ssh_target, remote_instruments_dir)
    return {"status": "synced", "node_id": str(node_id), "host": host, "remote_dir": remote_instruments_dir}


def sync_stock_pool_to_compute_node_by_id(node_id: str | None, stock_pool_path: str | None) -> dict[str, str] | None:
    """Resolve a compute node and sync the filtered pool if needed."""
    if not node_id or not is_filtered_stock_pool(stock_pool_path):
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT node_id, api_base_url, ssh_user, qlib_data_path
                FROM infra.compute_nodes
                WHERE node_id = %s
                """,
                (node_id,),
            )
            row = cur.fetchone()
    if not row:
        raise RuntimeError(f"compute node does not exist: {node_id}")
    node = {
        "node_id": row[0],
        "api_base_url": row[1],
        "ssh_user": row[2],
        "qlib_data_path": row[3],
    }
    return sync_stock_pool_to_remote_node(str(stock_pool_path), node)

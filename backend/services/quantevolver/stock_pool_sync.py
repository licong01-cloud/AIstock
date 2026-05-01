"""Remote stock-pool synchronization helpers for QE runs.

Filtered instrument pools are generated in the local WSL qlib data directory.
When a QE run is submitted to a remote RDAgent node, the same file must exist in
the remote node's qlib instruments directory before Qlib starts.  These helpers
fail fast instead of letting the remote run silently fall back or fail later.
"""
from __future__ import annotations

import logging
import os
import re
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
_LINUX_USER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,63}$")
_LINUX_PATH_KEYS = (
    "qlib_data_path",
    "qlib_minute_path",
    "workspace_base",
    "factor_data_dir",
    "qlib_rdagent_root",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SSH_CONNECT_TIMEOUT_SECONDS = 10
_SSH_COMMAND_TIMEOUT_SECONDS = 30
_SCP_TRANSFER_TIMEOUT_SECONDS = 60
_SSH_OPTIONS = [
    "-o",
    "BatchMode=yes",
    "-o",
    f"ConnectTimeout={_SSH_CONNECT_TIMEOUT_SECONDS}",
    "-o",
    "ConnectionAttempts=1",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "NumberOfPasswordPrompts=0",
    "-o",
    "ServerAliveInterval=5",
    "-o",
    "ServerAliveCountMax=2",
]
_SCP_OPTIONS = " ".join(shlex.quote(part) for part in _SSH_OPTIONS)


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


def _validate_linux_user(node_id: str, ssh_user: str, *, source: str) -> str:
    user = str(ssh_user or "").strip()
    if not user:
        return ""
    if not _LINUX_USER_RE.fullmatch(user):
        raise RuntimeError(
            f"node {node_id} has invalid ssh_user from {source}: {user!r}; "
            "cannot sync stock_pool"
        )
    return user


def _linux_home_user(path_value: Any) -> str | None:
    path = str(path_value or "").strip()
    if not path.startswith("/home/"):
        return None
    parts = path.split("/")
    return parts[2] if len(parts) > 2 and parts[2] else None


def _node_path(node: dict[str, Any], key: str) -> str:
    value = str(node.get(key) or "").strip()
    if value:
        return value
    workspace_config = node.get("workspace_config")
    if isinstance(workspace_config, dict):
        return str(workspace_config.get(key) or "").strip()
    return ""


def _resolve_ssh_user(node: dict[str, Any], *, purpose: str) -> str:
    """Resolve SSH user explicitly, or derive it from unambiguous /home/<user> paths."""
    node_id = str(node.get("node_id") or "<unknown>")
    explicit = _validate_linux_user(node_id, str(node.get("ssh_user") or ""), source="ssh_user")
    if explicit:
        return explicit

    derived: dict[str, str] = {}
    for key in _LINUX_PATH_KEYS:
        user = _linux_home_user(_node_path(node, key))
        if user:
            derived[key] = _validate_linux_user(node_id, user, source=key)

    users = {user for user in derived.values() if user}
    if len(users) == 1:
        user = next(iter(users))
        logger.warning(
            "node %s missing ssh_user; derived ssh_user=%s from Linux home paths for %s",
            node_id,
            user,
            purpose,
        )
        return user
    if len(users) > 1:
        raise RuntimeError(
            f"node {node_id} missing ssh_user and Linux paths imply multiple users "
            f"{sorted(users)} from {derived}; cannot {purpose}"
        )
    raise RuntimeError(
        f"node {node_id} missing ssh_user and no /home/<user> node path is available; "
        f"set infra.compute_nodes.ssh_user or configure node paths under /home/<user>; cannot {purpose}"
    )


def _safe_cmd_for_error(cmd: list[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in cmd)


def _run_checked(cmd: list[str], *, timeout: int, error_prefix: str) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(cmd, timeout=timeout, check=False, capture_output=True)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{error_prefix}: command timed out after {timeout}s: {_safe_cmd_for_error(cmd)}"
        ) from exc
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
        stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
        detail = stderr.strip() or stdout.strip()
        raise RuntimeError(
            f"{error_prefix}: {detail or 'command failed without output'}; "
            f"command={_safe_cmd_for_error(cmd)}"
        )
    return result


def _ssh_command(ssh_target: str, remote_command: str) -> list[str]:
    return ["ssh", *_SSH_OPTIONS, ssh_target, remote_command]


def _extract_sha256(output: bytes, *, context: str) -> str:
    """Parse sha256sum output into the exact 64-hex digest and reject anything invalid."""
    text = output.decode("utf-8", errors="replace").strip()
    digest = text.split()[0] if text else ""
    if not _SHA256_RE.fullmatch(digest):
        raise RuntimeError(f"invalid sha256 output for {context}: {text!r}")
    return digest


def _assert_wsl_file_exists(stock_pool_path: str) -> None:
    _run_checked(
        _wsl_bash_command(f"test -f {shlex.quote(stock_pool_path)}"),
        timeout=10,
        error_prefix=f"stock_pool file does not exist in WSL: {stock_pool_path}",
    )


def _wsl_sha256(stock_pool_path: str) -> str:
    result = _run_checked(
        _wsl_bash_command(f"sha256sum {shlex.quote(stock_pool_path)}"),
        timeout=10,
        error_prefix=f"failed to checksum WSL stock_pool file: {stock_pool_path}",
    )
    return _extract_sha256(result.stdout, context=f"local stock_pool {stock_pool_path}")


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

    remote_qlib_data = _node_path(node, "qlib_data_path")
    if not remote_qlib_data:
        raise RuntimeError(f"node {node_id} missing qlib_data_path; cannot sync stock_pool")

    ssh_user = _resolve_ssh_user(node, purpose="sync stock_pool")
    _assert_wsl_file_exists(local_stock_pool_path)
    local_sha256 = _wsl_sha256(local_stock_pool_path)

    ssh_target = f"{ssh_user}@{host}"
    remote_instruments_dir = f"{remote_qlib_data.rstrip('/')}/instruments"
    remote_path = f"{remote_instruments_dir}/{os.path.basename(local_stock_pool_path)}"

    _run_checked(
        _ssh_command(ssh_target, f"mkdir -p -- {shlex.quote(remote_instruments_dir)}"),
        timeout=_SSH_COMMAND_TIMEOUT_SECONDS,
        error_prefix=f"failed to create remote instruments dir {ssh_target}:{remote_instruments_dir}",
    )

    _run_checked(
        _wsl_bash_command(
            f"scp {_SCP_OPTIONS} "
            f"{shlex.quote(local_stock_pool_path)} "
            f"{shlex.quote(f'{ssh_target}:{remote_instruments_dir}/')}"
        ),
        timeout=_SCP_TRANSFER_TIMEOUT_SECONDS,
        error_prefix=f"failed to sync stock_pool {local_stock_pool_path} -> {ssh_target}:{remote_instruments_dir}/",
    )
    remote_result = _run_checked(
        _ssh_command(ssh_target, f"sha256sum {shlex.quote(remote_path)}"),
        timeout=_SSH_COMMAND_TIMEOUT_SECONDS,
        error_prefix=f"failed to checksum remote stock_pool {ssh_target}:{remote_path}",
    )
    remote_checksum = _extract_sha256(
        remote_result.stdout,
        context=f"remote stock_pool {ssh_target}:{remote_path}",
    )
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

    remote_qlib_data = _node_path(node, "qlib_data_path")
    if not remote_qlib_data:
        raise RuntimeError(f"node {node_id} missing qlib_data_path; cannot sync filtered pools")

    ssh_user = _resolve_ssh_user(node, purpose="sync filtered pools")
    ssh_target = f"{ssh_user}@{host}"
    remote_instruments_dir = f"{remote_qlib_data.rstrip('/')}/instruments"
    local_qlib_data = os.getenv("QLIB_DATA_PATH_WSL", "").strip().rstrip("/")
    if not local_qlib_data:
        raise RuntimeError("QLIB_DATA_PATH_WSL is required to sync filtered pools")
    local_instruments_dir = f"{local_qlib_data}/instruments"

    _run_checked(
        _ssh_command(ssh_target, f"mkdir -p -- {shlex.quote(remote_instruments_dir)}"),
        timeout=_SSH_COMMAND_TIMEOUT_SECONDS,
        error_prefix=f"failed to create remote instruments dir {ssh_target}:{remote_instruments_dir}",
    )
    _run_checked(
        _wsl_bash_command(
            f"scp {_SCP_OPTIONS} "
            f"{shlex.quote(local_instruments_dir)}/filtered_pool_*.txt "
            f"{shlex.quote(f'{ssh_target}:{remote_instruments_dir}/')}"
        ),
        timeout=_SCP_TRANSFER_TIMEOUT_SECONDS,
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

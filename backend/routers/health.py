import re
import subprocess
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from ..deps import get_app_settings


router = APIRouter(tags=["health"])

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _capture_runtime_identity(repo_root: Path) -> dict[str, Any]:
    """Freeze the clean checkout identity observed when this process imports the router."""

    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        merge_commit = completed.stdout.strip().lower()
        if not _GIT_SHA_RE.fullmatch(merge_commit):
            raise ValueError("git HEAD is not a canonical commit SHA")
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if status.stdout.strip():
            raise ValueError("tracked runtime checkout is dirty")
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        return {
            "status": "unavailable",
            "reason_code": "AISTOCK_RUNTIME_IDENTITY_UNAVAILABLE",
            "message": str(exc),
        }
    return {"status": "ready", "merge_commit": merge_commit}


_PROCESS_RUNTIME_IDENTITY = _capture_runtime_identity(_REPO_ROOT)


def _require_process_runtime_identity() -> dict[str, Any]:
    if _PROCESS_RUNTIME_IDENTITY.get("status") != "ready":
        raise HTTPException(status_code=503, detail=dict(_PROCESS_RUNTIME_IDENTITY))
    return dict(_PROCESS_RUNTIME_IDENTITY)


@router.get("/health", summary="健康检查")
def health_check():
    """简单健康检查端点。

    返回应用名称与状态，便于前端和部署脚本探活。
    """

    settings = get_app_settings()
    return {"status": "ok", "app": settings.app_name}


@router.get("/runtime-identity", summary="Backend process runtime identity")
def runtime_identity():
    """Return the Git identity frozen when this backend process imported the router."""

    return _require_process_runtime_identity()

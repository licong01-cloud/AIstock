import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from ..deps import get_app_settings


router = APIRouter(tags=["health"])

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _run_git_text(repo_root: Path, *arguments: str) -> str:
    """Run git without PIPE reader threads while the router import lock is held."""

    command = ["git", *arguments]
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="") as output:
        subprocess.run(
            command,
            cwd=repo_root,
            check=True,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5,
        )
        output.seek(0)
        return output.read()


def _capture_runtime_identity(repo_root: Path) -> dict[str, Any]:
    """Freeze the clean checkout identity observed when this process imports the router."""

    try:
        merge_commit = _run_git_text(repo_root, "rev-parse", "HEAD").strip().lower()
        if not _GIT_SHA_RE.fullmatch(merge_commit):
            raise ValueError("git HEAD is not a canonical commit SHA")
        status = _run_git_text(repo_root, "status", "--porcelain", "--untracked-files=no")
        if status.strip():
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


@router.get("/runtime-contracts/hmm-risk-c010-a5", summary="HMM C-010-A5 runtime contract smoke")
def hmm_risk_c010_a5_runtime_contract():
    """Verify that the running process can load the exact C-010-A5 contract implementation."""

    identity = _require_process_runtime_identity()
    try:
        from backend.services.hmm_risk.stock_fact_observation import (
            C010_ELIGIBILITY_RECEIPT_VERSION,
            C010_EXPECTED_OPPORTUNITY_CONTRACT,
            C010_POLICY_VERSION,
            C010_PROVIDER_ABSENCE_PARTITION_VERSION,
        )
    except (ImportError, RuntimeError) as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unavailable",
                "reason_code": "HMM_RISK_C010_A5_RUNTIME_IMPORT_FAILED",
                "message": str(exc),
            },
        ) from exc
    expected = {
        "policy_version": "hmm_risk_c010_feature_domain_policy_v2",
        "eligibility_receipt_version": "hmm_risk_c010_train_observation_eligibility_v2",
        "expected_opportunity_contract": "hmm_risk_c010_expected_opportunity_dates_v2",
        "provider_absence_partition_version": "hmm_risk_c010_provider_absence_domain_partition_v1",
    }
    observed = {
        "policy_version": C010_POLICY_VERSION,
        "eligibility_receipt_version": C010_ELIGIBILITY_RECEIPT_VERSION,
        "expected_opportunity_contract": C010_EXPECTED_OPPORTUNITY_CONTRACT,
        "provider_absence_partition_version": C010_PROVIDER_ABSENCE_PARTITION_VERSION,
    }
    if observed != expected:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "invalid",
                "reason_code": "HMM_RISK_C010_A5_RUNTIME_CONTRACT_DRIFT",
                "expected": expected,
                "observed": observed,
            },
        )
    return {
        "status": "ok",
        "contract": "hmm_risk_c010_a5_runtime_contract_v1",
        "merge_commit": identity["merge_commit"],
        **observed,
    }

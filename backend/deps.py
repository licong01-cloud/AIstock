from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import Header, HTTPException

from .config import Settings, get_settings


def get_app_settings() -> Settings:
    """FastAPI dependency for the global settings object."""

    return get_settings()


DATASET_RELEASE_TOKEN_FILE_ENV = "DATASET_RELEASE_OPERATOR_TOKEN_FILE"
DATASET_RELEASE_TOKEN_HEADER = "X-Dataset-Release-Operator-Token"


@dataclass(frozen=True)
class DatasetReleasePrincipal:
    principal_id: str
    token_file_id: str
    cursor_signing_key: bytes = field(repr=False, compare=False)


def _is_reparse_or_symlink(path: Path) -> bool:
    stat = path.lstat()
    return path.is_symlink() or bool(getattr(stat, "st_file_attributes", 0) & 0x400)


def _dataset_release_token_file() -> Path:
    configured = os.getenv(DATASET_RELEASE_TOKEN_FILE_ENV, "").strip()
    if not configured:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_NOT_CONFIGURED",
                "message": "Dataset release operator token file is not configured.",
                "retryable": False,
                "context_ref": None,
            },
        )
    path = Path(configured).expanduser()
    if not path.is_absolute():
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_PATH_INVALID",
                "message": "Dataset release operator token file must be absolute.",
                "retryable": False,
                "context_ref": None,
            },
        )
    try:
        current = path
        while True:
            if current.exists() and _is_reparse_or_symlink(current):
                raise ValueError("reparse point")
            if current.parent == current:
                break
            current = current.parent
        resolved = path.resolve(strict=True)
        if not resolved.is_file() or _is_reparse_or_symlink(resolved):
            raise ValueError("not a plain file")
    except (OSError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_PATH_INVALID",
                "message": "Dataset release operator token file failed path validation.",
                "retryable": False,
                "context_ref": None,
            },
        ) from exc
    return resolved


def _read_dataset_release_token(path: Path) -> str:
    try:
        with path.open("rb") as stream:
            raw = stream.read(4097)
    except OSError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_READ_FAILED",
                "message": "Dataset release operator token file cannot be read.",
                "retryable": True,
                "context_ref": None,
            },
        ) from exc
    if not raw or len(raw) > 4096 or b"\x00" in raw:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_TOKEN_INVALID",
                "message": "Dataset release operator token file is empty or invalid.",
                "retryable": False,
                "context_ref": None,
            },
        )
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_TOKEN_INVALID",
                "message": "Dataset release operator token file must be UTF-8.",
                "retryable": False,
                "context_ref": None,
            },
        ) from exc
    if len(token) < 32 or len(token) > 4096:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "DATASET_RELEASE_AUTH_TOKEN_INVALID",
                "message": "Dataset release operator token file is empty or invalid.",
                "retryable": False,
                "context_ref": None,
            },
        )
    return token


def require_dataset_release_operator(
    x_dataset_release_operator_token: str | None = Header(
        None,
        alias=DATASET_RELEASE_TOKEN_HEADER,
    ),
) -> DatasetReleasePrincipal:
    if not x_dataset_release_operator_token or len(x_dataset_release_operator_token) > 4096:
        raise HTTPException(
            status_code=401,
            detail={
                "error_code": "DATASET_RELEASE_OPERATOR_UNAUTHORIZED",
                "message": f"Dataset release control requires {DATASET_RELEASE_TOKEN_HEADER}.",
                "retryable": False,
                "context_ref": None,
            },
        )
    path = _dataset_release_token_file()
    expected = _read_dataset_release_token(path)
    if not hmac.compare_digest(x_dataset_release_operator_token, expected):
        raise HTTPException(
            status_code=401,
            detail={
                "error_code": "DATASET_RELEASE_OPERATOR_UNAUTHORIZED",
                "message": f"Dataset release control requires {DATASET_RELEASE_TOKEN_HEADER}.",
                "retryable": False,
                "context_ref": None,
            },
        )
    file_id = hashlib.sha256(str(path).casefold().encode("utf-8")).hexdigest()[:16]
    return DatasetReleasePrincipal(
        # Actor identity follows the allowlisted credential file, not the
        # credential bytes.  Rotating the token therefore cannot expose a
        # low-entropy token-derived digest through durable rows or responses.
        principal_id=f"dataset-operator:{file_id}",
        token_file_id=file_id,
        cursor_signing_key=hashlib.sha256(b"dataset-release-cursor-v1\x00" + expected.encode("utf-8")).digest(),
    )

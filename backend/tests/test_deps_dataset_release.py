from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.deps import (
    DATASET_RELEASE_TOKEN_FILE_ENV,
    DatasetReleasePrincipal,
    require_dataset_release_operator,
)


def test_dataset_release_auth_fails_closed_without_token_file(monkeypatch) -> None:
    monkeypatch.delenv(DATASET_RELEASE_TOKEN_FILE_ENV, raising=False)
    with pytest.raises(HTTPException) as error:
        require_dataset_release_operator("secret")
    assert error.value.status_code == 503
    assert error.value.detail["error_code"] == "DATASET_RELEASE_AUTH_NOT_CONFIGURED"
    assert error.value.detail["retryable"] is False


def test_dataset_release_auth_missing_header_is_401_before_runtime_config(monkeypatch) -> None:
    monkeypatch.delenv(DATASET_RELEASE_TOKEN_FILE_ENV, raising=False)
    with pytest.raises(HTTPException) as error:
        require_dataset_release_operator(None)
    assert error.value.status_code == 401
    assert error.value.detail["error_code"] == "DATASET_RELEASE_OPERATOR_UNAUTHORIZED"


def test_dataset_release_auth_uses_token_file_and_constant_identity(
    monkeypatch,
    tmp_path: Path,
) -> None:
    token_path = tmp_path / "operator.token"
    credential_value = "first-value-" * 3
    token_path.write_text(credential_value + "\n", encoding="utf-8")
    monkeypatch.setenv(DATASET_RELEASE_TOKEN_FILE_ENV, str(token_path))
    principal = require_dataset_release_operator(credential_value)
    assert isinstance(principal, DatasetReleasePrincipal)
    assert principal.principal_id.startswith("dataset-operator:")
    assert credential_value not in principal.principal_id
    with pytest.raises(HTTPException) as error:
        require_dataset_release_operator("wrong")
    assert error.value.status_code == 401
    assert error.value.detail["error_code"] == "DATASET_RELEASE_OPERATOR_UNAUTHORIZED"


def test_dataset_release_token_rotation_is_read_on_each_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    token_path = tmp_path / "operator.token"
    first_value = "first-value-" * 3
    second_value = "second-value-" * 3
    token_path.write_text(first_value, encoding="utf-8")
    monkeypatch.setenv(DATASET_RELEASE_TOKEN_FILE_ENV, str(token_path))
    first = require_dataset_release_operator(first_value)
    token_path.write_text(second_value, encoding="utf-8")
    second = require_dataset_release_operator(second_value)
    assert first.principal_id == second.principal_id
    assert first.cursor_signing_key != second.cursor_signing_key
    with pytest.raises(HTTPException):
        require_dataset_release_operator(first_value)


def test_dataset_release_auth_rejects_low_entropy_token_file(monkeypatch, tmp_path: Path) -> None:
    token_path = tmp_path / "operator.token"
    token_path.write_text("short-token", encoding="utf-8")
    monkeypatch.setenv(DATASET_RELEASE_TOKEN_FILE_ENV, str(token_path))
    with pytest.raises(HTTPException) as error:
        require_dataset_release_operator("short-token")
    assert error.value.status_code == 503
    assert error.value.detail["error_code"] == "DATASET_RELEASE_AUTH_TOKEN_INVALID"


def test_dataset_release_auth_rejects_relative_token_file(monkeypatch) -> None:
    monkeypatch.setenv(DATASET_RELEASE_TOKEN_FILE_ENV, "relative-token.txt")
    with pytest.raises(HTTPException) as error:
        require_dataset_release_operator("secret")
    assert error.value.detail["error_code"] == "DATASET_RELEASE_AUTH_PATH_INVALID"

"""Fixed normal-loop registration adapter for F-014 Phase 2."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Mapping

DESCRIPTOR_SCHEMA = "qe_long_trend_postprocess_descriptor_v1"
REGISTRATION_SCHEMA = "qe_long_trend_registration_v1"
PENDING_SCHEMA = "qe_long_trend_registration_pending_v1"
PENDING_INDEX_SCHEMA = "qe_long_trend_registration_pending_index_v1"


def main() -> int:
    descriptor_path = Path("qe_long_trend_postprocess_descriptor.json").resolve()
    descriptor: dict[str, Any] = {}
    try:
        descriptor = _read_json(descriptor_path)
        if descriptor.get("schema_version") != DESCRIPTOR_SCHEMA:
            raise RuntimeError("QELT_CONTROL_STATE_CONFLICT: invalid frozen postprocess descriptor")
        recorder = _read_json(Path("qe_current_recorder.json").resolve())
        experiment_id = _safe_component(recorder.get("experiment_id"), "experiment_id")
        recorder_id = _safe_component(recorder.get("recorder_id"), "recorder_id")
        catalog = _build_registration_catalog(
            Path.cwd().resolve(),
            experiment_id=experiment_id,
            recorder_id=recorder_id,
            backtest_freq=_safe_component(descriptor.get("backtest_freq"), "backtest_freq"),
        )
        resource_secret = _read_json(Path("qe_resource_session_secret.json").resolve())
        token = str(resource_secret.get("token") or "")
        if not token:
            raise RuntimeError("QELT_CONTROL_STATE_CONFLICT: original Loop resource token is missing")
        payload = {
            "schema_version": "qe_long_trend_normal_registration_request_v1",
            "task_id": descriptor["task_id"],
            "loop_index": descriptor["loop_index"],
            "node_id": descriptor["node_id"],
            "run_id": descriptor.get("run_id"),
            "long_trend_evaluation": descriptor["long_trend_evaluation"],
            "frozen_identity": descriptor["frozen_identity"],
            "label_horizon": descriptor.get("label_horizon"),
            "strategy_topk": descriptor.get("strategy_topk"),
            "recorder_ref": {"experiment_id": experiment_id, "recorder_id": recorder_id},
            "registration_catalog": catalog,
            "parent_resource_session_id": resource_secret.get("session_id"),
            "parent_resource_source_run_key": resource_secret.get("source_run_key"),
        }
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        request = urllib.request.Request(
            _registration_url(),
            data=body,
            headers={"Content-Type": "application/json", "X-QE-Resource-Token": token},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - configured AIstock internal endpoint.
                response_body = response.read()
                status = int(response.status)
        except urllib.error.HTTPError as exc:
            response_body = exc.read()
            status = int(exc.code)
        if status < 200 or status >= 300:
            raise RuntimeError(
                f"QELT_REGISTRATION_REJECTED: http_status={status} body={response_body[:2000].decode('utf-8', errors='replace')}"
            )
        response_payload = json.loads(response_body.decode("utf-8"))
        receipt = response_payload.get("data") if isinstance(response_payload, Mapping) else None
        if not isinstance(receipt, Mapping) or receipt.get("schema_version") != REGISTRATION_SCHEMA:
            raise RuntimeError("QELT_NODE_JOB_IDENTITY_CONFLICT: AIstock registration receipt is malformed")
        _atomic_json(Path("qe_long_trend_registration.json"), dict(receipt))
        pending_index_cleared = False
        try:
            _clear_pending_index(descriptor)
            pending_index_cleared = True
        except Exception as cleanup_exc:
            print(
                "[WARNING] reason_code=QELT_REGISTRATION_PENDING_INDEX_CLEANUP_FAILED "
                f"error_type={type(cleanup_exc).__name__} message={cleanup_exc}"
            )
        # Retain the hashed pending receipt while its index still exists.  The
        # lifecycle-owned replayer can rerun this exact adapter and finish
        # cleanup instead of being stranded with an unverifiable index.
        if pending_index_cleared:
            try:
                Path("postprocess_registration_pending.json").unlink(missing_ok=True)
            except OSError as cleanup_exc:
                print(
                    "[WARNING] reason_code=QELT_REGISTRATION_PENDING_RECEIPT_CLEANUP_FAILED "
                    f"error_type={type(cleanup_exc).__name__} message={cleanup_exc}"
                )
        print(
            "[INFO] reason_code=QELT_REGISTRATION_ACCEPTED "
            f"evaluation_id={receipt.get('evaluation_id')} task_status={receipt.get('task_status')}"
        )
    except Exception as exc:
        pending = {
            "schema_version": PENDING_SCHEMA,
            "receipt_stage": "registration_pending",
            "status": "pending",
            "reason_code": _reason_code(exc),
            "reason_json": {"error_type": type(exc).__name__, "message": str(exc)},
            "descriptor_sha256": _sha256_file_or_null(descriptor_path),
        }
        _atomic_json(Path("postprocess_registration_pending.json"), pending)
        try:
            _write_pending_index(descriptor, descriptor_path=descriptor_path, pending=pending)
        except Exception as index_exc:
            pending["pending_index_error"] = {
                "error_type": type(index_exc).__name__,
                "message": str(index_exc),
            }
            _atomic_json(Path("postprocess_registration_pending.json"), pending)
        print(
            "[WARNING] reason_code=" + pending["reason_code"]
            + " long-trend registration is pending; qrun/read_exp_res status is unchanged"
        )
    return 0


def _build_registration_catalog(root: Path, *, experiment_id: str, recorder_id: str, backtest_freq: str) -> dict[str, Any]:
    prefix = Path("mlruns") / experiment_id / recorder_id / "artifacts"
    candidates = [
        prefix / "pred.pkl",
        prefix / "label.pkl",
        prefix / "sig_analysis" / "label.pkl",
        prefix / "params.pkl",
        prefix / "params_pkl",
        prefix / "portfolio_analysis" / "report_normal_1day.pkl",
        prefix / "portfolio_analysis" / "positions_normal_1day.pkl",
        prefix / "portfolio_analysis" / f"indicators_normal_{backtest_freq}.pkl",
        prefix / "portfolio_analysis" / f"indicators_normal_{backtest_freq}_obj.pkl",
    ]
    rows = []
    for relative in candidates:
        target = (root / relative).resolve()
        target.relative_to(root)
        if not target.is_file() or target.is_symlink():
            continue
        digest, size = _sha256_file(target)
        rows.append(
            {
                "relative_path": relative.as_posix(),
                "sha256": digest,
                "size_bytes": size,
                "parser_contract": None,
            }
        )
    return {
        "schema_version": "qe_long_trend_registration_catalog_v1",
        "catalog_completeness": "complete",
        "files": rows,
    }


def _registration_url() -> str:
    base = next(
        (
            str(os.getenv(name) or "").strip()
            for name in ("AISTOCK_QE_CALLBACK_BASE_URL", "AISTOCK_BACKEND_CALLBACK_BASE_URL", "AISTOCK_BACKEND_BASE_URL")
            if str(os.getenv(name) or "").strip()
        ),
        "",
    )
    if not base.startswith(("http://", "https://")):
        raise RuntimeError("QELT_CONTROL_STATE_CONFLICT: no explicit AIstock callback base URL")
    path = "/api/v1/quantevolver/evolution/internal/long-trend-postprocess-registrations"
    base = base.rstrip("/")
    return base + (path.removeprefix("/api/v1") if base.endswith("/api/v1") else path)


def _pending_index_path(descriptor: Mapping[str, Any]) -> Path:
    workspace_text = str(os.getenv("QE_WORKSPACE_WSL") or "").strip()
    if not workspace_text:
        raise RuntimeError("QELT_CONTROL_STATE_CONFLICT: QE_WORKSPACE_WSL is unavailable for pending recovery")
    workspace = Path(workspace_text).resolve(strict=True)
    loop_root = Path.cwd().resolve(strict=True)
    loop_root.relative_to(workspace)
    task_id = _safe_component(descriptor.get("task_id"), "task_id")
    loop_index = int(descriptor.get("loop_index") or 0)
    if loop_index < 1:
        raise RuntimeError("QELT_CONTROL_STATE_CONFLICT: loop_index is invalid for pending recovery")
    return workspace / ".qe_long_trend_registration_pending" / f"{task_id}__Loop{loop_index}.json"


def _write_pending_index(
    descriptor: Mapping[str, Any],
    *,
    descriptor_path: Path,
    pending: Mapping[str, Any],
) -> None:
    index_path = _pending_index_path(descriptor)
    workspace = Path(str(os.environ["QE_WORKSPACE_WSL"])).resolve(strict=True)
    loop_root = Path.cwd().resolve(strict=True)
    _atomic_json(
        index_path,
        {
            "schema_version": PENDING_INDEX_SCHEMA,
            "task_id": descriptor["task_id"],
            "loop_id": f"Loop{int(descriptor['loop_index'])}",
            "loop_relative_path": loop_root.relative_to(workspace).as_posix(),
            "descriptor_sha256": _sha256_file_or_null(descriptor_path),
            "adapter_sha256": _sha256_file_or_null(Path(__file__).resolve()),
            "pending_receipt_sha256": hashlib.sha256(
                json.dumps(
                    dict(pending),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest(),
        },
    )


def _clear_pending_index(descriptor: Mapping[str, Any]) -> None:
    try:
        _pending_index_path(descriptor).unlink(missing_ok=True)
    except FileNotFoundError:
        return


def _safe_component(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or "/" in text or "\\" in text or text in {".", ".."}:
        raise RuntimeError(f"QELT_RECORDER_REF_MISSING: invalid {field_name}")
    return text


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON must be an object: {path.name}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_file_or_null(path: Path) -> str | None:
    return _sha256_file(path)[0] if path.is_file() else None


def _reason_code(exc: Exception) -> str:
    text = str(exc)
    for code in (
        "QELT_CONTROL_STATE_CONFLICT",
        "QELT_RECORDER_REF_MISSING",
        "QELT_REGISTRATION_REJECTED",
        "QELT_NODE_JOB_IDENTITY_CONFLICT",
    ):
        if code in text:
            return code
    return "QELT_REGISTRATION_TRANSPORT_FAILED"


if __name__ == "__main__":
    raise SystemExit(main())

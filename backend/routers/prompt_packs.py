from __future__ import annotations

from typing import Any, Dict, List, Optional

from pathlib import Path
from datetime import datetime, timezone
import hashlib
import difflib
import os

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from psycopg2.extras import Json

import yaml

from ..db.pg_pool import get_conn
from ..schema_registry.prompt_pack_allowlist import REQUIRED_PROMPT_FILES


router = APIRouter(prefix="/prompt-packs", tags=["prompt-packs"])


class ValidateImportDirRequest(BaseModel):
    dir: str


class ImportFromDirRequest(BaseModel):
    dir: str
    overwrite: bool = False


class PublishRequest(BaseModel):
    message: str


class SetActiveRequest(BaseModel):
    message: str


def _resolve_pack_dir(dir_value: str) -> Path:
    raw = (dir_value or "").strip()
    if not raw:
        raise ValueError("dir is empty")

    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate

    inbox_root = os.getenv("PROMPT_PACK_INBOX_DIR")
    if inbox_root and inbox_root.strip():
        return Path(inbox_root.strip()) / raw

    return Path("F:/Dev/AIstock/prompt_packs_inbox") / raw


def _raise_error(status_code: int, code: str, message: str, details: Any | None = None) -> None:
    payload: Dict[str, Any] = {"error": {"code": code, "message": message}}
    if details is not None:
        payload["error"]["details"] = details
    raise HTTPException(status_code=status_code, detail=payload)


def _parse_meta_created_at(value: Any) -> datetime:
    if not value:
        raise ValueError("meta.yaml created_at is empty")
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _scan_pack_dir(pack_dir: Path) -> Dict[str, Any]:
    meta_path = pack_dir / "meta.yaml"
    files_root = pack_dir / "files"

    meta_ok = False
    meta: Dict[str, Any] | None = None
    meta_error: str | None = None
    meta_created_at: datetime | None = None

    if not meta_path.exists() or not meta_path.is_file():
        meta_error = "meta.yaml not found"
    else:
        try:
            meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
            if not isinstance(meta, dict):
                raise ValueError("meta.yaml must be a mapping")
            if not (
                meta.get("id")
                and meta.get("created_at")
                and meta.get("description")
                and meta.get("source")
                and meta.get("usage_scene")
                and meta.get("requirements")
                and meta.get("limitations")
            ):
                raise ValueError(
                    "meta.yaml missing required fields: id/created_at/description/source/usage_scene/requirements/limitations"
                )

            if "tags" in meta and meta.get("tags") is not None and not isinstance(meta.get("tags"), (list, dict)):
                raise ValueError("meta.yaml tags must be list/dict/null")
            if not isinstance(meta.get("source"), str):
                raise ValueError("meta.yaml source must be string")

            meta_created_at = _parse_meta_created_at(meta.get("created_at"))

            meta_ok = True
        except Exception as exc:  # noqa: BLE001
            meta_error = str(exc)

    rel_paths: List[str] = []
    if files_root.exists() and files_root.is_dir():
        for p in files_root.rglob("*"):
            if p.is_file():
                rel_paths.append(p.relative_to(files_root).as_posix())

    required_set = set(REQUIRED_PROMPT_FILES)
    present_set = set(rel_paths)
    missing_files = sorted(required_set - present_set)
    extra_files = sorted(present_set - required_set)

    yaml_parse_errors: List[Dict[str, str]] = []
    for rp in rel_paths:
        if not (rp.endswith(".yaml") or rp.endswith(".yml")):
            continue
        try:
            content = (files_root / Path(rp)).read_text(encoding="utf-8")
            yaml.safe_load(content)
        except Exception as exc:  # noqa: BLE001
            yaml_parse_errors.append({"rel_path": rp, "error": str(exc)})

    pack_id: Optional[str] = None
    if isinstance(meta, dict):
        pack_id = str(meta.get("id") or "").strip() or None

    meta_summary: Dict[str, Any] | None = None
    if isinstance(meta, dict):
        meta_summary = {
            "id": pack_id,
            "created_at": meta_created_at,
            "description": meta.get("description"),
            "usage_scene": meta.get("usage_scene"),
            "requirements": meta.get("requirements"),
            "limitations": meta.get("limitations"),
            "tags": meta.get("tags"),
            "source": meta.get("source"),
            "base_pack_id": meta.get("base_pack_id"),
        }

    ok = bool(meta_ok and not yaml_parse_errors and not missing_files)

    return {
        "ok": ok,
        "pack_id": pack_id,
        "meta_ok": meta_ok,
        "meta_error": meta_error,
        "meta": meta_summary,
        "required_files_count": len(required_set),
        "files_count": len(rel_paths),
        "missing_files": missing_files,
        "extra_files": extra_files,
        "yaml_parse_errors": yaml_parse_errors,
    }


def _compute_pack_checksum(files: Dict[str, str]) -> str:
    h = hashlib.sha256()
    for k in sorted(files.keys()):
        h.update(k.encode("utf-8"))
        h.update(b"\0")
        h.update(files[k].encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def _load_pack_files_from_db(pack_id: str) -> Dict[str, str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rel_path, content
                FROM app.prompt_pack_file
                WHERE pack_id = %s
                """,
                (pack_id,),
            )
            rows = cur.fetchall()

    return {r[0]: r[1] for r in rows}


def _validate_pack_for_publish(pack_id: str) -> Dict[str, Any]:
    files = _load_pack_files_from_db(pack_id)
    present_set = set(files.keys())
    required_set = set(REQUIRED_PROMPT_FILES)
    missing_files = sorted(required_set - present_set)
    extra_files = sorted(present_set - required_set)

    yaml_parse_errors: List[Dict[str, str]] = []
    for rp, content in files.items():
        if not (rp.endswith(".yaml") or rp.endswith(".yml")):
            continue
        try:
            yaml.safe_load(content)
        except Exception as exc:  # noqa: BLE001
            yaml_parse_errors.append({"rel_path": rp, "error": str(exc)})

    checks: List[Dict[str, Any]] = []
    checks.append(
        {
            "name": "allowlist",
            "ok": len(missing_files) == 0,
            "severity": "error" if len(missing_files) else "warn",
            "message": "allowlist coverage",
            "details": {
                "required_files_count": len(required_set),
                "present_files_count": len(present_set),
                "missing_files": missing_files,
                "extra_files": extra_files,
            },
        }
    )
    checks.append(
        {
            "name": "yaml_parse",
            "ok": len(yaml_parse_errors) == 0,
            "severity": "error" if len(yaml_parse_errors) else "warn",
            "message": "yaml parse",
            "details": {"errors": yaml_parse_errors},
        }
    )

    ok = all(c["ok"] or c["severity"] != "error" for c in checks)
    report = {
        "pack_id": pack_id,
        "status_at_validate": None,
        "ok": ok,
        "checks": checks,
    }
    return {"ok": ok, "report": report, "missing_files": missing_files, "yaml_parse_errors": yaml_parse_errors}


@router.get("/active", summary="获取当前全局 Active Pack")
def get_active_pack() -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT active_pack_id, updated_at, updated_by
                FROM app.prompt_global_active
                WHERE id = 1
                """
            )
            row = cur.fetchone()

    if not row:
        return {"active_pack_id": None, "updated_at": None, "updated_by": None}

    return {"active_pack_id": row[0], "updated_at": row[1], "updated_by": row[2]}


@router.get("", summary="列出 Prompt Packs")
def list_prompt_packs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: str | None = Query(None),
    query: str | None = Query(None),
) -> Dict[str, Any]:
    offset = (page - 1) * page_size

    where_parts: List[str] = []
    params: List[Any] = []

    if status:
        where_parts.append("status = %s")
        params.append(status)

    if query:
        where_parts.append("(id ILIKE %s OR description ILIKE %s)")
        params.extend([f"%{query}%", f"%{query}%"])

    where_sql = ""
    if where_parts:
        where_sql = "WHERE " + " AND ".join(where_parts)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM app.prompt_pack {where_sql}", params)
            total = int(cur.fetchone()[0])

            cur.execute(
                f"""
                SELECT id, description, source, status, base_pack_id, created_at, updated_at
                FROM app.prompt_pack
                {where_sql}
                ORDER BY updated_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [page_size, offset],
            )
            rows = cur.fetchall()

    items = [
        {
            "id": r[0],
            "description": r[1],
            "source": r[2],
            "status": r[3],
            "base_pack_id": r[4],
            "created_at": r[5],
            "updated_at": r[6],
        }
        for r in rows
    ]

    return {"page": page, "page_size": page_size, "total": total, "items": items}


@router.get("/{pack_id}", summary="获取 Pack 详情")
def get_prompt_pack(pack_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, description, usage_scene, requirements, limitations,
                       tags, source, status, created_by, base_pack_id, checksum,
                       created_at, updated_at
                FROM app.prompt_pack
                WHERE id = %s
                """,
                (pack_id,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"prompt_pack not found: {pack_id}")

    return {
        "id": row[0],
        "description": row[1],
        "usage_scene": row[2],
        "requirements": row[3],
        "limitations": row[4],
        "tags": row[5],
        "source": row[6],
        "status": row[7],
        "created_by": row[8],
        "base_pack_id": row[9],
        "checksum": row[10],
        "created_at": row[11],
        "updated_at": row[12],
    }


@router.get("/diff", summary="对比两个 Pack（文件级 + 行级）")
def diff_prompt_packs(
    from_pack: str = Query(..., alias="from"),
    to_pack: str = Query(..., alias="to"),
) -> Dict[str, Any]:
    a_files = _load_pack_files_from_db(from_pack)
    b_files = _load_pack_files_from_db(to_pack)
    if not a_files:
        _raise_error(404, "PACK_NOT_FOUND", "pack not found or has no files", {"pack_id": from_pack})
    if not b_files:
        _raise_error(404, "PACK_NOT_FOUND", "pack not found or has no files", {"pack_id": to_pack})

    a_keys = set(a_files.keys())
    b_keys = set(b_files.keys())
    all_paths = sorted(a_keys | b_keys)

    file_summaries: List[Dict[str, Any]] = []
    diffs: Dict[str, str] = {}
    for rp in all_paths:
        a = a_files.get(rp)
        b = b_files.get(rp)
        if a is None:
            file_summaries.append({"rel_path": rp, "change": "added"})
            diffs[rp] = "".join(
                difflib.unified_diff([], (b or "").splitlines(keepends=True), fromfile="/dev/null", tofile=rp)
            )
            continue
        if b is None:
            file_summaries.append({"rel_path": rp, "change": "removed"})
            diffs[rp] = "".join(
                difflib.unified_diff((a or "").splitlines(keepends=True), [], fromfile=rp, tofile="/dev/null")
            )
            continue
        if a == b:
            file_summaries.append({"rel_path": rp, "change": "unchanged"})
            continue

        file_summaries.append({"rel_path": rp, "change": "modified"})
        diffs[rp] = "".join(
            difflib.unified_diff(
                (a or "").splitlines(keepends=True),
                (b or "").splitlines(keepends=True),
                fromfile=rp,
                tofile=rp,
            )
        )

    return {
        "from": from_pack,
        "to": to_pack,
        "summary": {
            "added": sum(1 for x in file_summaries if x["change"] == "added"),
            "removed": sum(1 for x in file_summaries if x["change"] == "removed"),
            "modified": sum(1 for x in file_summaries if x["change"] == "modified"),
            "unchanged": sum(1 for x in file_summaries if x["change"] == "unchanged"),
        },
        "files": file_summaries,
        "diffs": diffs,
    }


@router.post("/validate-import-dir", summary="预校验导入目录（不入库）")
def validate_import_dir(req: ValidateImportDirRequest) -> Dict[str, Any]:
    try:
        pack_dir = _resolve_pack_dir(req.dir)
    except Exception as exc:  # noqa: BLE001
        _raise_error(400, "INVALID_DIR", "invalid dir", {"dir": req.dir, "error": str(exc)})
    if not pack_dir.exists() or not pack_dir.is_dir():
        _raise_error(
            400,
            "DIR_NOT_FOUND",
            "dir not found",
            {"dir": req.dir, "resolved_dir": pack_dir.as_posix()},
        )
    return _scan_pack_dir(pack_dir)


@router.post("/import-from-dir", summary="从目录导入 Prompt Pack（落库为 draft）")
def import_from_dir(req: ImportFromDirRequest) -> Dict[str, Any]:
    try:
        pack_dir = _resolve_pack_dir(req.dir)
    except Exception as exc:  # noqa: BLE001
        _raise_error(400, "INVALID_DIR", "invalid dir", {"dir": req.dir, "error": str(exc)})
    if not pack_dir.exists() or not pack_dir.is_dir():
        _raise_error(
            400,
            "DIR_NOT_FOUND",
            "dir not found",
            {"dir": req.dir, "resolved_dir": pack_dir.as_posix()},
        )

    scan = _scan_pack_dir(pack_dir)
    if not scan.get("meta_ok"):
        _raise_error(
            400,
            "INVALID_META",
            "invalid meta.yaml",
            {"meta_error": scan.get("meta_error")},
        )
    if scan.get("missing_files"):
        _raise_error(
            400,
            "ALLOWLIST_MISSING",
            "missing required prompt files",
            {"missing_files": scan.get("missing_files"), "required_files_count": scan.get("required_files_count")},
        )
    if scan.get("yaml_parse_errors"):
        _raise_error(400, "YAML_PARSE_FAILED", "yaml parse failed", {"errors": scan["yaml_parse_errors"]})

    meta = yaml.safe_load((pack_dir / "meta.yaml").read_text(encoding="utf-8"))
    if not (
        meta.get("id")
        and meta.get("created_at")
        and meta.get("description")
        and meta.get("source")
        and meta.get("usage_scene")
        and meta.get("requirements")
        and meta.get("limitations")
    ):
        _raise_error(
            400,
            "INVALID_META",
            "meta.yaml missing required fields",
            {"required": ["id", "created_at", "description", "source", "usage_scene", "requirements", "limitations"]},
        )
    if "tags" in meta and meta.get("tags") is not None and not isinstance(meta.get("tags"), (list, dict)):
        _raise_error(400, "INVALID_META", "meta.yaml tags must be list/dict/null")
    if not isinstance(meta.get("source"), str):
        _raise_error(400, "INVALID_META", "meta.yaml source must be string")

    pack_id = str(meta.get("id") or "").strip()
    if not pack_id:
        _raise_error(400, "INVALID_META", "meta.yaml id is empty")

    try:
        meta_created_at = _parse_meta_created_at(meta.get("created_at"))
    except Exception as exc:  # noqa: BLE001
        _raise_error(400, "INVALID_META", "meta.yaml created_at invalid", {"error": str(exc)})

    files_root = pack_dir / "files"
    if not files_root.exists() or not files_root.is_dir():
        _raise_error(
            400,
            "FILES_ROOT_NOT_FOUND",
            "files/ not found",
            {"dir": req.dir, "resolved_dir": pack_dir.as_posix()},
        )

    rel_paths: List[str] = []
    for p in files_root.rglob("*"):
        if p.is_file():
            rel_paths.append(p.relative_to(files_root).as_posix())

    files: Dict[str, str] = {}
    for rp in rel_paths:
        files[rp] = (files_root / Path(rp)).read_text(encoding="utf-8")

    checksum = _compute_pack_checksum(files)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT active_pack_id
                FROM app.prompt_global_active
                WHERE id = 1
                """
            )
            row = cur.fetchone()
            active_pack_id = row[0] if row else None

            cur.execute("SELECT 1 FROM app.prompt_pack WHERE id = %s", (pack_id,))
            exists = cur.fetchone() is not None
            if exists and not req.overwrite:
                _raise_error(409, "PACK_ALREADY_EXISTS", "prompt_pack already exists", {"pack_id": pack_id})

            from_status: str | None = None
            if exists and req.overwrite:
                if active_pack_id and str(active_pack_id) == pack_id:
                    _raise_error(
                        409,
                        "ACTIVE_PACK_LOCKED",
                        "active pack cannot be overwritten",
                        {"pack_id": pack_id, "active_pack_id": active_pack_id},
                    )

                cur.execute("SELECT status FROM app.prompt_pack WHERE id = %s", (pack_id,))
                row = cur.fetchone()
                from_status = row[0] if row else None

                cur.execute(
                    """
                    UPDATE app.prompt_pack
                    SET description = %s,
                        usage_scene = %s,
                        requirements = %s,
                        limitations = %s,
                        tags = %s,
                        source = %s,
                        status = 'draft',
                        base_pack_id = %s,
                        checksum = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        meta.get("description"),
                        meta.get("usage_scene"),
                        meta.get("requirements"),
                        meta.get("limitations"),
                        Json(meta.get("tags")),
                        meta.get("source") or "manual",
                        meta.get("base_pack_id"),
                        checksum,
                        pack_id,
                    ),
                )

                cur.execute("DELETE FROM app.prompt_pack_file WHERE pack_id = %s", (pack_id,))

            if not exists:
                cur.execute(
                    """
                    INSERT INTO app.prompt_pack (
                        id, description, usage_scene, requirements, limitations,
                        tags, source, status, base_pack_id, checksum,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'draft', %s, %s, %s, %s)
                    """,
                    (
                        pack_id,
                        meta.get("description"),
                        meta.get("usage_scene"),
                        meta.get("requirements"),
                        meta.get("limitations"),
                        Json(meta.get("tags")),
                        meta.get("source") or "manual",
                        meta.get("base_pack_id"),
                        checksum,
                        meta_created_at,
                        meta_created_at,
                    ),
                )

            for rp, content in files.items():
                cur.execute(
                    """
                    INSERT INTO app.prompt_pack_file (pack_id, rel_path, content, content_hash)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        pack_id,
                        rp,
                        content,
                        hashlib.sha256(content.encode("utf-8")).hexdigest(),
                    ),
                )

            event_type = "overwrite_import" if (exists and req.overwrite) else "import"
            cur.execute(
                """
                INSERT INTO app.prompt_pack_event (pack_id, event_type, from_status, to_status, message, metadata)
                VALUES (%s, %s, %s, 'draft', %s, %s)
                """,
                (
                    pack_id,
                    event_type,
                    from_status,
                    f"import from dir: {req.dir}",
                    Json(
                        {
                            "dir": req.dir,
                            "files_count": len(files),
                            "extra_files": scan.get("extra_files"),
                            "checksum": checksum,
                        }
                    ),
                ),
            )

    return {
        "pack_id": pack_id,
        "status": "draft",
        "checksum": checksum,
        "files_count": len(files),
        "active_pack_id": active_pack_id,
        "meta": {
            "id": pack_id,
            "created_at": meta_created_at,
            "description": meta.get("description"),
            "usage_scene": meta.get("usage_scene"),
            "requirements": meta.get("requirements"),
            "limitations": meta.get("limitations"),
            "tags": meta.get("tags"),
            "source": meta.get("source"),
            "base_pack_id": meta.get("base_pack_id"),
        },
    }


@router.post("/{pack_id}/publish", summary="发布 Pack（通过校验后置为 published）")
def publish_pack(pack_id: str, req: PublishRequest) -> Dict[str, Any]:
    validate = _validate_pack_for_publish(pack_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                FROM app.prompt_pack
                WHERE id = %s
                """,
                (pack_id,),
            )
            row = cur.fetchone()
            if not row:
                _raise_error(404, "PACK_NOT_FOUND", "prompt_pack not found", {"pack_id": pack_id})
            from_status = row[0]

            report = validate["report"]
            report["status_at_validate"] = from_status
            ok = bool(validate["ok"])
            cur.execute(
                """
                INSERT INTO app.prompt_pack_validation_run (pack_id, actor, trigger, ok, report, summary)
                VALUES (%s, NULL, 'publish', %s, %s, %s)
                RETURNING id
                """,
                (
                    pack_id,
                    ok,
                    Json(report),
                    "ok" if ok else "failed",
                ),
            )
            validation_run_id = int(cur.fetchone()[0])

            if not ok:
                _raise_error(
                    400,
                    "PUBLISH_GATE_FAILED",
                    "publish gate failed",
                    {"validation_run_id": validation_run_id, "report": report},
                )

            cur.execute(
                """
                UPDATE app.prompt_pack
                SET status = 'published', updated_at = NOW()
                WHERE id = %s
                """,
                (pack_id,),
            )

            cur.execute(
                """
                INSERT INTO app.prompt_pack_event (pack_id, event_type, from_status, to_status, message, metadata)
                VALUES (%s, 'publish', %s, 'published', %s, %s)
                """,
                (
                    pack_id,
                    from_status,
                    req.message,
                    Json({"validation_run_id": validation_run_id}),
                ),
            )

    return {
        "pack_id": pack_id,
        "from_status": from_status,
        "to_status": "published",
        "validation_run_id": validation_run_id,
    }


@router.post("/{pack_id}/set-active", summary="设置全局 Active Pack（仅 published）")
def set_active_pack(pack_id: str, req: SetActiveRequest) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                FROM app.prompt_pack
                WHERE id = %s
                """,
                (pack_id,),
            )
            row = cur.fetchone()
            if not row:
                _raise_error(404, "PACK_NOT_FOUND", "prompt_pack not found", {"pack_id": pack_id})
            if row[0] != "published":
                _raise_error(
                    400,
                    "INVALID_STATUS",
                    "only published pack can be set active",
                    {"pack_id": pack_id, "current": row[0]},
                )

            cur.execute(
                """
                SELECT active_pack_id
                FROM app.prompt_global_active
                WHERE id = 1
                """,
            )
            prev = cur.fetchone()
            prev_active = prev[0] if prev else None

            cur.execute(
                """
                UPDATE app.prompt_global_active
                SET active_pack_id = %s, updated_at = NOW(), updated_by = NULL
                WHERE id = 1
                """,
                (pack_id,),
            )

            cur.execute(
                """
                INSERT INTO app.prompt_pack_event (pack_id, event_type, from_status, to_status, message, metadata)
                VALUES (%s, 'set_active', 'published', 'published', %s, %s)
                """,
                (
                    pack_id,
                    req.message,
                    Json({"prev_active_pack_id": prev_active, "ts": datetime.now(timezone.utc).isoformat()}),
                ),
            )

    return {"active_pack_id": pack_id, "prev_active_pack_id": prev_active}

"""Asynchronous PDF/HTML downloader for Tushare anns_d announcements.

This script scans market.anns for rows with download_status='pending',
tries to download the announcement document to ANNOUNCE_PDF_ROOT, and
updates local_path / file_ext / file_size / file_hash / download_status.

It is designed to be triggered by the existing ingestion/job system
(similar to other Python-based ingestion scripts) and can be run in small
batches to avoid blocking.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv

import requests


load_dotenv(override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-download-anns-pdf",
)

ANN_ROOT = os.getenv("ANNOUNCE_PDF_ROOT", "D:/AIstockDB/data/anns")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download announcement PDFs/HTML for market.anns")
    p.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max number of pending records to process in one run",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between downloads (optional throttling)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="HTTP request timeout in seconds",
    )
    p.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Existing ingestion_jobs.job_id to update status for anns_pdf task",
    )
    p.add_argument(
        "--retry-failed",
        action="store_true",
        help="Also retry records with download_status='failed' and no local_path",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed diagnostics (status, content-type, snippet) especially for HTML responses",
    )
    return p.parse_args()


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _ensure_root(root: str) -> Path:
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _start_existing_job(conn, job_id: str) -> None:
    """Mark an existing ingestion job as running for anns_pdf.

    job_id 由 API 层 (/api/ingestion/run) 预先在 market.ingestion_jobs 中创建，
    这里仅更新其状态，保持与其它 Python ingestion 脚本的行为一致。
    """

    if not job_id:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running',
                   started_at=COALESCE(started_at, NOW())
             WHERE job_id=%s
            """,
            (job_id,),
        )


def _finish_job(conn, job_id: str, status: str, summary: Optional[Dict[str, Any]] = None) -> None:
    """Finalize an existing ingestion job for anns_pdf.

    这里只是简单更新 status / finished_at，并将统计信息合并进 summary JSON。
    """

    if not job_id:
        return
    payload = summary or {}
    with conn.cursor() as cur:
        if payload:
            cur.execute(
                """
                UPDATE market.ingestion_jobs
                   SET status=%s,
                       finished_at=NOW(),
                       summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                 WHERE job_id=%s
                """,
                (status, json.dumps(payload, ensure_ascii=False), job_id),
            )
        else:
            cur.execute(
                """
                UPDATE market.ingestion_jobs
                   SET status=%s,
                       finished_at=NOW()
                 WHERE job_id=%s
                """,
                (status, job_id),
            )


def _guess_ext_from_url(url: str) -> str:
    lower = url.lower()
    if ".pdf" in lower:
        return ".pdf"
    if lower.endswith(".html") or lower.endswith(".htm"):
        return ".html"
    return ""


def _normalize_ext_from_response(content: bytes, content_type: str, fallback: str) -> str:
    ct = (content_type or "").lower()
    if content.startswith(b"%PDF") or "application/pdf" in ct:
        return ".pdf"
    if "text/html" in ct:
        return ".html"
    # fallback from URL guess or leave empty
    return fallback or ""


def _safe_filename_part(text: str) -> str:
    s = text.strip() or "ann"
    # Remove/replace characters not suitable for Windows paths
    bad = '\\/:*?"<>|'
    for ch in bad:
        s = s.replace(ch, "_")
    if len(s) > 80:
        s = s[:80]
    return s


def _download_one(url: str, timeout: float, verbose: bool = False) -> Optional[Tuple[bytes, str, int, str]]:
    if not url:
        return None
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
        if verbose:
            print(
                f"[HTTP] GET {resp.url} status={resp.status_code} "
                f"content-type={resp.headers.get('Content-Type', '')} len={len(resp.content)}",
            )
        resp.raise_for_status()
        content = resp.content
        ctype = resp.headers.get("Content-Type", "")
        final_url = str(resp.url)
        status = int(resp.status_code)
        return content, ctype, status, final_url
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] download failed: {exc} url={url}")
        return None


def _load_pending(conn, limit: int, retry_failed: bool) -> List[Dict[str, Any]]:
    if retry_failed:
        sql = (
            "SELECT id, ann_date, ts_code, title, url "
            "FROM market.anns "
            "WHERE download_status IN ('pending','failed') "
            "  AND (local_path IS NULL OR local_path = '') "
            "  AND url IS NOT NULL AND url <> '' "
            "ORDER BY ann_date ASC, id ASC "
            "LIMIT %s"
        )
    else:
        sql = (
            "SELECT id, ann_date, ts_code, title, url "
            "FROM market.anns "
            "WHERE download_status = 'pending' AND url IS NOT NULL AND url <> '' "
            "ORDER BY ann_date ASC, id ASC "
            "LIMIT %s"
        )
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    cols = ["id", "ann_date", "ts_code", "title", "url"]
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = {col: row[idx] for idx, col in enumerate(cols)}
        out.append(item)
    return out


def _update_success(
    conn,
    row_id: int,
    rel_path: str,
    ext: str,
    size: int,
    file_hash: str,
) -> None:
    sql = (
        "UPDATE market.anns "
        "SET local_path=%s, file_ext=%s, file_size=%s, file_hash=%s, "
        "    download_status='success', updated_at=NOW() "
        "WHERE id=%s"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (rel_path, ext, size, file_hash, row_id))


def _update_failed(conn, row_id: int, reason: str) -> None:
    # 先简单标记 failed，详细错误可以后续扩展到单独日志表
    sql = (
        "UPDATE market.anns "
        "SET download_status='failed', updated_at=NOW() "
        "WHERE id=%s"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (row_id,))
    print(f"[WARN] mark download failed for id={row_id}: {reason}")


def _is_cninfo_notice_shell(content: bytes, content_type: str) -> bool:
    """Best-effort detection of cninfo notice-detail shell HTML.

    These pages are Vue SPA shells that load real content via JS. We don't treat
    them as successfully downloaded announcement documents.
    """

    ct = (content_type or "").lower()
    if "text/html" not in ct and b"<html" not in content[:200].lower():
        return False
    haystack = content[:2000].lower()
    markers = [
        b"id=\"noticedetail\"",
        b"notice-detail.js".lower(),
        b"var announcementid".lower(),
        b"fundtitle =",  # seen in sample
    ]
    return any(m in haystack for m in markers)


def process_batch(conn, limit: int, timeout: float, root: str, sleep_s: float, verbose: bool = False, retry_failed: bool = False) -> Dict[str, Any]:
    root_path = _ensure_root(root)
    pending = _load_pending(conn, limit, retry_failed=retry_failed)
    if not pending:
        print("[INFO] no pending announcements to download")
        return {"total": 0, "success": 0, "failed": 0}

    print(f"[INFO] found {len(pending)} pending announcements to download")

    stats = {"total": len(pending), "success": 0, "failed": 0}

    for item in pending:
        row_id = int(item["id"])
        ann_date = item.get("ann_date")
        ts_code = str(item.get("ts_code") or "").strip()
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()

        if not url:
            _update_failed(conn, row_id, "empty url")
            conn.commit()
            continue

        date_str = "unknown-date"
        try:
            if ann_date is not None:
                date_str = ann_date.isoformat()
        except Exception:
            date_str = "unknown-date"

        print(f"[STEP] downloading id={row_id} ts_code={ts_code} date={date_str}")

        result = _download_one(url, timeout=timeout, verbose=verbose)
        if not result:
            _update_failed(conn, row_id, "download error")
            conn.commit()
            stats["failed"] += 1
            if sleep_s > 0:
                time.sleep(sleep_s)
            continue

        content, content_type, status_code, final_url = result
        ext_guess = _guess_ext_from_url(url)
        ext = _normalize_ext_from_response(content, content_type, ext_guess)

        safe_code = _safe_filename_part(ts_code or "")
        safe_title = _safe_filename_part(title or "")

        rel_dir = f"{date_str}" if date_str else "unknown-date"
        target_dir = root_path / rel_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        if not ext:
            ext = ".bin"
        filename = f"{safe_code}_{safe_title}{ext}"
        target_path = target_dir / filename

        try:
            with target_path.open("wb") as f:
                f.write(content)
        except Exception as exc:  # noqa: BLE001
            _update_failed(conn, row_id, f"save file error: {exc}")
            conn.commit()
            stats["failed"] += 1
            if sleep_s > 0:
                time.sleep(sleep_s)
            continue

        size = target_path.stat().st_size
        file_hash = _sha256_bytes(content)

        rel_path = str(Path(rel_dir) / filename)

        # 当前业务假设：所有以 HTML 形式返回的公告都视为“未获取到有效文档”，
        # 因此一律标记为 failed 并删除本地文件，后续如需二次抓取再单独处理。
        if ext == ".html":
            _update_failed(conn, row_id, "html document (no pdf) treated as failed")
            conn.commit()
            stats["failed"] += 1
            try:
                target_path.unlink(missing_ok=True)
            except Exception:
                pass
            if verbose:
                snippet = ""
                try:
                    snippet = content[:400].decode("utf-8", errors="replace")
                except Exception:
                    snippet = str(content[:200])
                print("[HTML-FAILED] id=", row_id, "status=", status_code, "final_url=", final_url)
                print("[HTML-FAILED] snippet:\n" + snippet.replace("\n", " ")[:400])
            if sleep_s > 0:
                time.sleep(sleep_s)
            continue

        _update_success(conn, row_id, rel_path, ext.lstrip("."), size, file_hash)
        conn.commit()
        stats["success"] += 1

        print(
            f"[OK] saved id={row_id} -> {target_path} size={size}B ext={ext} sha256={file_hash[:12]}...",
        )

        if sleep_s > 0:
            time.sleep(sleep_s)

    return stats


def main() -> int:
    args = parse_args()
    try:
        conn = psycopg2.connect(**DB_CFG)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to connect DB: {exc}")
        return 1

    job_id = args.job_id or None

    try:
        if job_id:
            _start_existing_job(conn, job_id)
        stats = process_batch(
            conn,
            limit=args.limit,
            timeout=args.timeout,
            root=ANN_ROOT,
            sleep_s=args.sleep,
            verbose=args.verbose,
            retry_failed=args.retry_failed,
        )
        if job_id:
            status = "success" if stats.get("failed", 0) == 0 else "failed"
            _finish_job(conn, job_id, status, {"stats": stats})
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

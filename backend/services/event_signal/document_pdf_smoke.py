"""Small-batch PDF parsing and DeepSeek smoke validation for event signals.

The module is intentionally read-only for the database.  It downloads a tiny
number of announcement PDFs to an artifact directory, preprocesses extracted
text into auditable chunks, and calls DeepSeek with a strict JSON prompt.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import zipfile
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import psycopg2.extras
import requests
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.infra.deepseek_client import DeepSeekClient
from backend.infra.deepseek_config import DEFAULT_DEEPSEEK_MODEL, DeepSeekConfigError, resolve_deepseek_config
from backend.services.announcements.title_classifier import RULE_VERSION as ANNOUNCEMENT_RULE_VERSION
from backend.services.event_signal.document_preprocessor import EvidenceChunk, preprocess_document


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = ROOT / "reports" / "event_signal" / "pdf_smoke"
DEFAULT_ARTIFACT_DIR = Path(
    os.getenv("AISTOCK_EVENT_SIGNAL_PDF_ARTIFACT_ROOT", "F:/Dev/AIstock_artifacts/event_signal_pdf_smoke")
)
DEFAULT_EVENT_TYPES: tuple[str, ...] = (
    "regulatory_investigation_penalty",
    "debt_default_overdue",
    "litigation_arbitration_freeze",
    "capital_occupation_illegal_guarantee",
    "audit_opinion_internal_control_risk",
)
PDF_MAGIC = b"%PDF"


class PdfSmokeError(RuntimeError):
    """Raised for expected smoke-validation failures."""


@dataclass(frozen=True)
class PdfSmokeCandidate:
    classification_id: int
    ann_id: int
    ts_code: str
    ann_date: Optional[dt.date]
    effective_trade_date: Optional[dt.date]
    event_type: str
    risk_level: str
    title: str
    source_url: str


def _json_dumps(value: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=indent)


def _load_env() -> None:
    for env_path in (ROOT / ".env", Path("F:/Dev/AIstock/.env")):
        if env_path.exists():
            load_dotenv(env_path, override=False)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def _normalize_cninfo_pdf_url(url: str) -> str:
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return "https://static.cninfo.com.cn" + url
    return url


def _cninfo_download_url(detail_url: str) -> Optional[str]:
    try:
        parsed = urlparse(detail_url)
        qs = parse_qs(parsed.query)
        ann_id = qs.get("announcementId") or qs.get("bulletinId")
        ann_time = qs.get("announcementTime") or qs.get("announceTime")
        if ann_id and ann_time:
            return (
                "https://www.cninfo.com.cn/new/announcement/download"
                f"?bulletinId={quote_plus(ann_id[0])}&announceTime={quote_plus(ann_time[0])}"
            )
    except Exception:
        return None
    return None


def resolve_pdf_url(source_url: str) -> Optional[str]:
    """Resolve known announcement detail URLs into direct PDF/download URLs."""

    url = (source_url or "").strip()
    if not url:
        return None
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    lower = url.lower()

    if "pdf.dfcfw.com" in host:
        return url
    if lower.endswith(".pdf"):
        return _normalize_cninfo_pdf_url(url)

    if "eastmoney.com" in host:
        match = re.search(r"/(AN\d+)\.html(?:$|[?#])", url, re.I)
        if not match:
            qs = parse_qs(parsed.query)
            art_code = qs.get("art_code") or qs.get("artCode")
            if art_code:
                match = re.match(r"(AN\d+)", art_code[0], re.I)
        if match:
            return f"https://pdf.dfcfw.com/pdf/H2_{match.group(1)}_1.pdf"

    if "cninfo.com.cn" in host:
        download_url = _cninfo_download_url(url)
        if download_url:
            return download_url
        match = re.search(r"(/finalpage/[^?#]+\.pdf)", url, re.I)
        if match:
            return _normalize_cninfo_pdf_url(match.group(1))

    return None


def _eastmoney_challenge_cookies(script_text: str) -> Optional[dict[str, str]]:
    """Extract pdf.dfcfw.com bot-check cookies from its lightweight JS gate."""

    if "EO_Bot_Ssid" not in script_text or "__tst_status" not in script_text:
        return None
    eo_match = re.search(r"EO_Bot_Ssid=.*?case\"3\".*?,(\d{6,});continue", script_text, re.S)
    if not eo_match:
        eo_match = re.search(r"EO_Bot_Ssid.*?(\d{6,})", script_text, re.S)
    if not eo_match:
        return None
    eo_value = int(eo_match.group(1))

    status_numbers: list[int] = []
    object_match = re.search(r"var\s+\w+\s*=\{(?P<body>.*?)\},t=0;return", script_text, re.S)
    if object_match:
        status_numbers = [int(item) for item in re.findall(r":(\d{6,})", object_match.group("body"))]
    if len(status_numbers) < 3:
        all_numbers = [int(item) for item in re.findall(r"\d{6,}", script_text)]
        status_numbers = [item for item in all_numbers if item != eo_value]
    if len(status_numbers) < 3:
        return None

    status_value = sum(status_numbers[:3])
    return {"__tst_status": f"{status_value}#", "EO_Bot_Ssid": str(eo_value)}


def _download_with_session(
    session: requests.Session,
    url: str,
    *,
    origin_url: Optional[str],
    timeout: float,
    cookies: Optional[Mapping[str, str]] = None,
) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/pdf,*/*;q=0.9",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    if origin_url:
        headers["Referer"] = origin_url
    response = session.get(
        url,
        headers=headers,
        cookies=dict(cookies or {}),
        timeout=timeout,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.content


def _extract_pdf_from_zip(content: bytes) -> Optional[bytes]:
    if not content.startswith(b"PK"):
        return None
    try:
        with zipfile.ZipFile(BytesIO(content)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".pdf"):
                    return zf.read(name)
    except Exception:
        return None
    return None


def _extract_pdf_link_from_html(html_text: str, base_url: str) -> Optional[str]:
    patterns = (
        r"https?://static\.cninfo\.com\.cn/[^\"'<>]+\.pdf",
        r"data-pdf=\"([^\"]+\.pdf)\"",
        r"href=\"([^\"]+\.pdf)\"",
    )
    for pattern in patterns:
        match = re.search(pattern, html_text, re.I)
        if not match:
            continue
        target = match.group(1) if match.groups() else match.group(0)
        return urljoin(base_url, target)
    return None


def download_pdf_bytes(
    pdf_url: str,
    *,
    origin_url: Optional[str] = None,
    timeout: float = 20.0,
    max_bytes: int = 8_000_000,
    _depth: int = 0,
) -> bytes:
    """Download a PDF, handling Eastmoney and CNInfo redirects safely."""

    if not pdf_url or _depth > 2:
        raise PdfSmokeError(f"invalid pdf url or redirect depth exceeded: {pdf_url}")
    with requests.Session() as session:
        content = _download_with_session(session, pdf_url, origin_url=origin_url, timeout=timeout)
        if len(content) > max_bytes:
            raise PdfSmokeError(f"pdf exceeds max_bytes: {len(content)} > {max_bytes}")
        if content.startswith(PDF_MAGIC):
            return content

        zipped_pdf = _extract_pdf_from_zip(content)
        if zipped_pdf:
            if len(zipped_pdf) > max_bytes:
                raise PdfSmokeError(f"zipped pdf exceeds max_bytes: {len(zipped_pdf)} > {max_bytes}")
            return zipped_pdf

        stripped = content.lstrip()
        text = stripped[:4096].decode("utf-8", errors="ignore")
        if "pdf.dfcfw.com" in urlparse(pdf_url).netloc.lower() and stripped.startswith(b"<script"):
            cookies = _eastmoney_challenge_cookies(text)
            if cookies:
                content = _download_with_session(
                    session,
                    pdf_url,
                    origin_url=origin_url,
                    timeout=timeout,
                    cookies=cookies,
                )
                if len(content) > max_bytes:
                    raise PdfSmokeError(f"pdf exceeds max_bytes after cookie retry: {len(content)} > {max_bytes}")
                if content.startswith(PDF_MAGIC):
                    return content

        html_text = content.decode("utf-8", errors="ignore")
        if "<html" in html_text.lower() or stripped.startswith(b"<"):
            next_url = _extract_pdf_link_from_html(html_text, pdf_url)
            if next_url:
                return download_pdf_bytes(
                    next_url,
                    origin_url=origin_url or pdf_url,
                    timeout=timeout,
                    max_bytes=max_bytes,
                    _depth=_depth + 1,
                )

    raise PdfSmokeError(f"downloaded content is not a PDF: {pdf_url}")


def extract_pdf_text(pdf_bytes: bytes, *, max_pages: int = 8, max_chars: int = 20000) -> str:
    """Extract text from PDF bytes with PyMuPDF first and PyPDF2 fallback."""

    if not pdf_bytes.startswith(PDF_MAGIC):
        raise PdfSmokeError("PDF parser received non-PDF bytes")

    text_parts: list[str] = []
    try:
        import fitz  # type: ignore

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            for page_index in range(min(max_pages, doc.page_count)):
                page_text = doc.load_page(page_index).get_text("text") or ""
                if page_text.strip():
                    text_parts.append(page_text)
        finally:
            doc.close()
    except Exception:
        text_parts = []

    if not text_parts:
        try:
            import PyPDF2  # type: ignore

            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            for page in reader.pages[:max_pages]:
                page_text = page.extract_text() or ""
                if page_text.strip():
                    text_parts.append(page_text)
        except Exception as exc:
            raise PdfSmokeError(f"PDF text extraction failed: {exc}") from exc

    text = "\n--- page break ---\n".join(text_parts).strip()
    if len(text) > max_chars:
        text = text[:max_chars]
    if not text:
        raise PdfSmokeError("PDF text extraction returned empty text")
    return text


def _chunk_payload(chunks: Iterable[EvidenceChunk]) -> list[dict[str, Any]]:
    return [
        {
            "chunk_id": chunk.chunk_id,
            "page_no": chunk.page_no,
            "section_title": chunk.section_title,
            "score": chunk.score,
            "matched_keywords": list(chunk.matched_keywords),
            "text": chunk.text,
        }
        for chunk in chunks
    ]


def build_deepseek_messages(candidate: PdfSmokeCandidate, chunks: list[EvidenceChunk]) -> list[dict[str, str]]:
    schema = {
        "ann_id": candidate.ann_id,
        "ts_code": candidate.ts_code,
        "event_type": candidate.event_type,
        "risk_level": "low|medium|high|critical",
        "direction": "risk|neutral|positive|mixed",
        "conclusion": "short conclusion in Chinese",
        "confidence": 0.0,
        "risk_items": [{"item": "", "severity": "low|medium|high|critical", "evidence_chunk_ids": [""]}],
        "missing_fields": [""],
        "should_escalate_to_human": False,
    }
    user_payload = {
        "task": "Analyze only the evidence chunks from an A-share announcement PDF. Do not infer beyond evidence.",
        "announcement": asdict(candidate),
        "required_json_schema": schema,
        "evidence_chunks": _chunk_payload(chunks),
    }
    return [
        {
            "role": "system",
            "content": (
                "You are an A-share announcement risk analyst. Return JSON only. "
                "Use the provided evidence_chunk_ids for every material conclusion."
            ),
        },
        {"role": "user", "content": _json_dumps(user_payload)},
    ]


def extract_json_object(response_text: str) -> dict[str, Any]:
    text = (response_text or "").strip()
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S | re.I)
    if fence:
        text = fence.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end < start:
            raise PdfSmokeError("DeepSeek response does not contain a JSON object")
        text = text[start : end + 1]
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise PdfSmokeError(f"DeepSeek JSON parse failed: {exc}") from exc


def _safe_artifact_name(candidate: PdfSmokeCandidate) -> str:
    title = re.sub(r"[\\/:*?\"<>|\s]+", "_", candidate.title or "announcement").strip("_")
    if len(title) > 64:
        title = title[:64]
    date_part = candidate.ann_date.isoformat() if candidate.ann_date else "unknown_date"
    return f"{date_part}_{candidate.ts_code}_{candidate.ann_id}_{title or 'announcement'}.pdf"


def process_candidate(
    candidate: PdfSmokeCandidate,
    *,
    deepseek_client: Optional[Any],
    model: str = DEFAULT_DEEPSEEK_MODEL,
    downloader: Callable[..., bytes] = download_pdf_bytes,
    text_extractor: Callable[..., str] = extract_pdf_text,
    artifact_dir: Path = DEFAULT_ARTIFACT_DIR,
    save_pdf: bool = False,
    use_deepseek: bool = True,
    timeout: float = 20.0,
    max_pdf_bytes: int = 8_000_000,
    max_pages: int = 8,
    max_chars: int = 20000,
    max_chunks: int = 6,
    min_score: float = 2.0,
    max_tokens: int = 1200,
) -> dict[str, Any]:
    pdf_url = resolve_pdf_url(candidate.source_url)
    result: dict[str, Any] = {
        "candidate": asdict(candidate),
        "resolved_pdf_url": pdf_url,
        "status": "RUNNING",
    }
    if not pdf_url:
        result.update({"status": "FAILED", "error": "could_not_resolve_pdf_url"})
        return result

    try:
        pdf_bytes = downloader(
            pdf_url,
            origin_url=candidate.source_url,
            timeout=timeout,
            max_bytes=max_pdf_bytes,
        )
        result["pdf_bytes"] = len(pdf_bytes)
        if save_pdf:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            pdf_path = artifact_dir / _safe_artifact_name(candidate)
            pdf_path.write_bytes(pdf_bytes)
            result["pdf_artifact_path"] = str(pdf_path)

        text = text_extractor(pdf_bytes, max_pages=max_pages, max_chars=max_chars)
        preprocess = preprocess_document(
            text,
            event_type=candidate.event_type,
            max_chunks=max_chunks,
            min_score=min_score,
        )
        result["preprocess_stats"] = preprocess.stats
        result["chunk_ids"] = [chunk.chunk_id for chunk in preprocess.chunks]
        if not preprocess.chunks:
            result.update({"status": "NO_CHUNKS", "error": "no_evidence_chunks_selected"})
            return result

        if not use_deepseek:
            result.update({"status": "PREPROCESSED", "deepseek_skipped": True})
            return result
        if deepseek_client is None:
            raise PdfSmokeError("deepseek_client is required when use_deepseek=True")

        messages = build_deepseek_messages(candidate, preprocess.chunks)
        response_text = deepseek_client.call_api(
            messages,
            model=model,
            temperature=0.1,
            max_tokens=max_tokens,
        )
        result["deepseek_response_excerpt"] = response_text[:1200]
        if response_text.startswith("API") and ("fail" in response_text.lower() or "失败" in response_text):
            raise PdfSmokeError(response_text[:300])
        parsed = extract_json_object(response_text)
        result["deepseek_json"] = parsed
        result["status"] = "ANALYZED"
        return result
    except Exception as exc:  # noqa: BLE001
        result.update({"status": "FAILED", "error": str(exc)})
        return result


def fetch_pdf_smoke_candidates(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    event_types: Iterable[str],
    limit: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> list[PdfSmokeCandidate]:
    sql = """
        SELECT
            c.classification_id,
            c.ann_id,
            a.ts_code,
            a.ann_date,
            c.effective_trade_date,
            c.event_type,
            c.risk_level,
            a.title,
            a.url
        FROM market.ann_event_classification c
        JOIN market.anns a ON a.id = c.ann_id
        WHERE c.rule_version = %s
          AND c.time_mode = %s
          AND c.event_type = ANY(%s)
          AND a.url IS NOT NULL
          AND a.url <> ''
          AND COALESCE(a.ts_code, '') NOT LIKE %s
          AND (%s IS NULL OR a.ann_date >= %s)
          AND (%s IS NULL OR a.ann_date <= %s)
        ORDER BY c.effective_trade_date DESC NULLS LAST, c.classification_id DESC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            sql,
            (
                rule_version,
                time_mode,
                list(event_types),
                "%.BJ",
                start_date,
                start_date,
                end_date,
                end_date,
                limit,
            ),
        )
        rows = cur.fetchall()
    return [
        PdfSmokeCandidate(
            classification_id=int(row["classification_id"]),
            ann_id=int(row["ann_id"]),
            ts_code=str(row["ts_code"] or ""),
            ann_date=row["ann_date"],
            effective_trade_date=row["effective_trade_date"],
            event_type=str(row["event_type"] or ""),
            risk_level=str(row["risk_level"] or ""),
            title=str(row["title"] or ""),
            source_url=str(row["url"] or ""),
        )
        for row in rows
    ]


def summarize_results(results: list[dict[str, Any]], *, candidates_scanned: int) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    by_event_type: dict[str, int] = {}
    for item in results:
        status = str(item.get("status") or "UNKNOWN")
        by_status[status] = by_status.get(status, 0) + 1
        event_type = str((item.get("candidate") or {}).get("event_type") or "")
        if event_type:
            by_event_type[event_type] = by_event_type.get(event_type, 0) + 1
    return {
        "candidates_scanned": candidates_scanned,
        "processed_rows": len(results),
        "analyzed_rows": by_status.get("ANALYZED", 0),
        "preprocessed_rows": by_status.get("PREPROCESSED", 0),
        "failed_rows": by_status.get("FAILED", 0),
        "by_status": dict(sorted(by_status.items())),
        "by_event_type": dict(sorted(by_event_type.items())),
    }


def write_pdf_smoke_report(*, payload: dict[str, Any], output_dir: Path, report_id: str) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(payload, indent=2), encoding="utf-8")

    summary = payload["summary"]
    lines = [
        "# Announcement PDF DeepSeek Smoke Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{payload['rule_version']}`",
        f"- Time mode: `{payload['time_mode']}`",
        f"- Model: `{payload['model']}`",
        f"- Candidates scanned: `{summary['candidates_scanned']}`",
        f"- Processed rows: `{summary['processed_rows']}`",
        f"- Analyzed rows: `{summary['analyzed_rows']}`",
        f"- Failed rows: `{summary['failed_rows']}`",
        "",
        "## Status",
        "",
        "| status | rows |",
        "| --- | ---: |",
    ]
    for status, rows in summary["by_status"].items():
        lines.append(f"| {status} | {rows} |")
    lines.extend(
        [
            "",
            "## Results",
            "",
            "| ann_id | ts_code | event_type | status | pdf_bytes | conclusion |",
            "| ---: | --- | --- | --- | ---: | --- |",
        ]
    )
    for item in payload["results"]:
        candidate = item.get("candidate") or {}
        conclusion = ""
        parsed = item.get("deepseek_json") or {}
        if isinstance(parsed, dict):
            conclusion = str(parsed.get("conclusion") or parsed.get("direction") or "")
        if not conclusion:
            conclusion = str(item.get("error") or "")[:80]
        conclusion = conclusion.replace("|", " ")[:120]
        lines.append(
            f"| {candidate.get('ann_id')} | {candidate.get('ts_code')} | {candidate.get('event_type')} | "
            f"{item.get('status')} | {item.get('pdf_bytes', 0)} | {conclusion} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def run_pdf_smoke(
    *,
    rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    time_mode: str = "backtest",
    event_types: Iterable[str] = DEFAULT_EVENT_TYPES,
    limit: int = 2,
    candidate_scan_limit: int = 20,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    artifact_dir: Path = DEFAULT_ARTIFACT_DIR,
    save_pdf: bool = True,
    use_deepseek: bool = True,
    model: str = DEFAULT_DEEPSEEK_MODEL,
    max_pages: int = 8,
    max_chars: int = 20000,
    max_chunks: int = 6,
    min_score: float = 2.0,
    max_tokens: int = 1200,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if candidate_scan_limit < limit:
        raise ValueError("candidate_scan_limit must be >= limit")

    _load_env()
    if use_deepseek:
        try:
            resolve_deepseek_config(model=model)
        except DeepSeekConfigError as exc:
            raise PdfSmokeError(str(exc)) from exc

    with get_conn() as conn:
        candidates = fetch_pdf_smoke_candidates(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            event_types=event_types,
            limit=candidate_scan_limit,
            start_date=start_date,
            end_date=end_date,
        )

    client = DeepSeekClient(model=model) if use_deepseek else None
    results: list[dict[str, Any]] = []
    analyzed_or_preprocessed = 0
    for candidate in candidates:
        item = process_candidate(
            candidate,
            deepseek_client=client,
            model=model,
            artifact_dir=artifact_dir,
            save_pdf=save_pdf,
            use_deepseek=use_deepseek,
            max_pages=max_pages,
            max_chars=max_chars,
            max_chunks=max_chunks,
            min_score=min_score,
            max_tokens=max_tokens,
        )
        results.append(item)
        if item.get("status") in {"ANALYZED", "PREPROCESSED"}:
            analyzed_or_preprocessed += 1
        if analyzed_or_preprocessed >= limit:
            break

    report_id = "document_pdf_smoke_{}".format(dt.datetime.now().strftime("%Y%m%d_%H%M%S"))
    payload = {
        "report_id": report_id,
        "rule_version": rule_version,
        "time_mode": time_mode,
        "event_types": list(event_types),
        "limit": limit,
        "candidate_scan_limit": candidate_scan_limit,
        "start_date": start_date,
        "end_date": end_date,
        "use_deepseek": use_deepseek,
        "model": model,
        "artifact_dir": str(artifact_dir),
        "summary": summarize_results(results, candidates_scanned=len(candidates)),
        "results": results,
    }
    payload["outputs"] = write_pdf_smoke_report(payload=payload, output_dir=output_dir, report_id=report_id)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Small-batch announcement PDF + DeepSeek smoke validation")
    parser.add_argument("--rule-version", default=ANNOUNCEMENT_RULE_VERSION)
    parser.add_argument("--time-mode", default="backtest", choices=["backtest", "paper", "live", "observed"])
    parser.add_argument("--event-type", action="append", default=None)
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--candidate-scan-limit", type=int, default=20)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--artifact-dir", default=str(DEFAULT_ARTIFACT_DIR))
    parser.add_argument("--no-save-pdf", action="store_true")
    parser.add_argument("--no-deepseek", action="store_true")
    parser.add_argument("--model", default=DEFAULT_DEEPSEEK_MODEL)
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--max-chars", type=int, default=20000)
    parser.add_argument("--max-chunks", type=int, default=6)
    parser.add_argument("--min-score", type=float, default=2.0)
    parser.add_argument("--max-tokens", type=int, default=1200)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = run_pdf_smoke(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        event_types=tuple(args.event_type or DEFAULT_EVENT_TYPES),
        limit=args.limit,
        candidate_scan_limit=args.candidate_scan_limit,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_dir=Path(args.output_dir),
        artifact_dir=Path(args.artifact_dir),
        save_pdf=not args.no_save_pdf,
        use_deepseek=not args.no_deepseek,
        model=args.model,
        max_pages=args.max_pages,
        max_chars=args.max_chars,
        max_chunks=args.max_chunks,
        min_score=args.min_score,
        max_tokens=args.max_tokens,
    )
    print(
        _json_dumps(
            {
                "report_id": payload["report_id"],
                "summary": payload["summary"],
                "outputs": payload["outputs"],
                "artifact_dir": payload["artifact_dir"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

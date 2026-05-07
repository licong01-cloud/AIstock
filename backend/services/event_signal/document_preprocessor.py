"""Deterministic announcement text preprocessing before future LLM analysis.

The module accepts text already extracted from PDF/HTML and returns small,
auditable evidence chunks. It deliberately does not download files, parse PDF
binaries, call LLMs, or write trading signals.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Iterable, Optional


BOILERPLATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"本公司及董事会全体成员保证.*不存在任何虚假记载.*误导性陈述.*重大遗漏"),
    re.compile(r"公司董事会及全体董事保证.*公告内容.*真实.*准确.*完整"),
    re.compile(r"备查文件[:：]?$"),
    re.compile(r"目\s*录$"),
    re.compile(r"证券代码[:：].*证券简称[:：].*公告编号[:：]"),
    re.compile(r"第\s*\d+\s*页\s*(共\s*\d+\s*页)?"),
)

DEFAULT_ROUTE_KEYWORDS: tuple[str, ...] = (
    "重大", "风险", "影响", "金额", "比例", "原因", "整改", "进展", "不确定性",
)

EVENT_ROUTE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "audit_opinion_internal_control_risk": (
        "非标准审计", "保留意见", "否定意见", "无法表示意见", "内部控制", "重大缺陷", "持续经营", "强调事项",
    ),
    "regulatory_investigation_penalty": (
        "立案", "调查", "处罚", "违法", "违规", "罚款", "市场禁入", "警示函", "纪律处分", "监管措施",
    ),
    "capital_occupation_illegal_guarantee": (
        "资金占用", "非经营性占用", "违规担保", "担保余额", "整改", "解除担保", "责任人",
    ),
    "debt_default_overdue": (
        "逾期", "违约", "债务", "兑付", "本息", "流动性", "展期", "偿付", "到期",
    ),
    "litigation_arbitration_freeze": (
        "诉讼", "仲裁", "冻结", "查封", "判决", "裁定", "执行", "涉案金额", "拍卖",
    ),
    "inquiry_concern_letter": (
        "问询", "关注函", "回复", "核查", "说明", "交易所", "监管", "落实函",
    ),
    "performance_forecast_revision_impairment": (
        "业绩", "预告", "修正", "亏损", "减值", "商誉", "资产减值", "会计差错", "更正",
    ),
}

HEADING_PATTERN = re.compile(r"^(一|二|三|四|五|六|七|八|九|十|[0-9]+)[、.．]\s*(.{2,40})$")
AMOUNT_PATTERN = re.compile(r"(\d+(?:\.\d+)?\s*(?:亿元|万元|元|%|％)|占\s*[^，。；;]{0,20}\s*比例)")
DATE_PATTERN = re.compile(r"20\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日|20\d{6}")
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[。；;])")


@dataclass(frozen=True)
class DocumentPage:
    page_no: int
    text: str


@dataclass(frozen=True)
class TextBlock:
    block_id: str
    page_no: int
    text: str
    section_title: Optional[str] = None
    block_type: str = "paragraph"
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceChunk:
    chunk_id: str
    event_type: str
    page_no: int
    section_title: Optional[str]
    text: str
    text_hash: str
    score: float
    matched_keywords: tuple[str, ...]
    token_estimate: int
    source_block_ids: tuple[str, ...]


@dataclass(frozen=True)
class PreprocessResult:
    event_type: str
    chunks: list[EvidenceChunk]
    dropped_block_ids: list[str]
    stats: dict[str, int | float]


def normalize_text(text: str) -> str:
    text = (text or "").replace("\u3000", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def stable_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


def estimate_tokens(text: str) -> int:
    # Conservative mixed Chinese/ASCII estimate for routing, not model billing.
    normalized = normalize_text(text)
    ascii_words = len(re.findall(r"[A-Za-z0-9_]+", normalized))
    cjk_chars = len(re.findall(r"[\u4e00-\u9fff]", normalized))
    other_chars = max(0, len(normalized) - cjk_chars)
    return max(1, int(cjk_chars * 0.6 + other_chars / 4 + ascii_words * 0.3))


def _line_key(line: str) -> str:
    return re.sub(r"\s+", "", line.strip())


def repeated_line_keys(pages: Iterable[DocumentPage], *, min_pages: int = 2) -> set[str]:
    counts: dict[str, set[int]] = {}
    for page in pages:
        for line in normalize_text(page.text).splitlines():
            key = _line_key(line)
            if not key or len(key) > 80:
                continue
            counts.setdefault(key, set()).add(page.page_no)
    return {key for key, page_set in counts.items() if len(page_set) >= min_pages}


def is_boilerplate_line(line: str, repeated_keys: set[str]) -> bool:
    clean = normalize_text(line)
    if not clean:
        return True
    key = _line_key(clean)
    if key in repeated_keys and (len(clean) <= 80 or "证券代码" in clean or "公告编号" in clean):
        return True
    return any(pattern.search(clean) for pattern in BOILERPLATE_PATTERNS)


def pages_from_text(text: str) -> list[DocumentPage]:
    parts = re.split(r"\n\s*---\s*page\s*break\s*---\s*\n", text or "", flags=re.IGNORECASE)
    return [DocumentPage(idx + 1, part) for idx, part in enumerate(parts) if normalize_text(part)]


def split_pages_to_blocks(pages: list[DocumentPage]) -> tuple[list[TextBlock], list[str]]:
    repeated_keys = repeated_line_keys(pages)
    blocks: list[TextBlock] = []
    dropped: list[str] = []
    current_section: Optional[str] = None
    for page in pages:
        kept_lines: list[str] = []
        for line_no, line in enumerate(normalize_text(page.text).splitlines(), start=1):
            block_id = f"p{page.page_no}:l{line_no}"
            if not normalize_text(line):
                kept_lines.append("")
                continue
            if is_boilerplate_line(line, repeated_keys):
                dropped.append(block_id)
                continue
            kept_lines.append(line.strip())
        paragraph = normalize_text("\n".join(kept_lines))
        if not paragraph:
            continue
        raw_blocks = [part for part in re.split(r"\n\s*\n", paragraph) if normalize_text(part)]
        for idx, raw_block in enumerate(raw_blocks, start=1):
            text = normalize_text(raw_block)
            heading = HEADING_PATTERN.match(text.splitlines()[0]) if text else None
            if heading:
                current_section = heading.group(2).strip()
                block_type = "heading"
            else:
                block_type = "paragraph"
            block_id = f"p{page.page_no}:b{idx}"
            blocks.append(
                TextBlock(
                    block_id=block_id,
                    page_no=page.page_no,
                    text=text,
                    section_title=current_section,
                    block_type=block_type,
                )
            )
    return blocks, dropped


def route_keywords(event_type: str) -> tuple[str, ...]:
    return EVENT_ROUTE_KEYWORDS.get(event_type, DEFAULT_ROUTE_KEYWORDS) + DEFAULT_ROUTE_KEYWORDS


def matched_keywords(text: str, event_type: str) -> tuple[str, ...]:
    normalized = normalize_text(text)
    seen: list[str] = []
    for keyword in route_keywords(event_type):
        if keyword in normalized and keyword not in seen:
            seen.append(keyword)
    return tuple(seen)


def score_block(block: TextBlock, event_type: str) -> float:
    text = normalize_text(block.text)
    if len(text) < 20:
        return 0.0
    keywords = matched_keywords(text, event_type)
    score = float(len(keywords) * 2)
    if AMOUNT_PATTERN.search(text):
        score += 1.5
    if DATE_PATTERN.search(text):
        score += 0.8
    if block.section_title and matched_keywords(block.section_title, event_type):
        score += 1.0
    if block.block_type == "heading":
        score += 0.5
    return score


def split_long_text(text: str, *, max_chars: int) -> list[str]:
    normalized = normalize_text(text)
    if len(normalized) <= max_chars:
        return [normalized]
    chunks: list[str] = []
    current = ""
    for sentence in SENTENCE_SPLIT_PATTERN.split(normalized):
        sentence = sentence.strip()
        if not sentence:
            continue
        if len(current) + len(sentence) > max_chars and current:
            chunks.append(current)
            current = sentence
        else:
            current = f"{current}{sentence}" if current else sentence
    if current:
        chunks.append(current)
    return chunks or [normalized[:max_chars]]


def build_evidence_chunks(
    blocks: list[TextBlock],
    *,
    event_type: str,
    max_chunks: int = 8,
    min_score: float = 2.0,
    max_chars: int = 1600,
) -> list[EvidenceChunk]:
    scored = [(score_block(block, event_type), block) for block in blocks]
    candidates = [(score, block) for score, block in scored if score >= min_score]
    candidates.sort(key=lambda item: (-item[0], item[1].page_no, item[1].block_id))

    chunks: list[EvidenceChunk] = []
    seen_hashes: set[str] = set()
    for score, block in candidates:
        for part_no, text in enumerate(split_long_text(block.text, max_chars=max_chars), start=1):
            text_hash = stable_hash(text)
            if text_hash in seen_hashes:
                continue
            seen_hashes.add(text_hash)
            chunk_id = f"chunk:{event_type}:{block.block_id}:{part_no}:{text_hash[:12]}"
            chunks.append(
                EvidenceChunk(
                    chunk_id=chunk_id,
                    event_type=event_type,
                    page_no=block.page_no,
                    section_title=block.section_title,
                    text=text,
                    text_hash=text_hash,
                    score=score,
                    matched_keywords=matched_keywords(text, event_type),
                    token_estimate=estimate_tokens(text),
                    source_block_ids=(block.block_id,),
                )
            )
            if len(chunks) >= max_chunks:
                return chunks
    return chunks


def preprocess_document(
    text_or_pages: str | list[DocumentPage],
    *,
    event_type: str,
    max_chunks: int = 8,
    min_score: float = 2.0,
    max_chars: int = 1600,
) -> PreprocessResult:
    pages = pages_from_text(text_or_pages) if isinstance(text_or_pages, str) else text_or_pages
    blocks, dropped = split_pages_to_blocks(pages)
    chunks = build_evidence_chunks(
        blocks,
        event_type=event_type,
        max_chunks=max_chunks,
        min_score=min_score,
        max_chars=max_chars,
    )
    stats = {
        "pages": len(pages),
        "blocks": len(blocks),
        "dropped_blocks": len(dropped),
        "chunks": len(chunks),
        "input_chars": sum(len(page.text or "") for page in pages),
        "chunk_chars": sum(len(chunk.text) for chunk in chunks),
        "token_estimate": sum(chunk.token_estimate for chunk in chunks),
    }
    return PreprocessResult(event_type=event_type, chunks=chunks, dropped_block_ids=dropped, stats=stats)

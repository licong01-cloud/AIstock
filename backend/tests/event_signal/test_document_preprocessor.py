from backend.services.event_signal.document_preprocessor import (
    DocumentPage,
    build_evidence_chunks,
    estimate_tokens,
    matched_keywords,
    pages_from_text,
    preprocess_document,
    split_pages_to_blocks,
    stable_hash,
)


def test_pages_from_text_splits_manual_page_breaks():
    pages = pages_from_text("第一页\n--- page break ---\n第二页")

    assert [page.page_no for page in pages] == [1, 2]
    assert pages[1].text == "第二页"


def test_split_pages_to_blocks_removes_repeated_headers_and_board_boilerplate():
    pages = [
        DocumentPage(
            1,
            """
证券代码：000001 证券简称：测试股份 公告编号：2024-001
本公司及董事会全体成员保证信息披露内容的真实、准确、完整，不存在任何虚假记载、误导性陈述或者重大遗漏。
一、处罚事项
公司收到行政处罚决定书，涉及违法违规事实，罚款金额为100万元。
第 1 页 共 2 页
""",
        ),
        DocumentPage(
            2,
            """
证券代码：000001 证券简称：测试股份 公告编号：2024-001
二、对公司的影响
上述行政处罚可能对公司经营和声誉产生重大影响，公司将进行整改。
第 2 页 共 2 页
""",
        ),
    ]

    blocks, dropped = split_pages_to_blocks(pages)
    joined = "\n".join(block.text for block in blocks)

    assert dropped
    assert "董事会全体成员保证" not in joined
    assert "证券代码" not in joined
    assert "处罚事项" in joined
    assert "重大影响" in joined


def test_matched_keywords_are_event_specific():
    text = "公司收到行政处罚决定书，因违法违规被罚款100万元。"

    assert "处罚" in matched_keywords(text, "regulatory_investigation_penalty")
    assert "罚款" in matched_keywords(text, "regulatory_investigation_penalty")
    assert "处罚" not in matched_keywords(text, "debt_default_overdue")


def test_build_evidence_chunks_prefers_scored_risk_blocks_and_hashes_are_stable():
    pages = [
        DocumentPage(
            1,
            """
一、普通说明
公司日常经营正常。

二、债务逾期情况
截至2024年5月6日，公司存在债务逾期，本金金额1.2亿元，存在流动性风险。
""",
        )
    ]
    blocks, _ = split_pages_to_blocks(pages)
    chunks = build_evidence_chunks(blocks, event_type="debt_default_overdue", max_chunks=2)

    assert len(chunks) == 1
    assert chunks[0].section_title == "债务逾期情况"
    assert "1.2亿元" in chunks[0].text
    assert "逾期" in chunks[0].matched_keywords
    assert chunks[0].text_hash == stable_hash(chunks[0].text)
    assert chunks[0].token_estimate > 0


def test_preprocess_document_returns_small_auditable_chunks_not_full_document():
    text = """
证券代码：000001 证券简称：测试股份 公告编号：2024-002
本公司及董事会全体成员保证信息披露内容的真实、准确、完整，不存在任何虚假记载、误导性陈述或者重大遗漏。

一、事项概述
公司控股股东存在非经营性资金占用，占用金额为2,000万元，占最近一期净资产比例为3%。

二、整改安排
控股股东承诺于2024年6月30日前归还全部占用资金，公司将持续推进整改。
"""

    result = preprocess_document(
        text,
        event_type="capital_occupation_illegal_guarantee",
        max_chunks=3,
        max_chars=120,
    )

    assert result.stats["pages"] == 1
    assert result.stats["blocks"] >= 1
    assert result.stats["chunks"] >= 1
    assert result.stats["chunk_chars"] < result.stats["input_chars"]
    assert all("董事会全体成员保证" not in chunk.text for chunk in result.chunks)
    assert any("资金占用" in chunk.text or "占用资金" in chunk.text for chunk in result.chunks)


def test_estimate_tokens_is_positive_for_chinese_and_ascii_text():
    assert estimate_tokens("资金占用金额 100 万元") > 1

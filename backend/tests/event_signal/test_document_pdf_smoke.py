import datetime as dt

from backend.services.event_signal.document_pdf_smoke import (
    PdfSmokeCandidate,
    _eastmoney_challenge_cookies,
    build_deepseek_messages,
    extract_json_object,
    process_candidate,
    resolve_pdf_url,
)


def _candidate() -> PdfSmokeCandidate:
    return PdfSmokeCandidate(
        classification_id=1,
        ann_id=100,
        ts_code="000001.SZ",
        ann_date=dt.date(2026, 5, 6),
        effective_trade_date=dt.date(2026, 5, 7),
        event_type="regulatory_investigation_penalty",
        risk_level="P1_HIGH",
        title="sample risk announcement",
        source_url="https://data.eastmoney.com/notices/detail/000001/AN202605061821996143.html",
    )


def test_resolve_eastmoney_detail_url_to_direct_pdf():
    assert (
        resolve_pdf_url("https://data.eastmoney.com/notices/detail/000001/AN202605061821996143.html")
        == "https://pdf.dfcfw.com/pdf/H2_AN202605061821996143_1.pdf"
    )


def test_resolve_cninfo_detail_download_url():
    url = (
        "https://www.cninfo.com.cn/new/disclosure/detail"
        "?announcementId=1212345678&announcementTime=2026-05-06"
    )

    assert resolve_pdf_url(url) == (
        "https://www.cninfo.com.cn/new/announcement/download"
        "?bulletinId=1212345678&announceTime=2026-05-06"
    )


def test_eastmoney_challenge_cookie_parser_handles_current_script_shape():
    script = (
        '<script>function a(a){function n(){for(var a={wQzOV:_0x649a("0x4"),'
        'iTyzs:function(a,n){return a+n}},n=a[_0x649a("0x5")][_0x649a("0x6")]("|"),e=0;;)'
        '{switch(n[e++]){case"0":t+="EO_Bot_Ssid=";continue;case"1":return t;'
        'case"2":t+="";continue;case"3":t=a[_0x649a("0x7")](t,274137088);continue;'
        'case"4":var t="";continue}break}}var e={WTKkN:379021965,bOYDu:1168090757,'
        'dtzqS:function(a,n){return a+n},wyeCN:180322605,pCQRM:function(a){return a()}},'
        't=0;return t+=e[_0x649a("0x0")],t+=e[_0x649a("0x1")],'
        't=e[_0x649a("0x2")](t,e[_0x649a("0x3")]),[t,e[_0x649a("0x8")](n)][a]}'
        'document[_0x649a("0x9")]="__tst_status="+a(0)+"#;",'
        'document[_0x649a("0x9")]=a(1)+";";</script>'
    )

    assert _eastmoney_challenge_cookies(script) == {
        "__tst_status": "1727435327#",
        "EO_Bot_Ssid": "274137088",
    }


def test_extract_json_object_accepts_fenced_response():
    payload = extract_json_object(
        """Here is the result:
        ```json
        {"risk_level": "high", "confidence": 0.82, "risk_items": []}
        ```
        """
    )

    assert payload["risk_level"] == "high"
    assert payload["confidence"] == 0.82


def test_build_deepseek_messages_include_chunk_ids():
    class Chunk:
        chunk_id = "chunk:1"
        page_no = 2
        section_title = "risk"
        score = 3.5
        matched_keywords = ("penalty",)
        text = "short evidence text"

    messages = build_deepseek_messages(_candidate(), [Chunk()])  # type: ignore[list-item]

    assert messages[0]["role"] == "system"
    assert "JSON only" in messages[0]["content"]
    assert "chunk:1" in messages[1]["content"]
    assert "short evidence text" in messages[1]["content"]


def test_process_candidate_uses_injected_downloader_extractor_and_client(tmp_path):
    class FakeClient:
        def call_api(self, messages, model=None, temperature=0.7, max_tokens=2000):
            assert model == "deepseek-chat"
            assert "chunk:" in messages[1]["content"]
            return (
                '{"risk_level":"high","direction":"risk","conclusion":"needs review",'
                '"confidence":0.9,"risk_items":[],"missing_fields":[],'
                '"should_escalate_to_human":true}'
            )

    def fake_downloader(url, **kwargs):
        assert url == "https://pdf.dfcfw.com/pdf/H2_AN202605061821996143_1.pdf"
        return b"%PDF fake bytes"

    def fake_extractor(pdf_bytes, **kwargs):
        assert pdf_bytes.startswith(b"%PDF")
        return "This is a long enough evidence paragraph for smoke validation."

    result = process_candidate(
        _candidate(),
        deepseek_client=FakeClient(),
        downloader=fake_downloader,
        text_extractor=fake_extractor,
        artifact_dir=tmp_path,
        save_pdf=True,
        min_score=0.0,
    )

    assert result["status"] == "ANALYZED"
    assert result["pdf_bytes"] == len(b"%PDF fake bytes")
    assert result["deepseek_json"]["conclusion"] == "needs review"
    assert result["pdf_artifact_path"].startswith(str(tmp_path))

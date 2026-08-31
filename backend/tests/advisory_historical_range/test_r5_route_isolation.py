from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_new_ui_has_no_legacy_replay_call_or_creation_card() -> None:
    page = (ROOT / "frontend/src/app/paper-v2/advisory/page.tsx").read_text(encoding="utf-8")
    assert "advisoryApi.replay(" not in page
    assert "历史荐股生命周期回放" not in page
    assert "执行回放" not in page
    assert "在历史验证中研究" in page


def test_legacy_client_remains_compatible() -> None:
    client = (ROOT / "frontend/src/lib/api/advisory.ts").read_text(encoding="utf-8")
    assert "async replay(programId" in client
    assert "return data.replay" in client
    router = (ROOT / "backend/routers/advisory.py").read_text(encoding="utf-8")
    assert '"deprecated": True' in router
    assert '"replay": service.run_replay(' in router

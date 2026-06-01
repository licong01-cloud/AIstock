from __future__ import annotations

import json
from typing import Any

from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService


class ExternalResearchLoopLlm:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def complete(self, **kwargs: Any) -> LlmCallResult:
        self.calls.append(kwargs)
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        if len(self.calls) == 1:
            return LlmCallResult(
                content=json.dumps(
                    {
                        "tool_calls": [
                            {
                                "server_key": "aistock-external-research",
                                "tool_name": "external_research_search_web",
                                "payload_json": {"query": "HMM factor paper", "limit": 1},
                                "stable_call_id": "external_search",
                                "reason": "Need external evidence.",
                            }
                        ]
                    }
                ),
                provider="fake",
                model="fake-react",
                duration_ms=1,
                usage={},
            )
        assert "TOOL_RESULT" in joined
        assert "external_research_search_web" in joined
        assert "evidence_policy" in joined
        assert "not_final_conclusion" in joined
        return LlmCallResult(
            content=(
                "External evidence found; source=research_assistant_catalog_summary_adapter "
                "as_of=2026-06-01. It supports a hypothesis only, not a buy/sell conclusion."
            ),
            provider="fake",
            model="fake-react",
            duration_ms=1,
            usage={},
        )


def test_external_research_tool_result_is_backfilled_into_react_messages_before_answer() -> None:
    fake = ExternalResearchLoopLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)
    svc.seed_catalogs()

    result = svc.chat_turn(ChatTurnRequest(message="Search external research about HMM factor timing.", dialogue_mode_override="analysis"))

    assert len(fake.calls) >= 2
    second_messages = fake.calls[1]["messages"]
    second_joined = "\n".join(str(item.get("content", "")) for item in second_messages if isinstance(item, dict))
    assert "TOOL_RESULT" in second_joined
    assert "aistock-external-research" in second_joined
    assert "external_research_search_web" in second_joined
    assert "evidence_policy" in second_joined
    text = result["assistant_message"]["content_text"]
    assert "source=" in text
    assert "as_of=" in text
    assert "XX" not in text
    assert result["cards"]["react_grounding"]["tool_result_count"] >= 1
    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True


def test_external_save_evidence_is_preflight_only_not_approved_memory_write(monkeypatch) -> None:
    class SaveEvidenceLlm:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def complete(self, **kwargs: Any) -> LlmCallResult:
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return LlmCallResult(
                    content=json.dumps(
                        {
                            "tool_calls": [
                                {
                                    "server_key": "aistock-external-research",
                                    "tool_name": "external_research_save_evidence",
                                    "payload_json": {
                                        "evidence": {
                                            "source": "paper_search",
                                            "url": "https://example.org/paper",
                                            "as_of": "2026-06-01",
                                            "evidence_ref": "external-evidence:test",
                                            "summary": "paper evidence",
                                        },
                                        "target_branch": "external.factor.hmm",
                                    },
                                    "stable_call_id": "save_external_evidence",
                                }
                            ]
                        }
                    ),
                    provider="fake",
                    model="fake-react",
                    duration_ms=1,
                    usage={},
                )
            return LlmCallResult(
                content="Insufficient evidence: draft evidence save needs user confirmation.",
                provider="fake",
                model="fake-react",
                duration_ms=1,
                usage={},
            )

    fake = SaveEvidenceLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)
    svc.seed_catalogs()
    executed_calls: list[dict[str, Any]] = []
    memory_calls: list[dict[str, Any]] = []

    def forbidden_create_memory(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        memory_calls.append({"called": True})
        raise AssertionError("save_evidence must not create approved memory in ReAct loop")

    def forbidden_execute_action_proposal(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        executed_calls.append({"called": True})
        raise AssertionError("draft_only save_evidence must not execute inside ReAct loop")

    monkeypatch.setattr(svc, "create_memory", forbidden_create_memory)
    monkeypatch.setattr(svc, "execute_action_proposal", forbidden_execute_action_proposal)

    result = svc.chat_turn(ChatTurnRequest(message="Save this external paper evidence candidate."))

    assert executed_calls == []
    assert memory_calls == []
    assert result["cards"]["mcp_execution_result"]["executed"] is False
    assert result["cards"]["mcp_execution_result"]["status"] in {"preflight_required", "approval_required", "preflight_failed"}
    assert result["cards"]["action_proposals"]

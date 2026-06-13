from __future__ import annotations

import json
from typing import Any

from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.react_grounding import McpToolResult, tool_result_message
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService, _litellm_compatible_messages


class DeterministicToolLoopLlm:
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
                                "server_key": "aistock-factor-library",
                                "tool_name": "factor_library_list",
                                "payload_json": {"limit": 3},
                                "stable_call_id": "call_factor_list",
                                "reason": "Need audited factor catalog summary.",
                            }
                        ]
                    }
                ),
                provider="fake",
                model="fake-react",
                duration_ms=1,
                usage={"prompt_tokens": 10, "completion_tokens": 4},
            )
        assert "TOOL_RESULT" in joined
        assert "aistock-factor-library" in joined
        assert "factor_library_list" in joined
        return LlmCallResult(
            content=(
                "thought: keep private\n"
                "observation: tool returned summary\n"
                "Reflexion: retry notes stay in trace\n"
                "Final answer: factor catalog has 1 summary item; "
                "source=research_assistant_catalog_summary_adapter as_of=2026-06-01."
            ),
            provider="fake",
            model="fake-react",
            duration_ms=2,
            usage={"prompt_tokens": 12, "completion_tokens": 8},
        )


def _service(fake: DeterministicToolLoopLlm) -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)
    svc.seed_catalogs()
    return svc


def test_chat_turn_reacts_tool_result_back_into_messages_before_final_answer() -> None:
    fake = DeterministicToolLoopLlm()
    svc = _service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="List factor library entries as a compact summary."))

    assert len(fake.calls) >= 2
    second_messages = fake.calls[1]["messages"]
    second_joined = "\n".join(str(item.get("content", "")) for item in second_messages if isinstance(item, dict))
    assert "TOOL_RESULT" in second_joined
    assert "factor_library_list" in second_joined
    text = result["assistant_message"]["content_text"]
    assert "source=" not in text
    assert "as_of=" not in text
    assert "summary-first" not in text
    assert "research_assistant_catalog_summary_adapter" not in text
    lowered = text.lower()
    assert "thought:" not in lowered
    assert "observation:" not in lowered
    assert "reflexion:" not in lowered
    assert result["cards"]["react_grounding"]["tool_result_count"] >= 1
    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True
    assert any(event["event_type"] == "mcp_done" for event in result["task_events"])


def test_react_tool_observations_are_not_provider_native_tool_messages() -> None:
    message = tool_result_message(
        McpToolResult(
            server_key="aistock-factor-library",
            tool_name="factor_library_list",
            status="succeeded",
            summary="factor list",
            source_refs=["test://factor-library"],
            as_of="2026-06-01",
            stable_call_id="call_factor_list",
        )
    )

    assert message["role"] == "user"
    assert "TOOL_RESULT" in str(message["content"])
    assert "tool_call_id" not in message


def test_litellm_compatible_messages_wrap_legacy_tool_messages() -> None:
    messages = _litellm_compatible_messages(
        [
            {"role": "system", "content": "system"},
            {"role": "tool", "content": {"type": "TOOL_RESULT", "summary": "legacy observation"}},
            {"role": "user", "content": "continue"},
        ]
    )

    assert [item["role"] for item in messages] == ["system", "user", "user"]
    wrapped = json.loads(str(messages[1]["content"]))
    assert wrapped["type"] == "INTERNAL_TOOL_OBSERVATION"
    assert "legacy observation" in wrapped["content"]
    assert all(item["role"] != "tool" or item.get("tool_call_id") for item in messages)

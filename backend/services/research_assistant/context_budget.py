"""Config-driven context budget planning for Research Assistant chat turns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContextBudgetPlan:
    runtime_config: dict[str, Any]
    model_context_window_tokens: int
    safety_buffer_tokens: int
    response_reserved_tokens: int
    effective_window_tokens: int
    prompt_bundle_budget_tokens: int
    context_pack_budget_tokens: int
    compact_summary_budget_tokens: int
    fresh_tail_budget_tokens: int
    retrieved_raw_budget_tokens: int
    history_budget_tokens: int
    estimated_input_tokens: int
    utilization_ratio: float
    should_compact: bool
    mandatory_compaction: bool
    compaction_allowed_by_config: bool
    compaction_max_output_tokens: int
    llm_temperature: float
    llm_max_tokens: int
    history_page_size: int
    history_max_pages: int
    history_include_roles: set[str]
    fresh_tail_min_messages: int
    trace_response_preview_chars: int

    def as_trace_payload(self) -> dict[str, Any]:
        return {
            "model_context_window_tokens": self.model_context_window_tokens,
            "safety_buffer_tokens": self.safety_buffer_tokens,
            "response_reserved_tokens": self.response_reserved_tokens,
            "effective_window_tokens": self.effective_window_tokens,
            "prompt_bundle_budget_tokens": self.prompt_bundle_budget_tokens,
            "context_pack_budget_tokens": self.context_pack_budget_tokens,
            "compact_summary_budget_tokens": self.compact_summary_budget_tokens,
            "fresh_tail_budget_tokens": self.fresh_tail_budget_tokens,
            "retrieved_raw_budget_tokens": self.retrieved_raw_budget_tokens,
            "history_budget_tokens": self.history_budget_tokens,
            "estimated_input_tokens": self.estimated_input_tokens,
            "utilization_ratio": self.utilization_ratio,
            "should_compact": self.should_compact,
            "mandatory_compaction": self.mandatory_compaction,
            "history_page_size": self.history_page_size,
            "history_max_pages": self.history_max_pages,
            "fresh_tail_min_messages": self.fresh_tail_min_messages,
        }


class ContextBudgetPlanner:
    def estimate_tokens(self, text: str, runtime_config: dict[str, Any]) -> int:
        estimator = runtime_config["model_context"]["token_estimator"]
        chars_per_token = float(estimator["fallback_chars_per_token"])
        return max(1, int(len(text) / chars_per_token))

    def plan(
        self,
        *,
        model_profile: dict[str, Any],
        runtime_config: dict[str, Any],
        prompt_bundle_text: str = "",
        context_pack_summary: str = "",
        prior_messages: list[dict[str, Any]] | None = None,
        compact_summaries: list[dict[str, Any]] | None = None,
        current_user_message: str = "",
    ) -> ContextBudgetPlan:
        window = self._model_context_window(model_profile, runtime_config)
        safety = self._ratio_or_min(window, runtime_config["model_context"]["safety_buffer"])
        response_cfg = runtime_config["budget"]["response"]
        response_reserved = max(int(window * float(response_cfg["reserved_ratio"])), int(response_cfg["min_reserved_tokens"]))
        effective = max(1, window - safety - response_reserved)
        budget_cfg = runtime_config["budget"]
        prompt_budget = int(effective * float(budget_cfg["prompt_bundle"]["max_ratio"]))
        context_budget = max(int(effective * float(budget_cfg["context_pack"]["max_ratio"])), int(budget_cfg["context_pack"]["min_tokens"]))
        compact_budget = int(effective * float(budget_cfg["compact_summaries"]["max_ratio"]))
        fresh_budget = int(effective * float(budget_cfg["fresh_tail"]["max_ratio"]))
        raw_budget = int(effective * float(budget_cfg["retrieved_raw_snippets"]["max_ratio"]))
        history_budget = int(effective * float(budget_cfg["history"]["max_ratio"]))
        prior_messages = prior_messages or []
        compact_summaries = compact_summaries or []
        estimated = (
            self.estimate_tokens(prompt_bundle_text, runtime_config)
            + self.estimate_tokens(context_pack_summary, runtime_config)
            + self.estimate_tokens(current_user_message, runtime_config)
            + sum(self.estimate_tokens(str(item.get("content") or item.get("content_text") or ""), runtime_config) for item in prior_messages)
            + sum(self.estimate_tokens(str(item.get("content_text") or ""), runtime_config) for item in compact_summaries)
        )
        utilization = estimated / max(1, effective)
        trigger = runtime_config["compaction"]["trigger"]
        min_messages = int(trigger["min_messages_before_compaction"])
        min_turns = int(trigger["min_turns_before_compaction"])
        estimated_turns = max(1, len(prior_messages) // 2)
        compaction_enabled = bool(runtime_config["compaction"].get("enabled", True))
        compaction_age_ready = len(prior_messages) >= min_messages and estimated_turns >= min_turns
        should_compact = compaction_enabled and compaction_age_ready and utilization >= float(trigger["proactive_utilization_ratio"])
        mandatory = compaction_enabled and compaction_age_ready and utilization >= float(trigger["mandatory_utilization_ratio"])
        worker = runtime_config["compaction"]["worker"]
        return ContextBudgetPlan(
            runtime_config=runtime_config,
            model_context_window_tokens=window,
            safety_buffer_tokens=safety,
            response_reserved_tokens=response_reserved,
            effective_window_tokens=effective,
            prompt_bundle_budget_tokens=prompt_budget,
            context_pack_budget_tokens=context_budget,
            compact_summary_budget_tokens=compact_budget,
            fresh_tail_budget_tokens=fresh_budget,
            retrieved_raw_budget_tokens=raw_budget,
            history_budget_tokens=history_budget,
            estimated_input_tokens=estimated,
            utilization_ratio=utilization,
            should_compact=should_compact,
            mandatory_compaction=mandatory,
            compaction_allowed_by_config=compaction_enabled,
            compaction_max_output_tokens=max(1, int(effective * float(worker["max_output_ratio"]))),
            llm_temperature=float(worker["temperature"]),
            llm_max_tokens=int(response_cfg["max_tokens"]),
            history_page_size=int(runtime_config["history_fetch"]["page_size"]),
            history_max_pages=int(runtime_config["history_fetch"]["max_pages"]),
            history_include_roles={str(role) for role in runtime_config["history_fetch"]["include_roles"]},
            fresh_tail_min_messages=int(runtime_config["fresh_tail"]["min_messages"]),
            trace_response_preview_chars=int(runtime_config["trace"]["response_preview_chars"]),
        )

    @staticmethod
    def _model_context_window(model_profile: dict[str, Any], runtime_config: dict[str, Any]) -> int:
        for field in ("capabilities_json", "limits_json"):
            payload = model_profile.get(field) or {}
            if isinstance(payload, dict) and payload.get("context_window_tokens"):
                return int(payload["context_window_tokens"])
        return int(runtime_config["model_context"]["fallback_context_window_tokens"])

    @staticmethod
    def _ratio_or_min(window: int, config: dict[str, Any]) -> int:
        if config.get("mode") == "ratio":
            return max(int(window * float(config["ratio"])), int(config["min_tokens"]))
        return int(config["min_tokens"])

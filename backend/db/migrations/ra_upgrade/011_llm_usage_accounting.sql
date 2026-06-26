-- Research Assistant LLM usage accounting ledger.
-- Run with psql --single-transaction -v ON_ERROR_STOP=1.
-- This migration is idempotent and only creates the append-only ledger used by
-- Research Assistant token/cost observability. It must not be auto-applied by
-- runtime services.

CREATE TABLE IF NOT EXISTS assistant_llm_usage_events (
    usage_event_id TEXT PRIMARY KEY,
    trace_id TEXT NULL REFERENCES assistant_trace_events(trace_id) ON DELETE SET NULL,
    task_id TEXT NULL,
    conversation_id TEXT NULL,
    message_id TEXT NULL,
    call_group_id TEXT NULL,
    call_index INTEGER NOT NULL DEFAULT 1,
    phase TEXT NOT NULL,
    component TEXT NOT NULL DEFAULT 'research_assistant.llm',
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    model_profile_id TEXT NULL,
    litellm_model TEXT NULL,
    prompt_tokens INTEGER NULL,
    completion_tokens INTEGER NULL,
    total_tokens INTEGER NULL,
    reasoning_tokens INTEGER NULL,
    cache_creation_input_tokens INTEGER NULL,
    cache_read_input_tokens INTEGER NULL,
    prompt_tokens_estimated BOOLEAN NOT NULL DEFAULT FALSE,
    completion_tokens_estimated BOOLEAN NOT NULL DEFAULT FALSE,
    usage_source TEXT NOT NULL,
    usage_status TEXT NOT NULL DEFAULT 'recorded',
    usage_reason_code TEXT NULL,
    prompt_cost_usd NUMERIC(18, 10) NULL,
    completion_cost_usd NUMERIC(18, 10) NULL,
    total_cost_usd NUMERIC(18, 10) NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    cost_source TEXT NOT NULL DEFAULT 'unavailable',
    cost_status TEXT NOT NULL DEFAULT 'unavailable',
    cost_reason_code TEXT NULL,
    pricing_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    usage_raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    duration_ms INTEGER NULL,
    started_at TIMESTAMPTZ NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_aluer_usage_source CHECK (usage_source IN ('provider_reported','litellm_usage_object','litellm_token_counter_estimated','unavailable')),
    CONSTRAINT ck_aluer_usage_status CHECK (usage_status IN ('recorded','estimated','unavailable','failed')),
    CONSTRAINT ck_aluer_cost_status CHECK (cost_status IN ('recorded','estimated','unavailable','failed')),
    CONSTRAINT ck_aluer_nonnegative_tokens CHECK (
        COALESCE(prompt_tokens, 0) >= 0 AND
        COALESCE(completion_tokens, 0) >= 0 AND
        COALESCE(total_tokens, 0) >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_aluer_completed_at ON assistant_llm_usage_events(completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_aluer_trace ON assistant_llm_usage_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_aluer_task ON assistant_llm_usage_events(task_id);
CREATE INDEX IF NOT EXISTS idx_aluer_conversation ON assistant_llm_usage_events(conversation_id, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_aluer_model_day ON assistant_llm_usage_events(model, completed_at DESC);

COMMENT ON TABLE assistant_llm_usage_events IS 'Append-only Research Assistant LLM token and cost ledger; one row per provider call. This is the authoritative source for RA token/cost accounting.';
COMMENT ON COLUMN assistant_llm_usage_events.usage_event_id IS 'Stable llmu_* usage event id for one LLM provider call.';
COMMENT ON COLUMN assistant_llm_usage_events.trace_id IS 'Optional assistant_trace_events row associated with this LLM call.';
COMMENT ON COLUMN assistant_llm_usage_events.task_id IS 'Optional Research Assistant task id associated with this LLM call.';
COMMENT ON COLUMN assistant_llm_usage_events.conversation_id IS 'Optional conversation id associated with this LLM call.';
COMMENT ON COLUMN assistant_llm_usage_events.message_id IS 'Optional assistant message id associated with this LLM call.';
COMMENT ON COLUMN assistant_llm_usage_events.call_group_id IS 'Grouping id for calls in the same chat turn or ReAct loop.';
COMMENT ON COLUMN assistant_llm_usage_events.call_index IS 'One-based call index within call_group_id.';
COMMENT ON COLUMN assistant_llm_usage_events.phase IS 'LLM call phase such as initial_chat, react_iteration, or recovery.';
COMMENT ON COLUMN assistant_llm_usage_events.component IS 'Component that emitted the LLM call usage event.';
COMMENT ON COLUMN assistant_llm_usage_events.provider IS 'Logical provider such as deepseek, openai, or fake test provider.';
COMMENT ON COLUMN assistant_llm_usage_events.model IS 'Provider model id used for the call.';
COMMENT ON COLUMN assistant_llm_usage_events.model_profile_id IS 'Research Assistant model profile selected for the call.';
COMMENT ON COLUMN assistant_llm_usage_events.litellm_model IS 'LiteLLM model id passed to litellm.completion when available.';
COMMENT ON COLUMN assistant_llm_usage_events.prompt_tokens IS 'Prompt/input tokens reported by provider or estimated when explicitly marked.';
COMMENT ON COLUMN assistant_llm_usage_events.completion_tokens IS 'Completion/output tokens reported by provider or estimated when explicitly marked.';
COMMENT ON COLUMN assistant_llm_usage_events.total_tokens IS 'Total tokens reported by provider or derived from prompt plus completion.';
COMMENT ON COLUMN assistant_llm_usage_events.reasoning_tokens IS 'Reasoning tokens when returned by provider usage details.';
COMMENT ON COLUMN assistant_llm_usage_events.cache_creation_input_tokens IS 'Input cache creation tokens when returned by provider usage details.';
COMMENT ON COLUMN assistant_llm_usage_events.cache_read_input_tokens IS 'Input cache read tokens when returned by provider usage details.';
COMMENT ON COLUMN assistant_llm_usage_events.prompt_tokens_estimated IS 'True when prompt_tokens came from LiteLLM token_counter rather than provider billing usage.';
COMMENT ON COLUMN assistant_llm_usage_events.completion_tokens_estimated IS 'True when completion_tokens came from LiteLLM token_counter rather than provider billing usage.';
COMMENT ON COLUMN assistant_llm_usage_events.usage_source IS 'Token source: provider_reported, litellm_usage_object, litellm_token_counter_estimated, or unavailable.';
COMMENT ON COLUMN assistant_llm_usage_events.usage_status IS 'Token accounting status: recorded, estimated, unavailable, or failed.';
COMMENT ON COLUMN assistant_llm_usage_events.usage_reason_code IS 'Explicit reason code when token usage is estimated, unavailable, or failed.';
COMMENT ON COLUMN assistant_llm_usage_events.prompt_cost_usd IS 'Prompt/input cost in USD when LiteLLM or operator pricing is available.';
COMMENT ON COLUMN assistant_llm_usage_events.completion_cost_usd IS 'Completion/output cost in USD when LiteLLM or operator pricing is available.';
COMMENT ON COLUMN assistant_llm_usage_events.total_cost_usd IS 'Total USD cost when available.';
COMMENT ON COLUMN assistant_llm_usage_events.currency IS 'Cost currency; first phase records USD.';
COMMENT ON COLUMN assistant_llm_usage_events.cost_source IS 'Cost source such as litellm_model_cost or unavailable.';
COMMENT ON COLUMN assistant_llm_usage_events.cost_status IS 'Cost accounting status: recorded, estimated, unavailable, or failed.';
COMMENT ON COLUMN assistant_llm_usage_events.cost_reason_code IS 'Explicit reason code when cost is unavailable or failed.';
COMMENT ON COLUMN assistant_llm_usage_events.pricing_snapshot_json IS 'Pricing metadata snapshot used to interpret this call cost; no prompt text.';
COMMENT ON COLUMN assistant_llm_usage_events.usage_raw_json IS 'Safe JSON copy of provider/LiteLLM usage object; no prompt text.';
COMMENT ON COLUMN assistant_llm_usage_events.request_meta_json IS 'Prompt-free request metadata such as message_count and tool_schema_count.';
COMMENT ON COLUMN assistant_llm_usage_events.response_meta_json IS 'Prompt-free response metadata such as finish_reason, content_chars and tool_call_count.';
COMMENT ON COLUMN assistant_llm_usage_events.duration_ms IS 'Provider call duration in milliseconds.';
COMMENT ON COLUMN assistant_llm_usage_events.started_at IS 'Optional provider call start timestamp.';
COMMENT ON COLUMN assistant_llm_usage_events.completed_at IS 'Provider call completion timestamp.';
COMMENT ON COLUMN assistant_llm_usage_events.created_at IS 'Ledger row creation timestamp.';

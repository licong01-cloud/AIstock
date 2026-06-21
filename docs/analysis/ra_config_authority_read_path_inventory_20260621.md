# RA declarative config read-path inventory (2026-06-21)

## Scope

本盘点用于 B1-P1 Level-1 配置重构前的安全网，只记录 Research Assistant 声明式配置的读取路径，不改变任何产品逻辑。
重点字段包括 `mcp_tool_refs`、`skill_refs`、`allowed_tool_side_effect`、dialogue mode、prompt node、mode router、required confirmation token。

## Cross-check

- `rtk proxy rg -n "active_runtime_config\(|active_runtime_config_activation\(|runtime_config_activations|config_json|workflow_capabilities|mcp_tool_refs|skill_refs|allowed_tool_side_effect|dialogue_modes|mode_router|prompt_nodes|required_confirmations|required_confirmation_text|confirmation_text" backend/services/research_assistant/service.py backend/services/research_assistant/execution.py backend/services/research_assistant/runtime_config.py backend/services/research_assistant/mcp_catalog_sync.py backend/services/research_assistant/agent_teams -g "*.py"`
- `rtk proxy rg -n "prompt_nodes|allowed_tool_side_effect|workflow_capabilities|mcp_tool_refs|skill_refs|required_confirmations|mode_router|dialogue_modes|confirmation" configs/research_assistant -g "*.yaml"`
- `rtk proxy rg -n "mcp_tool_refs|skill_refs|allowed_tool_side_effect|dialogue_modes|mode_router|prompt_nodes|required_confirmations|required_confirmation_text|confirmation_text|runtime_config_activations|active_runtime_config\(" backend configs scripts tests -g "*.py" -g "*.yaml" -g "*.yml" -g "*.md"`

## Inventory table

| file:line | Read what | Source | Canonicalize / validation |
|---|---|---|---|
| `configs/research_assistant/runtime_context.yaml:133` | `dialogue_modes.default_mode` and mode map | YAML | Validated by `runtime_config.py:151` and `runtime_config.py:308`; no ref canonicalize needed. |
| `configs/research_assistant/runtime_context.yaml:137` | `dialogue.modes.dialogue.prompt_nodes` | YAML | Validated as non-empty string list by `runtime_config.py:324`; consumed from active runtime config by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:140` | `dialogue.modes.dialogue.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; consumed by `service.py:2966`. |
| `configs/research_assistant/runtime_context.yaml:160` | `dialogue.modes.analysis.prompt_nodes` | YAML | Validated by `runtime_config.py:324`; consumed by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:163` | `dialogue.modes.analysis.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; used by `service.py:2966` and tool gating at `service.py:4331`. |
| `configs/research_assistant/runtime_context.yaml:180` | `dialogue.modes.planning.prompt_nodes` | YAML | Validated by `runtime_config.py:324`; consumed by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:183` | `dialogue.modes.planning.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; used by `service.py:2966` and tool gating at `service.py:4331`. |
| `configs/research_assistant/runtime_context.yaml:193` | `dialogue.modes.preflight.prompt_nodes` | YAML | Validated by `runtime_config.py:324`; consumed by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:196` | `dialogue.modes.preflight.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; used by `service.py:2966`. |
| `configs/research_assistant/runtime_context.yaml:207` | `dialogue.modes.execution.prompt_nodes` | YAML | Validated by `runtime_config.py:324`; consumed by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:210` | `dialogue.modes.execution.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; used by `service.py:2966`. |
| `configs/research_assistant/runtime_context.yaml:221` | `dialogue.modes.audit.prompt_nodes` | YAML | Validated by `runtime_config.py:324`; consumed by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:224` | `dialogue.modes.audit.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; audit mode does not expose function tools at `service.py:4331`. |
| `configs/research_assistant/runtime_context.yaml:234` | `dialogue.modes.recovery.prompt_nodes` | YAML | Validated by `runtime_config.py:324`; consumed by `service.py:2815`. |
| `configs/research_assistant/runtime_context.yaml:237` | `dialogue.modes.recovery.allowed_tool_side_effect` | YAML | Runtime config is validated before active read; recovery mode does not expose function tools at `service.py:4331`. |
| `configs/research_assistant/runtime_context.yaml:247` | `mode_router.confidence_thresholds`, fallback, user override patterns | YAML | Validated by `runtime_config.py:152` and `runtime_config.py:334`; consumed by `service.py:2884`, `service.py:2914`, and `service.py:2987`. |
| `configs/research_assistant/runtime_context.yaml:566` | `planner.workflow_capabilities` list | YAML | Validated by `runtime_config.py:216`; read into defaults by `service.py:867`; active DB copy is revalidated by `service.py:2068`. |
| `configs/research_assistant/runtime_context.yaml:577` | Capability `required_confirmations` | YAML | Validated as list by `runtime_config.py:226`; consumed after canonical capability flow by `execution.py:206` and `service.py:5542`. |
| `configs/research_assistant/runtime_context.yaml:582` | Capability `mcp_tool_refs` | YAML | Validated by `runtime_config.py:238`; canonicalized by `service.py:1852` through `_workflow_capabilities()`. |
| `configs/research_assistant/runtime_context.yaml:585` | Capability `skill_refs` | YAML | Validated by `runtime_config.py:226`; canonicalized by `service.py:1906` through `_workflow_capabilities()`. |
| `configs/research_assistant/runtime_context.yaml:623` | Side-effecting capability confirmation tokens | YAML | Validated as list by `runtime_config.py:226`; consumed by action proposal confirmation gates at `execution.py:373` and `execution.py:477`. |
| `configs/research_assistant/runtime_context.yaml:874` | `skill_library.reuse.required_confirmations` | YAML | Validated as list by `runtime_config.py:226`; consumed through capability sync and action proposal gating. |
| `prompt_packs/research_assistant/main/pack.yaml:1` | Prompt pack schema and node declarations | YAML | Validated by `prompt_pack.py:75`; prompt files are loaded and checksummed by `prompt_pack.py:33`. |
| `prompt_packs/research_assistant/main/pack.yaml:11` | Prompt node keys, tree path, phase, prompt file refs | YAML | Validated by `prompt_pack.py:75`; seeded into DB by `service.py:2473`; selected from DB by `service.py:2732` and `service.py:2815`. |
| `configs/research_assistant/agent_teams.yaml:8` | Agent team orchestrator `prompt_nodes` | YAML | Parsed by `agent_teams/config.py:27`; list-shaped by `_as_tuple`; consumed by `agent_teams/runtime.py:94`. |
| `configs/research_assistant/agent_teams.yaml:33` | Worker `qe_experiment_designer.prompt_nodes` | YAML | Parsed by `agent_teams/config.py:66`; consumed by `agent_teams/runtime.py:94`. |
| `configs/research_assistant/agent_teams.yaml:52` | Worker `hmm_evolution.prompt_nodes` | YAML | Parsed by `agent_teams/config.py:66`; consumed by `agent_teams/runtime.py:94`. |
| `configs/research_assistant/agent_teams.yaml:73` | Worker `factor_developer.prompt_nodes` | YAML | Parsed by `agent_teams/config.py:66`; consumed by `agent_teams/runtime.py:94`. |
| `configs/research_assistant/agent_teams.yaml:93` | Worker `local_data_doctor.prompt_nodes` | YAML | Parsed by `agent_teams/config.py:66`; consumed by `agent_teams/runtime.py:94`. |
| `backend/services/research_assistant/runtime_config.py:75` | YAML runtime config loader | YAML | Calls `validate_runtime_config_payload()` before returning `RuntimeConfigSnapshot`. |
| `backend/services/research_assistant/runtime_config.py:151` | `dialogue_modes` validation entrypoint | YAML or active DB payload | Validates shape; no canonicalize needed. |
| `backend/services/research_assistant/runtime_config.py:216` | `planner.workflow_capabilities` validation entrypoint | YAML or active DB payload | Validates list/object shape plus ref list fields; catches BUG-431 class before active read proceeds. |
| `backend/services/research_assistant/runtime_config.py:238` | `mcp_tool_refs` list and object-entry validation | YAML or active DB payload | Type validation only; server key canonicalize happens later in `service.py:1852`. |
| `backend/services/research_assistant/runtime_config.py:249` | `skill_refs` and other list-field entry validation | YAML or active DB payload | Type validation only; `skill_refs` canonicalized by `service.py:1906`. |
| `backend/services/research_assistant/runtime_config.py:308` | Dialogue mode prompt-node validation | YAML or active DB payload | Validates mode map and prompt-node list shape. |
| `backend/services/research_assistant/runtime_config.py:334` | Mode router validation | YAML or active DB payload | Validates thresholds, fallback, and override lists. |
| `backend/services/research_assistant/service.py:867` | `DEFAULT_WORKFLOW_CAPABILITIES` from YAML plus manifest-derived capabilities | YAML + manifest | Filter for retired `issue.create_candidate`; later canonicalized by `_workflow_capabilities()`. |
| `backend/services/research_assistant/service.py:1228` | `default_workflow_capabilities()` | In-memory default derived from YAML + manifest | Caller-dependent; safe path is `_workflow_capabilities()` canonicalization. |
| `backend/services/research_assistant/service.py:1828` | Active `planner.workflow_capabilities` | Active DB `runtime_config_activations.config_json` | `active_runtime_config()` validates DB payload first; `_workflow_capabilities()` canonicalizes `mcp_tool_refs` and `skill_refs`. |
| `backend/services/research_assistant/service.py:1852` | Capability `mcp_tool_refs` | Workflow capability object | Canonicalize: validates list/object entries, canonicalizes `server_key`, raises loud config error on invalid non-empty type. |
| `backend/services/research_assistant/service.py:1906` | Capability `skill_refs` | Workflow capability object | Canonicalize: validates list/string entries, raises loud config error on invalid non-empty type. |
| `backend/services/research_assistant/service.py:2068` | Active runtime config `config_json` | DB: `runtime_config_activations` | Fail-closed validation via `validate_runtime_config_payload()`; not a raw bypass after BUG-431. |
| `backend/services/research_assistant/service.py:2144` | Active runtime config activation lookup | DB: `runtime_config_activations` | Direct activation row read; `config_json` consumer must go through `active_runtime_config()`. |
| `backend/services/research_assistant/service.py:2162` | `query_limits.*` through `configured_limit()` | Active DB runtime config | Validated by `active_runtime_config()` before limit use. |
| `backend/services/research_assistant/service.py:2252` | `load_runtime_config()` during seed | YAML | YAML validated before seeding `runtime_config_sources` and `runtime_config_activations`. |
| `backend/services/research_assistant/service.py:2368` | Capability sync source `mcp_tool_refs` and `skill_refs` | `_workflow_capabilities()` | Already canonicalized; sync refuses invalid existing DB ref shapes at `service.py:4211`. |
| `backend/services/research_assistant/service.py:2405` | `capability_sync` config | Active DB runtime config | Validated by `active_runtime_config()`; no ref canonicalize needed. |
| `backend/services/research_assistant/service.py:2473` | Prompt pack nodes | YAML prompt pack | Validated by `prompt_pack.py:75`; seeded into DB prompt tables. |
| `backend/services/research_assistant/service.py:2557` | Runtime config source and activation seeding | YAML runtime config | Uses `load_runtime_config()` output; writes validated config snapshot to DB. |
| `backend/services/research_assistant/service.py:2732` | Prompt bundle active prompt activation and prompt nodes | DB: `prompt_activations`, `prompt_nodes` | Prompt selection is DB-backed; prompt-node list in mode config comes from validated `active_runtime_config()`. |
| `backend/services/research_assistant/service.py:2815` | Mode-specific `prompt_nodes` | Active DB runtime config | Shape validated by `runtime_config.py:308`; no canonicalize needed. |
| `backend/services/research_assistant/service.py:2865` | `dialogue_intent` runtime config | Active DB runtime config | `active_runtime_config()` validation first; returns `{}` if wrong shape. |
| `backend/services/research_assistant/service.py:2871` | `dialogue_modes` runtime config | Active DB runtime config | `active_runtime_config()` validation first; consumed by mode and prompt selection. |
| `backend/services/research_assistant/service.py:2884` | `mode_router` runtime config | Active DB runtime config | `active_runtime_config()` validation first; consumed by mode router. |
| `backend/services/research_assistant/service.py:2914` | Mode-router overrides and thresholds | Active DB runtime config | Validated by `runtime_config.py:334`; no ref canonicalize needed. |
| `backend/services/research_assistant/service.py:2966` | Mode `allowed_tool_side_effect`, approval flags | Active DB runtime config | Validated mode map first; determines available tool side-effect ceiling. |
| `backend/services/research_assistant/service.py:3142` | Chat-turn runtime activation and runtime config | DB activation + validated active DB runtime config | Uses `active_runtime_config_activation()` for trace id and `active_runtime_config()` for validated config. |
| `backend/services/research_assistant/service.py:4294` | Approved capability `mcp_tool_refs` | DB: `capabilities`, plus `_workflow_capabilities()` | Canonicalizes each capability via `_canonicalize_capability_mcp_refs()` before building executable set. |
| `backend/services/research_assistant/service.py:4331` | Function-tool supply gated by `allowed_tool_side_effect` | Capability-backed manifest tools + mode decision | Uses executable refs from canonicalized approved capabilities; side-effect filtering is direct from tool rows. |
| `backend/services/research_assistant/service.py:4811` | Prompt text by prompt key | DB: `prompt_nodes` | Direct DB prompt-node read; not a capability ref path. Loud error if missing. |
| `backend/services/research_assistant/service.py:4831` | Human-card capability keys and mode config | DB: `capabilities`; active DB runtime config for key lists | Runtime config validated; capability rows are direct DB read for cards, not canonicalized there. |
| `backend/services/research_assistant/service.py:5135` | ReAct tool catalog `required_confirmations` | DB/manifest `mcp_tools` | Direct list coercion from tool row; no capability ref canonicalize. |
| `backend/services/research_assistant/service.py:5327` | `_capability_has_tool_ref()` reads `mcp_tool_refs` | Capability dict, often DB `capabilities` | **Bypass risk:** direct read treats non-list as empty; canonicalize only if caller did it first. |
| `backend/services/research_assistant/service.py:5364` | `_capability_key_for_tool()` searches DB capabilities and calls `_capability_allows_tool()` | DB: `capabilities` | **Bypass risk:** falls through to `_capability_has_tool_ref()` on raw DB rows. |
| `backend/services/research_assistant/service.py:5456` | Read-only auto-execution capability coverage | DB `capabilities` plus approved ref set | Coverage set uses canonicalized `_approved_capability_mcp_tool_refs()`; domain capability existence is direct DB read. |
| `backend/services/research_assistant/service.py:5542` | Capability cards `required_confirmations`, risk, side effect | DB: `capabilities` | Direct DB presentation read; no ref canonicalize. |
| `backend/services/research_assistant/service.py:6094` | Context-pack `memory_tree` config | Active DB runtime config | Validated by `active_runtime_config()` before use. |
| `backend/services/research_assistant/service.py:6504` | Preflight tool risk, side effect, required confirmations | DB/manifest `mcp_tools` | Direct DB tool-row read; no capability ref canonicalize. |
| `backend/services/research_assistant/service.py:6794` | Model routing policies matched against runtime config | Active DB runtime config + DB `routing_policies` | Runtime config validated before selector use. |
| `backend/services/research_assistant/service.py:7286` | Prompt text for prompt lab target key | DB: `prompt_node_versions`, fallback `prompt_nodes` | Direct DB prompt text read; no capability ref canonicalize. |
| `backend/services/research_assistant/execution.py:43` | Default workflow capability refs during execution-mixin sync | In-memory default derived from YAML + manifest | Direct default read, not active DB; service override at `service.py:2368` uses `_workflow_capabilities()`. |
| `backend/services/research_assistant/execution.py:85` | `capability_sync` config | Active DB runtime config | Validated by `active_runtime_config()`. |
| `backend/services/research_assistant/execution.py:146` | `_capability_tool_refs()` reads capability `mcp_tool_refs` | DB: `capabilities` in action proposal paths | **Bypass risk:** `list(capability.get(...))` without canonicalize; malformed DB rows can behave unexpectedly. |
| `backend/services/research_assistant/execution.py:206` | Effective `required_confirmations`, risk, side effect | DB: `capabilities` or `mcp_tools` | Direct DB row read; low/read-only clears confirmations. |
| `backend/services/research_assistant/execution.py:222` | Execution timeout/retry policy | Active DB runtime config | Validated by `active_runtime_config()`. |
| `backend/services/research_assistant/execution.py:244` | Proposal digest includes refs and confirmations | DB: selected capability row | Direct DB row read; no canonicalize at digest construction. |
| `backend/services/research_assistant/execution.py:270` | Action proposal capability/tool resolution | DB: `capabilities`, `mcp_tools`; active prompt/runtime activations | Capability tool refs use `_capability_tool_refs()` bypass risk; active runtime config used for approval TTL is validated. |
| `backend/services/research_assistant/execution.py:368` | Proposal confirmation token check | DB: `capabilities`, `mcp_tools`, `action_proposals` | Direct `required_confirmations` from effective profile; no capability ref canonicalize. |
| `backend/services/research_assistant/execution.py:472` | Approval confirmation token check | DB: `capabilities`, `mcp_tools`, `action_proposals` | Direct `required_confirmations` from effective profile; no capability ref canonicalize. |
| `backend/services/research_assistant/mcp_catalog_sync.py:373` | Manifest-derived tool `required_confirmations` | Manifest constants | Generated from manifest/confirmation map; no runtime canonicalize. |
| `backend/services/research_assistant/mcp_catalog_sync.py:500` | Manifest-derived tool risk, side effect, approval, confirmations | Manifest constants | Generated catalog row; no runtime canonicalize. |
| `backend/services/research_assistant/mcp_catalog_sync.py:587` | Manifest-derived workflow capabilities and `mcp_tool_refs` | Manifest/domain ontology | Generated refs are plain dicts; canonicalized when merged by `_workflow_capabilities()`. |
| `backend/services/research_assistant/agent_teams/config.py:27` | Agent team YAML | YAML | Parser validates mapping; `_as_tuple` shapes `prompt_nodes`, `allowed_servers`, `allowed_tools`. |
| `backend/services/research_assistant/agent_teams/config.py:66` | Agent team worker `prompt_nodes` | YAML | `_as_tuple` list conversion; no runtime canonicalize needed. |
| `backend/services/research_assistant/agent_teams/runtime.py:94` | Worker `prompt_nodes` in worker input | Parsed YAML config | Direct consumption of parsed tuple. |
| `backend/routers/research_assistant.py:241` | API lists prompt nodes | DB: `prompt_nodes` | Direct read through `service.list_records()`; presentation route. |
| `backend/routers/research_assistant.py:271` | API lists runtime config activations | DB: `runtime_config_activations` | Direct presentation read; does not validate/canonicalize `config_json`. |
| `backend/routers/research_assistant.py:672` | API lists capabilities | DB: `capabilities` | Direct presentation read; no ref canonicalize. |
| `backend/mcp/modules/research_assistant.py:69` | MCP lists prompt nodes | DB: `prompt_nodes` | Direct presentation read through service/router facade. |
| `backend/mcp/modules/research_assistant.py:99` | MCP lists tools | DB/manifest tool catalog | Direct presentation read; no capability ref canonicalize. |

## DB direct reads that bypass capability canonicalize

These are the read paths closest to the BUG-431 failure mode because they consume DB-backed declarative capability/tool data without first passing through `_workflow_capabilities()` plus `_canonicalize_capability_*`.

| file:line | Object read | Risk note |
|---|---|---|
| `backend/services/research_assistant/service.py:5327` | `capability["mcp_tool_refs"]` | Non-list is treated as `[]`; this can hide a malformed DB capability unless an earlier catalog-ready or approved-ref path canonicalizes it. |
| `backend/services/research_assistant/service.py:5364` | DB `capabilities` inside `_capability_key_for_tool()` | Calls `_capability_allows_tool()` / `_capability_has_tool_ref()` on raw DB rows; malformed refs can become false negatives. |
| `backend/services/research_assistant/execution.py:146` | `capability["mcp_tool_refs"]` inside action resolution | Uses `list(capability.get("mcp_tool_refs") or [])`; malformed DB values can produce surprising iteration or empty resolution. |
| `backend/services/research_assistant/execution.py:206` | `required_confirmations`, `risk_level`, `side_effect_level` from capability/tool rows | Direct DB row profile read; malformed confirmation shape is not canonicalized here. |
| `backend/services/research_assistant/execution.py:244` | Proposal digest over `mcp_tool_refs`, `skill_refs`, `required_confirmations` | Direct DB row values are part of the digest. |
| `backend/services/research_assistant/service.py:5135` | Tool catalog `required_confirmations` | Direct DB/manifest tool-row read for ReAct tool entries. |
| `backend/services/research_assistant/service.py:5542` | Capability card `required_confirmations` | Direct DB presentation read. |
| `backend/services/research_assistant/service.py:6504` | Preflight risk/side-effect/required confirmations | Direct `mcp_tools` read; not a capability ref path but still declarative gate data. |
| `backend/routers/research_assistant.py:271` | `runtime_config_activations.config_json` presentation list | Direct DB presentation read; unlike `active_runtime_config()`, this route does not validate before returning records. |
| `backend/routers/research_assistant.py:672` | `capabilities.mcp_tool_refs` presentation list | Direct DB presentation read; no canonicalize. |

## Characterization snapshot covered by the new test

`backend/tests/research_assistant/test_config_authority_characterization.py` pins the current supplied behavior from repository YAML/manifest:

- Seeded catalogs: 1 active runtime config, 27 approved capabilities, 378 MCP tools.
- Runtime workflow capability keys: 27, with retired `issue.create_candidate` filtered.
- Approved capability-backed executable refs: 109.
- External research and stock analysis orchestration refs are both present, including `external_research_search_web` and `external_research_fetch_extract`.
- Mode gating snapshot: dialogue/audit/recovery expose 0 tools, analysis exposes 78 read-only tools, planning/preflight/execution expose 109 capability-backed tools.
- Dialogue modes pin prompt-node lists, side-effect ceilings, approval flags, and representative mode-router decisions.

"""Advisory Center MCP wrappers over /api/v1/advisory."""

from __future__ import annotations

from ._gateway_specs import ToolSpec, register_spec_tools

CREATE_ADVISORY_PROGRAM_CONFIRM = "CREATE_ADVISORY_PROGRAM"
UPDATE_ADVISORY_PROGRAM_CONFIRM = "UPDATE_ADVISORY_PROGRAM"
APPLY_ADVISORY_BINDING_CONFIRM = "APPLY_ADVISORY_BINDING"
CHANGE_ADVISORY_STATUS_CONFIRM = "CHANGE_ADVISORY_PROGRAM_STATUS"
CLONE_ADVISORY_PROGRAM_CONFIRM = "CLONE_ADVISORY_PROGRAM"
RUN_ADVISORY_REVIEW_CONFIRM = "RUN_ADVISORY_REVIEW"
RUN_ADVISORY_REPLAY_CONFIRM = "RUN_ADVISORY_REPLAY"

SPECS = (
    ToolSpec("advisory_list_programs", "GET", "/programs", query_defaults={"include_archived": False}),
    ToolSpec("advisory_get_program", "GET", "/programs/{program_id}", path_params=("program_id",)),
    ToolSpec("advisory_list_bindings", "GET", "/programs/{program_id}/bindings", path_params=("program_id",)),
    ToolSpec("advisory_get_active_binding", "GET", "/programs/{program_id}/bindings/active", path_params=("program_id",)),
    ToolSpec("advisory_get_leaderboard", "GET", "/leaderboard", query_defaults={"sort_by": "win_rate", "include_archived": False}),
    ToolSpec("advisory_get_active_pool", "GET", "/programs/{program_id}/active-pool", path_params=("program_id",)),
    ToolSpec("advisory_list_reviews", "GET", "/programs/{program_id}/reviews", path_params=("program_id",), query_defaults={"limit": 20, "offset": 0}, limit_caps={"limit": 500}),
    ToolSpec("advisory_list_versions", "GET", "/programs/{program_id}/list-versions", path_params=("program_id",), query_defaults={"limit": 20, "offset": 0}, limit_caps={"limit": 500}),
    ToolSpec("advisory_get_list_version", "GET", "/list-versions/{list_version_id}", path_params=("list_version_id",)),
    ToolSpec("advisory_get_returns", "GET", "/programs/{program_id}/returns", path_params=("program_id",)),
    ToolSpec("advisory_preview_review", "POST", "/programs/{program_id}/reviews/preview", path_params=("program_id",)),
    ToolSpec("advisory_quality_report", "POST", "/quality-report"),
    ToolSpec("advisory_create_program_confirmed", "POST", "/programs", confirm_token=CREATE_ADVISORY_PROGRAM_CONFIRM),
    ToolSpec("advisory_update_program_confirmed", "PATCH", "/programs/{program_id}", path_params=("program_id",), confirm_token=UPDATE_ADVISORY_PROGRAM_CONFIRM),
    ToolSpec("advisory_apply_binding_confirmed", "POST", "/programs/{program_id}/bindings/apply", path_params=("program_id",), confirm_token=APPLY_ADVISORY_BINDING_CONFIRM),
    ToolSpec("advisory_set_status_confirmed", "POST", "/programs/{program_id}/status", path_params=("program_id",), confirm_token=CHANGE_ADVISORY_STATUS_CONFIRM),
    ToolSpec("advisory_enable_program_confirmed", "POST", "/programs/{program_id}/enable", path_params=("program_id",), confirm_token=CHANGE_ADVISORY_STATUS_CONFIRM),
    ToolSpec("advisory_pause_program_confirmed", "POST", "/programs/{program_id}/pause", path_params=("program_id",), confirm_token=CHANGE_ADVISORY_STATUS_CONFIRM),
    ToolSpec("advisory_archive_program_confirmed", "POST", "/programs/{program_id}/archive", path_params=("program_id",), confirm_token=CHANGE_ADVISORY_STATUS_CONFIRM),
    ToolSpec("advisory_clone_program_confirmed", "POST", "/programs/{program_id}/clone", path_params=("program_id",), confirm_token=CLONE_ADVISORY_PROGRAM_CONFIRM),
    ToolSpec("advisory_run_review_confirmed", "POST", "/programs/{program_id}/reviews/run", path_params=("program_id",), confirm_token=RUN_ADVISORY_REVIEW_CONFIRM),
    ToolSpec("advisory_run_replay_confirmed", "POST", "/programs/{program_id}/replay", path_params=("program_id",), confirm_token=RUN_ADVISORY_REPLAY_CONFIRM),
)

TOOL_NAMES = tuple(spec.name for spec in SPECS)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry) -> None:
    register_spec_tools(registry, module_name="advisory", client_prefix="advisory", specs=SPECS)

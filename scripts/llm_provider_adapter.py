"""Provider adapter for AIstock validation LLM triage.

Phase 1 is deliberately provider/config focused: it validates model selection,
credential resolution, and redaction without writing GitHub Issues or BUG JSON.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.infra.deepseek_config import (  # noqa: E402
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekConfigError,
    redact_secret_text,
    resolve_deepseek_config,
)
from backend.services.validation.plan_catalog import (  # noqa: E402
    FORBIDDEN_BACKEND_PORTS,
    ValidationCatalogError,
    ValidationPlanCatalog,
)

GITHUB_MODELS_DEEPSEEK_MODEL_FAMILY = "deepseek-r1"
GITHUB_MODELS_DEEPSEEK_MODEL_ID = "deepseek/deepseek-r1"
TRIAGE_ADVICE_SCHEMA_VERSION = "aistock_deepseek_triage_advice_v1"
TEST_PLAN_ADVICE_SCHEMA_VERSION = "aistock_deepseek_test_plan_advice_v1"
NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION = "aistock_deepseek_nightly_scheduler_advice_v1"
PROMPT_EVALUATION_SCHEMA_VERSION = "aistock_validation_llm_prompt_evaluation_v1"
GUARDED_ROLLOUT_SCHEMA_VERSION = "aistock_validation_llm_guarded_rollout_v1"
LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION = "aistock_llm_invocation_evidence_v1"
ADVISORY_LLM_PURPOSES = {"test_plan_advice", "nightly_scheduler_advice"}
FORBIDDEN_FRONTEND_PORTS = {3000}
FORBIDDEN_MARKET_DATA_PORTS = {19080}
SHELL_COMMAND_FIELDS = {"command", "shell_command", "nox_command", "run_command"}
CODEGRAPH_FRESHNESS_VALUES = {"fresh", "stale", "missing", "unknown"}
GUARDED_ROLLOUT_MODES = {"off", "warning_only", "opt_in_auto_file"}
DEEPSEEK_ENV_FILE_ENV_VARS = ("AISTOCK_LLM_ENV_FILE", "AISTOCK_ENV_FILE")
DEEPSEEK_ENV_ROOT_VARS = ("AISTOCK_SELF_HOSTED_SOURCE", "AISTOCK_CANONICAL_ROOT", "AISTOCK_ROOT")
DEEPSEEK_ENV_KEYS = ("DEEPSEEK_API_KEY", "DEEPSEEK_BASE_URL")
EVALUATION_INFRA_SIGNATURE_TOKENS = (
    "self-hosted runner unavailable",
    "runner unavailable",
    "dependency installation timeout",
    "github rate limit",
    "rate limit",
    "checkout failed",
    "artifact upload failed",
)
EVALUATION_ISSUE_BODY_SECTIONS = (
    "Failure Summary",
    "Regression Locator",
    "Agent Handoff",
    "Token Policy",
    "Production Gates",
)
EVALUATION_MODULE_PLAN_MAP = {
    "validation.runner": ["validation_catalog_integrity"],
    "validation.center": ["validation_center_backend"],
    "qe.archive": ["qe_archive_backend"],
    "qe": ["qe_mcp_backend"],
    "paper_v2_selection_center": ["simulation_core_l2"],
    "research_assistant": ["research_assistant_backend"],
    "model_registry": ["model_registry_backend"],
    "market.regime_label": ["market_regime_label"],
    "rl_execution": ["rl_execution_smoke"],
    "local_data": ["data_sync_autonomy_backend"],
}
_BOOTSTRAPPED_DEEPSEEK_ENV_FILES: set[str] = set()


DEFAULT_CONFIG_PATH = ROOT / "configs" / "validation" / "llm_triage.yaml"
DEFAULT_PROMPT_PACK_ROOT = ROOT / "prompt_packs" / "validation_llm"
DEFAULT_EVALUATION_CASES = DEFAULT_PROMPT_PACK_ROOT / "evaluation_cases" / "historical_failure_fixtures.json"
EVALUATION_BLOCKING_PATH_PREFIXES = ("prompt_packs/validation_llm/", "configs/validation/llm_triage.yaml")
EVALUATION_BLOCKING_FILES = ("scripts/llm_provider_adapter.py",)


class ProviderAdapterError(RuntimeError):
    """Raised when provider config or model selection fails closed."""


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    if not isinstance(config, dict):
        raise ProviderAdapterError("llm triage config must be a mapping")
    return config


def _candidate_deepseek_env_files() -> list[Path]:
    candidates: list[Path] = []
    for env_name in DEEPSEEK_ENV_FILE_ENV_VARS:
        raw = os.environ.get(env_name)
        if raw:
            candidates.append(Path(raw).expanduser())
    for env_name in DEEPSEEK_ENV_ROOT_VARS:
        raw_root = os.environ.get(env_name)
        if raw_root:
            candidates.append(Path(raw_root).expanduser() / ".env")
    candidates.append(ROOT / ".env")
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return unique


def _bootstrap_deepseek_env() -> list[str]:
    loaded_sources: list[str] = []
    for candidate in _candidate_deepseek_env_files():
        candidate_key = str(candidate.resolve(strict=False))
        if candidate_key in _BOOTSTRAPPED_DEEPSEEK_ENV_FILES:
            continue
        if not candidate.exists() or not candidate.is_file():
            continue
        _BOOTSTRAPPED_DEEPSEEK_ENV_FILES.add(candidate_key)
        try:
            lines = candidate.read_text(encoding="utf-8-sig", errors="replace").splitlines()
        except OSError:
            continue
        loaded_any = False
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key.lower().startswith("export "):
                key = key[7:].strip()
            if key not in DEEPSEEK_ENV_KEYS:
                continue
            value = value.strip()
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            if not os.environ.get(key):
                os.environ[key] = value
                loaded_any = True
        if loaded_any:
            loaded_sources.append(str(candidate))
    return loaded_sources


def validate_config(config: dict[str, Any]) -> None:
    _bootstrap_deepseek_env()
    if config.get("schema_version") != "aistock_llm_triage_config_v1":
        raise ProviderAdapterError("unsupported llm triage config schema_version")
    providers = config.get("providers")
    if not isinstance(providers, dict):
        raise ProviderAdapterError("providers must be configured")
    deepseek_api = providers.get("deepseek_api") or {}
    if deepseek_api.get("model") != DEFAULT_DEEPSEEK_MODEL:
        raise ProviderAdapterError("deepseek_api.model must be deepseek-v4-pro")
    if not isinstance(deepseek_api.get("enabled"), bool):
        raise ProviderAdapterError("deepseek_api.enabled must be a boolean")
    github_models = providers.get("github_models") or {}
    selector = (github_models.get("model_selector") or {})
    if selector.get("required_model_family") != GITHUB_MODELS_DEEPSEEK_MODEL_FAMILY:
        raise ProviderAdapterError("github_models required_model_family must be deepseek-r1")
    preferred_models = selector.get("preferred_models") or []
    if GITHUB_MODELS_DEEPSEEK_MODEL_ID not in preferred_models:
        raise ProviderAdapterError("github_models preferred_models must include deepseek/deepseek-r1")
    if selector.get("allow_lower_tier_fallback") is not False:
        raise ProviderAdapterError("lower-tier model fallback must be disabled")
    limits = config.get("limits") or {}
    if limits.get("fail_closed_when_schema_invalid") is not True:
        raise ProviderAdapterError("invalid schema handling must fail closed")
    rollout = config.get("guarded_rollout") or {}
    if rollout:
        default_mode = str(rollout.get("default_mode") or "")
        if default_mode not in GUARDED_ROLLOUT_MODES:
            raise ProviderAdapterError("guarded_rollout.default_mode invalid")
        supported_modes = set(str(item) for item in rollout.get("supported_modes") or [])
        if supported_modes and not GUARDED_ROLLOUT_MODES.issubset(supported_modes):
            raise ProviderAdapterError("guarded_rollout.supported_modes must include all guarded rollout modes")
        if rollout.get("deterministic_auto_file_preserved") is not True:
            raise ProviderAdapterError("guarded rollout must preserve deterministic auto-file behavior")


def _model_id(model: dict[str, Any]) -> str:
    return str(model.get("id") or model.get("model_id") or model.get("name") or "").strip()


def _publisher(model: dict[str, Any]) -> str:
    publisher = model.get("publisher")
    if isinstance(publisher, dict):
        return str(publisher.get("name") or publisher.get("id") or "").strip()
    return str(publisher or model.get("publisher_name") or "").strip()


def _capabilities(model: dict[str, Any]) -> list[str]:
    raw = model.get("capabilities") or model.get("supported_capabilities") or []
    if isinstance(raw, dict):
        return sorted(str(key) for key, enabled in raw.items() if enabled)
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return []


def normalize_catalog(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = payload.get("models") or payload.get("data") or payload.get("items") or []
    else:
        items = []
    return [item for item in items if isinstance(item, dict)]


def parse_json_response(text: str) -> dict[str, Any]:
    """Parse provider JSON output and fail closed on malformed/non-object data."""

    payload = _parse_json_value_response(text)
    if isinstance(payload, dict):
        return payload
    raise ProviderAdapterError("provider output JSON schema invalid")


def _parse_json_value_response(text: str) -> Any:
    """Parse a single JSON value embedded in provider prose."""

    text = str(text or "")
    candidates = [text]
    fence = re.search(r"```(?:json)?\s*([\[{].*?[\]}])\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence:
        candidates.append(fence.group(1))
    extracted_object = _extract_json_object(text)
    if extracted_object:
        candidates.append(extracted_object)
    extracted_array = _extract_json_array(text)
    if extracted_array:
        candidates.append(extracted_array)

    last_error: Exception | None = None
    for candidate in dict.fromkeys(candidates):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_error = exc
            continue
    raise ProviderAdapterError("provider output JSON schema invalid") from last_error


def _extract_json_object(text: str) -> str | None:
    """Return the first balanced JSON object embedded in provider prose."""

    return _extract_json_value(text, "{", "}")


def _extract_json_array(text: str) -> str | None:
    """Return the first balanced JSON array embedded in provider prose."""

    return _extract_json_value(text, "[", "]")


def _extract_json_value(text: str, opener: str, closer: str) -> str | None:
    """Return the first balanced JSON object or array embedded in provider prose."""

    start = text.find(opener)
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def _strict_json_retry_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    retry = list(messages)
    retry.append(
        {
            "role": "user",
            "content": "Your previous response was not a single JSON object. Return only one valid JSON object.",
        }
    )
    return retry


def _invoke_provider_raw_chat(
    provider: str,
    config: dict[str, Any],
    *,
    messages: list[dict[str, str]],
    max_tokens: int,
    timeout_seconds: int,
) -> tuple[dict[str, Any], str, str, str]:
    endpoint, model, auth_token, credential_source = _provider_chat_endpoint(config, provider)
    body = {
        "model": model,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            raw_response = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        retry_after = exc.headers.get("retry-after") or exc.headers.get("Retry-After")
        suffix = f" retry_after={retry_after}" if retry_after else ""
        raise ProviderAdapterError(
            f"{provider} inference request failed status={exc.code}{suffix}"
        ) from exc
    except urllib.error.URLError as exc:
        raise ProviderAdapterError(f"{provider} inference request failed: {exc.reason}") from exc
    return raw_response, model, credential_source, endpoint


def select_github_model(catalog_payload: Any, selector: dict[str, Any]) -> dict[str, Any]:
    models = normalize_catalog(catalog_payload)
    required_family = str(selector.get("required_model_family") or "").lower()
    required_publisher = str(selector.get("publisher") or "").lower()
    required_capabilities = {
        str(item).lower() for item in selector.get("required_capabilities") or []
    }
    preferred_models = [str(item).lower() for item in selector.get("preferred_models") or []]

    candidates: list[dict[str, Any]] = []
    for model in models:
        model_id = _model_id(model)
        publisher = _publisher(model)
        capabilities = _capabilities(model)
        haystack = f"{model_id} {model.get('display_name', '')} {model.get('name', '')}".lower()
        if required_family and required_family not in haystack:
            continue
        if required_publisher and required_publisher not in publisher.lower():
            continue
        capability_set = {item.lower() for item in capabilities}
        if required_capabilities and not required_capabilities.issubset(capability_set):
            continue
        candidates.append(
            {
                "model_id": model_id,
                "publisher": publisher,
                "capabilities": capabilities,
            }
        )

    if not candidates:
        raise ProviderAdapterError("no GitHub Models catalog entry matches DeepSeek model requirements")

    candidates.sort(
        key=lambda item: (
            preferred_models.index(item["model_id"].lower())
            if item["model_id"].lower() in preferred_models
            else len(preferred_models),
            item["model_id"],
        )
    )
    return candidates[0]


def _github_token_from_gh_cli() -> str | None:
    try:
        completed = subprocess.run(
            ["gh", "auth", "token"],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    token = (completed.stdout or "").strip()
    return token or None


def _resolve_github_models_token(config: dict[str, Any], *, token: str | None = None) -> tuple[str | None, str]:
    github_models = config["providers"]["github_models"]
    token_env = str((github_models.get("auth") or {}).get("token_env") or "GITHUB_TOKEN")
    if token:
        return token, "explicit_token"
    candidate_envs = [token_env, "GITHUB_TOKEN", "GH_TOKEN"]
    for env_name in dict.fromkeys(name for name in candidate_envs if name):
        value = os.getenv(env_name)
        if value:
            return value, env_name
    gh_token = _github_token_from_gh_cli()
    if gh_token:
        return gh_token, "gh_auth_token"
    return None, token_env


def fetch_github_models_catalog(config: dict[str, Any], *, token: str | None = None) -> Any:
    github_models = config["providers"]["github_models"]
    base_url = str(github_models.get("base_url") or "").rstrip("/")
    catalog_path = str(github_models.get("catalog_path") or "/catalog/models")
    url = f"{base_url}{catalog_path if catalog_path.startswith('/') else '/' + catalog_path}"
    auth_token, credential_source = _resolve_github_models_token(config, token=token)
    if not auth_token:
        raise ProviderAdapterError(
            f"{credential_source} or gh auth token is required for GitHub Models catalog discovery"
        )
    request = urllib.request.Request(url)
    request.add_header("Authorization", f"Bearer {auth_token}")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        retry_after = exc.headers.get("retry-after") or exc.headers.get("Retry-After")
        suffix = f" retry_after={retry_after}" if retry_after else ""
        raise ProviderAdapterError(f"GitHub Models catalog request failed status={exc.code}{suffix}") from exc
    except urllib.error.URLError as exc:
        raise ProviderAdapterError(f"GitHub Models catalog request failed: {exc.reason}") from exc


def _provider_chat_endpoint(config: dict[str, Any], provider: str) -> tuple[str, str, str, str]:
    """Resolve a safe OpenAI-compatible chat endpoint for a configured provider."""

    if provider == "github_models":
        github_models = config["providers"]["github_models"]
        base_url = str(github_models.get("base_url") or "").rstrip("/")
        selector = github_models["model_selector"]
        model = str((selector.get("preferred_models") or [GITHUB_MODELS_DEEPSEEK_MODEL_ID])[0])
        auth_token, credential_source = _resolve_github_models_token(config)
        if not auth_token:
            raise ProviderAdapterError(
                f"{credential_source} or gh auth token is required for GitHub Models inference"
            )
        return f"{base_url}/inference/chat/completions", model, auth_token, credential_source
    if provider == "deepseek_api":
        _bootstrap_deepseek_env()
        try:
            # Some DB fallback paths print connection diagnostics to stdout;
            # keep JSON-mode CLI output parseable and report sanitized errors.
            with contextlib.redirect_stdout(io.StringIO()):
                resolved = resolve_deepseek_config(model=DEFAULT_DEEPSEEK_MODEL, require_api_key=True)
        except DeepSeekConfigError as exc:
            raise ProviderAdapterError(str(exc)) from exc
        return (
            f"{resolved.base_url.rstrip('/')}/chat/completions",
            resolved.model,
            resolved.api_key,
            resolved.credential_source,
        )
    raise ProviderAdapterError(f"provider does not support live LLM invocation: {provider}")


def invoke_provider_json(
    provider: str,
    config: dict[str, Any],
    *,
    purpose: str,
    messages: list[dict[str, str]],
    max_tokens: int = 900,
    timeout_seconds: int = 45,
) -> dict[str, Any]:
    """Invoke a configured provider and parse a strict JSON-object response.

    The caller decides whether the parsed content can influence deterministic
    gates. This helper never logs or returns secrets.
    """

    last_error: ProviderAdapterError | None = None
    for attempt, attempt_messages in enumerate((messages, _strict_json_retry_messages(messages)), start=1):
        try:
            raw_response, model, credential_source, _endpoint = _invoke_provider_raw_chat(
                provider,
                config,
                messages=attempt_messages,
                max_tokens=max_tokens,
                timeout_seconds=timeout_seconds,
            )
        except ProviderAdapterError:
            raise
        choices = raw_response.get("choices") if isinstance(raw_response, dict) else None
        if not isinstance(choices, list) or not choices:
            raise ProviderAdapterError("provider output JSON schema invalid: choices missing")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = (message or {}).get("content") if isinstance(message, dict) else choices[0].get("text")
        try:
            if purpose in ADVISORY_LLM_PURPOSES:
                payload = _normalize_advisory_payload(
                    _parse_json_value_response(content or ""),
                    purpose=purpose,
                )
            else:
                payload = parse_json_response(content or "")
            break
        except ProviderAdapterError as exc:
            last_error = exc
            if attempt == 2:
                raise
    else:
        raise last_error or ProviderAdapterError("provider output JSON schema invalid")
    return {
        "provider": provider,
        "model": model,
        "purpose": purpose,
        "payload": payload,
        "credential_source": credential_source,
        "usage": raw_response.get("usage") if isinstance(raw_response, dict) else None,
        "response_id": raw_response.get("id") if isinstance(raw_response, dict) else None,
    }


def validate_deepseek_provider(config: dict[str, Any], *, require_api_key: bool) -> dict[str, Any]:
    provider = config["providers"]["deepseek_api"]
    _bootstrap_deepseek_env()
    with contextlib.redirect_stdout(io.StringIO()):
        resolved = resolve_deepseek_config(
            model=str(provider.get("model") or DEFAULT_DEEPSEEK_MODEL),
            require_api_key=require_api_key,
        )
    summary = resolved.as_safe_dict()
    summary["enabled"] = bool(provider.get("enabled"))
    return summary


def _test_plan_keys(root: Path = ROOT) -> set[str]:
    path = root / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    plans = payload.get("plans") if isinstance(payload, dict) else []
    return {str(plan.get("plan_key")) for plan in plans if isinstance(plan, dict) and plan.get("plan_key")}


def _catalog_plans_by_key(
    root: Path = ROOT,
    catalog_path: Path | None = None,
    *,
    allowed_command_keys: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    path = catalog_path or root / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml"
    try:
        plans = ValidationPlanCatalog(path, allowed_command_keys=allowed_command_keys).list_plans()
    except ValidationCatalogError as exc:
        raise ProviderAdapterError(str(exc)) from exc
    return {str(plan["plan_key"]): plan for plan in plans}


def _issue_flow_validation_select(changed_files: list[str], module: str | None) -> dict[str, Any] | None:
    if not changed_files and not module:
        return None
    try:
        from scripts import issue_flow  # noqa: PLC0415

        return issue_flow.select_validation(changed_files, module=module)
    except Exception as exc:
        raise ProviderAdapterError(f"validation-select compatibility check failed: {exc}") from exc


def _registered_worktrees(root: Path = ROOT) -> set[str]:
    proc = subprocess.run(
        ["git", "-C", str(root), "worktree", "list", "--porcelain"],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise ProviderAdapterError("git worktree registry cannot be inspected")
    worktrees: set[str] = set()
    for line in proc.stdout.splitlines():
        if not line.startswith("worktree "):
            continue
        raw = line.split(" ", 1)[1].strip()
        try:
            worktrees.add(str(Path(raw).resolve()).casefold())
        except OSError:
            worktrees.add(str(Path(raw)).casefold())
    return worktrees


def _workspace_gate(workspace_path: str | None, root: Path = ROOT) -> dict[str, Any]:
    if not workspace_path:
        return {
            "allowed": True,
            "workspace_path": None,
            "reason": "not_required",
        }
    path = Path(workspace_path)
    if not path.exists():
        return {
            "allowed": False,
            "workspace_path": str(path),
            "reason": "workspace_path_missing",
        }
    resolved = str(path.resolve()).casefold()
    if resolved not in _registered_worktrees(root):
        return {
            "allowed": False,
            "workspace_path": str(path.resolve()),
            "reason": "workspace_path_not_registered_git_worktree",
        }
    return {
        "allowed": True,
        "workspace_path": str(path.resolve()),
        "reason": "registered_git_worktree",
    }


def _plan_gate(plan_key: str, plan: dict[str, Any] | None) -> dict[str, Any]:
    reasons: list[str] = []
    if plan is None:
        return {
            "plan_key": plan_key,
            "allowed": False,
            "rejection_reasons": ["unknown_plan_key"],
        }
    if not plan.get("enabled"):
        reasons.append("plan_disabled")
    if not plan.get("runner_enabled"):
        reasons.append("runner_not_enabled")
    if plan.get("writes_business_state"):
        reasons.append("writes_business_state")
    if plan.get("requires_confirmation"):
        reasons.append("requires_confirmation")
    backend_ports = {int(port) for port in plan.get("allowed_backend_ports") or []}
    frontend_ports = {int(port) for port in plan.get("allowed_frontend_ports") or []}
    if backend_ports & (FORBIDDEN_BACKEND_PORTS | FORBIDDEN_MARKET_DATA_PORTS):
        reasons.append("forbidden_backend_or_market_data_port")
    if frontend_ports & FORBIDDEN_FRONTEND_PORTS:
        reasons.append("forbidden_frontend_port")
    return {
        "plan_key": plan_key,
        "title": plan.get("title"),
        "module": plan.get("module"),
        "level": plan.get("level"),
        "command_key": plan.get("command_key"),
        "nox_session": plan.get("nox_session"),
        "runner_enabled": bool(plan.get("runner_enabled")),
        "writes_business_state": bool(plan.get("writes_business_state")),
        "allowed": not reasons,
        "rejection_reasons": reasons,
    }


def _no_shell_commands(payload: Any) -> bool:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key) in SHELL_COMMAND_FIELDS:
                return False
            if not _no_shell_commands(value):
                return False
    elif isinstance(payload, list):
        return all(_no_shell_commands(item) for item in payload)
    return True


def _default_advised_plan_keys(changed_files: list[str], module: str | None) -> list[str]:
    selection = _issue_flow_validation_select(changed_files, module)
    if not selection:
        return ["l0"]
    plans_by_key = _catalog_plans_by_key()
    keys = [
        plan_key
        for plan_key in selection.get("required_plans") or []
        if (plans_by_key.get(plan_key) or {}).get("runner_enabled")
    ]
    keys.extend(
        plan_key
        for plan_key in selection.get("recommended_plans") or []
        if (plans_by_key.get(plan_key) or {}).get("runner_enabled")
    )
    return keys or ["l0"]


def _priority_rank(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2, "baseline": 3}.get(priority, 9)


def _append_unique(items: list[dict[str, str]], plan_key: str, *, priority: str, reason: str) -> None:
    if any(item["plan_key"] == plan_key for item in items):
        return
    items.append({"plan_key": plan_key, "priority": priority, "reason": reason})


def _module_scheduler_plan_intents(module: str) -> list[dict[str, str]]:
    module_key = module.strip().lower()
    items: list[dict[str, str]] = []
    if not module_key:
        return items
    if "qe" in module_key and ("ui" in module_key or "archive" in module_key or "l3" in module_key):
        _append_unique(items, "qe_archive_l3", priority="high", reason=f"recent failure module={module}")
        _append_unique(items, "qe_archive_backend", priority="high", reason=f"safe runner fallback for module={module}")
        return items
    if "qe" in module_key:
        _append_unique(items, "qe_mcp_backend", priority="high", reason=f"recent failure module={module}")
        _append_unique(items, "qe_archive_backend", priority="medium", reason=f"QE archive contract coverage for module={module}")
        return items
    if "paper" in module_key or "watchlist" in module_key or "simulation" in module_key:
        _append_unique(items, "paper_v2_l3", priority="high", reason=f"recent failure module={module}")
        _append_unique(items, "simulation_core_l2", priority="high", reason=f"safe runner fallback for module={module}")
        return items
    if "research_assistant" in module_key or module_key.startswith("ra"):
        _append_unique(items, "research_assistant_backend", priority="high", reason=f"recent failure module={module}")
        return items
    if "validation" in module_key or "workflow" in module_key or "runner" in module_key:
        _append_unique(items, "validation_catalog_integrity", priority="high", reason=f"recent failure module={module}")
        _append_unique(items, "validation_center_backend", priority="medium", reason=f"validation backend coverage for module={module}")
        return items
    if "data" in module_key:
        _append_unique(items, "data_quality_deep", priority="high", reason=f"recent failure module={module}")
        return items
    _append_unique(items, "l0", priority="medium", reason=f"generic recent failure module={module}")
    return items


def _changed_file_scheduler_plan_intents(changed_files: list[str]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    default_keys = _default_advised_plan_keys(changed_files, module=None) if changed_files else []
    for plan_key in default_keys:
        _append_unique(items, plan_key, priority="medium", reason="changed file validation selector")
    lowered = [path.replace("\\", "/").lower() for path in changed_files]
    if any(path.startswith("frontend/src/app/quantevolver") or "/qe" in path for path in lowered):
        _append_unique(items, "qe_archive_backend", priority="medium", reason="QE UI/archive changed file")
    if any(path.startswith("scripts/") or path.startswith(".github/workflows/") for path in lowered):
        _append_unique(items, "validation_catalog_integrity", priority="medium", reason="workflow/script changed file")
    if any(path.startswith("backend/") and "validation" in path for path in lowered):
        _append_unique(items, "validation_center_backend", priority="medium", reason="validation backend changed file")
    return items


def _nightly_scheduler_intents(
    *,
    changed_files: list[str],
    recent_failure_modules: list[str],
    recent_failure_plan_keys: list[str],
) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for plan_key in recent_failure_plan_keys:
        _append_unique(items, plan_key, priority="high", reason="recent failure plan key")
    for module in recent_failure_modules:
        for item in _module_scheduler_plan_intents(module):
            _append_unique(items, item["plan_key"], priority=item["priority"], reason=item["reason"])
    for item in _changed_file_scheduler_plan_intents(changed_files):
        _append_unique(items, item["plan_key"], priority=item["priority"], reason=item["reason"])
    if not items:
        _append_unique(items, "l0", priority="baseline", reason="fixed nightly baseline")
    return sorted(items, key=lambda item: (_priority_rank(item["priority"]), item["plan_key"]))


def _nightly_deferred_reason(gate_result: dict[str, Any], *, over_budget: bool) -> str | None:
    reasons = list(gate_result.get("rejection_reasons") or [])
    if over_budget:
        reasons.append("resource_budget_exceeded")
    return ",".join(reasons) if reasons else None


def _nightly_deferred_reasons(item: dict[str, Any]) -> set[str]:
    return {
        reason.strip()
        for reason in str(item.get("deferred_reason") or "").split(",")
        if reason.strip()
    }


def _nightly_deferred_is_hard_block(item: dict[str, Any]) -> bool:
    reasons = _nightly_deferred_reasons(item)
    hard_reasons = {
        "unknown_plan_key",
        "plan_disabled",
        "writes_business_state",
        "requires_confirmation",
        "forbidden_backend_or_market_data_port",
        "forbidden_frontend_port",
    }
    return bool(reasons & hard_reasons)


def _nightly_deferred_is_warning(item: dict[str, Any]) -> bool:
    reasons = _nightly_deferred_reasons(item)
    return bool(reasons) and reasons != {"resource_budget_exceeded"} and not _nightly_deferred_is_hard_block(item)


def _provider_model_summary(config: dict[str, Any], provider: str) -> dict[str, Any]:
    if provider == "github_models":
        selector = config["providers"]["github_models"]["model_selector"]
        preferred = selector.get("preferred_models") or [GITHUB_MODELS_DEEPSEEK_MODEL_ID]
        token_env = config["providers"]["github_models"].get("auth", {}).get("token_env") or "GITHUB_TOKEN"
        return {
            "provider": provider,
            "model": str(preferred[0]),
            "credential_source": f"{token_env}|GH_TOKEN|gh_auth_token",
            "invoked": False,
        }
    if provider == "deepseek_api":
        resolved = validate_deepseek_provider(config, require_api_key=False)
        return {
            "provider": provider,
            "model": resolved["model"],
            "credential_source": resolved["credential_source"],
            "invoked": False,
        }
    return {
        "provider": "deterministic",
        "model": "deterministic-baseline-v1",
        "credential_source": "not_required",
        "invoked": False,
    }


def _enabled_provider_chain(config: dict[str, Any], provider: str) -> list[str]:
    if provider == "deterministic":
        return []
    providers = config.get("providers") if isinstance(config.get("providers"), dict) else {}
    chain = [provider]
    if provider == "github_models":
        deepseek = providers.get("deepseek_api") if isinstance(providers.get("deepseek_api"), dict) else {}
        if deepseek.get("enabled") is True:
            chain.append("deepseek_api")
    if provider == "deepseek_api":
        github_models = providers.get("github_models") if isinstance(providers.get("github_models"), dict) else {}
        if github_models.get("enabled") is True:
            chain.append("github_models")
    return list(dict.fromkeys(chain))


def _merge_usage(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    found = False
    for record in records:
        usage = record.get("usage")
        if not isinstance(usage, dict):
            continue
        found = True
        for key in totals:
            try:
                totals[key] += int(usage.get(key) or 0)
            except (TypeError, ValueError):
                pass
    return totals if found else None


def _redact_llm_error(error: Any) -> str:
    redacted = redact_secret_text(str(error))
    return re.sub(
        r"(?i)(\btoken\s*[:=]\s*)['\"]?[^'\"\s,;]+",
        r"\1<redacted>",
        redacted,
    )


def _llm_evidence(
    *,
    provider_summary: dict[str, Any],
    invoked: bool,
    reason: str,
    input_policy: str,
    usage: dict[str, Any] | None = None,
    error: str | None = None,
    provider_chain: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    evidence = {
        "schema_version": LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION,
        "provider": provider_summary["provider"],
        "model": provider_summary["model"],
        "invoked": invoked,
        "reason": reason,
        "input_policy": input_policy,
        "redaction_applied": True,
    }
    if provider_chain is not None:
        safe_chain = []
        for item in provider_chain:
            safe_item = dict(item)
            if safe_item.get("error"):
                safe_item["error"] = _redact_llm_error(safe_item["error"])
            safe_chain.append(safe_item)
        evidence["provider_chain"] = json.loads(redact_secret_text(json.dumps(safe_chain, ensure_ascii=False)))
        evidence["fallback_used"] = len(provider_chain) > 1
        evidence["fallback_reason"] = (
            _redact_llm_error(provider_chain[0].get("error"))
            if len(provider_chain) > 1 and provider_chain[0].get("error")
            else None
        )
    if usage:
        evidence["usage_summary"] = {
            "prompt_units": usage.get("prompt_tokens"),
            "completion_units": usage.get("completion_tokens"),
            "total_units": usage.get("total_tokens"),
        }
    if error:
        evidence["error"] = _redact_llm_error(error)
    return evidence


def _compact_llm_messages(*, kind: str, payload: dict[str, Any]) -> list[dict[str, str]]:
    system = (
        "You are an AIstock validation advisor. Return one strict JSON object only. "
        "Suggest plan_key intent, rationale, risk, and confidence. Do not include shell "
        "commands, source code patches, secrets, production actions, or issue closure decisions."
    )
    user_payload = {
        "kind": kind,
        "schema": {
            "summary": "short string",
            "rationale": "short string",
            "risk": "low|medium|high",
            "suggested_plan_keys": ["plan_key"],
            "confidence": 0.0,
        },
        "input": payload,
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, sort_keys=True)},
    ]


def _extract_advisory_plan_keys(raw: Any) -> list[str]:
    """Extract provider-suggested plan keys from common advisory JSON shapes."""

    keys: list[str] = []

    def add(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            if value.strip():
                keys.append(value.strip())
            return
        if isinstance(value, list):
            for item in value:
                add(item)
            return
        if isinstance(value, dict):
            for field in (
                "plan_key",
                "planKey",
                "plan",
                "suggested_plan_keys",
                "plan_keys",
                "advised_plan_keys",
                "recommended_plan_keys",
                "allowed_plan_keys",
            ):
                if field in value:
                    add(value.get(field))
            for field in (
                "queue",
                "plans",
                "recommended_plans",
                "scheduler_advice",
                "test_plan_advice",
                "advice",
            ):
                child = value.get(field)
                if isinstance(child, (list, dict)):
                    add(child)

    add(raw)
    return list(dict.fromkeys(keys))[:8]


def _normalize_advisory_payload(raw: Any, *, purpose: str) -> dict[str, Any]:
    """Normalize advisory-only provider JSON without broadening its authority."""

    if purpose not in ADVISORY_LLM_PURPOSES:
        if isinstance(raw, dict):
            return raw
        raise ProviderAdapterError("provider output JSON schema invalid")
    if not _no_shell_commands(raw):
        raise ProviderAdapterError("provider advice must not contain shell command fields")
    plan_keys = _extract_advisory_plan_keys(raw)
    if isinstance(raw, dict):
        payload = dict(raw)
    elif isinstance(raw, list):
        payload = {
            "summary": "provider returned plan list",
            "rationale": "normalized advisory-only list response",
            "risk": "unknown",
        }
    else:
        raise ProviderAdapterError("provider output JSON schema invalid")
    payload["suggested_plan_keys"] = plan_keys
    payload.setdefault("summary", "provider advisory parsed")
    payload.setdefault("rationale", "advisory-only provider response normalized")
    payload.setdefault("risk", "unknown")
    return payload


def _safe_llm_advice(raw: dict[str, Any], *, allowed_plan_keys: set[str]) -> dict[str, Any]:
    if not _no_shell_commands(raw):
        raise ProviderAdapterError("provider advice must not contain shell command fields")
    suggested = raw.get("suggested_plan_keys") or raw.get("plan_keys") or []
    if not isinstance(suggested, list):
        suggested = []
    filtered_plan_keys = [
        str(item)
        for item in suggested
        if str(item) in allowed_plan_keys
    ][:8]
    confidence = raw.get("confidence")
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        confidence_value = None
    return {
        "summary": str(raw.get("summary") or raw.get("reason") or "")[:500],
        "rationale": str(raw.get("rationale") or "")[:1000],
        "risk": str(raw.get("risk") or "unknown")[:50],
        "suggested_plan_keys": filtered_plan_keys,
        "ignored_plan_key_count": max(0, len(suggested) - len(filtered_plan_keys)),
        "confidence": confidence_value,
        "advisory_only": True,
    }


def _maybe_invoke_advisory_llm(
    provider: str,
    config: dict[str, Any],
    *,
    provider_summary: dict[str, Any],
    kind: str,
    prompt_payload: dict[str, Any],
    allowed_plan_keys: set[str],
    invoke_llm: bool,
    fallback_on_error: bool,
    input_policy: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not invoke_llm or provider == "deterministic":
        reason = f"{kind}_dry_run_no_network"
        return None, _llm_evidence(
            provider_summary=provider_summary,
            invoked=False,
            reason=reason,
            input_policy=input_policy,
            provider_chain=[],
        )
    messages = _compact_llm_messages(kind=kind, payload=prompt_payload)
    attempts: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    last_error: ProviderAdapterError | None = None
    for candidate_provider in _enabled_provider_chain(config, provider):
        candidate_summary = _provider_model_summary(config, candidate_provider)
        try:
            result = invoke_provider_json(
                candidate_provider,
                config,
                purpose=kind,
                messages=messages,
            )
            attempts.append(
                {
                    "provider": candidate_provider,
                    "model": result.get("model") or candidate_summary["model"],
                    "status": "invoked",
                }
            )
            results.append(result)
            provider_summary = {
                **candidate_summary,
                "model": result.get("model") or candidate_summary["model"],
                "credential_source": result.get("credential_source") or candidate_summary["credential_source"],
            }
            break
        except ProviderAdapterError as exc:
            last_error = exc
            attempts.append(
                {
                    "provider": candidate_provider,
                    "model": candidate_summary["model"],
                    "status": "failed",
                    "error": redact_secret_text(str(exc)),
                }
            )
    else:
        if not fallback_on_error:
            raise last_error or ProviderAdapterError(f"{kind} live provider failed")
        return None, _llm_evidence(
            provider_summary=provider_summary,
            invoked=False,
            reason=f"{kind}_live_provider_failed_fallback",
            input_policy=input_policy,
            error=str(last_error or "all providers failed"),
            provider_chain=attempts,
        )
    try:
        advice = _safe_llm_advice(result["payload"], allowed_plan_keys=allowed_plan_keys)
        return advice, _llm_evidence(
            provider_summary=provider_summary,
            invoked=True,
            reason=f"{kind}_live_provider_json",
            input_policy=input_policy,
            usage=_merge_usage(results),
            provider_chain=attempts,
        )
    except ProviderAdapterError as exc:
        if not fallback_on_error:
            raise
        return None, _llm_evidence(
            provider_summary=provider_summary,
            invoked=False,
            reason=f"{kind}_live_provider_failed_fallback",
            input_policy=input_policy,
            error=str(exc),
            provider_chain=attempts,
        )


def build_test_plan_advice(
    provider: str,
    config: dict[str, Any],
    *,
    plan_keys: list[str] | None = None,
    changed_files: list[str] | None = None,
    module: str | None = None,
    workspace_path: str | None = None,
    root: Path = ROOT,
    catalog_path: Path | None = None,
    allowed_command_keys: dict[str, str] | None = None,
    invoke_llm: bool = False,
    fallback_on_llm_error: bool = True,
) -> dict[str, Any]:
    """Build schema-checked test-plan advice without allowing shell commands."""

    validate_config(config)
    provider_summary = _provider_model_summary(config, provider)
    changed_files = [str(path) for path in changed_files or [] if str(path).strip()]
    advised_keys = [str(key) for key in (plan_keys or _default_advised_plan_keys(changed_files, module))]
    plans_by_key = _catalog_plans_by_key(root, catalog_path=catalog_path, allowed_command_keys=allowed_command_keys)
    selection = _issue_flow_validation_select(changed_files, module)
    selected_by_catalog = set((selection or {}).get("required_plans") or [])
    selected_by_catalog.update((selection or {}).get("recommended_plans") or [])
    workspace = _workspace_gate(workspace_path, root=root)
    plan_results = [_plan_gate(plan_key, plans_by_key.get(plan_key)) for plan_key in advised_keys]
    all_plans_allowed = all(item["allowed"] for item in plan_results)
    catalog_compatible = not selected_by_catalog or set(advised_keys).issubset(selected_by_catalog)
    workflow_gate = "ready" if all_plans_allowed and workspace["allowed"] and catalog_compatible else "blocked"
    llm_advice, llm_evidence = _maybe_invoke_advisory_llm(
        provider,
        config,
        provider_summary=provider_summary,
        kind="test_plan_advice",
        prompt_payload={
            "changed_files": changed_files,
            "module": module,
            "deterministic_plan_keys": advised_keys,
            "validation_select": {
                "required_plans": (selection or {}).get("required_plans") or [],
                "recommended_plans": (selection or {}).get("recommended_plans") or [],
            },
            "workspace_path_allowed": workspace["allowed"],
        },
        allowed_plan_keys=set(plans_by_key),
        invoke_llm=invoke_llm,
        fallback_on_error=fallback_on_llm_error,
        input_policy="plan_key_intent_plus_catalog_context_only",
    )
    advice = {
        "schema_version": TEST_PLAN_ADVICE_SCHEMA_VERSION,
        "provider": provider_summary["provider"],
        "model": provider_summary["model"],
        "effective_provider": llm_evidence["provider"],
        "effective_model": llm_evidence["model"],
        "llm_gate": "ready" if llm_evidence["invoked"] else "degraded",
        "module": module,
        "changed_files": changed_files,
        "test_plan_advice": plan_results,
        "deterministic_plan_keys": advised_keys,
        "advised_plan_keys": (llm_advice or {}).get("suggested_plan_keys") or [],
        "advice_consumption": {
            "advice_consumed": bool(
                set((llm_advice or {}).get("suggested_plan_keys") or [])
                & {item["plan_key"] for item in plan_results if item.get("allowed")}
            ),
            "consumption_mode": "allowlisted_intersection_only",
            "executed_plan_keys": [],
            "warning_only": True,
        },
        "validation_select": {
            "required_plans": (selection or {}).get("required_plans") or [],
            "recommended_plans": (selection or {}).get("recommended_plans") or [],
            "compatible": catalog_compatible,
        },
        "workspace_gate": workspace,
        "llm_advice": llm_advice,
        "llm_invocation_evidence": llm_evidence,
        "deterministic_gate": {
            "workflow_gate": workflow_gate,
            "schema_valid": True,
            "plan_keys_exist": all("unknown_plan_key" not in item["rejection_reasons"] for item in plan_results),
            "runner_enabled_only": all(
                "runner_not_enabled" not in item["rejection_reasons"] for item in plan_results
            ),
            "command_keys_allowlisted": all(bool(item.get("command_key")) for item in plan_results),
            "workspace_path_allowed": workspace["allowed"],
            "validation_select_compatible": catalog_compatible,
            "shell_commands_allowed": False,
            "production_ports_allowed": False,
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        },
    }
    validate_test_plan_advice(advice)
    return advice


def validate_test_plan_advice(advice: dict[str, Any]) -> None:
    if advice.get("schema_version") != TEST_PLAN_ADVICE_SCHEMA_VERSION:
        raise ProviderAdapterError("test plan advice schema_version invalid")
    if not isinstance(advice.get("test_plan_advice"), list):
        raise ProviderAdapterError("test plan advice missing list field: test_plan_advice")
    if not _no_shell_commands(advice):
        raise ProviderAdapterError("test plan advice must not contain shell command fields")
    if not isinstance(advice.get("deterministic_gate"), dict):
        raise ProviderAdapterError("test plan advice missing deterministic_gate")
    if advice["deterministic_gate"].get("shell_commands_allowed") is not False:
        raise ProviderAdapterError("test plan advice must keep shell command execution disabled")


def build_nightly_scheduler_advice(
    provider: str,
    config: dict[str, Any],
    *,
    changed_files: list[str] | None = None,
    recent_failure_modules: list[str] | None = None,
    recent_failure_plan_keys: list[str] | None = None,
    codegraph_freshness: str = "unknown",
    resource_budget_seconds: int | None = None,
    workspace_path: str | None = None,
    root: Path = ROOT,
    catalog_path: Path | None = None,
    allowed_command_keys: dict[str, str] | None = None,
    invoke_llm: bool = False,
    fallback_on_llm_error: bool = True,
) -> dict[str, Any]:
    """Build a deterministic nightly queue without scheduling production actions."""

    validate_config(config)
    provider_summary = _provider_model_summary(config, provider)
    changed_files = [str(path) for path in changed_files or [] if str(path).strip()]
    recent_failure_modules = [str(item) for item in recent_failure_modules or [] if str(item).strip()]
    recent_failure_plan_keys = [str(item) for item in recent_failure_plan_keys or [] if str(item).strip()]
    freshness = codegraph_freshness if codegraph_freshness in CODEGRAPH_FRESHNESS_VALUES else "unknown"
    budget = int(resource_budget_seconds) if resource_budget_seconds is not None else 900
    budget = max(0, budget)
    intents = _nightly_scheduler_intents(
        changed_files=changed_files,
        recent_failure_modules=recent_failure_modules,
        recent_failure_plan_keys=recent_failure_plan_keys,
    )
    plan_keys = [item["plan_key"] for item in intents]
    plans_by_key = _catalog_plans_by_key(root, catalog_path=catalog_path, allowed_command_keys=allowed_command_keys)
    workspace = _workspace_gate(workspace_path, root=root)
    test_plan_advice = build_test_plan_advice(
        provider,
        config,
        plan_keys=plan_keys,
        changed_files=[],
        module=None,
        workspace_path=workspace_path,
        root=root,
        catalog_path=catalog_path,
        allowed_command_keys=allowed_command_keys,
        invoke_llm=False,
    )
    advice_by_key = {item["plan_key"]: item for item in test_plan_advice["test_plan_advice"]}
    queue: list[dict[str, Any]] = []
    elapsed_budget = 0
    for intent in intents:
        plan_key = intent["plan_key"]
        plan = plans_by_key.get(plan_key) or {}
        duration = int(plan.get("max_duration_seconds") or 300)
        gate_result = advice_by_key.get(plan_key) or _plan_gate(plan_key, None)
        over_budget = elapsed_budget + duration > budget and intent["priority"] != "baseline"
        allowed = bool(gate_result.get("allowed")) and workspace["allowed"] and not over_budget
        if allowed:
            elapsed_budget += duration
        queue.append(
            {
                "plan_key": plan_key,
                "priority": intent["priority"],
                "reason": intent["reason"],
                "budget_seconds": duration,
                "allowed": allowed,
                "deferred_reason": _nightly_deferred_reason(gate_result, over_budget=over_budget),
                "module": plan.get("module"),
                "level": plan.get("level"),
                "runner_enabled": bool(plan.get("runner_enabled")),
            }
        )
    codegraph_warning = freshness in {"missing", "stale", "unknown"}
    allowed_count = len([item for item in queue if item["allowed"]])
    blocked_by_gate = any(_nightly_deferred_is_hard_block(item) for item in queue)
    warning_by_gate = any(_nightly_deferred_is_warning(item) for item in queue)
    workflow_gate = "blocked" if blocked_by_gate or not workspace["allowed"] else ("warning" if codegraph_warning or warning_by_gate else "ready")
    llm_advice, llm_evidence = _maybe_invoke_advisory_llm(
        provider,
        config,
        provider_summary=provider_summary,
        kind="nightly_scheduler_advice",
        prompt_payload={
            "changed_files": changed_files[:20],
            "recent_failure_modules": recent_failure_modules[:20],
            "recent_failure_plan_keys": recent_failure_plan_keys[:20],
            "codegraph_freshness": freshness,
            "resource_budget_seconds": budget,
            "deterministic_queue": [
                {
                    "plan_key": item["plan_key"],
                    "priority": item["priority"],
                    "allowed": item["allowed"],
                    "deferred_reason": item.get("deferred_reason"),
                }
                for item in queue
            ],
        },
        allowed_plan_keys=set(plans_by_key),
        invoke_llm=invoke_llm,
        fallback_on_error=fallback_on_llm_error,
        input_policy="changed_files_recent_failures_codegraph_freshness_catalog_only",
    )
    advice = {
        "schema_version": NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION,
        "provider": provider_summary["provider"],
        "model": provider_summary["model"],
        "effective_provider": llm_evidence["provider"],
        "effective_model": llm_evidence["model"],
        "llm_gate": "ready" if llm_evidence["invoked"] else "degraded",
        "changed_files": changed_files,
        "recent_failures": {
            "modules": recent_failure_modules,
            "plan_keys": recent_failure_plan_keys,
        },
        "codegraph": {
            "freshness": freshness,
            "warning_only": codegraph_warning,
            "warning": "codegraph_freshness_not_fresh" if codegraph_warning else None,
        },
        "resource_budget_seconds": budget,
        "queue": queue,
        "deterministic_plan_keys": [item["plan_key"] for item in queue],
        "advised_plan_keys": (llm_advice or {}).get("suggested_plan_keys") or [],
        "executed_plan_keys": [item["plan_key"] for item in queue if item.get("allowed")],
        "advice_consumption": {
            "advice_consumed": bool(
                set((llm_advice or {}).get("suggested_plan_keys") or [])
                & {item["plan_key"] for item in queue if item.get("allowed")}
            ),
            "consumption_mode": "allowlisted_queue_intersection_only",
            "warning_only": True,
        },
        "test_plan_advice_gate": {
            "workflow_gate": test_plan_advice["deterministic_gate"]["workflow_gate"],
            "shell_commands_allowed": test_plan_advice["deterministic_gate"]["shell_commands_allowed"],
            "llm_invoked": test_plan_advice["llm_invocation_evidence"]["invoked"],
        },
        "workspace_gate": workspace,
        "llm_advice": llm_advice,
        "llm_invocation_evidence": llm_evidence,
        "deterministic_gate": {
            "workflow_gate": workflow_gate,
            "schema_valid": True,
            "queue_only_plan_keys": True,
            "allowed_plan_count": allowed_count,
            "deferred_plan_count": len(queue) - allowed_count,
            "workspace_path_allowed": workspace["allowed"],
            "resource_budget_enforced": True,
            "codegraph_warning_only": codegraph_warning,
            "shell_commands_allowed": False,
            "production_actions_allowed": False,
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        },
    }
    validate_nightly_scheduler_advice(advice)
    return advice


def validate_nightly_scheduler_advice(advice: dict[str, Any]) -> None:
    if advice.get("schema_version") != NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION:
        raise ProviderAdapterError("nightly scheduler advice schema_version invalid")
    if not isinstance(advice.get("queue"), list):
        raise ProviderAdapterError("nightly scheduler advice missing queue")
    if not _no_shell_commands(advice):
        raise ProviderAdapterError("nightly scheduler advice must not contain shell command fields")
    for item in advice["queue"]:
        if not isinstance(item, dict) or not item.get("plan_key"):
            raise ProviderAdapterError("nightly scheduler queue entries must contain plan_key")
        if item.get("allowed") and item.get("deferred_reason"):
            raise ProviderAdapterError("allowed nightly queue item cannot include deferred_reason")
    gate = advice.get("deterministic_gate")
    if not isinstance(gate, dict):
        raise ProviderAdapterError("nightly scheduler advice missing deterministic_gate")
    if gate.get("shell_commands_allowed") is not False:
        raise ProviderAdapterError("nightly scheduler must keep shell command execution disabled")
    if gate.get("production_actions_allowed") is not False:
        raise ProviderAdapterError("nightly scheduler must not allow production actions")


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def _load_prompt_pack_versions(prompt_root: Path = DEFAULT_PROMPT_PACK_ROOT) -> dict[str, str]:
    versions: dict[str, str] = {}
    for path in sorted(prompt_root.glob("*.prompt.yml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ProviderAdapterError(f"prompt file must be a mapping: {path}")
        prompt_id = str(payload.get("prompt_id") or path.stem.replace(".prompt", ""))
        prompt_version = str(payload.get("prompt_version") or "unknown")
        versions[prompt_id] = f"{prompt_version}:{path.relative_to(ROOT).as_posix()}"
    return versions


def _load_evaluation_cases(path: Path = DEFAULT_EVALUATION_CASES) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("cases"), list):
        raise ProviderAdapterError("evaluation cases payload must contain cases list")
    return [item for item in payload["cases"] if isinstance(item, dict)]


def _safe_repo_rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _config_guarded_rollout(config: dict[str, Any]) -> dict[str, Any]:
    rollout = config.get("guarded_rollout") if isinstance(config.get("guarded_rollout"), dict) else {}
    return rollout or {
        "default_mode": "warning_only",
        "mode_env": "AISTOCK_LLM_TRIAGE_MODE",
        "opt_in_env": "AISTOCK_LLM_AUTO_FILE",
        "module_allowlist": [],
        "required_issue_sections": list(EVALUATION_ISSUE_BODY_SECTIONS),
    }


def _guarded_rollout_mode(config: dict[str, Any], explicit_mode: str | None = None) -> str:
    rollout = _config_guarded_rollout(config)
    mode_env = str(rollout.get("mode_env") or "AISTOCK_LLM_TRIAGE_MODE")
    mode = str(explicit_mode or os.environ.get(mode_env) or rollout.get("default_mode") or "warning_only")
    if mode not in GUARDED_ROLLOUT_MODES:
        raise ProviderAdapterError(f"unsupported guarded rollout mode: {mode}")
    return mode


def _guarded_rollout_opted_in(config: dict[str, Any], explicit_opt_in: bool | None = None) -> bool:
    if explicit_opt_in is not None:
        return explicit_opt_in
    rollout = _config_guarded_rollout(config)
    opt_in_env = str(rollout.get("opt_in_env") or "AISTOCK_LLM_AUTO_FILE")
    return str(os.environ.get(opt_in_env) or "").strip().lower() in {"1", "true", "yes", "on"}


def _module_in_guarded_allowlist(module: str | None, allowlist: list[str]) -> bool:
    module_key = str(module or "").strip().lower()
    if not module_key:
        return False
    normalized = [str(item).strip().lower() for item in allowlist if str(item).strip()]
    return any(module_key == item or module_key.startswith(f"{item}.") or item in module_key for item in normalized)


def _evaluation_actionable(event: dict[str, Any]) -> bool:
    signature = str(event.get("error_signature") or "").lower()
    failed_job = str(event.get("failed_job") or "").lower()
    combined = f"{signature} {failed_job}"
    return not any(token in combined for token in EVALUATION_INFRA_SIGNATURE_TOKENS)


def _evaluation_plan_keys(event: dict[str, Any], *, actionable: bool) -> list[str]:
    if not actionable:
        return []
    module = str(event.get("module") or "").strip().lower()
    if module in EVALUATION_MODULE_PLAN_MAP:
        return list(EVALUATION_MODULE_PLAN_MAP[module])
    for module_key, plan_keys in EVALUATION_MODULE_PLAN_MAP.items():
        if module_key in module:
            return list(plan_keys)
    return ["l0"]


def _evaluation_dedupe_hit(event: dict[str, Any]) -> bool:
    return bool(
        event.get("existing_issue_number")
        or event.get("existing_github_issue")
        or event.get("existing_issue_url")
        or event.get("dedupe_status") == "hit"
        or event.get("dedupe_hit") is True
    )


def _deterministic_case_prediction(case: dict[str, Any]) -> dict[str, Any]:
    event = case.get("failure_event") if isinstance(case.get("failure_event"), dict) else {}
    module = str(event.get("module") or "validation.runner")
    actionable = _evaluation_actionable(event)
    dedupe_hit = _evaluation_dedupe_hit(event)
    recommended_plans = _evaluation_plan_keys(event, actionable=actionable)
    advice = build_test_plan_advice(
        "deterministic",
        load_config(),
        plan_keys=recommended_plans or ["l0"],
        module=module,
    )
    recommended_action = "skip"
    if actionable:
        recommended_action = "comment_existing_issue" if dedupe_hit else "create_github_issue"
    return {
        "case_id": case.get("case_id"),
        "actionable": actionable,
        "dedupe_hit": dedupe_hit,
        "recommended_action": recommended_action,
        "recommended_plan_keys": recommended_plans,
        "issue_body_sections": list(EVALUATION_ISSUE_BODY_SECTIONS) if actionable else [],
        "test_plan_gate": {
            "workflow_gate": advice["deterministic_gate"]["workflow_gate"],
            "llm_invoked": advice["llm_invocation_evidence"]["invoked"],
        },
        "usage": {
            "prompt_tokens": _estimate_tokens(json.dumps(event, ensure_ascii=False, sort_keys=True)),
            "completion_tokens": None,
            "estimated_cost_usd": None,
        },
    }


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 1.0


def build_prompt_evaluation(
    provider: str,
    config: dict[str, Any],
    *,
    cases_path: Path = DEFAULT_EVALUATION_CASES,
    prompt_root: Path = DEFAULT_PROMPT_PACK_ROOT,
    changed_files: list[str] | None = None,
    false_positive_threshold: float = 0.10,
) -> dict[str, Any]:
    """Evaluate prompt behavior against fixtures without invoking an LLM."""

    validate_config(config)
    provider_summary = _provider_model_summary(config, provider)
    prompt_versions = _load_prompt_pack_versions(prompt_root)
    cases = _load_evaluation_cases(cases_path)
    if len(cases) < 20:
        raise ProviderAdapterError("prompt evaluation requires at least 20 historical failure fixtures")
    rows: list[dict[str, Any]] = []
    counts = {
        "actionable_expected": 0,
        "actionable_correct": 0,
        "false_positive": 0,
        "dedupe_expected": 0,
        "dedupe_correct": 0,
        "plan_expected": 0,
        "plan_correct": 0,
        "issue_body_expected": 0,
        "issue_body_complete": 0,
    }
    prompt_tokens: list[int] = []
    completion_tokens: list[int] = []
    for case in cases:
        expected = case.get("expected") if isinstance(case.get("expected"), dict) else {}
        prediction = _deterministic_case_prediction(case)
        expected_actionable = bool(expected.get("actionable"))
        expected_dedupe = bool(expected.get("dedupe_hit"))
        expected_plans = [str(item) for item in expected.get("expected_plan_keys") or []]
        expected_sections = [str(item) for item in expected.get("issue_body_required_sections") or []]
        actionable_correct = prediction["actionable"] == expected_actionable
        dedupe_correct = prediction["dedupe_hit"] == expected_dedupe
        plan_correct = set(prediction["recommended_plan_keys"]) == set(expected_plans)
        issue_body_complete = set(expected_sections).issubset(set(prediction["issue_body_sections"]))
        counts["actionable_expected"] += int(expected_actionable)
        counts["actionable_correct"] += int(actionable_correct and expected_actionable)
        counts["false_positive"] += int(prediction["actionable"] and bool(expected.get("false_positive")))
        counts["dedupe_expected"] += int(expected_dedupe)
        counts["dedupe_correct"] += int(dedupe_correct and expected_dedupe)
        counts["plan_expected"] += int(bool(expected_plans))
        counts["plan_correct"] += int(plan_correct and bool(expected_plans))
        counts["issue_body_expected"] += int(bool(expected_sections))
        counts["issue_body_complete"] += int(issue_body_complete and bool(expected_sections))
        prompt_tokens.append(int((prediction["usage"] or {}).get("prompt_tokens") or 0))
        if isinstance((prediction["usage"] or {}).get("completion_tokens"), int):
            completion_tokens.append(int(prediction["usage"]["completion_tokens"]))
        rows.append(
            {
                "case_id": case.get("case_id"),
                "actionable_correct": actionable_correct,
                "dedupe_correct": dedupe_correct,
                "plan_correct": plan_correct,
                "issue_body_complete": issue_body_complete,
                "recommended_action": prediction["recommended_action"],
            }
        )
    false_positive_rate = _ratio(counts["false_positive"], len(cases))
    metrics = {
        "case_count": len(cases),
        "actionability_precision": _ratio(counts["actionable_correct"], counts["actionable_expected"]),
        "false_positive_auto_file_rate": false_positive_rate,
        "dedupe_hit_rate": _ratio(counts["dedupe_correct"], counts["dedupe_expected"]),
        "plan_recommendation_accuracy": _ratio(counts["plan_correct"], counts["plan_expected"]),
        "issue_body_completeness": _ratio(counts["issue_body_complete"], counts["issue_body_expected"]),
        "average_prompt_tokens": round(sum(prompt_tokens) / len(prompt_tokens), 2) if prompt_tokens else None,
        "average_completion_tokens": round(sum(completion_tokens) / len(completion_tokens), 2) if completion_tokens else None,
    }
    changed_files = [str(item).replace("\\", "/") for item in changed_files or [] if str(item).strip()]
    blocking_relevant = any(
        path.startswith(EVALUATION_BLOCKING_PATH_PREFIXES) or path in EVALUATION_BLOCKING_FILES
        for path in changed_files
    )
    policy_gate = "blocked" if false_positive_rate > false_positive_threshold and blocking_relevant else (
        "warning" if false_positive_rate > false_positive_threshold else "passed"
    )
    return {
        "schema_version": PROMPT_EVALUATION_SCHEMA_VERSION,
        "provider": provider_summary["provider"],
        "model": provider_summary["model"],
        "prompt_pack_versions": prompt_versions,
        "cases_path": _safe_repo_rel(cases_path),
        "changed_files": changed_files,
        "metrics": metrics,
        "policy_gate": {
            "workflow_gate": policy_gate,
            "blocking_relevant_change": blocking_relevant,
            "false_positive_threshold": false_positive_threshold,
            "auto_file_enabled": false_positive_rate <= false_positive_threshold,
            "warnings": ["false_positive_auto_file_rate"] if false_positive_rate > false_positive_threshold else [],
            "blocking": ["false_positive_auto_file_rate"] if policy_gate == "blocked" else [],
        },
        "llm_invocation_evidence": {
            "schema_version": LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION,
            "provider": provider_summary["provider"],
            "model": provider_summary["model"],
            "invoked": False,
            "reason": "prompt_evaluation_fixture_dry_run_no_network",
            "prompt_pack_versions": prompt_versions,
            "input_policy": "historical_fixture_expected_labels_only",
            "redaction_applied": True,
        },
        "rows": rows,
    }


def build_guarded_rollout_gate(
    provider: str,
    config: dict[str, Any],
    *,
    mode: str | None = None,
    opt_in: bool | None = None,
    module: str | None = None,
    issue_sections: list[str] | None = None,
    deterministic_issue_allowed: bool = True,
    llm_workflow_gate: str = "ready",
    false_positive_rate: float | None = None,
    false_positive_threshold: float = 0.10,
) -> dict[str, Any]:
    """Decide whether LLM advice may influence auto-file behavior without replacing deterministic gates."""

    validate_config(config)
    provider_summary = _provider_model_summary(config, provider)
    rollout = _config_guarded_rollout(config)
    effective_mode = _guarded_rollout_mode(config, explicit_mode=mode)
    explicit_opt_in = _guarded_rollout_opted_in(config, explicit_opt_in=opt_in)
    allowlist = [str(item) for item in rollout.get("module_allowlist") or []]
    required_sections = [str(item) for item in rollout.get("required_issue_sections") or EVALUATION_ISSUE_BODY_SECTIONS]
    sections = [str(item) for item in issue_sections or [] if str(item).strip()]
    missing_sections = [section for section in required_sections if section not in sections]
    module_allowed = _module_in_guarded_allowlist(module, allowlist)
    fp_rate = 0.0 if false_positive_rate is None else float(false_positive_rate)
    reasons: list[str] = []
    if effective_mode == "off":
        reasons.append("llm_triage_mode_off")
    if effective_mode != "opt_in_auto_file":
        reasons.append("llm_auto_file_not_in_opt_in_mode")
    if not explicit_opt_in:
        reasons.append("llm_auto_file_not_explicitly_opted_in")
    if not module_allowed:
        reasons.append("module_not_allowlisted")
    if missing_sections:
        reasons.append("issue_body_missing_required_sections")
    if not deterministic_issue_allowed:
        reasons.append("deterministic_issue_policy_denied")
    if llm_workflow_gate not in {"ready", "passed"}:
        reasons.append("llm_workflow_gate_not_ready")
    if fp_rate > false_positive_threshold:
        reasons.append("false_positive_threshold_exceeded")
    llm_can_enhance_issue = effective_mode != "off" and not missing_sections and llm_workflow_gate in {"ready", "passed"}
    auto_file_allowed = not reasons
    return {
        "schema_version": GUARDED_ROLLOUT_SCHEMA_VERSION,
        "provider": provider_summary["provider"],
        "model": provider_summary["model"],
        "mode": effective_mode,
        "opt_in": explicit_opt_in,
        "module": module,
        "module_allowlist": allowlist,
        "module_allowlisted": module_allowed,
        "deterministic_issue_allowed": deterministic_issue_allowed,
        "llm_workflow_gate": llm_workflow_gate,
        "issue_sections_present": sections,
        "missing_required_issue_sections": missing_sections,
        "false_positive_auto_file_rate": fp_rate,
        "false_positive_threshold": false_positive_threshold,
        "auto_file_allowed": auto_file_allowed,
        "llm_can_enhance_issue": llm_can_enhance_issue,
        "llm_enhancement_allowed": auto_file_allowed,
        "deterministic_issue_creation_unaffected": True,
        "workflow_gate": "ready" if auto_file_allowed else ("off" if effective_mode == "off" else "warning"),
        "rejection_reasons": reasons,
        "fallback": "deterministic_issue_workflow",
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
        "llm_invocation_evidence": {
            "schema_version": LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION,
            "provider": provider_summary["provider"],
            "model": provider_summary["model"],
            "invoked": False,
            "reason": "guarded_rollout_policy_gate_no_network",
            "input_policy": "mode_module_issue_sections_evaluation_metrics_only",
            "redaction_applied": True,
        },
    }


def build_triage_quality_smoke(provider: str, config: dict[str, Any]) -> dict[str, Any]:
    """Build a schema-checked triage advice fixture without calling an LLM."""

    validate_config(config)
    provider_summary = _provider_model_summary(config, provider)
    suggested_plans = ["validation_module_registry_l0", "l0"]
    plan_keys = _test_plan_keys()
    missing_plans = [plan for plan in suggested_plans if plan not in plan_keys]
    if missing_plans:
        raise ProviderAdapterError(f"triage quality smoke selected unknown plan_key(s): {missing_plans}")
    advice = {
        "schema_version": TRIAGE_ADVICE_SCHEMA_VERSION,
        "provider": provider_summary["provider"],
        "model": provider_summary["model"],
        "actionability": {
            "is_actionable": True,
            "reason": "fixture contains a concrete failed plan, suspected files, and a reproduce command",
            "confidence": 0.88,
            "handoff_mode": "bug_promotion",
        },
        "classification": {
            "severity": "P1",
            "module": "validation.runner",
            "failure_kind": "workflow_regression",
            "infra_only": False,
        },
        "issue_draft": {
            "title": "[P1][validation.runner] Nightly failure intake context quality regression",
            "body_sections": [
                "Failure Summary",
                "Regression Locator",
                "Agent Handoff",
                "Production Gates",
            ],
            "contains_reproduce_command": True,
            "contains_suspected_files": True,
            "full_logs_included": False,
        },
        "test_plan_advice": [
            {
                "plan_key": "validation_module_registry_l0",
                "reason": "validates workflow/catalog routing without production runtime access",
                "risk": "low",
                "budget_seconds": 300,
                "expected_evidence": "pytest and catalog guardrail evidence",
            },
            {
                "plan_key": "l0",
                "reason": "runs the standard static guardrail for changed workflow files",
                "risk": "low",
                "budget_seconds": 300,
                "expected_evidence": "L0 static guardrail evidence",
            },
        ],
        "prompt_quality": {
            "failure_summary_included": True,
            "reproduce_command_included": True,
            "suspected_files_included": True,
            "code_intelligence_refs_required": True,
            "full_repo_scan_allowed": False,
            "full_logs_included": False,
        },
        "llm_invocation_evidence": {
            "schema_version": LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION,
            "provider": provider_summary["provider"],
            "model": provider_summary["model"],
            "invoked": False,
            "reason": "schema_quality_smoke_no_network",
            "input_policy": "compact_failure_event_plus_code_intelligence_refs_only",
            "redaction_applied": True,
        },
        "deterministic_gate": {
            "schema_valid": True,
            "plan_keys_allowlisted": True,
            "issue_creation_allowed": True,
            "bug_json_write_allowed_from_ci": False,
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        },
    }
    validate_triage_advice(advice, plan_keys=plan_keys)
    return advice


def validate_triage_advice(advice: dict[str, Any], *, plan_keys: set[str] | None = None) -> None:
    if advice.get("schema_version") != TRIAGE_ADVICE_SCHEMA_VERSION:
        raise ProviderAdapterError("triage advice schema_version invalid")
    for key in ("actionability", "classification", "issue_draft", "test_plan_advice", "llm_invocation_evidence"):
        if not isinstance(advice.get(key), dict if key != "test_plan_advice" else list):
            raise ProviderAdapterError(f"triage advice missing object/list field: {key}")
    if advice["issue_draft"].get("full_logs_included") is not False:
        raise ProviderAdapterError("triage advice must not include full logs")
    if advice["prompt_quality"].get("full_repo_scan_allowed") is not False:
        raise ProviderAdapterError("triage advice must keep full repo scans disabled")
    plan_keys = plan_keys or _test_plan_keys()
    for item in advice.get("test_plan_advice") or []:
        plan_key = str(item.get("plan_key") or "")
        if plan_key not in plan_keys:
            raise ProviderAdapterError(f"triage advice selected unknown plan_key: {plan_key}")


def _print_success(label: str, payload: dict[str, Any], *, as_json: bool) -> None:
    safe_payload = json.loads(redact_secret_text(json.dumps(payload, ensure_ascii=False)))
    if as_json:
        print(json.dumps({"gate": "passed", "check": label, **safe_payload}, ensure_ascii=False, sort_keys=True))
        return
    details = " ".join(f"{key}={value}" for key, value in safe_payload.items())
    print(f"gate=passed check={label} {details}".strip())


def llm_invocation_public_summary(record: dict[str, Any] | None) -> dict[str, Any]:
    """Return the small non-secret LLM status summary allowed in artifacts."""

    source = record if isinstance(record, dict) else {}
    summary: dict[str, Any] = {
        "schema_version": source.get("schema_version"),
        "provider": source.get("provider"),
        "model": source.get("model"),
        "invoked": bool(source.get("invoked")),
        "reason": source.get("reason"),
        "input_policy": source.get("input_policy"),
        "redaction_applied": bool(source.get("redaction_applied", True)),
    }
    units = source.get("usage_summary")
    if isinstance(units, dict):
        summary["usage_units"] = {
            "prompt": units.get("prompt_units"),
            "completion": units.get("completion_units"),
            "total": units.get("total_units"),
        }
    provider_chain = source.get("provider_chain")
    if isinstance(provider_chain, list):
        summary["provider_chain"] = provider_chain
        summary["fallback_used"] = bool(source.get("fallback_used"))
        summary["fallback_reason"] = source.get("fallback_reason")
    if source.get("error"):
        summary["error"] = redact_secret_text(str(source.get("error")))
    return json.loads(redact_secret_text(json.dumps(summary, ensure_ascii=False)))


def public_advisory_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    """Build an allowlisted compact artifact for CI/Nightly LLM advisory outputs."""

    llm_summary = llm_invocation_public_summary(payload.get("llm_invocation_evidence"))
    if payload.get("schema_version") == TEST_PLAN_ADVICE_SCHEMA_VERSION:
        gate = payload.get("deterministic_gate") if isinstance(payload.get("deterministic_gate"), dict) else {}
        return {
            "schema_version": payload.get("schema_version"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "effective_provider": payload.get("effective_provider"),
            "effective_model": payload.get("effective_model"),
            "llm_gate": payload.get("llm_gate"),
            "module": payload.get("module"),
            "changed_files": payload.get("changed_files") or [],
            "test_plan_advice": payload.get("test_plan_advice") or [],
            "deterministic_plan_keys": payload.get("deterministic_plan_keys") or [],
            "advised_plan_keys": payload.get("advised_plan_keys") or [],
            "advice_consumption": payload.get("advice_consumption") or {},
            "validation_select": payload.get("validation_select") or {},
            "workspace_gate": payload.get("workspace_gate") or {},
            "llm_advice": payload.get("llm_advice"),
            "llm_invoked": llm_summary["invoked"],
            "llm_invocation_evidence": llm_summary,
            "deterministic_gate": gate,
        }
    if payload.get("schema_version") == NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION:
        return {
            "schema_version": payload.get("schema_version"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "effective_provider": payload.get("effective_provider"),
            "effective_model": payload.get("effective_model"),
            "llm_gate": payload.get("llm_gate"),
            "changed_files": payload.get("changed_files") or [],
            "recent_failures": payload.get("recent_failures") or {},
            "codegraph": payload.get("codegraph") or {},
            "resource_budget_seconds": payload.get("resource_budget_seconds"),
            "queue": payload.get("queue") or [],
            "deterministic_plan_keys": payload.get("deterministic_plan_keys") or [],
            "advised_plan_keys": payload.get("advised_plan_keys") or [],
            "executed_plan_keys": payload.get("executed_plan_keys") or [],
            "advice_consumption": payload.get("advice_consumption") or {},
            "test_plan_advice_gate": payload.get("test_plan_advice_gate") or {},
            "workspace_gate": payload.get("workspace_gate") or {},
            "llm_advice": payload.get("llm_advice"),
            "llm_invoked": llm_summary["invoked"],
            "llm_invocation_evidence": llm_summary,
            "deterministic_gate": payload.get("deterministic_gate") or {},
        }
    if payload.get("schema_version") == PROMPT_EVALUATION_SCHEMA_VERSION:
        return {
            "schema_version": payload.get("schema_version"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "prompt_pack_versions": payload.get("prompt_pack_versions") or {},
            "cases_path": payload.get("cases_path"),
            "changed_files": payload.get("changed_files") or [],
            "metrics": payload.get("metrics") or {},
            "policy_gate": payload.get("policy_gate") or {},
            "llm_invoked": False,
            "llm_invocation_evidence": llm_summary,
            "rows": payload.get("rows") or [],
        }
    if payload.get("schema_version") == GUARDED_ROLLOUT_SCHEMA_VERSION:
        return {
            "schema_version": payload.get("schema_version"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "mode": payload.get("mode"),
            "opt_in": payload.get("opt_in"),
            "module": payload.get("module"),
            "module_allowlist": payload.get("module_allowlist") or [],
            "module_allowlisted": payload.get("module_allowlisted"),
            "deterministic_issue_allowed": payload.get("deterministic_issue_allowed"),
            "llm_workflow_gate": payload.get("llm_workflow_gate"),
            "issue_sections_present": payload.get("issue_sections_present") or [],
            "missing_required_issue_sections": payload.get("missing_required_issue_sections") or [],
            "false_positive_auto_file_rate": payload.get("false_positive_auto_file_rate"),
            "false_positive_threshold": payload.get("false_positive_threshold"),
            "auto_file_allowed": payload.get("auto_file_allowed"),
            "llm_can_enhance_issue": payload.get("llm_can_enhance_issue"),
            "llm_enhancement_allowed": payload.get("llm_enhancement_allowed"),
            "deterministic_issue_creation_unaffected": payload.get("deterministic_issue_creation_unaffected"),
            "workflow_gate": payload.get("workflow_gate"),
            "rejection_reasons": payload.get("rejection_reasons") or [],
            "fallback": payload.get("fallback"),
            "production_gates": payload.get("production_gates") or {},
            "llm_invoked": False,
            "llm_invocation_evidence": llm_summary,
        }
    return {
        "schema_version": payload.get("schema_version"),
        "provider": payload.get("provider"),
        "model": payload.get("model"),
        "llm_invoked": False,
        "llm_invocation_evidence": llm_summary,
    }


def _write_public_json_artifact(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact = public_advisory_artifact(payload)
    artifact["public_artifact"] = True
    serialized = json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    artifact_path.write_text(serialized, encoding="utf-8")


def cmd_validate_config(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    validate_config(config)
    provider = args.provider
    payload: dict[str, Any] = {"provider": provider}
    if provider == "deepseek_api":
        payload.update(validate_deepseek_provider(config, require_api_key=args.require_api_key))
    elif provider == "github_models":
        payload["enabled"] = bool(config["providers"]["github_models"].get("enabled"))
        payload["required_model_family"] = config["providers"]["github_models"]["model_selector"][
            "required_model_family"
        ]
    elif provider == "deterministic":
        payload["enabled"] = bool(config["providers"].get("deterministic", {}).get("enabled", True))
    else:
        raise ProviderAdapterError(f"unsupported provider: {provider}")
    _print_success("validate-config", payload, as_json=args.json)
    return 0


def cmd_discover_github_models(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    validate_config(config)
    if args.catalog_file:
        payload = json.loads(Path(args.catalog_file).read_text(encoding="utf-8-sig"))
    else:
        payload = fetch_github_models_catalog(config)
    selected = select_github_model(
        payload,
        config["providers"]["github_models"]["model_selector"],
    )
    _print_success("github-models-catalog", selected, as_json=args.json)
    return 0


def cmd_triage_quality_smoke(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    provider = args.provider or str(config.get("default_provider") or "deterministic")
    advice = build_triage_quality_smoke(provider, config)
    if args.output:
        _write_public_json_artifact(args.output, advice)
    compact = {
        "provider": advice["provider"],
        "model": advice["model"],
        "schema_version": advice["schema_version"],
        "actionable": advice["actionability"]["is_actionable"],
        "suggested_plan_count": len(advice["test_plan_advice"]),
        "issue_draft": "present",
        "full_logs_included": advice["issue_draft"]["full_logs_included"],
        "llm_invoked": advice["llm_invocation_evidence"]["invoked"],
        "artifact": args.output,
    }
    _print_success("triage-quality-smoke", compact, as_json=args.json)
    return 0


def _split_csv(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend(item.strip() for item in str(value).split(",") if item.strip())
    return result


def cmd_test_plan_advice(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    provider = args.provider or str(config.get("default_provider") or "deterministic")
    advice = build_test_plan_advice(
        provider,
        config,
        plan_keys=_split_csv(args.plan_key),
        changed_files=_split_csv(args.changed_file),
        module=args.module,
        workspace_path=args.workspace_path,
        invoke_llm=args.invoke_llm,
        fallback_on_llm_error=not args.fail_on_llm_error,
    )
    if args.output:
        _write_public_json_artifact(args.output, advice)
    gate = advice["deterministic_gate"]
    compact = {
        "provider": advice["provider"],
        "model": advice["model"],
        "schema_version": advice["schema_version"],
        "workflow_gate": gate["workflow_gate"],
        "advised_plan_count": len(advice["test_plan_advice"]),
        "allowed_plan_count": len([item for item in advice["test_plan_advice"] if item["allowed"]]),
        "validation_select_compatible": gate["validation_select_compatible"],
        "workspace_path_allowed": gate["workspace_path_allowed"],
        "llm_invoked": advice["llm_invocation_evidence"]["invoked"],
        "artifact": args.output,
    }
    _print_success("test-plan-advice", compact, as_json=args.json)
    return 0


def cmd_nightly_scheduler_advice(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    provider = args.provider or str(config.get("default_provider") or "deterministic")
    advice = build_nightly_scheduler_advice(
        provider,
        config,
        changed_files=_split_csv(args.changed_file),
        recent_failure_modules=_split_csv(args.recent_failure_module),
        recent_failure_plan_keys=_split_csv(args.recent_failure_plan_key),
        codegraph_freshness=args.codegraph_freshness,
        resource_budget_seconds=args.resource_budget_seconds,
        workspace_path=args.workspace_path,
        invoke_llm=args.invoke_llm,
        fallback_on_llm_error=not args.fail_on_llm_error,
    )
    if args.output:
        _write_public_json_artifact(args.output, advice)
    gate = advice["deterministic_gate"]
    compact = {
        "provider": advice["provider"],
        "model": advice["model"],
        "schema_version": advice["schema_version"],
        "workflow_gate": gate["workflow_gate"],
        "queue_count": len(advice["queue"]),
        "allowed_plan_count": gate["allowed_plan_count"],
        "deferred_plan_count": gate["deferred_plan_count"],
        "codegraph_warning": advice["codegraph"]["warning"],
        "llm_invoked": advice["llm_invocation_evidence"]["invoked"],
        "artifact": args.output,
    }
    _print_success("nightly-scheduler-advice", compact, as_json=args.json)
    return 0


def cmd_prompt_evaluation(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    provider = args.provider or str(config.get("default_provider") or "deterministic")
    evaluation = build_prompt_evaluation(
        provider,
        config,
        cases_path=Path(args.cases),
        prompt_root=Path(args.prompt_root),
        changed_files=_split_csv(args.changed_file),
        false_positive_threshold=args.false_positive_threshold,
    )
    if args.output:
        _write_public_json_artifact(args.output, evaluation)
    metrics = evaluation["metrics"]
    compact = {
        "provider": evaluation["provider"],
        "model": evaluation["model"],
        "schema_version": evaluation["schema_version"],
        "workflow_gate": evaluation["policy_gate"]["workflow_gate"],
        "case_count": metrics["case_count"],
        "actionability_precision": metrics["actionability_precision"],
        "false_positive_auto_file_rate": metrics["false_positive_auto_file_rate"],
        "dedupe_hit_rate": metrics["dedupe_hit_rate"],
        "plan_recommendation_accuracy": metrics["plan_recommendation_accuracy"],
        "issue_body_completeness": metrics["issue_body_completeness"],
        "average_prompt_tokens": metrics["average_prompt_tokens"],
        "average_completion_tokens": metrics["average_completion_tokens"],
        "llm_invoked": evaluation["llm_invocation_evidence"]["invoked"],
        "artifact": args.output,
    }
    _print_success("prompt-evaluation", compact, as_json=args.json)
    return 2 if evaluation["policy_gate"]["workflow_gate"] == "blocked" else 0


def cmd_guarded_rollout_gate(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    provider = args.provider or str(config.get("default_provider") or "deterministic")
    gate = build_guarded_rollout_gate(
        provider,
        config,
        mode=args.mode,
        opt_in=args.opt_in,
        module=args.module,
        issue_sections=_split_csv(args.issue_section),
        deterministic_issue_allowed=not args.deterministic_denied,
        llm_workflow_gate=args.llm_workflow_gate,
        false_positive_rate=args.false_positive_rate,
        false_positive_threshold=args.false_positive_threshold,
    )
    if args.output:
        _write_public_json_artifact(args.output, gate)
    compact = {
        "provider": gate["provider"],
        "model": gate["model"],
        "schema_version": gate["schema_version"],
        "workflow_gate": gate["workflow_gate"],
        "mode": gate["mode"],
        "opt_in": gate["opt_in"],
        "module_allowlisted": gate["module_allowlisted"],
        "auto_file_allowed": gate["auto_file_allowed"],
        "llm_can_enhance_issue": gate["llm_can_enhance_issue"],
        "llm_enhancement_allowed": gate["llm_enhancement_allowed"],
        "rejection_reasons": gate["rejection_reasons"],
        "llm_invoked": gate["llm_invocation_evidence"]["invoked"],
        "artifact": args.output,
    }
    _print_success("guarded-rollout-gate", compact, as_json=args.json)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock validation LLM provider adapter")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--json", action="store_true", help="Emit compact JSON output")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-config")
    validate.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    validate.add_argument(
        "--provider",
        default="deterministic",
        choices=["deterministic", "github_models", "deepseek_api"],
    )
    validate.add_argument("--require-api-key", action="store_true")
    validate.set_defaults(func=cmd_validate_config)

    discover = subparsers.add_parser("discover-github-models")
    discover.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    discover.add_argument("--catalog-file", default=None)
    discover.set_defaults(func=cmd_discover_github_models)

    quality = subparsers.add_parser("triage-quality-smoke")
    quality.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    quality.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default=None)
    quality.add_argument("--output", default=None, help="Optional ignored artifact path for full triage advice JSON.")
    quality.set_defaults(func=cmd_triage_quality_smoke)

    plan = subparsers.add_parser("test-plan-advice")
    plan.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    plan.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default=None)
    plan.add_argument("--plan-key", action="append", default=None, help="Plan key intent; may be repeated or comma-separated.")
    plan.add_argument("--changed-file", action="append", default=None, help="Changed file; may be repeated or comma-separated.")
    plan.add_argument("--module", default=None)
    plan.add_argument("--workspace-path", default=None)
    plan.add_argument(
        "--invoke-llm",
        action="store_true",
        help="Call the configured provider for advisory JSON. Deterministic gates remain authoritative.",
    )
    plan.add_argument(
        "--fail-on-llm-error",
        action="store_true",
        help="Fail instead of falling back to deterministic advice if live LLM invocation errors.",
    )
    plan.add_argument("--output", default=None, help="Optional ignored artifact path for full test-plan advice JSON.")
    plan.set_defaults(func=cmd_test_plan_advice)

    scheduler = subparsers.add_parser("nightly-scheduler-advice")
    scheduler.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    scheduler.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default=None)
    scheduler.add_argument("--changed-file", action="append", default=None, help="Changed file; may be repeated or comma-separated.")
    scheduler.add_argument(
        "--recent-failure-module",
        action="append",
        default=None,
        help="Recent failed module; may be repeated or comma-separated.",
    )
    scheduler.add_argument(
        "--recent-failure-plan-key",
        action="append",
        default=None,
        help="Recent failed plan_key; may be repeated or comma-separated.",
    )
    scheduler.add_argument("--codegraph-freshness", choices=sorted(CODEGRAPH_FRESHNESS_VALUES), default="unknown")
    scheduler.add_argument("--resource-budget-seconds", type=int, default=900)
    scheduler.add_argument("--workspace-path", default=None)
    scheduler.add_argument(
        "--invoke-llm",
        action="store_true",
        help="Call the configured provider for advisory JSON. Deterministic queue gates remain authoritative.",
    )
    scheduler.add_argument(
        "--fail-on-llm-error",
        action="store_true",
        help="Fail instead of falling back to deterministic advice if live LLM invocation errors.",
    )
    scheduler.add_argument("--output", default=None, help="Optional ignored artifact path for full scheduler advice JSON.")
    scheduler.set_defaults(func=cmd_nightly_scheduler_advice)

    evaluation = subparsers.add_parser("prompt-evaluation")
    evaluation.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    evaluation.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default=None)
    evaluation.add_argument("--cases", default=str(DEFAULT_EVALUATION_CASES))
    evaluation.add_argument("--prompt-root", default=str(DEFAULT_PROMPT_PACK_ROOT))
    evaluation.add_argument("--changed-file", action="append", default=None, help="Changed file; may be repeated or comma-separated.")
    evaluation.add_argument("--false-positive-threshold", type=float, default=0.10)
    evaluation.add_argument("--output", default=None, help="Optional ignored artifact path for full prompt evaluation JSON.")
    evaluation.set_defaults(func=cmd_prompt_evaluation)

    rollout = subparsers.add_parser("guarded-rollout-gate")
    rollout.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Emit compact JSON output")
    rollout.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default=None)
    rollout.add_argument("--mode", choices=sorted(GUARDED_ROLLOUT_MODES), default=None)
    rollout.add_argument("--opt-in", action="store_true", default=None)
    rollout.add_argument("--module", default=None)
    rollout.add_argument("--issue-section", action="append", default=None, help="Issue section; may be repeated or comma-separated.")
    rollout.add_argument("--deterministic-denied", action="store_true")
    rollout.add_argument("--llm-workflow-gate", default="ready")
    rollout.add_argument("--false-positive-rate", type=float, default=0.0)
    rollout.add_argument("--false-positive-threshold", type=float, default=0.10)
    rollout.add_argument("--output", default=None, help="Optional ignored artifact path for full guarded rollout gate JSON.")
    rollout.set_defaults(func=cmd_guarded_rollout_gate)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ProviderAdapterError, DeepSeekConfigError) as exc:
        message = redact_secret_text(str(exc))
        if args.json:
            print(json.dumps({"gate": "failed", "error": message}, ensure_ascii=False), file=sys.stderr)
        else:
            print(f"gate=failed error={message}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

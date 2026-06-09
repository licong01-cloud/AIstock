"""Provider adapter for AIstock validation LLM triage.

Phase 1 is deliberately provider/config focused: it validates model selection,
credential resolution, and redaction without writing GitHub Issues or BUG JSON.
"""

from __future__ import annotations

import argparse
import json
import os
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

GITHUB_MODELS_DEEPSEEK_MODEL_FAMILY = "deepseek-r1"
GITHUB_MODELS_DEEPSEEK_MODEL_ID = "deepseek/deepseek-r1"
TRIAGE_ADVICE_SCHEMA_VERSION = "aistock_deepseek_triage_advice_v1"
LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION = "aistock_llm_invocation_evidence_v1"


DEFAULT_CONFIG_PATH = ROOT / "configs" / "validation" / "llm_triage.yaml"


class ProviderAdapterError(RuntimeError):
    """Raised when provider config or model selection fails closed."""


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    if not isinstance(config, dict):
        raise ProviderAdapterError("llm triage config must be a mapping")
    return config


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != "aistock_llm_triage_config_v1":
        raise ProviderAdapterError("unsupported llm triage config schema_version")
    providers = config.get("providers")
    if not isinstance(providers, dict):
        raise ProviderAdapterError("providers must be configured")
    deepseek_api = providers.get("deepseek_api") or {}
    if deepseek_api.get("model") != DEFAULT_DEEPSEEK_MODEL:
        raise ProviderAdapterError("deepseek_api.model must be deepseek-v4-pro")
    if deepseek_api.get("enabled") is not False:
        raise ProviderAdapterError("deepseek_api must be disabled by default")
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

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProviderAdapterError("provider output JSON schema invalid") from exc
    if not isinstance(payload, dict):
        raise ProviderAdapterError("provider output JSON schema invalid")
    return payload


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


def fetch_github_models_catalog(config: dict[str, Any], *, token: str | None = None) -> Any:
    github_models = config["providers"]["github_models"]
    base_url = str(github_models.get("base_url") or "").rstrip("/")
    catalog_path = str(github_models.get("catalog_path") or "/catalog/models")
    url = f"{base_url}{catalog_path if catalog_path.startswith('/') else '/' + catalog_path}"
    token_env = str((github_models.get("auth") or {}).get("token_env") or "GITHUB_TOKEN")
    auth_token = token if token is not None else os.getenv(token_env)
    if not auth_token:
        raise ProviderAdapterError(f"{token_env} is required for GitHub Models catalog discovery")
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


def validate_deepseek_provider(config: dict[str, Any], *, require_api_key: bool) -> dict[str, Any]:
    provider = config["providers"]["deepseek_api"]
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


def _provider_model_summary(config: dict[str, Any], provider: str) -> dict[str, Any]:
    if provider == "github_models":
        selector = config["providers"]["github_models"]["model_selector"]
        preferred = selector.get("preferred_models") or [GITHUB_MODELS_DEEPSEEK_MODEL_ID]
        return {
            "provider": provider,
            "model": str(preferred[0]),
            "credential_source": config["providers"]["github_models"].get("auth", {}).get("token_env") or "GITHUB_TOKEN",
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
            "credential_source": provider_summary["credential_source"],
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
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(advice, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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

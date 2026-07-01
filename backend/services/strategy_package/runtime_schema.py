"""Shared runtime schema helpers for StrategyPackage frozen assets."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

import yaml

from backend.services.trading_core.errors import PackageAssetInvalidError

ALPHA158_LOADER_CLASS = "qlib.contrib.data.loader.Alpha158DL"
ALPHA158_SCHEMA_VERSION = "strategy_package_alpha158_schema_v1"


def load_conf_yaml_bytes(payload: bytes, *, source_uri: str | None = None) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PackageAssetInvalidError(
            "QE conf.yaml is not UTF-8",
            context={"reason_code": "strategy_package_conf_yaml_invalid", "source_uri": source_uri, "error": str(exc)},
        ) from exc
    return load_conf_yaml_text(text, source_uri=source_uri)


def load_conf_yaml_text(text: str, *, source_uri: str | None = None) -> dict[str, Any]:
    try:
        loaded = yaml.safe_load(text) or {}
    except Exception as first_exc:
        sanitized, changed = _sanitize_unresolved_jinja_for_yaml(text)
        if not changed:
            raise PackageAssetInvalidError(
                "failed to parse QE conf.yaml",
                context={"reason_code": "strategy_package_conf_yaml_invalid", "source_uri": source_uri, "error": str(first_exc)},
            ) from first_exc
        try:
            loaded = yaml.safe_load(sanitized) or {}
        except Exception as second_exc:
            raise PackageAssetInvalidError(
                "failed to parse QE conf.yaml after sanitizing unresolved templates",
                context={
                    "reason_code": "strategy_package_conf_yaml_invalid",
                    "source_uri": source_uri,
                    "error": str(second_exc),
                    "original_error": str(first_exc),
                    "template_placeholders_sanitized": True,
                },
            ) from second_exc
    if not isinstance(loaded, dict):
        raise PackageAssetInvalidError(
            "QE conf.yaml must be a mapping",
            context={"reason_code": "strategy_package_conf_yaml_invalid", "source_uri": source_uri, "actual_type": type(loaded).__name__},
        )
    return loaded


def load_conf_yaml_file(conf_path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        payload = conf_path.read_bytes()
    except Exception as exc:
        raise PackageAssetInvalidError(
            f"failed to read QE conf.yaml for {purpose}",
            context={"reason_code": "strategy_package_conf_yaml_missing", "conf_path": str(conf_path), "error": str(exc)},
        ) from exc
    return load_conf_yaml_bytes(payload, source_uri=str(conf_path))


def find_alpha158_node(node: Any) -> dict[str, Any] | None:
    if isinstance(node, dict):
        if node.get("class") == ALPHA158_LOADER_CLASS:
            return copy.deepcopy(node)
        for value in node.values():
            found = find_alpha158_node(value)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = find_alpha158_node(value)
            if found is not None:
                return found
    return None


def extract_alpha158_aliases(node: Any) -> list[str]:
    alpha_node = node if isinstance(node, dict) and node.get("class") == ALPHA158_LOADER_CLASS else find_alpha158_node(node)
    if not isinstance(alpha_node, dict):
        return []
    try:
        feature = alpha_node["kwargs"]["config"]["feature"]
        aliases = feature[1]
    except Exception:
        aliases = None
    if isinstance(aliases, list) and all(isinstance(item, str) for item in aliases):
        return [str(item) for item in aliases]
    return []


def alpha158_schema_payload(conf: dict[str, Any], *, source_conf_relpath: str = "conf.yaml") -> dict[str, Any] | None:
    node = find_alpha158_node(conf)
    if node is None:
        return None
    aliases = extract_alpha158_aliases(node)
    if not aliases:
        raise PackageAssetInvalidError(
            "Alpha158DL node exists but aliases are missing",
            context={"reason_code": "strategy_package_alpha158_aliases_missing", "source_conf_relpath": source_conf_relpath},
        )
    expressions = _alpha158_expressions(node)
    if not expressions:
        raise PackageAssetInvalidError(
            "Alpha158DL node exists but expressions are missing",
            context={"reason_code": "strategy_package_alpha158_expressions_missing", "source_conf_relpath": source_conf_relpath},
        )
    return {
        "schema_version": ALPHA158_SCHEMA_VERSION,
        "loader_class": ALPHA158_LOADER_CLASS,
        "loader_node": node,
        "aliases": aliases,
        "expression_count": len(expressions),
        "alias_count": len(aliases),
        "source_conf_relpath": source_conf_relpath,
    }


def alpha158_schema_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def minimal_conf_with_alpha158(schema_payload: dict[str, Any]) -> str:
    node = schema_payload.get("loader_node")
    aliases = schema_payload.get("aliases")
    if not isinstance(node, dict) or node.get("class") != ALPHA158_LOADER_CLASS:
        raise PackageAssetInvalidError(
            "frozen Alpha158 schema payload does not contain a valid loader_node",
            context={"reason_code": "strategy_package_alpha158_schema_invalid"},
        )
    if not isinstance(aliases, list) or not aliases:
        raise PackageAssetInvalidError(
            "frozen Alpha158 schema payload does not contain aliases",
            context={"reason_code": "strategy_package_alpha158_schema_invalid"},
        )
    expressions = _alpha158_expressions(node)
    if not expressions:
        raise PackageAssetInvalidError(
            "frozen Alpha158 schema payload does not contain expressions",
            context={"reason_code": "strategy_package_alpha158_schema_invalid"},
        )
    conf = {"task": {"dataset": {"kwargs": {"handler": {"kwargs": {"data_loader": node}}}}}}
    return yaml.safe_dump(conf, allow_unicode=True, sort_keys=False)


def pt_model_uri_from_conf(conf: dict[str, Any]) -> str | None:
    for path in (
        ("task", "model", "kwargs", "pt_model_uri"),
        ("model", "kwargs", "pt_model_uri"),
        ("task", "dataset", "kwargs", "handler", "kwargs", "pt_model_uri"),
    ):
        current: Any = conf
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        text = str(current or "").strip()
        if text:
            return text
    found = _find_key(conf, "pt_model_uri")
    return str(found).strip() if found else None


def model_code_module_from_pt_uri(pt_model_uri: str | None) -> str | None:
    text = str(pt_model_uri or "").strip()
    if not text or "." not in text:
        return None
    module = text.rsplit(".", 1)[0].strip()
    if not module or module.startswith(("qlib", "sklearn", "lightgbm", "xgboost", "catboost", "torch", "numpy", "pandas")):
        return None
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", module):
        raise PackageAssetInvalidError(
            "pt_model_uri points to an unsafe Python module path",
            context={"reason_code": "strategy_package_model_code_module_invalid", "pt_model_uri": pt_model_uri, "module_name": module},
        )
    return module


def _alpha158_expressions(node: dict[str, Any]) -> list[Any]:
    try:
        feature = node["kwargs"]["config"]["feature"]
        expressions = feature[0]
    except Exception:
        expressions = []
    return expressions if isinstance(expressions, list) else []


def _find_key(node: Any, key: str) -> Any:
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for value in node.values():
            found = _find_key(value, key)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _find_key(value, key)
            if found is not None:
                return found
    return None


def _sanitize_unresolved_jinja_for_yaml(text: str) -> tuple[str, bool]:
    changed = False
    sanitized_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith(("{%", "{#")):
            indent = line[: len(line) - len(stripped)]
            newline = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
            sanitized_lines.append(f"{indent}# {stripped.rstrip()}{newline}")
            changed = True
            continue
        sanitized, line_changed = _replace_unquoted_jinja_expressions(line)
        sanitized_lines.append(sanitized)
        changed = changed or line_changed
    return "".join(sanitized_lines), changed


def _replace_unquoted_jinja_expressions(line: str) -> tuple[str, bool]:
    result: list[str] = []
    changed = False
    in_single = False
    in_double = False
    i = 0
    while i < len(line):
        if not in_single and not in_double and line.startswith("{{", i):
            end = line.find("}}", i + 2)
            if end != -1:
                expr = line[i + 2 : end].strip()
                safe_expr = re.sub(r"[^0-9A-Za-z_]+", "_", expr).strip("_")[:64] or "expr"
                result.append(json.dumps(f"__AISTOCK_UNRESOLVED_JINJA_{safe_expr}__"))
                changed = True
                i = end + 2
                continue
        char = line[i]
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        result.append(char)
        i += 1
    return "".join(result), changed

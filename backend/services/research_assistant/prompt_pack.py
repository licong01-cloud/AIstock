"""File-backed prompt pack loader for Research Assistant runtime prompts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import sha256_json

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROMPT_PACK_PATH = REPO_ROOT / "prompt_packs" / "research_assistant" / "main" / "pack.yaml"

@dataclass(frozen=True)
class PromptPackSnapshot:
    pack_key: str
    pack_version: str
    source_path: str
    source_sha256: str
    nodes: list[dict[str, Any]]

    @property
    def source_id(self) -> str:
        return f"prompt_source_{self.pack_key.replace('.', '_')}_{self.pack_version.replace('.', '_')}"

    @property
    def activation_id(self) -> str:
        return f"prompt_activation_{self.pack_key.replace('.', '_')}_{self.pack_version.replace('.', '_')}_active"


def load_prompt_pack(path: Path | None = None) -> PromptPackSnapshot:
    pack_path = Path(path or DEFAULT_PROMPT_PACK_PATH)
    if not pack_path.exists():
        raise FileNotFoundError(f"Research Assistant prompt pack not found: {pack_path}")
    raw_text = pack_path.read_text(encoding="utf-8-sig")
    payload = yaml.safe_load(raw_text) or {}
    _validate_pack_payload(payload, pack_path)
    nodes: list[dict[str, Any]] = []
    for item in payload["nodes"]:
        prompt_file = pack_path.parent / str(item["prompt_file"])
        if not prompt_file.exists():
            raise FileNotFoundError(f"prompt node file not found for {item['prompt_key']}: {prompt_file}")
        prompt_text = prompt_file.read_text(encoding="utf-8-sig").strip()
        _validate_prompt_text(str(item["prompt_key"]), prompt_text)
        node = {k: v for k, v in dict(item).items() if k != "prompt_file"}
        node.setdefault("version", str(payload["pack_version"] or "1.0.0"))
        node["prompt_text"] = prompt_text
        node["source_ref"] = _repo_relative(prompt_file)
        node["checksum"] = sha256_json(
            {
                "prompt_key": node["prompt_key"],
                "version": node["version"],
                "prompt_text": prompt_text,
                "source_ref": node["source_ref"],
            }
        )
        nodes.append(node)
    source_sha256 = sha256_json(
        {
            "pack": payload,
            "nodes": [{"prompt_key": node["prompt_key"], "checksum": node["checksum"]} for node in nodes],
        }
    )
    return PromptPackSnapshot(
        pack_key=str(payload["pack_key"]),
        pack_version=str(payload["pack_version"]),
        source_path=_repo_relative(pack_path),
        source_sha256=source_sha256,
        nodes=nodes,
    )


def _validate_pack_payload(payload: dict[str, Any], pack_path: Path) -> None:
    required = {"schema_version", "pack_key", "pack_version", "nodes"}
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"prompt pack {pack_path} missing required keys: {missing}")
    if payload["schema_version"] != "aistock_prompt_pack_v1":
        raise ValueError(f"unsupported prompt pack schema: {payload['schema_version']}")
    if not isinstance(payload["nodes"], list) or not payload["nodes"]:
        raise ValueError("prompt pack must contain at least one node")
    seen: set[str] = set()
    for item in payload["nodes"]:
        if not isinstance(item, dict):
            raise ValueError("prompt pack node must be an object")
        node_required = {"prompt_key", "title", "category", "tree_path", "phase", "risk_level", "trigger_json", "prompt_file"}
        missing_node = sorted(node_required - set(item))
        if missing_node:
            raise ValueError(f"prompt node missing required keys: {missing_node}")
        key = str(item["prompt_key"])
        if key in seen:
            raise ValueError(f"duplicate prompt_key in pack: {key}")
        seen.add(key)


def _validate_prompt_text(prompt_key: str, prompt_text: str) -> None:
    if not prompt_text:
        raise ValueError(f"prompt node {prompt_key} has empty text")


def _repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())

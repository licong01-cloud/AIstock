"""Versioned evidence bridge from the selected original MA publication.

Only a new manifest is enriched. Frozen run rows and historical aliases are
never repaired in place; exact PIT pins replace an unproven alias in evidence.
"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.trading_core.execution_algo_retirement import require_execution_algo_active

SCHEMA = "strategy_parent_original_context_v1"


def require(condition, message):
    if not condition:
        raise StrategyPackageValidationError(message, context={"reason_code": "multi_alpha_original_context_invalid"})


def context_from_artifacts(*, run_id, child_id, attempt_id, files, topk, original_config, algo_resolver):
    """Pure extraction after the publication/attempt identity has been verified."""
    decoded, evidence = {}, {}
    for name in ("conf.yaml", "qe_direct_v2_dataset_binding.json", "qe_frozen_build_spec.json"):
        item = files[name]
        raw = item["content"]
        require(isinstance(raw, bytes) and len(raw) <= 1024 * 1024, "original context file size is invalid")
        digest = hashlib.sha256(raw).hexdigest()
        require(digest == item["sha256"], "original context artifact SHA mismatch: " + name)
        decoded[name] = yaml.safe_load(raw) if name.endswith("yaml") else json.loads(raw)
        evidence[name] = {"sha256": digest, "encoding": "base64", "content": base64.b64encode(raw).decode("ascii")}
    conf, binding, spec = (decoded[n] for n in ("conf.yaml", "qe_direct_v2_dataset_binding.json", "qe_frozen_build_spec.json"))
    port = conf["port_analysis_config"]
    kwargs = port["strategy"]["kwargs"]
    require(kwargs["topk"] == topk and type(kwargs["n_drop"]) is int, "original strategy/topk differs")
    pins = binding["selection_pins"]
    require(pins["instruments_file"] == conf["market"] + ".txt"
            and pins["instrument_name"] == conf["market"]
            and spec["pins"]["instruments_sha256"] == pins["instruments_sha256"],
            "original pool pins/config/build spec differ")
    inner = port["executor"]["kwargs"]["inner_strategy"]
    algo = algo_resolver(inner)
    require_execution_algo_active(algo, operation="multi_alpha_original_context_bridge",
                                  semantic_path="selected_original_executor")
    loader = conf["data_handler_config"]["data_loader"]["kwargs"]
    horizon = loader["label_horizon"]
    require(type(horizon) is int and horizon > 0, "original label horizon unavailable")
    derived = {"n_drop": kwargs["n_drop"], "stock_pool": conf["market"], "label_horizon": horizon,
               "execution_algo": algo,
               "filtered_pool": {"schema_version": "original_pit_selection_pins_v1", **dict(pins)}}
    # A legacy alias is neither fabricated nor overwritten. If one exists,
    # callers must prove that alias using the old complete context path.
    require(not original_config.get("filtered_pool"), "explicit legacy filtered_pool cannot be replaced by a pin bridge")
    for key, value in derived.items():
        if key in original_config and original_config[key] is not None:
            require(original_config[key] == value, "original flattened context conflicts: " + key)
    bridge = {"schema_version": SCHEMA, "run_id": run_id, "child_id": child_id,
              "selected_attempt_id": attempt_id, "source_artifacts": evidence, "derived": derived,
              "daily_strategy": {k: v for k, v in kwargs.items() if k != "signal"},
              "executor": port["executor"], "exchange_kwargs": port["backtest"]["exchange_kwargs"],
              "dataset_binding": binding, "legacy_filtered_pool_alias": "UNKNOWN",
              "authority": "original_source_evidence_only", "source_rows_modified": False}
    bridge["bridge_sha256"] = hashlib.sha256(json.dumps(bridge, sort_keys=True, ensure_ascii=False,
                                                       separators=(",", ":"), default=str).encode()).hexdigest()
    return {**original_config, **derived}, bridge


def resolve_execution(inner):
    from backend.services.quantevolver.config_composer import ConfigComposer, SUPPORTED_QE_EXECUTION_ALGOS

    matches = []
    for algo in sorted(SUPPORTED_QE_EXECUTION_ALGOS):
        try:
            effective = ConfigComposer._execution_algo_config(algo, inner.get("kwargs") or {})
        except ValueError:
            # A candidate requiring an absent original parameter is not a
            # match. If it is the actual strategy, zero matches fails closed.
            continue
        if (effective["class"], effective["module_path"], effective["kwargs"]) == (
                inner.get("class"), inner.get("module_path"), inner.get("kwargs") or {}):
            ConfigComposer._execution_algo_catalog_entry(algo)  # Real enabled catalog, no synthetic defaults.
            matches.append(algo)
    require(len(matches) == 1, "original executable has no unique supported non-retired QE mapping")
    return matches[0]


class OriginalContextResolver:
    def __init__(self, *, repository=None, roots: Sequence[str | Path] = (), algo_resolver=resolve_execution):
        from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepository

        self.repository = repository or MultiAlphaDurableRepository()
        self.roots = tuple(Path(p) for p in roots)
        self.algo_resolver = algo_resolver

    def resolve(self, *, run_id: str, scheme: str, topk: int, original_config: Mapping[str, Any]):
        from backend.services.multi_alpha.durable_execution_adapter import QEWorkspacePredBacktestAdapter

        children = [r for r in self.repository.list_children(run_id) if r.get("child_kind") == "scheme"
                    and r.get("weighting_scheme") == scheme and r.get("status") == "succeeded"]
        require(len(children) == 1 and children[0].get("selected_attempt_id"), "unique selected original context unavailable")
        child = children[0]
        attempt = self.repository.get_attempt(child["selected_attempt_id"])
        require(attempt and attempt["status"] == "succeeded" and attempt["child_id"] == child["child_id"],
                "original context attempt is not the successful selected child")
        candidates = []
        for root in self.roots or (None,):
            adapter = QEWorkspacePredBacktestAdapter(repository=self.repository, workspace_root=root)
            directory = adapter._attempt_workspace(run_id, child["child_id"], child["selected_attempt_id"])
            if (directory / "artifact_manifest.json").is_file():
                candidates.append((adapter, directory))
        require(len(candidates) == 1, "selected original context publication missing or ambiguous")
        adapter, directory = candidates[0]
        published = adapter.load_published_artifacts(run_id=run_id, child_id=child["child_id"],
                                                     attempt_id=child["selected_attempt_id"])
        declared = (attempt.get("artifact_manifest_json") or {}).get("files", {})
        refs = {}
        for name in ("conf.yaml", "qe_direct_v2_dataset_binding.json", "qe_frozen_build_spec.json"):
            expected = declared.get(name)
            require(expected and published.artifact_manifest["files"].get(name) == expected,
                    "original context publication differs from selected attempt: " + name)
            path = directory / name
            require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 1024 * 1024,
                    "original context asset unavailable or oversized")
            raw = path.read_bytes()
            require(len(raw) == expected["size"], "original context asset size differs from the selected attempt")
            refs[name] = {"sha256": expected["sha256"], "content": raw}
        return context_from_artifacts(run_id=run_id, child_id=child["child_id"],
                                      attempt_id=child["selected_attempt_id"], files=refs, topk=topk,
                                      original_config=original_config, algo_resolver=self.algo_resolver)

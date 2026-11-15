"""Strict export evidence for an explicitly frozen QE multi-alpha formula."""
from __future__ import annotations

import base64
import hashlib
import math
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse
from urllib.request import url2pathname

from backend.services.multi_alpha.combine_backtest import REQUEST_SNAPSHOT_KEY, parse_request
from backend.services.trading_core.errors import StrategyPackageValidationError


def require(condition: bool, message: str) -> None:
    if not condition:
        raise StrategyPackageValidationError(message, context={"reason_code": "multi_alpha_frozen_contract_invalid"})


def freeze_source_contract(run: Mapping[str, Any], result: Mapping[str, Any], roster: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    config = run.get("backtest_config_json")
    require(isinstance(config, Mapping) and isinstance(config.get(REQUEST_SNAPSHOT_KEY), Mapping), "frozen export requires the exact persisted QE request")
    request = parse_request(config[REQUEST_SNAPSHOT_KEY])
    require(request.weighting_schemes == ("frozen_weighted",) and request.normalize_method == "rank"
            and request.walk_forward.get("enabled") is False, "frozen export requires the original rank/fixed-weight contract")
    expected_roster = [(leg.leg_id, tuple(leg.seed_run_ids)) for leg in request.roster]
    require(expected_roster == [(str(leg["leg_id"]), tuple(leg["seed_run_ids"])) for leg in roster], "frozen run roster differs from its request")
    seeds = [seed for _, seed_ids in expected_roster for seed in seed_ids]
    require(len(set(seeds)) == len(seeds), "frozen export cannot duplicate a seed source")
    identity = run.get("execution_identity_json") or {}
    sources = identity.get("prediction_sources") if isinstance(identity, Mapping) else None
    require(isinstance(sources, list), "frozen export requires original execution-identity prediction sources")
    source_hashes = {str(row.get("seed_run_id")): str(row.get("artifact_sha256")) for row in sources if isinstance(row, Mapping)}
    require(len(source_hashes) == len(sources) and set(source_hashes) == set(seeds)
            and all(len(value) == 64 and all(char in "0123456789abcdef" for char in value) for value in source_hashes.values()),
            "frozen original prediction hashes do not exactly cover the seed roster")
    require(run.get("normalize_method") == "rank" and isinstance(run.get("walk_forward_json"), Mapping)
            and run["walk_forward_json"].get("enabled") is False, "frozen run normalization or walk-forward differs from its request")
    contract = dict(request.frozen_weights)
    declared = dict(contract["weights"])
    actual = result.get("weights_json")
    require(isinstance(actual, Mapping), "frozen scheme weights are missing")
    actual = actual.get("leg_weights", actual)
    require(isinstance(actual, Mapping) and all(key in actual for key in declared), "frozen scheme weights omit a source leg")
    require(all(type(actual[key]) in (int, float) and math.isfinite(float(actual[key]))
                and float(actual[key]) == declared[key] for key in declared), "actual weights differ from the frozen declaration")
    windows = result.get("per_window_weights_json")
    require(isinstance(windows, list) and bool(windows)
            and all(isinstance(row, Mapping) and row.get("frozen_weights") == contract for row in windows), "frozen scheme lacks matching per-window provenance")
    positive = {key: value for key, value in declared.items() if value > 0}
    require(len(positive) >= 2 and math.isclose(math.fsum(positive.values()), 1, rel_tol=0, abs_tol=1e-12), "frozen runtime requires at least two positive legs whose original weights sum to one")
    source = Path(contract["source_ref"])
    require(source.is_file() and source.stat().st_size <= 8 * 1024**2, "frozen weight source document is unavailable or exceeds the bounded contract")
    raw = source.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == contract["source_sha256"], "frozen weight source document SHA changed")
    require(source.stat().st_size == len(raw) and hashlib.sha256(source.read_bytes()).hexdigest() == contract["source_sha256"], "frozen weight source changed while reading")
    return {"schema_version": "strategy_package_frozen_qe_weights_v1", "contract": contract,
        "source_roster": [{"leg_id": leg_id, "seed_run_ids": list(seeds)} for leg_id, seeds in expected_roster],
        "positive_weights": positive, "zero_weight_legs": [key for key, value in declared.items() if value == 0],
        "source_prediction_sha256": source_hashes,
        "source_topk": request.topk,
        "source_document": {"sha256": contract["source_sha256"], "encoding": "base64", "content": base64.b64encode(raw).decode("ascii")},
        "aggregation": "original_dtype_mean_then_daily_average_rank_minus1_plus1", "normalization": "rank", "weights_reestimated": False}


def prediction_contract(seed_run_id: str) -> dict[str, Any]:
    import pandas as pd
    from backend.services.model_store import ModelStoreService
    from backend.services.multi_alpha.combiner import normalize_frozen_prediction_frame
    source = ModelStoreService().prediction_path(run_id=seed_run_id)
    require(source.is_file() and source.stat().st_size <= 160 * 1024**2, "source seed prediction is unavailable or exceeds bounds")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    raw = pd.read_pickle(source)
    require(isinstance(raw, (pd.Series, pd.DataFrame)) and 0 < len(raw) < 4000000, "source seed prediction schema/size is invalid")
    try:
        frame = normalize_frozen_prediction_frame(raw, run_id=seed_run_id)
    except ValueError as exc:
        raise StrategyPackageValidationError("source seed prediction is invalid", context={"reason_code": "multi_alpha_frozen_contract_invalid", "seed_run_id": seed_run_id}) from exc
    dtype = str(frame["score"].dtype)
    require(dtype in {"float32", "float64"}, "source seed score dtype is unsupported")
    require(hashlib.sha256(source.read_bytes()).hexdigest() == digest, "source seed prediction changed while reading")
    return {"score_dtype": dtype, "source_prediction_sha256": digest}


def exact_runtime_weights(manifest: Any, leg_ids: Sequence[str]) -> dict[str, float]:
    evidence = (manifest.source_evidence or {}).get("multi_alpha") or {}
    frozen = evidence.get("frozen_weight_contract") or {}
    require(frozen.get("schema_version") == "strategy_package_frozen_qe_weights_v1", "new frozen runtime has no approved source contract")
    require(frozen.get("combined_score_dtype") in {"float32", "float64"}, "new frozen runtime lacks original combined-score precision")
    weights = frozen.get("positive_weights")
    require(isinstance(weights, Mapping) and set(weights) == set(leg_ids), "frozen runtime weights differ from active legs")
    require(all(type(value) in (int, float) and math.isfinite(value) and value > 0 for value in weights.values())
            and math.isclose(math.fsum(weights.values()), 1, rel_tol=0, abs_tol=1e-12), "frozen runtime weights are invalid; no renormalization is allowed")
    require(dict(weights) == evidence.get("terminal_weights") == manifest.alpha_combination_policy.weights,
            "frozen runtime weights conflict with the immutable manifest")
    declared = (frozen.get("contract") or {}).get("weights")
    require(isinstance(declared, Mapping) and dict(weights) == {key: value for key, value in declared.items() if value > 0},
            "frozen runtime weights differ from the source declaration")
    return dict(weights)


def durable_prediction_ref(run_id: str, roots: Sequence[Path] = (), *, repository: Any = None) -> dict[str, str]:
    """Read the selected original publication; never regenerate missing scores."""
    from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepository
    from backend.services.multi_alpha.durable_execution_adapter import QEWorkspacePredBacktestAdapter
    repo = repository or MultiAlphaDurableRepository()
    children = [row for row in repo.list_children(run_id) if row.get("child_kind") == "scheme"
                and row.get("weighting_scheme") == "frozen_weighted" and row.get("status") == "succeeded"]
    require(len(children) == 1 and bool(children[0].get("selected_attempt_id")), "frozen export has no unique successful selected scheme")
    child = children[0]
    attempt_id = str(child["selected_attempt_id"])
    attempt = repo.get_attempt(attempt_id)
    require(isinstance(attempt, Mapping) and attempt.get("status") == "succeeded"
            and attempt.get("child_id") == child["child_id"], "frozen selected attempt is missing or inconsistent")
    candidates = []
    for root in roots or (None,):
        adapter = QEWorkspacePredBacktestAdapter(repository=repo, workspace_root=root)
        path = adapter._attempt_workspace(run_id, child["child_id"], attempt_id)
        if (path / "artifact_manifest.json").is_file():
            candidates.append(adapter)
    require(len(candidates) == 1, "frozen selected original publication is missing or ambiguous")
    published = candidates[0].load_published_artifacts(run_id=run_id, child_id=child["child_id"], attempt_id=attempt_id)
    declared = (attempt.get("artifact_manifest_json") or {}).get("files", {}).get("combined_prediction.pkl") or {}
    actual = published.artifact_manifest.get("files", {}).get("combined_prediction.pkl") or {}
    require(actual == declared and isinstance(actual.get("sha256"), str), "frozen published prediction differs from selected attempt evidence")
    return {"uri": published.prediction_path.resolve().as_uri(), "sha256": actual["sha256"],
            "ref_source": "durable_selected_original_artifact", "selected_attempt_id": attempt_id}


def combined_prediction_contract(ref: Mapping[str, str]) -> dict[str, str]:
    import pandas as pd
    parsed = urlparse(ref["uri"])
    require(parsed.scheme == "file" and not parsed.netloc, "frozen combined prediction requires a verifiable original local publication")
    path = Path(url2pathname(parsed.path))
    require(path.is_file() and path.stat().st_size <= 160 * 1024**2, "original combined prediction is unavailable or exceeds bounds")
    require(hashlib.sha256(path.read_bytes()).hexdigest() == ref["sha256"], "original combined prediction SHA mismatch")
    frame = pd.read_pickle(path)
    require(isinstance(frame, pd.DataFrame) and frame.shape[1] == 1 and frame.index.is_unique
            and 0 < len(frame) < 4000000, "original combined prediction schema is invalid")
    dtype = str(frame.iloc[:, 0].dtype)
    require(dtype in {"float32", "float64"} and frame.iloc[:, 0].abs().lt(float("inf")).all(), "original combined prediction precision or values are invalid")
    require(hashlib.sha256(path.read_bytes()).hexdigest() == ref["sha256"], "original combined prediction changed while reading")
    return {"combined_score_dtype": dtype, "combined_prediction_sha256": ref["sha256"]}

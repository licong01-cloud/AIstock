"""Code-owned read-only dataset probe executed inside WSL or node1.

The module accepts one canonical JSON request on stdin and emits one canonical
JSON result on stdout.  It never opens a database, starts training/experiments,
or mutates the candidate.
"""

from __future__ import annotations

from datetime import date, timedelta
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Any, Mapping, Sequence

from .canonical import canonical_json_bytes
from .monthly_hmm_derive import FROZEN_HMM_INPUT_SCHEMA, MARKET_VOLUME_DEFINITION
from .monthly_unified import REQUIRED_CONSUMERS


NODE_PROBE_REQUEST_SCHEMA = "aistock_monthly_node_probe_request_v1"
NODE_PROBE_RESULT_SCHEMA = "aistock_monthly_node_probe_result_v1"
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_FIELDS = {
    "schema_version",
    "consumer_id",
    "node_id",
    "candidate_root",
    "dataset_manifest_sha256",
    "required_window",
    "file_refs",
}
_REF_FIELDS = {"role", "path", "sha256", "size"}
_HMM_ROLES = {
    "sector_data_h5",
    "index_daily_h5",
    "sector_code_map_json",
    "market_context_parquet",
    "sector_membership_spans_parquet",
    "sector_quote_availability_json",
}


class MonthlyNodeProbeError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _plain_file(root: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        raise MonthlyNodeProbeError(f"node probe relative path is invalid: {relative}")
    current = root
    for part in pure.parts:
        current /= part
        if current.is_symlink():
            raise MonthlyNodeProbeError(f"node probe path traverses a link: {relative}")
    try:
        resolved = current.resolve(strict=True)
    except OSError as exc:
        raise MonthlyNodeProbeError(f"node probe file is unavailable: {relative}") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlyNodeProbeError(f"node probe file escapes candidate: {relative}")
    return resolved


def _validated_request(value: Any) -> tuple[dict[str, Any], Path, dict[str, tuple[Path, dict[str, Any]]]]:
    if not isinstance(value, Mapping) or set(value) != _REQUEST_FIELDS:
        raise MonthlyNodeProbeError("node probe request fields differ")
    request = dict(value)
    if request["schema_version"] != NODE_PROBE_REQUEST_SCHEMA:
        raise MonthlyNodeProbeError("node probe request schema differs")
    if request["consumer_id"] not in REQUIRED_CONSUMERS:
        raise MonthlyNodeProbeError("node probe consumer is unknown")
    if request["node_id"] not in {"wsl2-5080", "rdagent-node1"}:
        raise MonthlyNodeProbeError("node probe node is unknown")
    if _SHA_RE.fullmatch(str(request["dataset_manifest_sha256"])) is None:
        raise MonthlyNodeProbeError("node probe manifest identity is invalid")
    root_value = str(request["candidate_root"] or "")
    if not (PurePosixPath(root_value).is_absolute() or Path(root_value).is_absolute()):
        raise MonthlyNodeProbeError("node probe candidate root is not absolute")
    requested_root = Path(root_value)
    root = requested_root.resolve(strict=True)
    if requested_root.is_symlink() or not root.is_dir():
        raise MonthlyNodeProbeError("node probe candidate root is linked or invalid")
    window = request["required_window"]
    if not isinstance(window, Mapping) or not window:
        raise MonthlyNodeProbeError("node probe window is invalid")
    refs = request["file_refs"]
    if not isinstance(refs, list) or not refs:
        raise MonthlyNodeProbeError("node probe file refs are empty")
    files: dict[str, tuple[Path, dict[str, Any]]] = {}
    paths: set[str] = set()
    for raw in refs:
        if not isinstance(raw, Mapping) or set(raw) != _REF_FIELDS:
            raise MonthlyNodeProbeError("node probe file ref fields differ")
        role = str(raw["role"] or "")
        relative = str(raw["path"] or "")
        digest = str(raw["sha256"] or "")
        size = raw["size"]
        if (
            not role
            or role in files
            or relative in paths
            or _SHA_RE.fullmatch(digest) is None
            or type(size) is not int
            or size <= 0
        ):
            raise MonthlyNodeProbeError("node probe file ref identity is invalid")
        path = _plain_file(root, relative)
        if path.stat().st_size != size or _sha256(path) != digest:
            raise MonthlyNodeProbeError(f"node probe file bytes differ: {role}")
        files[role] = (path, dict(raw))
        paths.add(relative)
    manifest = files.get("dataset_manifest")
    if manifest is None:
        raise MonthlyNodeProbeError("node probe manifest file is absent")
    try:
        manifest_value = json.loads(manifest[0].read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyNodeProbeError("node probe manifest is unreadable") from exc
    if (
        not isinstance(manifest_value, Mapping)
        or manifest_value.get("dataset_manifest_sha256") != request["dataset_manifest_sha256"]
    ):
        raise MonthlyNodeProbeError("node probe manifest content identity differs")
    return request, root, files


def _read_sentinel(path: Path) -> int:
    suffix = path.suffix.lower()
    if suffix == ".json":
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise MonthlyNodeProbeError("node JSON sentinel is not an object")
        return len(value)
    if suffix in {".txt", ".csv"}:
        rows = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
        if rows <= 0:
            raise MonthlyNodeProbeError("node text sentinel is empty")
        return rows
    if suffix == ".parquet":
        import pyarrow.parquet as pq

        metadata = pq.ParquetFile(path).metadata
        if metadata is None or metadata.num_rows <= 0 or metadata.num_columns <= 0:
            raise MonthlyNodeProbeError("node Parquet sentinel is empty")
        return int(metadata.num_rows)
    if suffix == ".h5":
        import pandas as pd

        with pd.HDFStore(path, mode="r") as store:
            keys = store.keys()
            if not keys:
                raise MonthlyNodeProbeError("node HDF sentinel is empty")
            storer = store.get_storer(keys[0])
            rows = int(getattr(storer, "nrows", 0) or 0)
            if rows <= 0:
                rows = len(store.get(keys[0]))
            if rows <= 0:
                raise MonthlyNodeProbeError("node HDF sentinel is empty")
            return rows
    raise MonthlyNodeProbeError(f"node sentinel type is unsupported: {path.name}")


def _qlib_counts(root: Path, window: Mapping[str, Any]) -> dict[str, int]:
    try:
        import numpy as np
        import pandas as pd
        import qlib
        from qlib.constant import REG_CN
        from qlib.data import D
    except ImportError as exc:
        raise MonthlyNodeProbeError("node Qlib runtime is unavailable") from exc
    end = str(window.get("backtest_end") or window.get("test_end") or "")
    try:
        date.fromisoformat(end)
    except ValueError as exc:
        raise MonthlyNodeProbeError("node QE probe end date is invalid") from exc
    qlib.init(
        provider_uri={
            "day": str(root / "components/daily_bin_candidate"),
            "1min": str(root / "components/minute_bin_candidate"),
        },
        region=REG_CN,
        clear_mem_cache=True,
    )
    daily = D.features(
        ["000001.SZ"],
        ["$open", "$close", "$volume"],
        start_time=end,
        end_time=end,
        freq="day",
    )
    minute = D.features(
        ["000001.SZ"],
        ["$open", "$close", "$volume"],
        start_time=end,
        end_time=end,
        freq="1min",
    )
    for label, frame in (("daily", daily), ("minute", minute)):
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise MonthlyNodeProbeError(f"node Qlib {label} read returned no rows")
        values = frame.apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise MonthlyNodeProbeError(f"node Qlib {label} read contains non-finite values")
    if len(daily) != 1 or len(minute) < 5:
        raise MonthlyNodeProbeError("node Qlib day/minute row count differs")
    return {"qlib_daily_row_count": len(daily), "qlib_minute_row_count": len(minute)}


def _hmm_counts(
    root: Path,
    files: Mapping[str, tuple[Path, dict[str, Any]]],
    window: Mapping[str, Any],
    dataset_manifest_sha256: str,
) -> dict[str, int]:
    if not _HMM_ROLES.issubset(files):
        raise MonthlyNodeProbeError("node HMM frozen input refs are incomplete")
    try:
        test_start = date.fromisoformat(str(window["test_start"]))
        backtest_end = date.fromisoformat(str(window["backtest_end"]))
    except (KeyError, ValueError) as exc:
        raise MonthlyNodeProbeError("node HMM window is invalid") from exc
    bundle = {
        "schema_version": FROZEN_HMM_INPUT_SCHEMA,
        "dataset_root": str(root),
        "dataset_identity": {"dataset_manifest_sha256": dataset_manifest_sha256},
        "market_volume_definition": MARKET_VOLUME_DEFINITION,
        "files": {
            role: {
                "relative_path": files[role][1]["path"],
                "sha256": files[role][1]["sha256"],
            }
            for role in sorted(_HMM_ROLES)
        },
    }
    from scripts.precompute_hmm_coefficients import load_frozen_coefficient_inputs

    loaded = load_frozen_coefficient_inputs(
        bundle,
        history_start=test_start - timedelta(days=int(3.0 * 365 + 30)),
        test_start=test_start,
        backtest_end=backtest_end,
    )
    maps = loaded.get("stock_sector_maps_by_date")
    active = loaded.get("active_sector_codes")
    coefficient = loaded.get("coefficient_sector_codes")
    if not isinstance(maps, Mapping) or not maps or not isinstance(active, list) or not isinstance(coefficient, list):
        raise MonthlyNodeProbeError("node HMM frozen input readback is incomplete")
    return {
        "hmm_trade_date_count": len(maps),
        "hmm_active_sector_count": len(active),
        "hmm_coefficient_sector_count": len(coefficient),
    }


def run_node_probe(value: Any) -> dict[str, Any]:
    request, root, files = _validated_request(value)
    consumer = str(request["consumer_id"])
    rows = 0
    for role, (path, _ref) in files.items():
        if role in {"dataset_manifest", *_HMM_ROLES}:
            continue
        rows += _read_sentinel(path)
    counts: dict[str, int] = {
        "verified_file_count": len(files),
        "sentinel_row_count": rows,
    }
    if consumer.startswith("qe_"):
        counts.update(_qlib_counts(root, request["required_window"]))
    if consumer == "hmm_file_only":
        counts.update(
            _hmm_counts(
                root,
                files,
                request["required_window"],
                str(request["dataset_manifest_sha256"]),
            )
        )
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise MonthlyNodeProbeError("node probe counts are invalid")
    return {
        "schema_version": NODE_PROBE_RESULT_SCHEMA,
        "status": "PASS",
        "consumer_id": consumer,
        "node_id": request["node_id"],
        "candidate_root": request["candidate_root"],
        "dataset_manifest_sha256": request["dataset_manifest_sha256"],
        "request_sha256": hashlib.sha256(canonical_json_bytes(request)).hexdigest(),
        "coverage_counts": {"unresolved_count": 0, **counts},
        "file_refs_sha256": hashlib.sha256(
            canonical_json_bytes({"file_refs": request["file_refs"]})
        ).hexdigest(),
        "side_effect_flags": {
            "database_access": False,
            "outcomes_read": False,
            "training_started": False,
            "experiment_started": False,
            "runtime_action_performed": False,
            "silent_fallback": False,
        },
    }


def main() -> int:
    try:
        raw = sys.stdin.buffer.read()
        request = json.loads(raw.decode("utf-8"))
        if raw != canonical_json_bytes(request) + b"\n":
            raise MonthlyNodeProbeError("node probe stdin is not canonical JSON")
        result = run_node_probe(request)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, KeyError, MonthlyNodeProbeError) as exc:
        print(f"MONTHLY_NODE_PROBE_ERROR: {exc}", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(canonical_json_bytes(result) + b"\n")
    return 0


if __name__ == "__main__":  # pragma: no cover - subprocess entrypoint
    raise SystemExit(main())


__all__: Sequence[str] = (
    "MonthlyNodeProbeError",
    "NODE_PROBE_REQUEST_SCHEMA",
    "NODE_PROBE_RESULT_SCHEMA",
    "run_node_probe",
)

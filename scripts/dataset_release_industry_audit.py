"""按已保存的真实消费者样本诊断行业ID覆盖；只读、按需，不是月更门禁。

调用示例：python scripts/dataset_release_industry_audit.py --candidate-root <绝对路径>
--target-index <含segment列的消费者样本Parquet> --output-root <候选外的新目录>。
不构造模型/样本、不查询数据库、不修数据，也不证明行业数值特征全部完整。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PureWindowsPath
import stat
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aistock_models"))
from aistock_models.gats_industry_provider import (  # noqa: E402
    DEFAULT_MIN_COVERAGE,
    GatsIndustryProviderError,
    SectorDataIndustryIdProvider,
    _normalise_source_frame,
    _normalise_target_index,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _plain_chain(path: Path) -> None:
    for part in (path, *path.parents):
        if not part.exists():
            continue
        info = part.lstat()
        if part.is_symlink() or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
            raise ValueError("linked audit input/output path")


def validate_output_root(candidate: Path, output: Path) -> None:
    _plain_chain(candidate)
    _plain_chain(output)
    candidate = candidate.resolve(strict=True)
    output = output.resolve()
    if output == candidate or candidate in output.parents:
        raise ValueError("audit output must be outside candidate")
    if output.exists():
        raise FileExistsError("audit output already exists; overwrite forbidden")


def pinned_path(candidate: Path, pin: dict) -> Path:
    text = str(pin["path"])
    relative = Path(text.replace("\\", "/"))
    if relative.is_absolute() or PureWindowsPath(text).drive or ".." in relative.parts:
        raise ValueError("manifest component path is not release-relative")
    path = candidate / relative
    _plain_chain(path)
    if not path.is_file() or not path.resolve(strict=True).is_relative_to(candidate.resolve(strict=True)):
        raise ValueError("manifest component escapes candidate or is not a regular file")
    return path


def industry_file_pins(candidate: Path, manifest: dict) -> dict:
    components = manifest["components"]
    result = {"sector_data": components["sector_data"]}
    static_pin = components.get("static_factors")
    if static_pin is None:
        inventory_pin = components["factor_content_manifest"]
        inventory_path = pinned_path(candidate, inventory_pin)
        if sha256(inventory_path) != inventory_pin["sha256"]:
            raise ValueError("factor inventory differs from manifest pin")
        inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        matches = [row for row in inventory["files"]
                   if Path(str(row["path"]).replace("\\", "/")).name == "static_factors.parquet"]
        if len(matches) != 1:
            raise ValueError("static factors require one exact inventory pin")
        static_pin = matches[0]
    result["static_factors"] = static_pin
    return result


def summarize_ids(values: pd.Series) -> dict:
    index = values.index
    if not isinstance(index, pd.MultiIndex) or index.names != ["datetime", "instrument"]:
        raise ValueError("sample index must be datetime/instrument")
    if index.has_duplicates:
        raise ValueError("duplicate consumer sample keys")
    dates = pd.DatetimeIndex(index.get_level_values(0))
    if dates.hasnans or not len(dates):
        raise ValueError("consumer sample dates missing or empty")
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    finite = np.isfinite(numeric)
    if (finite & ((numeric < -1) | (numeric % 1 != 0))).any():
        raise ValueError("industry ID invalid")
    present = finite & (numeric >= 0)

    def counts(mask):
        total = int(np.sum(mask))
        covered = int(np.sum(present & mask))
        return {"rows": total, "covered_rows": covered, "missing_rows": total - covered,
                "coverage": covered / total if total else None}

    result = counts(np.ones(len(dates), dtype=bool))
    result.update(first_date=dates.min().date().isoformat(), last_date=dates.max().date().isoformat(),
                  symbol_count=int(index.get_level_values(1).nunique()),
                  by_year={str(y): counts(dates.year == y) for y in sorted(set(dates.year))})
    gaps = pd.DataFrame(index=index[~present]).reset_index()
    grouped = gaps.groupby("instrument")["datetime"].agg(["min", "max", "count"])
    result["missing_symbol_count"] = len(grouped)
    result["missing_by_symbol"] = {
        str(symbol): {"first_missing_date": row["min"].date().isoformat(),
                      "last_missing_date": row["max"].date().isoformat(), "missing_rows": int(row["count"])}
        for symbol, row in grouped.iterrows()
    }
    result["missing_ranges_are_bounds_not_contiguous_spans"] = True
    return result


def provider_for_file(path: Path) -> SectorDataIndustryIdProvider:
    provider = SectorDataIndustryIdProvider(path, min_coverage=DEFAULT_MIN_COVERAGE)
    provider.validate_source_available()
    if path.suffix == ".parquet":
        frame = pd.read_parquet(path, columns=["l2_code_id"])
    else:
        with pd.HDFStore(path, mode="r") as store:
            key = "/data" if "/data" in store.keys() else store.keys()[0]
            storer = store.get_storer(key)
            frame = store.select(key, columns=["l2_code_id"]) if storer.is_table else store[key][["l2_code_id"]]
    frame = _normalise_source_frame(frame, source=str(path)).sort_index()
    if frame.index.has_duplicates:
        raise ValueError("duplicate industry source keys")
    # Isolated diagnostic adapter: retain the consumer's normalization/as-of
    # implementation and unchanged threshold; never patch the consumer itself.
    provider._frame = frame
    provider._id_column = "l2_code_id"
    return provider


def audit_provider(path: Path, index: pd.MultiIndex, segment: str, *, provider=None) -> tuple[dict, pd.Series]:
    provider = provider or provider_for_file(path)
    passed = True
    try:
        values = provider.get_industry_ids(index, segment_name=segment)
    except GatsIndustryProviderError as exc:
        if "reason_code=qe_gats_industry_coverage_below_threshold:" not in str(exc):
            raise
        passed = False
        rows = provider._lookup_asof(provider._load_source(), _normalise_target_index(index))
        values = provider._industry_values_from_rows(rows)
        values.index = index
    report = summarize_ids(values)
    if any(report[field] != provider.last_coverage[field] for field in ("rows", "covered_rows", "missing_rows", "coverage")):
        raise ValueError("audit counts differ from actual QE provider")
    report.update({"pass": passed, "threshold": provider.min_coverage, "provider_coverage": provider.last_coverage})
    return report, values


def audit_provider_batched(path: Path, index: pd.MultiIndex, segment: str,
                           *, provider=None, batch_size: int = 32) -> tuple[dict, pd.Series]:
    """Only partition instruments, preserving each complete source history."""
    if not 1 <= batch_size <= 256:
        raise ValueError("diagnostic instrument batch size must be 1..256")
    provider = provider or provider_for_file(path)
    target = _normalise_target_index(index)
    if target.has_duplicates:
        raise ValueError("duplicate normalized consumer sample keys")
    source = provider._load_source()
    source_groups = source.groupby(level="instrument", sort=False).indices
    groups = pd.Series(np.arange(len(target)), index=target).groupby(level="instrument", sort=False).indices
    symbols = list(groups)
    values = pd.Series(np.nan, index=index, dtype="object")
    counts = [0, 0, 0]
    for offset in range(0, len(symbols), batch_size):
        batch = symbols[offset:offset + batch_size]
        positions = np.sort(np.concatenate([groups[s] for s in batch]))
        source_positions = [source_groups[s] for s in batch if s in source_groups]
        part = SectorDataIndustryIdProvider(path, min_coverage=provider.min_coverage)
        part._frame = source.iloc[np.sort(np.concatenate(source_positions)) if source_positions else np.array([], dtype=int)]
        part._id_column = provider._id_column
        _, matched = audit_provider(path, index[positions], segment, provider=part)
        values.iloc[positions] = matched.to_numpy(dtype=object)
        counts = [value + part.last_coverage[field] for value, field in zip(counts, ("rows", "covered_rows", "missing_rows"))]
    report = summarize_ids(values)
    if counts != [report[field] for field in ("rows", "covered_rows", "missing_rows")]:
        raise ValueError("partition counts differ from official provider counts")
    report.update({"pass": report["coverage"] >= provider.min_coverage, "threshold": provider.min_coverage,
                   "sample_lookup": "official_provider_instrument_partitions_v1", "lookup_batch_size": batch_size})
    return report, values


def consumer_indices(target: pd.DataFrame, *, cutoff: str | None = None) -> dict[str, pd.MultiIndex]:
    if "segment" not in target or set(target["segment"]) != {"train", "valid", "test"}:
        raise ValueError("consumer sample segments incomplete")
    normalized = _normalise_target_index(target.index)
    if normalized.has_duplicates:
        raise ValueError("duplicate normalized consumer sample keys")
    if cutoff and pd.DatetimeIndex(normalized.get_level_values(0)).max() > pd.Timestamp(cutoff):
        raise ValueError("consumer samples exceed release cutoff")
    return {name: target.loc[target["segment"] == name].index for name in ("train", "valid", "test")}


def build_audit(candidate: Path, target_path: Path, *, batch_size: int = 32) -> dict:
    _plain_chain(candidate)
    _plain_chain(target_path)
    manifest_path = pinned_path(candidate, {"path": "qe_dataset_manifest.json"})
    before = sha256(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sample_sha = sha256(target_path)
    target = pd.read_parquet(target_path)
    indices = consumer_indices(target, cutoff=manifest.get("cutoff_trade_date"))
    result = {"schema_version": "aistock_frozen_industry_id_audit_v2",
              "dataset_manifest_sha256": manifest["dataset_manifest_sha256"], "manifest_file_sha256": before,
              "candidate_root": str(candidate), "target_index_sha256": sample_sha,
              "sample_alignment": "provided_native_consumer_sample_index_v1", "files": {},
              "database_read": False, "database_write": False, "candidate_write": False,
              "active_profile_write": False, "model_fit_count": 0, "experiment_started": False,
              "runtime_action": False, "feature_completeness_evaluated": False,
              "provider_sha256": sha256(ROOT / "aistock_models/aistock_models/gats_industry_provider.py")}
    outputs = {}
    for name, pin in industry_file_pins(candidate, manifest).items():
        path = pinned_path(candidate, pin)
        source_identity = (path.stat().st_size, path.stat().st_mtime_ns)
        digest = sha256(path)
        if digest != pin["sha256"]:
            raise ValueError("source file differs from manifest pin")
        provider = provider_for_file(path)
        reports, outputs[name] = {}, {}
        for segment, index in indices.items():
            function = audit_provider_batched if batch_size else audit_provider
            kwargs = {"batch_size": batch_size} if batch_size else {}
            reports[segment], outputs[name][segment] = function(path, index, segment, provider=provider, **kwargs)
        if source_identity != (path.stat().st_size, path.stat().st_mtime_ns):
            raise ValueError("industry source changed during readback")
        result["files"][name] = {"path": str(path), "sha256": digest, "segments": reports}
    comparisons = {}
    for segment in indices:
        left, right = outputs["sector_data"][segment], outputs["static_factors"][segment]
        both = left.notna() & right.notna()
        comparisons[segment] = {"both_valid_rows": int(both.sum()), "conflicting_ids": int((left[both] != right[both]).sum()),
                                "h5_only_rows": int((left.notna() & right.isna()).sum()),
                                "parquet_only_rows": int((right.notna() & left.isna()).sum())}
    if before != sha256(manifest_path) or sample_sha != sha256(target_path):
        raise ValueError("audit inputs changed during readback")
    result["h5_parquet_comparison"] = comparisons
    result["industry_id_coverage_ready"] = (
        all(item["pass"] for file in result["files"].values() for item in file["segments"].values())
        and not any(item["conflicting_ids"] for item in comparisons.values()))
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--target-index", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--lookup-batch-size", type=int, default=32)
    args = parser.parse_args(argv)
    if not all(path.is_absolute() for path in (args.candidate_root, args.target_index, args.output_root)):
        raise ValueError("audit paths must be absolute")
    validate_output_root(args.candidate_root, args.output_root)
    result = build_audit(args.candidate_root, args.target_index, batch_size=args.lookup_batch_size)
    args.output_root.mkdir(parents=True, exist_ok=False)
    output = args.output_root / "industry_history_audit.json"
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(result, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        handle.write("\n")
    print(json.dumps({"receipt": str(output), "sha256": sha256(output),
                      "industry_id_coverage_ready": result["industry_id_coverage_ready"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

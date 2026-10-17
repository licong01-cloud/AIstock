"""Quote visibility follows real bins, independently of PIT buy eligibility.

Only finite positive close boundaries are read. Interior NaNs (including
suspensions) stay NaN; catalog visibility does not create quotes or trading
permission. Readers stream bounded edge blocks, never a market-wide frame.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import os
import bisect
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import stat
import struct
from typing import Callable

import numpy as np

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.release_successor import _manifest_identity, _require_manifest_identity


class QuoteCatalogError(RuntimeError):
    pass


def _plain(path: Path) -> None:
    for item in (path, *path.parents):
        metadata = item.lstat()
        if stat.S_ISLNK(metadata.st_mode) or getattr(metadata, "st_file_attributes", 0) & 0x400:
            raise QuoteCatalogError(f"quote path traverses a link/reparse point: {item}")


def canonical_stock_code(token: str) -> str:
    token = token.upper()
    if re.fullmatch(r"(?:SZ|SH)\d{6}", token):
        token = token[2:] + "." + token[:2]
    if not re.fullmatch(r"\d{6}\.(?:SZ|SH)", token):
        raise QuoteCatalogError(f"invalid stock feature directory: {token}")
    if token.startswith("000") and token.endswith(".SH"):
        raise QuoteCatalogError(f"benchmark/index must not enter stock quote catalog: {token}")
    return token


@dataclass(frozen=True)
class MinuteQuoteCatalog:
    payload: bytes
    ranges: dict[str, tuple[str, str]]
    empty_codes: tuple[str, ...]
    calendar_sha256: str
    calendar_rows: int
    feature_files: int

    def receipt(self) -> dict:
        return {
            "mode": "physical_quote_catalog_v1",
            "range_authority": "finite_positive_close_boundaries_v1",
            "frequency": "1min", "field": "close",
            "calendar_sha256": self.calendar_sha256, "calendar_rows": self.calendar_rows,
            "feature_files": self.feature_files, "output_instruments": len(self.ranges),
            "output_rows": len(self.ranges), "empty_quote_codes": list(self.empty_codes),
            "sha256": hashlib.sha256(self.payload).hexdigest(),
            "selection_eligibility_changed": False, "price_files_changed": False,
        }


def _finite_edge(handle, first: int, last: int, *, reverse: bool, checkpoint: Callable[[], None]) -> int | None:
    cursor = last if reverse else first
    while first <= cursor <= last:
        checkpoint()
        begin = max(first, cursor - 4095) if reverse else cursor
        finish = cursor if reverse else min(last, cursor + 4095)
        handle.seek(4 * (begin + 1))
        values = np.frombuffer(handle.read(4 * (finish - begin + 1)), dtype="<f4")
        if len(values) != finish - begin + 1:
            raise QuoteCatalogError("close bin changed or ended during boundary read")
        if np.isinf(values).any() or (np.isfinite(values) & (values <= 0)).any():
            raise QuoteCatalogError("close boundary block contains invalid prices")
        finite = np.flatnonzero(np.isfinite(values))
        if finite.size:
            return begin + int(finite[-1] if reverse else finite[0])
        cursor = begin - 1 if reverse else finish + 1
    return None


def build_minute_quote_catalog(
    root: Path, *, start: str | None = None, end: str | None = None,
    checkpoint: Callable[[], None] = lambda: None,
) -> MinuteQuoteCatalog:
    """Return catalog bytes without modifying bins, pools, or any database."""
    root = Path(root).absolute()
    _plain(root)
    calendar_path = root / "calendars/1min.txt"
    features = root / "features"
    _plain(calendar_path)
    _plain(features)
    calendar_bytes = calendar_path.read_bytes()
    calendar = calendar_bytes.decode("utf-8").splitlines()
    if not calendar or calendar != sorted(set(calendar)):
        raise QuoteCatalogError("minute calendar is empty/duplicate/unsorted")
    first = bisect.bisect_left(calendar, start) if start else 0
    last = bisect.bisect_right(calendar, end + " 23:59:59" if end and len(end) == 10 else end) - 1 if end else len(calendar) - 1
    if first > last:
        raise QuoteCatalogError("quote window does not intersect calendar")
    ranges, empty, seen = {}, [], set()
    for directory in sorted(features.iterdir(), key=lambda p: p.name):
        checkpoint()
        _plain(directory)
        if not directory.is_dir():
            raise QuoteCatalogError("feature namespace contains a non-directory")
        code = canonical_stock_code(directory.name)
        if code in seen:
            raise QuoteCatalogError(f"duplicate canonical feature identity: {code}")
        seen.add(code)
        path = directory / "close.1min.bin"
        _plain(path)
        before = path.stat()
        if not path.is_file() or before.st_size < 8 or before.st_size % 4:
            raise QuoteCatalogError(f"invalid close bin size: {path}")
        with path.open("rb") as handle:
            offset = struct.unpack("<f", handle.read(4))[0]
            if not np.isfinite(offset) or offset != int(offset) or offset < 0:
                raise QuoteCatalogError(f"invalid close bin header: {path}")
            offset = int(offset)
            count = before.st_size // 4 - 1
            if offset + count > len(calendar):
                raise QuoteCatalogError(f"feature bounds exceed calendar: {path}")
            low, high = max(0, first - offset), min(count - 1, last - offset)
            begin = _finite_edge(handle, low, high, reverse=False, checkpoint=checkpoint)
            finish = _finite_edge(handle, low, high, reverse=True, checkpoint=checkpoint) if begin is not None else None
        after = path.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise QuoteCatalogError(f"close bin changed while reading: {path}")
        if begin is None:
            empty.append(code)
        else:
            ranges[code] = (calendar[offset + begin], calendar[offset + finish])
    if not ranges:
        raise QuoteCatalogError("no finite positive minute quotes found")
    payload = "".join(f"{code}\t{a}\t{b}\n" for code, (a, b) in sorted(ranges.items())).encode("utf-8")
    return MinuteQuoteCatalog(payload, ranges, tuple(empty), hashlib.sha256(calendar_bytes).hexdigest(), len(calendar), len(seen))

MINUTE = "components/minute_bin_candidate"
CATALOG = f"{MINUTE}/instruments/all.txt"
META = f"{MINUTE}/meta_export.json"
RECEIPT = "reports/minute_quote_catalog_repair.json"
MUTATIONS = frozenset({CATALOG, META, "qe_dataset_manifest.json", "direct_monthly_state.json"})


def _sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def _pin(root, relative):
    path = root / relative
    return {"path": relative, "sha256": _sha(path), "size": path.stat().st_size}


def repair_quote_catalog(*, baseline_profile: Path, candidate_root: Path, profile_output: Path,
                         generation: str, revision: str, node_roots: dict[str, str], progress=lambda _v: None):
    """Reuse sealed immutable files by inode; detach every explicit writer.

    No all-history content hash, DB query, or price-file rewrite is performed.
    The predecessor's manifest pins and all reused inode identities close the
    reuse proof. An incomplete private staging remains available on failure.
    """
    _plain(baseline_profile)
    profile_bytes = baseline_profile.read_bytes()
    profile = json.loads(profile_bytes)
    baseline = Path(profile["controller_paths"]["candidate_root"]).absolute()
    _plain(baseline)
    candidate = Path(candidate_root).absolute()
    _plain(candidate.parent)
    if candidate.parent != baseline.parent or candidate == baseline or candidate.exists():
        raise ValueError("successor must be a new sibling, not the active dataset")
    if profile_output.exists() or profile_output == baseline_profile:
        raise FileExistsError("profile output must be new and must not replace active")
    _plain(profile_output.absolute().parent)
    if baseline in profile_output.absolute().parents or candidate in profile_output.absolute().parents:
        raise ValueError("profile output must be outside predecessor and successor dataset roots")
    if not generation or generation == profile["generation"] or not revision:
        raise ValueError("successor generation/revision must be explicit and new")
    if set(node_roots) != set(profile["node_bindings"]):
        raise ValueError("successor must retain all predecessor node bindings")
    manifest = _require_manifest_identity(
        baseline / "qe_dataset_manifest.json",
        expected_identity=profile["components"]["dataset_manifest_sha256"],
        expected_file_sha256=profile["components"]["dataset_manifest_file_sha256"],
    )
    for name in ("minute_provider_catalog", "minute_meta_export", "minute_calendar"):
        pin = manifest["components"][name]
        _plain(baseline / pin["path"])
        if _pin(baseline, pin["path"]) != {k: pin[k] for k in ("path", "sha256", "size")}:
            raise ValueError(f"predecessor minute pin differs: {name}")
    catalog = build_minute_quote_catalog(baseline / MINUTE, end=profile["cutoff"])
    previous = {}
    for line in (baseline / CATALOG).read_text(encoding="utf-8").splitlines():
        code, a, b = line.split("\t")
        previous.setdefault(code.upper(), []).append((a, b))
    changes = [
        {"symbol": code, "before": [list(span) for span in previous.get(code, [])], "after": list(span)}
        for code, span in sorted(catalog.ranges.items()) if previous.get(code) != [span]
    ]
    staging = candidate.with_name(f".{candidate.name}.BUG-1902.building")
    staging.mkdir(exist_ok=False)
    reused, byte_count = {}, 0
    for directory, children, files in os.walk(baseline, followlinks=False):
        parent = Path(directory)
        for child in children:
            _plain(parent / child)
        for name in files:
            origin = parent / name
            _plain(origin)
            relative = origin.relative_to(baseline).as_posix()
            if relative in MUTATIONS:
                continue
            if relative == RECEIPT:
                raise ValueError("predecessor already contains this repair receipt")
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            before = origin.stat()
            os.link(origin, target)
            reused[relative] = (before.st_ino, before.st_size, before.st_mtime_ns)
            byte_count += before.st_size
        if len(reused) and len(reused) % 10000 < len(files):
            progress({"phase": "REUSE_IMMUTABLE_FILES", "files": len(reused), "bytes_reused": byte_count})
    (staging / CATALOG).parent.mkdir(parents=True, exist_ok=True)
    with (staging / CATALOG).open("xb") as handle:
        handle.write(catalog.payload)
    meta = json.loads((baseline / META).read_text(encoding="utf-8"))
    meta["all_txt_rewrite"] = {**catalog.receipt(), "path": str(candidate / CATALOG)}
    meta["provider_catalog_contract"] = "physical_quote_catalog_v1"
    _write(staging / META, meta)
    receipt = {
        "schema_version": "aistock_minute_quote_catalog_repair_v1",
        "bug_id": "BUG-1902", "created_at": datetime.now(timezone.utc).isoformat(),
        "predecessor_generation": profile["generation"],
        "predecessor_manifest_identity": manifest["dataset_manifest_sha256"],
        "generation": generation, "revision": revision, "cutoff": profile["cutoff"],
        "catalog": catalog.receipt(), "catalog_changes": changes,
        "changed_symbol_count": len(changes), "reused_file_count": len(reused), "reused_bytes": byte_count,
        "reused_identity_contract": "same_immutable_file_inode_size_mtime_v1",
        "modified_paths": sorted(MUTATIONS | {RECEIPT}),
        "database_read": False, "database_write": False, "price_write": False,
        "stock_pool_write": False, "active_profile_write": False, "runtime_action": False,
    }
    _write(staging / RECEIPT, receipt)
    updated = copy.deepcopy(manifest)
    updated["components"]["minute_provider_catalog"] = _pin(staging, CATALOG)
    updated["components"]["minute_meta_export"] = _pin(staging, META)
    updated["components"]["minute_quote_catalog_repair"] = _pin(staging, RECEIPT)
    updated["deployment_snapshot_id"] = manifest["deployment_snapshot_id"] + "-quote-" + hashlib.sha256(catalog.payload).hexdigest()[:16]
    updated["dataset_manifest_sha256"] = _manifest_identity(updated)
    _write(staging / "qe_dataset_manifest.json", updated)
    state = json.loads((baseline / "direct_monthly_state.json").read_text(encoding="utf-8"))
    state.update(generation=generation, revision=revision, candidate_root=str(candidate), baseline_root=str(baseline))
    state["manifest"] = {**_pin(staging, "qe_dataset_manifest.json"), "dataset_manifest_sha256": updated["dataset_manifest_sha256"]}
    state["components"]["minute_quote_catalog"] = {"status": "PASS", "action": "CATALOG_ONLY", "receipt_sha256": _sha(staging / RECEIPT)}
    state["validation"]["minute_quote_catalog"] = catalog.receipt()
    state["status"] = "CANDIDATE_READY"
    _write(staging / "direct_monthly_state.json", state)
    for relative, before in reused.items():
        origin, target = baseline / relative, staging / relative
        after = origin.stat()
        if before != (after.st_ino, after.st_size, after.st_mtime_ns) or not os.path.samefile(origin, target):
            raise ValueError(f"immutable reuse identity changed: {relative}")
    if baseline_profile.read_bytes() != profile_bytes:
        raise ValueError("active profile changed during repair; do not publish stale successor")
    for name in ("minute_provider_catalog", "minute_meta_export", "minute_calendar"):
        pin = manifest["components"][name]
        if _sha(baseline / pin["path"]) != pin["sha256"]:
            raise ValueError("predecessor changed during repair")
    new_profile = copy.deepcopy(profile)
    new_profile["generation"] = generation
    new_profile["controller_paths"]["candidate_root"] = str(candidate)
    new_profile["controller_paths"]["stock_pool_root"] = str(candidate / "stock_pools")
    new_profile["node_bindings"] = {node: {"candidate_root": root} for node, root in sorted(node_roots.items())}
    new_profile["components"]["dataset_manifest_sha256"] = updated["dataset_manifest_sha256"]
    new_profile["components"]["dataset_manifest_file_sha256"] = _sha(staging / "qe_dataset_manifest.json")
    new_profile["components"]["minute_pins"]["instruments_sha256"] = updated["components"]["minute_provider_catalog"]["sha256"]
    new_profile["components"]["minute_pins"]["meta_export_sha256"] = updated["components"]["minute_meta_export"]["sha256"]
    os.rename(staging, candidate)
    _write(profile_output, new_profile)
    return {"candidate_root": str(candidate), "generation": generation, "revision": revision,
            "dataset_manifest_sha256": updated["dataset_manifest_sha256"],
            "dataset_manifest_file_sha256": _sha(candidate / "qe_dataset_manifest.json"),
            "profile_path": str(profile_output), "profile_sha256": _sha(profile_output),
            "receipt": _pin(candidate, RECEIPT), "changed_symbol_count": len(changes),
            "reused_files": len(reused), "reused_bytes": byte_count, "catalog": catalog.receipt()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-profile", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--profile-output", type=Path, required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--wsl-root", required=True)
    parser.add_argument("--node1-root", required=True)
    args = parser.parse_args()
    result = repair_quote_catalog(
        baseline_profile=args.baseline_profile, candidate_root=args.candidate_root,
        profile_output=args.profile_output, generation=args.generation, revision=args.revision,
        node_roots={"wsl2-5080": args.wsl_root, "rdagent-node1": args.node1_root},
        progress=lambda item: print(json.dumps(item), flush=True),
    )
    print(json.dumps(result), flush=True)



if __name__ == "__main__":
    main()

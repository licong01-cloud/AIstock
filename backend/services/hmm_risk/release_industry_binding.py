"""Research-only L2 projection: preserve C-013, append pinned release facts.

No classification dates are extended and no L3 identities are invented. Shared
integer IDs are only join keys. This adapter is deliberately not a live/as-known
authority or a new C-013 bundle.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from backend.services.dataset_release.shared_sector_context import (
    load_release_sw_l2_code_map,
    validate_membership_frame,
)
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.industry_pit_adapter import HMMIndustryProjection
from backend.services.hmm_risk.monthly_release_input import _BINDING_FIELDS, _Files, _manifest_pins


SCHEMA = "hmm_risk_release_l2_classification_binding_v1"
PREFIX = "components/sector_context_candidate_v1/"
FIELDS = {
    "schema_version",
    "baseline",
    "candidate_root",
    "release_binding",
    "extension_start",
    "extension_end",
    "component_receipt_sha256",
    "code_map_sha256",
    "membership_sha256",
    "receipt_sha256",
}


def require(condition, message):
    if not condition:
        from backend.services.hmm_risk.contracts import StateModelSetError

        raise StateModelSetError(f"HMM release L2 binding: {message}")


def _inputs(candidate_root, binding):
    require(isinstance(binding, dict) and set(binding) == _BINDING_FIELDS, "release binding fields differ")
    files = _Files(Path(candidate_root))
    path = files.path("qe_dataset_manifest.json")
    require(files.digest(path) == binding["manifest_file_sha256"], "manifest bytes differ")
    manifest = files.read(path)
    pins = _manifest_pins(manifest)
    require(
        manifest["dataset_manifest_sha256"] == binding["manifest_sha256"]
        and manifest["cutoff_trade_date"] == binding["cutoff"]
        and manifest["release_id"] == binding["release_id"]
        and manifest["revision"] == binding["revision"],
        "release identity differs",
    )
    paths = {}
    for key, relative in (
        ("receipt", PREFIX + "component_receipt.json"),
        ("code_map", PREFIX + "sector_code_map.json"),
        ("membership", PREFIX + "sector_membership_spans.parquet"),
    ):
        require(relative in pins, f"missing release pin: {relative}")
        paths[key] = files.verify(pins[relative])
    metadata = files.read(paths["receipt"])
    require(
        metadata["membership"]["sha256"] == files.digest(paths["membership"])
        and metadata["sector_code_map"]["sha256"] == files.digest(paths["code_map"])
        and metadata["membership"]["end"] == binding["cutoff"],
        "component identity/cutoff differs",
    )
    return files, paths, metadata


def build_binding(baseline, candidate_root, release_binding):
    """Resolve once from explicit inputs; do not search for latest or activate."""
    from backend.services.hmm_risk.rotation_l1_input_bundle import _industry_adapter

    require(isinstance(baseline, dict) and "schema_version" not in baseline, "one original C-013 baseline required")
    base = _industry_adapter(baseline, forbidden_roots=())
    require(baseline["research_basis"]["active_mode"] == "historical_replay", "research replay only")
    boundary = date.fromisoformat(base.authority_bundle.preflight_report["window_end"])
    require(boundary < date.fromisoformat(release_binding["cutoff"]), "no post-baseline interval requested")
    files, paths, _ = _inputs(candidate_root, release_binding)
    result = receipt(
        dict(
            schema_version=SCHEMA,
            baseline=baseline,
            candidate_root=str(candidate_root),
            release_binding=release_binding,
            extension_start=(boundary + timedelta(days=1)).isoformat(),
            extension_end=release_binding["cutoff"],
            component_receipt_sha256=files.digest(paths["receipt"]),
            code_map_sha256=files.digest(paths["code_map"]),
            membership_sha256=files.digest(paths["membership"]),
        )
    )
    ReleaseIndustryAdapter(result, forbidden_roots=())
    files.unchanged()
    return result


class _Transitions:
    """Only extends transition boundaries, never rewrites the original resolver."""

    def __init__(self, original, spans, start, end):
        self.original, self.spans, self.start, self.end = original, spans, start, end

    def transition_dates(self, symbol):
        values = set(self.original.transition_dates(symbol))
        values.update((self.start, self.end + timedelta(days=1)))
        for start, end, *_ in self.spans.get(symbol, ()):
            values.update((start, end + timedelta(days=1)))
        return tuple(sorted(values))


class ReleaseIndustryAdapter:
    def __init__(self, binding, *, forbidden_roots):
        from backend.services.hmm_risk.rotation_l1_input_bundle import _canonical_sector_codes, _industry_adapter

        require(
            isinstance(binding, dict) and set(binding) == FIELDS and binding["schema_version"] == SCHEMA,
            "schema/fields differ",
        )
        require(
            binding["receipt_sha256"] == canonical_sha256({k: v for k, v in binding.items() if k != "receipt_sha256"}),
            "binding hash differs",
        )
        require("schema_version" not in binding["baseline"], "nested release binding forbidden")
        require(binding["baseline"]["research_basis"]["active_mode"] == "historical_replay", "live use forbidden")
        self.base = _industry_adapter(binding["baseline"], forbidden_roots=forbidden_roots)
        self.binding = binding
        bounds = self.base.authority_bundle.preflight_report
        self.start, self.end = (date.fromisoformat(binding[k]) for k in ("extension_start", "extension_end"))
        require(
            self.start == date.fromisoformat(bounds["window_end"]) + timedelta(days=1)
            and self.start <= self.end
            and binding["extension_end"] == binding["release_binding"]["cutoff"],
            "extension must follow actual baseline and end at pinned release cutoff",
        )
        self.coverage_window = {"window_start": bounds["window_start"], "window_end": self.end.isoformat()}
        self.files, paths, metadata = _inputs(binding["candidate_root"], binding["release_binding"])
        for key, field in (
            ("receipt", "component_receipt_sha256"),
            ("code_map", "code_map_sha256"),
            ("membership", "membership_sha256"),
        ):
            require(self.files.digest(paths[key]) == binding[field], f"{key} identity differs")
        require(
            metadata["membership"]["current_snapshot_backfill"] is False
            and metadata["membership"]["default_industry_assignment"] is False
            and metadata["membership"]["silent_symbol_exclusion"] is False,
            "unsupported membership policy",
        )
        code_map = load_release_sw_l2_code_map(paths["code_map"])
        self.constituents = self.base.constituents
        self.classification_lookup = self.base.classification_lookup
        self.authority_bundle = self.base.authority_bundle  # Always the original, not relabelled as the extension.
        require(
            tuple(code_map.member_backed_codes) == _canonical_sector_codes(self.base)[1],
            "131 official text-code catalog differs",
        )
        self.projection = {r["canonical_l2_code"]: r for r in binding["baseline"]["l2_projection"]["rows"]}
        frame = pd.read_parquet(paths["membership"])
        frame = frame.copy()
        frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
        for field in ("start_date", "end_date"):
            frame[field] = pd.to_datetime(frame[field], errors="coerce").dt.date
        frame = frame.sort_values(["instrument", "start_date", "end_date", "l2_code_id"], kind="stable").reset_index(
            drop=True
        )
        validate_membership_frame(
            frame, id_to_code=code_map.id_to_code, required_start=self.start, required_end=self.end
        )
        require(
            len(frame) == metadata["membership"]["span_count"]
            and frame["instrument"].nunique() == metadata["membership"]["symbol_count"],
            "membership counts differ",
        )
        self.spans, self.starts = {}, {}
        for row in frame.itertuples(index=False):
            start, end = (
                max(self.start, pd.Timestamp(row.start_date).date()),
                min(self.end, pd.Timestamp(row.end_date).date()),
            )
            if start > end:
                continue
            code = code_map.id_to_code[int(row.l2_code_id)]
            require(code in self.projection, "member text code escapes C-013 catalog")
            row_hash = canonical_sha256(
                dict(
                    instrument=row.instrument,
                    start_date=str(row.start_date),
                    end_date=str(row.end_date),
                    canonical_l2_code=code,
                    membership_sha256=binding["membership_sha256"],
                )
            )
            self.spans.setdefault(row.instrument, []).append((start, end, code, row_hash))
        for symbol, spans in self.spans.items():
            self.spans[symbol] = tuple(spans)
            self.starts[symbol] = tuple(row[0] for row in spans)
        self.classification_resolver = _Transitions(self.base.classification_resolver, self.spans, self.start, self.end)
        self.index_membership_resolver = self.base.index_membership_resolver
        self.unchanged()

    def unchanged(self):
        self.files.unchanged()

    def resolve(self, symbol, day):
        if day < self.start:
            return self.base.resolve(symbol, day)
        code = row_hash = None
        if day <= self.end and symbol in self.spans:
            at = bisect_right(self.starts[symbol], day) - 1
            if at >= 0:
                start, end, candidate, candidate_hash = self.spans[symbol][at]
                if start <= day <= end:
                    code, row_hash = candidate, candidate_hash
        l2 = self.projection[code] if code is not None else None
        l1 = self.classification_lookup[("L1", l2["taxonomy_l1_code"])] if l2 else None
        return HMMIndustryProjection(
            status="resolved" if code is not None else "unavailable",
            canonical_symbol=symbol,
            trade_date=day,
            l1_code=l1["index_code"] if l1 else None,
            l1_name=l1["name"] if l1 else None,
            l2_code=code,
            l2_name=l2["canonical_l2_name"] if l2 else None,
            reason_code=None if code is not None else "classification:release_membership_unavailable",
            classification_receipt_hash=self.binding["receipt_sha256"],
            index_membership_receipt_hash=self.base.index_membership_resolver.receipt.receipt_hash,
            classification_row_hashes=(row_hash,) if row_hash is not None else (),
            index_membership_row_hashes=(),
            alignment_state="index_membership_unavailable",
            classification_research_basis="stable_taxonomy_backcast",
            non_as_known_taxonomy=True,
        )

    def mapping_manifest(self, *, universe_key, source_start, source_end):
        return dict(
            schema_version=SCHEMA + "_mapping",
            universe_key=universe_key,
            source_window_start=source_start.isoformat(),
            source_window_end=source_end.isoformat(),
            baseline=self.base.mapping_manifest(
                universe_key=universe_key,
                source_start=source_start,
                source_end=min(source_end, self.start - timedelta(days=1)),
            ),
            binding_sha256=self.binding["receipt_sha256"],
            release_binding=self.binding["release_binding"],
            membership_sha256=self.binding["membership_sha256"],
            code_map_sha256=self.binding["code_map_sha256"],
            canonical_l2_count=131,
            historical_as_known=False,
            l3_identity_inferred=False,
        )

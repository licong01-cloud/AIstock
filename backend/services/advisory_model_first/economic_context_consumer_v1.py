"""Advisory-only, read-only release/pool/context preparation. No native upgrade."""
from collections import Counter
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
import re

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_universe import normalize_advisory_universe_selection
from backend.services.dataset_release.shared_sector_context import (
    load_release_sw_l2_code_map, require_pinned_sector_context_files, validate_membership_frame,
)
from backend.services.industry_pit.artifact_store import read_candidate_bundle
from backend.services.industry_pit.contracts import (
    AuthorityType, KnowledgeTimePolicy, ResearchBasis, ResolutionRequest, ResolvedIndustryIdentity, UnavailableReason,
)
from backend.services.industry_pit.resolver import IndustryPitResolver
from backend.services.quantevolver.qe_active_dataset_profile import load_qe_profile
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


_SYMBOL = re.compile(r"\d{6}\.(SH|SZ|BJ)")
_SOFT = {UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED,
         UnavailableReason.CLASSIFICATION_AUTHORITY_UNAVAILABLE}


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_CONTEXT_INPUT_INVALID")


def _digest(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _pinned(path, expected):
    path = Path(path)
    if (not path.is_absolute() or path.resolve() != path.absolute() or not path.is_file()
            or not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None
            or _digest(path) != expected):
        _fail("context input path/hash differs from its explicit pin")
    return path


def context_roster_v1(candidates):
    """Normalize only frozen D/T/symbol keys, retaining original order/population."""
    if not isinstance(candidates, pd.DataFrame) or not candidates.columns.is_unique or not set(KEY) <= set(candidates):
        _fail("context needs original unique candidate keys")
    rows = candidates.loc[:, KEY].copy()
    for key in KEY[:2]:
        days = pd.to_datetime(rows[key], errors="coerce")
        if days.dt.tz is not None or days.isna().any() or not days.eq(days.dt.normalize()).all():
            _fail("context candidate dates are invalid or intraday")
        rows[key] = days.dt.date
    if (rows.empty or rows.duplicated(KEY).any()
            or not rows.instrument.map(lambda s: isinstance(s, str) and _SYMBOL.fullmatch(s) is not None).all()
            or not (rows[KEY[1]] > rows[KEY[0]]).all()
            or rows.groupby(KEY[0])[KEY[1]].nunique().gt(1).any()
            or rows.groupby(KEY[0]).size().gt(20).any()):
        _fail("context roster is empty, duplicated, foreign, or exceeds Top20")
    return rows.reset_index(drop=True)


def _pool_intervals(content):
    result, prior = {}, {}
    for line in content.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 3 or _SYMBOL.fullmatch(parts[0]) is None:
            _fail("context pool sidecar row is malformed")
        try:
            start, end = (date.fromisoformat(value) for value in parts[1:])
        except ValueError:
            _fail("context pool sidecar date is malformed")
        symbol = parts[0]
        if end < start or symbol in prior and start <= prior[symbol]:
            _fail("context pool sidecar intervals overlap or are unsorted")
        prior[symbol] = end
        result.setdefault(symbol, []).append((start, end))
    if not result:
        _fail("context pool sidecar is empty")
    return result


@dataclass(frozen=True)
class EconomicContextSourceV1:
    identity: dict
    pools: dict
    membership: pd.DataFrame | None
    classification: IndustryPitResolver | None
    pins: dict

    def verify_unchanged(self):
        for path, expected in self.pins.items():
            _pinned(path, expected)


def load_economic_context_source_v1(*, profile_path, profile_sha256, universe_selection, authority_root=None):
    """Read one explicit profile and its pinned sidecars, never activate/build."""
    path = _pinned(profile_path, profile_sha256)
    profile = load_qe_profile(path)  # Public validation only, no experiments/DB.
    if profile.profile_sha256 != profile_sha256:
        _fail("context profile changed during loading")
    if not isinstance(universe_selection, dict) or set(universe_selection) != {"mode", "pool_ids"}:
        _fail("context universe selection must be explicit")
    selection = normalize_advisory_universe_selection(universe_selection)
    pool_ids = ("stock_universe", *selection["pool_ids"])
    coverage_sha = profile.qe["coverage_receipt_sha256"]
    pins, pools, descriptors = {path: profile_sha256,
        _pinned(profile.coverage_receipt_path, coverage_sha): coverage_sha}, {}, {}
    for pool_id in dict.fromkeys(pool_ids):
        descriptor = profile.universes[pool_id]
        pool_path = _pinned(profile.controller_stock_pool_root/descriptor["filename"], descriptor["sha256"])
        content_bytes = pool_path.read_bytes()
        if hashlib.sha256(content_bytes).hexdigest() != descriptor["sha256"]:
            _fail("context pool changed while reading")
        content = content_bytes.decode("utf-8")
        pools[pool_id] = _pool_intervals(content)
        pins[pool_path] = descriptor["sha256"]
        descriptors[pool_id] = {key: descriptor[key] for key in ("filename", "sha256", "membership_revision")}
    membership, resolver, sector_identity = None, None, None
    if authority_root is not None:
        sector_pins = profile.raw["components"].get("sector_context_pins")
        if sector_pins is None:
            _fail("requested context has no profile sector binding")
        files = require_pinned_sector_context_files(profile.controller_candidate_root, sector_pins)
        for name, file in files.items():
            pins[file] = sector_pins[{"code_map": "code_map", "market_context": "market_context",
                "membership": "membership", "quote_availability": "quote_availability", "receipt": "receipt"}[name]+"_sha256"]
        membership = pd.read_parquet(files["membership"])
        code_map = load_release_sw_l2_code_map(files["code_map"])
        validate_membership_frame(membership, id_to_code=code_map.id_to_code)
        component = json.loads(files["receipt"].read_text(encoding="utf-8"))
        expected = component["membership"]["industry_pit_identity"]
        root = Path(authority_root)
        if not root.is_absolute() or root.resolve() != root.absolute():
            _fail("context authority must be an explicit original absolute root")
        bundle = read_candidate_bundle(artifact_root=root, forbidden_roots=(Path(__file__).resolve().parents[3],))
        for key in ("bundle_hash", "classification_candidate_hash"):
            if expected[key] != bundle.manifest[key]:
                _fail("context authority differs from the profile's sector component")
        receipt = bundle.classification_receipt
        if (expected["classification_receipt_hash"] != receipt.receipt_hash
                or receipt.authority_type is not AuthorityType.CLASSIFICATION
                or receipt.research_basis is not ResearchBasis.AS_PUBLISHED_PIT
                or receipt.knowledge_time_policy is not KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE):
            _fail("context classification identity or knowledge policy differs")
        for name, descriptor in bundle.manifest["files"].items():
            pins[root/name] = descriptor["sha256"]
        pins[root/"candidate_bundle_manifest.json"] = _digest(root/"candidate_bundle_manifest.json")
        resolver = IndustryPitResolver(receipt=receipt, intervals=bundle.classification_intervals,
            known_taxonomy_versions=((receipt.taxonomy_contract_id, receipt.taxonomy_version),))
        sector_identity = {"sector_pins_sha256": sha(sector_pins), "bundle_hash": bundle.manifest["bundle_hash"],
            "classification_receipt_hash": receipt.receipt_hash, "knowledge_policy": receipt.knowledge_time_policy.value,
            "research_basis": receipt.research_basis.value, "taxonomy_contract_id": receipt.taxonomy_contract_id,
            "taxonomy_version": receipt.taxonomy_version}
    identity = {"profile_sha256": profile_sha256, "generation": profile.generation, "release_id": profile.release_id,
        "cutoff": profile.cutoff.isoformat(), "universe_selection": selection,
        "pool_rule_version": profile.raw["components"]["day_pins"]["rule_version"],
        "pool_universe_key": profile.raw["components"]["day_pins"]["universe_key"],
        "pool_descriptors": descriptors, "sector": sector_identity}
    identity["coverage_receipt_sha256"] = coverage_sha
    source = EconomicContextSourceV1(identity, pools, membership, resolver, pins)
    source.verify_unchanged()
    return source


def build_economic_context_rows_v1(*, candidates, source, purpose="EXPLORATORY_SCREEN"):
    """Shared single-day/batch projection, not an admission or native receipt."""
    if purpose != "EXPLORATORY_SCREEN":
        _fail("context preparation cannot be used for formal/confirmation/live evidence")
    roster = context_roster_v1(candidates)
    if roster[KEY[1]].max() > date.fromisoformat(source.identity["cutoff"]):
        _fail("context target exceeds the pinned release cutoff")
    selection = source.identity["universe_selection"]
    if normalize_advisory_universe_selection(selection) != selection:
        _fail("context universe selection is not canonical")
    wanted = selection["pool_ids"]
    if set(("stock_universe", *wanted)) != set(source.pools):
        _fail("context selection/pool identities differ")
    resolver = source.classification
    if resolver is not None:
        receipt = resolver.receipt
        if (receipt.research_basis is not ResearchBasis.AS_PUBLISHED_PIT
                or receipt.knowledge_time_policy is not KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE
                or receipt.receipt_hash != source.identity["sector"]["classification_receipt_hash"]):
            _fail("context cannot substitute non-as-known classification")
    dated = {}
    if source.membership is not None:
        for row in source.membership.itertuples(index=False):
            dated.setdefault(row.instrument, []).append((pd.Timestamp(row.start_date).date(), pd.Timestamp(row.end_date).date(), row.l2_code_id))

    def member(pool, symbol, day):
        return any(start <= day <= end for start, end in source.pools[pool].get(symbol, ()))

    output = []
    for day, target, symbol in roster.itertuples(index=False, name=None):
        eligible = member("stock_universe", symbol, day) and (not wanted or any(member(pool, symbol, day) for pool in wanted))
        matches = [code for start, end, code in dated.get(symbol, ()) if start <= day <= end]
        if len(matches) > 1:
            _fail("context dated sector assignments are ambiguous")
        category, reason, known_from = None, "OPTIONAL_CONTEXT_NOT_REQUESTED", None
        if resolver is not None:
            value = resolver.resolve(ResolutionRequest(symbol, day, AuthorityType.CLASSIFICATION,
                receipt.taxonomy_contract_id, receipt.taxonomy_version, receipt.receipt_hash,
                KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE, ResearchBasis.AS_PUBLISHED_PIT))
            if isinstance(value, ResolvedIndustryIdentity):
                if value.non_as_known_taxonomy or value.known_from is None or value.known_from > day:
                    _fail("context classification lacks causal knowledge")
                category, reason, known_from = value.identity.l2_code, None, value.known_from
            elif value.reason in _SOFT:
                reason = value.reason.value
            else:
                _fail(f"context classification hard conflict: {value.reason.value}")
        output.append({**dict(zip(KEY, (day, target, symbol))), "current_pool_eligible": eligible,
            "pool_status": "ELIGIBLE" if eligible else "OUTSIDE_CURRENT_POOL",
            "dated_assignment_available": bool(matches), "classification_l2_code": category,
            "classification_known_from": known_from, "classification_unknown_reason": reason})
    rows = pd.DataFrame(output)
    available = int(rows.classification_l2_code.notna().sum())
    summary = {"schema_version": "advisory_economic_context_preparation_v1", "purpose": purpose,
        "decision_use": "NAVIGATION_ONLY", "deployable": False, "native_identity": "NOT_ASSESSED",
        "core_input_identity": "VERIFIED", "core_feature_values_validated": False,
        "optional_classification": "AVAILABLE" if available == len(rows) else "PARTIAL" if available else "UNAVAILABLE",
        "rows": len(rows), "decisions": int(rows[KEY[0]].nunique()), "symbols": int(rows.instrument.nunique()),
        "current_pool_eligible": int(rows.current_pool_eligible.sum()), "outside_current_pool": int((~rows.current_pool_eligible).sum()),
        "dated_assignment_available": int(rows.dated_assignment_available.sum()), "classification_available": available,
        "unknown_reasons": dict(Counter(rows.classification_unknown_reason.dropna())),
        "data_identity": source.identity,
        "candidate_keys_sha256": sha([{key: value.isoformat() if isinstance(value, date) else value
            for key, value in row.items()} for row in roster.to_dict("records")]),
        "outcomes_read": False, "fit_count": 0, "population_policy": "PRESERVE_ORIGINAL_FROZEN_KEYS"}
    return rows, summary

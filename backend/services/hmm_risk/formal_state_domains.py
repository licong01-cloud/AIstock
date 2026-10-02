"""Current A5 full-key partition/opportunity receipts, using shared validators.

The file constructor supplies source-specific authority evidence.  This module
does not infer industry identities, use dense category ids, or read a database.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from backend.services.hmm_risk.stock_fact_observation import (
    C010_ELIGIBILITY_RECEIPT_VERSION,
    C010_EXPECTED_OPPORTUNITY_CONTRACT,
    C010_POLICY_VERSION,
    C010_PROVIDER_ABSENCE_PARTITION_VERSION,
    _c010_expected_partition_predicate_status,
    _validate_c010_eligibility_receipt_v2,
    validate_c010_expected_opportunity_receipt,
    validate_c010_provider_absence_domain_partition,
)

PREDICATE_AUTHORITIES = {
    "pit_eligible": "pit_authority_identity",
    "price_authority_present": "price_source_identity",
    "sw_l1_identity_valid": "sw_mapping_classify_identity",
    "sw_l2_identity_valid": "sw_mapping_classify_identity",
}
OUTSIDE_REASONS = {
    "pit_eligible": "hmm_risk_c010_pit_ineligible_for_opportunity",
    "price_authority_present": "hmm_risk_c010_price_unavailable_for_opportunity",
    "sw_l1_identity_valid": "hmm_risk_c010_sw_identity_unavailable_for_opportunity",
    "sw_l2_identity_valid": "hmm_risk_c010_sw_identity_unavailable_for_opportunity",
}


def authority_identity(authority_type: str, authority: Mapping[str, Any]) -> dict[str, Any]:
    body = {"authority_type": authority_type, "authority": dict(authority)}
    return {**body, "identity_sha256": canonical_sha256(body)}


def partition_entry(
    *,
    provider_row: Mapping[str, Any],
    authorities: Mapping[str, Mapping[str, Any]],
    provider_resolution: Mapping[str, Any],
    price_resolution: Mapping[str, Any],
    pit_candidates: Sequence[Mapping[str, Any]],
    price_candidates: Sequence[Mapping[str, Any]],
    sw_candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Classify a single P_all key; ambiguity can never be disguised as P_out."""
    resolver = {
        "security_resolver_identity_sha256": authorities["security_resolver_identity"]["identity_sha256"],
        "provider_absence_source_resolution": dict(provider_resolution),
        "price_source_resolution": dict(price_resolution),
    }
    predicates = {}
    for field, candidates in (
        ("pit_eligible", pit_candidates),
        ("price_authority_present", price_candidates),
        ("sw_l1_identity_valid", sw_candidates),
        ("sw_l2_identity_valid", sw_candidates),
    ):
        evidence = {
            "authority_identity_sha256": authorities[PREDICATE_AUTHORITIES[field]]["identity_sha256"],
            "candidate_count": len(candidates),
            "candidates": [dict(candidate) for candidate in candidates],
        }
        if field == "price_authority_present":
            evidence["source_resolution"] = dict(price_resolution)
        elif field.startswith("sw_"):
            evidence["level"] = "L1" if field == "sw_l1_identity_valid" else "L2"
        status = _c010_expected_partition_predicate_status(field, evidence, resolver=resolver)
        if status == "invalid":
            raise FormalStateError("hmm_risk_c010_provider_absence_domain_partition_invalid", f"ambiguous {field}")
        predicates[field] = receipt(
            {
                "status": status,
                "authority_receipt": evidence,
                "authority_receipt_sha256": canonical_sha256(evidence),
            }
        )
    failed = [field for field in PREDICATE_AUTHORITIES if predicates[field]["status"] == "unavailable"]
    body = {
        "canonical_ts_code": provider_row["canonical_ts_code"],
        "source_ts_code": provider_row["source_ts_code"],
        "stable_security_identity": f"canonical:{provider_row['canonical_ts_code']}",
        "trade_date": provider_row["trade_date"],
        "provider_row_hash": provider_row["row_hash"],
        "security_resolver_receipt": resolver,
        "security_resolver_receipt_sha256": canonical_sha256(resolver),
        **predicates,
        "failed_predicates": failed,
        "partition": "out_of_domain" if failed else "in_domain",
        "primary_reason_code": OUTSIDE_REASONS[failed[0]] if failed else None,
        "policy_version": C010_POLICY_VERSION,
    }
    return {**body, "entry_sha256": canonical_sha256(body)}


def build_a5_receipts(
    *,
    authorities: Mapping[str, Mapping[str, Any]],
    partition_entries: Sequence[Mapping[str, Any]],
    opportunity_keys: Iterable[tuple[str, str]],
    provider_keys: Iterable[tuple[str, str]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Freeze O_sector and every P_all key, then derive train-only eligibility.

    O_sector is provided by independent PIT/price/industry predicates, not by
    provider absence counts.  Its full key set is retained, never count-only.
    """
    entries = sorted(partition_entries, key=lambda entry: (entry["canonical_ts_code"], entry["trade_date"]))
    observed = [(entry["canonical_ts_code"], entry["trade_date"]) for entry in entries]
    expected_provider = list(provider_keys)
    if (
        len(observed) != len(set(observed))
        or len(expected_provider) != len(set(expected_provider))
        or set(observed) != set(expected_provider)
    ):
        raise FormalStateError(
            "hmm_risk_c010_provider_absence_domain_partition_invalid", "P_all source keys not fully partitioned"
        )
    keys = [{"canonical_ts_code": symbol, "trade_date": day} for symbol, day in observed]
    body = {
        "schema_version": C010_PROVIDER_ABSENCE_PARTITION_VERSION,
        "contract_version": C010_PROVIDER_ABSENCE_PARTITION_VERSION,
        "policy_version": C010_POLICY_VERSION,
        "train_start": "2022-01-01",
        "train_end": "2024-06-30",
        **dict(authorities),
        "entries": entries,
        "partition_complete": True,
        "diagnostic_only": False,
        "formal_policy_activated": True,
    }
    for label, selected in (
        ("all", keys),
        ("in", [key for key, entry in zip(keys, entries) if entry["partition"] == "in_domain"]),
        ("out", [key for key, entry in zip(keys, entries) if entry["partition"] == "out_of_domain"]),
    ):
        body[f"p_{label}_entry_count"] = len(selected)
        body[f"p_{label}_ordered_key_sha256"] = canonical_sha256(selected)
    partition = validate_c010_provider_absence_domain_partition(receipt(body))
    opportunity_list = sorted(opportunity_keys)
    if len(opportunity_list) != len(set(opportunity_list)):
        raise FormalStateError("hmm_risk_c010_expected_opportunity_invalid", "duplicate opportunity key")
    by_symbol: dict[str, list[str]] = {}
    for symbol, day in opportunity_list:
        by_symbol.setdefault(symbol, []).append(day)
    opportunity_authorities = sorted(
        (
            authorities[field]
            for field in (
                "security_resolver_identity",
                "pit_authority_identity",
                "price_source_identity",
                "sw_mapping_classify_identity",
            )
        ),
        key=lambda item: item["identity_sha256"],
    )
    opportunity_entries = []
    for symbol, days in by_symbol.items():
        item = {
            "canonical_ts_code": symbol,
            "opportunity_dates": days,
            "opportunity_count": len(days),
            "opportunity_date_sha256": canonical_sha256(days),
            "authority_identity_sha256": canonical_sha256(opportunity_authorities),
        }
        opportunity_entries.append({**item, "entry_sha256": canonical_sha256(item)})
    opportunity = validate_c010_expected_opportunity_receipt(
        receipt(
            {
                "schema_version": C010_EXPECTED_OPPORTUNITY_CONTRACT,
                "train_start": "2022-01-01",
                "train_end": "2024-06-30",
                "authority_identities": opportunity_authorities,
                "entry_count": len(opportunity_entries),
                "opportunity_key_count": len(opportunity_list),
                "opportunity_ordered_key_sha256": canonical_sha256(
                    [{"canonical_ts_code": symbol, "trade_date": day} for symbol, day in opportunity_list]
                ),
                "entries": opportunity_entries,
            }
        )
    )
    p_in: dict[str, list[str]] = {}
    for entry in partition["entries"]:
        if entry["partition"] == "in_domain":
            p_in.setdefault(entry["canonical_ts_code"], []).append(entry["trade_date"])
    contributor_entries = []
    for item in opportunity_entries:
        symbol = item["canonical_ts_code"]
        missing = p_in.get(symbol, [])
        n = item["opportunity_count"]
        entry = {
            "canonical_ts_code": symbol,
            "expected_opportunity_count": n,
            "expected_opportunity_contract": C010_EXPECTED_OPPORTUNITY_CONTRACT,
            "expected_opportunity_date_sha256": item["opportunity_date_sha256"],
            "provider_absence_count": len(missing),
            "availability_ratio": (n - len(missing)) / n,
            "moneyflow_contributor_eligible": 10 * (n - len(missing)) >= 9 * n,
            "provider_absence_key_sha256": canonical_sha256(
                [{"canonical_ts_code": symbol, "trade_date": day} for day in missing]
            ),
            "p_in_date_sha256": canonical_sha256(missing),
        }
        contributor_entries.append({**entry, "entry_sha256": canonical_sha256(entry)})
    eligibility = receipt(
        {
            "schema_version": C010_ELIGIBILITY_RECEIPT_VERSION,
            "train_start": "2022-01-01",
            "train_end": "2024-06-30",
            "minimum_availability_ratio": 0.9,
            "availability_integer_contract": "10*(expected-missing) >= 9*expected",
            "entry_count": len(contributor_entries),
            "entries": contributor_entries,
            "excluded_moneyflow_symbols": [
                entry["canonical_ts_code"]
                for entry in contributor_entries
                if not entry["moneyflow_contributor_eligible"]
            ],
            "expected_opportunity_receipt": opportunity,
            "expected_opportunity_receipt_sha256": opportunity["receipt_sha256"],
            "provider_absence_partition_receipt": partition,
            "provider_absence_partition_receipt_sha256": partition["receipt_sha256"],
            "pit_universe_changed": False,
            "selection_universe_changed": False,
            "runtime_prediction_eligibility_changed": False,
            "diagnostic_only": False,
            "formal_policy_activated": True,
        }
    )
    _validate_c010_eligibility_receipt_v2(eligibility)
    return partition, opportunity, eligibility

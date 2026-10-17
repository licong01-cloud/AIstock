"""Conservative, read-only context enrichment for factor comparison research."""
from __future__ import annotations

import re


_QLIB_FIELD = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
_COLUMN_ACCESS = re.compile(r"\[['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\]")


def expression_dependencies(expression: object) -> dict:
    """Return only lexically explicit inputs; never execute catalog code."""
    if not isinstance(expression, str) or not expression.strip():
        return {"inputs": [], "confirmation": "unknown", "completeness": "unknown",
                "reason": "expression_missing"}
    inputs = sorted(set(_QLIB_FIELD.findall(expression)) | set(_COLUMN_ACCESS.findall(expression)))
    if not inputs:
        return {"inputs": [], "confirmation": "unknown", "completeness": "unknown",
                "reason": "no_explicit_field_reference"}
    return {"inputs": inputs, "confirmation": "lexically_observed", "completeness": "unknown",
            "reason": "dynamic_or_indirect_dependencies_may_exist"}


def enrich_catalog_context(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Annotate the bounded query result and compare only rows in that result."""
    enriched: list[dict] = []
    for row in rows:
        item = dict(row)
        item["input_dependencies"] = expression_dependencies(item.get("expression"))
        enriched.append(item)

    neighbors: list[dict] = []
    for left_index, left in enumerate(enriched):
        left_inputs = set(left["input_dependencies"]["inputs"])
        for right in enriched[left_index + 1:]:
            right_inputs = set(right["input_dependencies"]["inputs"])
            union = left_inputs | right_inputs
            same_expression = bool(left.get("expression")) and left.get("expression") == right.get("expression")
            if not union and not same_expression:
                continue
            neighbors.append({
                "factor_a": {"id": left.get("id"), "name": left.get("factor_name"), "source": left.get("source")},
                "factor_b": {"id": right.get("id"), "name": right.get("factor_name"), "source": right.get("source")},
                "input_jaccard": len(left_inputs & right_inputs) / len(union) if union else None,
                "same_expression": same_expression,
                "basis": "bounded_requested_catalog_rows",
                "interpretation": "retrieval_hint_not_equivalence",
            })
    neighbors.sort(key=lambda item: (
        -(item["input_jaccard"] if item["input_jaccard"] is not None else -1.0),
        str(item["factor_a"]["name"]), str(item["factor_b"]["name"]),
        str(item["factor_a"]["source"]), str(item["factor_b"]["source"]),
    ))
    return enriched, neighbors

"""Compact, pure research-memory projections; no DB, model or file side effects."""
from __future__ import annotations

import bisect
from datetime import datetime, timezone

from .assistance import _excerpt, _key, _normalize, _require, _strings, _text
from .models import json_object

CONDITION_FIELDS = ("research_role", "hypothesis_family", "input_fields", "horizons",
                    "dataset_ref", "universe_ref", "evaluation_ref")
# Same choices as comparison.validate_comparison_spec; no label arithmetic here.
FILTER_CHOICES = {"research_role": {"predictive_increment", "replacement", "conditional"},
                  "horizons": {"1d", "5d", "10d", "20d"}}
NOTE_TEXT = ("observation", "interpretation", "reason", "next_evidence")
HYPOTHESIS_TEXT = ("basis", "statement", "competing_explanation", "observable_difference", "falsifier", "proxy_limitations")


def validate_query(value):
    value = json_object(value)
    _require(value.get("schema_version") == "factor_research_memory_query_v1", "Unknown memory query schema")
    _require(not set(value) - {"schema_version", "query", "filters", "include_unknown", "limit", "offset"}, "Unknown memory query fields")
    query = value.get("query")
    _require(isinstance(query, dict) and _text(query.get("problem")), "Query problem is required")
    _require(not set(query) - {"problem", "terms", "factor_names", "input_fields"}, "Unknown query fields")
    _require(_strings(query.get("terms")) and query["terms"], "Explicit terms are required")
    for key in ("factor_names", "input_fields"):
        _require(_strings(query.get(key, [])), f"Invalid query {key}")
    filters = value.get("filters", {})
    _require(isinstance(filters, dict) and not set(filters) - {"research_role", "hypothesis_family", "horizons"}, "Invalid filters")
    for key, items in filters.items():
        _require(_strings(items) and items, f"Invalid filter {key}")
        _require(key not in FILTER_CHOICES or set(items) <= FILTER_CHOICES[key], f"Unknown filter {key}")
    limit, offset, unknown = value.get("limit", 20), value.get("offset", 0), value.get("include_unknown", True)
    _require(type(limit) is int and limit > 0 and type(offset) is int and offset >= 0, "Invalid pagination")
    _require(type(unknown) is bool, "include_unknown must be boolean")
    return dict(query=dict(query, filters=filters, include_unknown=unknown), limit=limit, offset=offset)


def source_ref(row, target):
    return dict(source_id=f"aistock_research:{target}", locator=dict(
        task_id=str(row["task_id"]), record_id=str(row["record_id"]) if row.get("record_id") else None,
        revision=row["revision"]))


def _text_values(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _text_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _text_values(item)


def project(row, target):
    """Read only declared note/context fields; never infer old facts from current task."""
    missing, observation, basis = [], {}, {"record_type": row.get("record_type", "task_projection")}
    for key in ("title", "objective", "completed_summary", "summary"):
        if isinstance(row.get(key), str):
            observation[key] = row[key]
    note = row.get("note")
    if note is not None and not isinstance(note, dict):
        missing.append("invalid_note")
        note = {}
    note = note or {}
    if set(note) - {*NOTE_TEXT, "conditions", "source_refs"}:
        missing.append("invalid_note:unsupported_fields")
    for key in NOTE_TEXT:
        if note.get(key) is not None:
            if isinstance(note[key], str):
                (observation if key == "observation" else basis)[key] = note[key]
            else:
                missing.append(f"invalid_note:{key}")
    refs = note.get("source_refs", [])
    if not isinstance(refs, list) or any(not isinstance(ref, dict) for ref in refs):
        missing.append("invalid_note:source_refs")
        refs = []
    basis["source_refs"] = refs
    contexts = row.get("contexts") or []
    declarations = [(f"context:{i}", context) for i, context in enumerate(contexts) if isinstance(context, dict)]
    if note.get("conditions") is not None:
        if isinstance(note["conditions"], dict):
            declarations.append(("experience_note.conditions", note["conditions"]))
        else:
            missing.append("invalid_note:conditions")
    hypothesis = row.get("hypothesis_note")
    if hypothesis is not None:
        if isinstance(hypothesis, dict):
            if set(hypothesis) - set(HYPOTHESIS_TEXT):
                missing.append("invalid_note:hypothesis.unsupported_fields")
            for key in HYPOTHESIS_TEXT:
                if isinstance(hypothesis.get(key), str):
                    observation[f"hypothesis.{key}"] = hypothesis[key]
                elif hypothesis.get(key) is not None:
                    missing.append(f"invalid_note:hypothesis.{key}")
        else:
            missing.append("invalid_note:hypothesis")
    if isinstance(row.get("assistance_summary"), dict):
        observation["assistance"] = row["assistance_summary"]
    conditions, evidence = {}, {}
    for key in CONDITION_FIELDS:
        values = [{"source": name, "value": ctx[key]} for name, ctx in declarations if ctx.get(key) is not None]
        evidence[key] = values
        normalized = {_key(sorted(set(v["value"]))) if _strings(v["value"]) else _key(v["value"]) for v in values}
        invalid = any(not (_strings(v["value"]) if key in {"horizons", "input_fields"} else
                          _text(v["value"]) if key in {"research_role", "hypothesis_family"} else
                          isinstance(v["value"], (dict, str))) for v in values)
        if key in FILTER_CHOICES:
            invalid |= any(not set(v["value"] if isinstance(v["value"], list) else [v["value"]]).issubset(FILTER_CHOICES[key])
                           for v in values if _text(v["value"]) or _strings(v["value"]))
        if len(normalized) > 1:
            missing.append(f"condition_conflict:{key}")
        elif invalid:
            missing.append(f"invalid_condition:{key}")
        elif values:
            conditions[key] = values[0]["value"]
    basis.update(condition_sources=evidence, context_refs=row.get("context_refs", []))
    fields = conditions.get("input_fields", [])
    names = row.get("factor_names") or []
    if not _strings(names):
        names = []
        missing.append("invalid_factor_names")
    searchable = dict(observation, **{k: basis[k] for k in NOTE_TEXT if k in basis})
    return dict(source_ref=source_ref(row, target), kind="research_record", observation=_excerpt(observation),
                basis=_excerpt(basis), conditions=conditions, source_refs=refs,
                match_text=_normalize(" ".join(_text_values([searchable, fields, names]))),
                names=names, fields=fields,
                applicability="unverified_historical", missing_information=missing + ["current_data_compatibility_not_verified", "market_value_not_inferred"],
                related_refs=[], relations_complete="not_checked" if not row.get("record_id") else False,
                relation_reasons=[], show_followup=dict(command="show", task_id=str(row["task_id"]),
                    paging="Read the task's records, following before_revision; no source paths are executed."))


def public_entry(entry):
    return {key: value for key, value in entry.items() if key not in {"match_text", "names", "fields"}}


def search_rows(rows, spec, target):
    query, pages, count, unknown_count, read_count = spec["query"], [], 0, 0, 0
    limit, offset = spec["limit"], spec["offset"]
    for row in rows:
        read_count += 1
        entry = project(row, target)
        matches = {}
        for key, values in (("factor_names", entry["names"]), ("input_fields", entry["fields"])):
            normalized = {_normalize(v) for v in values if _text(v)}
            matches[key] = sorted({_normalize(v) for v in query.get(key, []) if _normalize(v) in normalized})
        matches["terms"] = sorted({_normalize(v) for v in query["terms"] if _normalize(v) in entry["match_text"]})
        if not any(matches.values()):
            continue
        unknown, mismatch = [], False
        for key, wanted in query["filters"].items():
            actual = entry["conditions"].get(key)
            if actual is None or actual == []:
                unknown.append(key)
            elif not set(wanted).intersection(actual if isinstance(actual, list) else [actual]):
                mismatch = True
        if mismatch:
            continue
        unknown_count += bool(unknown)
        if unknown and not query["include_unknown"]:
            continue
        count += 1
        entry.update(match_basis=matches, unknown_filters=unknown)
        rank = tuple(-len(matches[k]) for k in ("factor_names", "input_fields", "terms")) + (_key(entry["source_ref"]),)
        bisect.insort(pages, (rank, public_entry(entry)), key=lambda pair: pair[0])
        if len(pages) > offset + limit:
            pages.pop()
    entries = [item for _, item in pages[offset:]]
    return dict(schema_version="factor_research_experience_v1", query=query, entries=entries,
                sources=[dict(source_id=f"aistock_research:{target}", read_state="read", read_count=read_count)],
                matched_count=count, unknown_filter_count=unknown_count, returned_count=len(entries),
                next_offset=offset + limit if offset + limit < count else None, scope="research_database",
                queried_at=datetime.now(timezone.utc).isoformat(), retrieval_status="available",
                match_status="matched" if count else "no_match_in_read_scope", related_entries=[], relations_complete="not_checked")


def attach_relations(result, read_component, target):
    """Each page record gets its own connected correction context, never a winner."""
    related, cache = {}, {}
    for entry in result["entries"]:
        loc = entry["source_ref"]["locator"]
        if loc["record_id"] is None:
            continue
        key = (loc["task_id"], loc["record_id"])
        if key not in cache:
            rows, reasons = read_component(*key)
            items = [public_entry(project(row, target)) for row in rows]
            refs = [item["source_ref"] for item in items]
            for item in items:
                item.update(related_refs=[ref for ref in refs if ref != item["source_ref"]],
                            relations_complete=not reasons, relation_reasons=reasons)
                cache[(loc["task_id"], item["source_ref"]["locator"]["record_id"])] = (items, reasons)
            cache[key] = (items, reasons)
        items, reasons = cache[key]
        entry.update(related_refs=[item["source_ref"] for item in items if item["source_ref"] != entry["source_ref"]],
                     relations_complete=not reasons, relation_reasons=reasons)
        for item in items:
            if item["source_ref"] != entry["source_ref"]:
                related[_key(item["source_ref"])] = item
    # Main entries are authoritative for their match annotations; don't create ambiguous copies.
    primary = {_key(e["source_ref"]): e for e in result["entries"]}
    result["related_entries"] = [primary.get(key, value) for key, value in sorted(related.items())]
    records = [e for e in result["entries"] if e["source_ref"]["locator"]["record_id"] is not None]
    result["relations_complete"] = all(e["relations_complete"] is True for e in records) if records else "not_checked"
    if any(e["relations_complete"] is False or e["observation"]["truncated"] or e["basis"]["truncated"]
           for e in result["entries"] + result["related_entries"]):
        result["retrieval_status"] = "partial"
        result["sources"][0]["read_state"] = "partial"
    return result

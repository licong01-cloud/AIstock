from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol


class GraphStorageProvider(Protocol):
    def list_records(
        self,
        kind: str,
        *,
        filters: Mapping[str, Any] | None = None,
        search: str | None = None,
        limit: int,
        offset: int = 0,
    ) -> dict[str, Any]:
        ...

    def get_record(self, kind: str, record_id: str) -> dict[str, Any] | None:
        ...


@dataclass(frozen=True)
class GraphNeighborExpansion:
    graph_relation_refs: list[str]
    relation_refs: list[dict[str, Any]]
    seed_entity_keys: list[str]
    neighbor_entity_keys: list[str]
    omitted_relation_refs: list[str] = field(default_factory=list)
    route_reason: dict[str, Any] = field(default_factory=dict)


def expand_neighbors(
    entity_keys: Iterable[str],
    *,
    repo: GraphStorageProvider,
    namespace: str = "aistock",
    hops: int = 1,
    relation_filter: Iterable[str] | str | None = None,
    limit: int = 12,
) -> GraphNeighborExpansion:
    """Return bounded, deterministic graph-neighbor summaries for context packs."""
    normalized_keys = _unique_sorted(_clean_key(key) for key in entity_keys)
    safe_hops = max(1, int(hops))
    safe_limit = max(1, int(limit))
    relation_types = _normalize_relation_filter(relation_filter)
    if not normalized_keys:
        return _empty_result(normalized_keys, safe_hops, safe_limit, relation_types)

    seed_entities = [_find_entity(repo, namespace, key) for key in normalized_keys]
    seed_entities = [entity for entity in seed_entities if entity]
    if not seed_entities:
        return _empty_result(normalized_keys, safe_hops, safe_limit, relation_types)

    entities_by_id = {str(entity["entity_id"]): entity for entity in seed_entities}
    frontier = set(entities_by_id)
    seen_entity_ids = set(frontier)
    relation_by_id: dict[str, dict[str, Any]] = {}

    for depth in range(1, safe_hops + 1):
        next_frontier: set[str] = set()
        for entity_id in sorted(frontier, key=lambda item: _entity_sort_key(entities_by_id.get(item, {"entity_id": item}))):
            for direction, relation in _relations_for_entity(repo, entity_id, relation_types, safe_limit * 4):
                relation_id = str(relation.get("relation_id") or "")
                if not relation_id:
                    continue
                source = _entity_for_relation(repo, relation, "source_entity_id", entities_by_id)
                target = _entity_for_relation(repo, relation, "target_entity_id", entities_by_id)
                if not source or not target:
                    continue
                neighbor = target if direction == "outgoing" else source
                neighbor_id = str(neighbor.get("entity_id") or "")
                if neighbor_id and neighbor_id not in seen_entity_ids:
                    next_frontier.add(neighbor_id)
                    seen_entity_ids.add(neighbor_id)
                if relation_id not in relation_by_id:
                    relation_by_id[relation_id] = _summarize_relation(
                        relation,
                        source=source,
                        target=target,
                        neighbor=neighbor,
                        direction=direction,
                        depth=depth,
                    )
        frontier = next_frontier
        if not frontier:
            break

    relation_refs_all = sorted(relation_by_id.values(), key=_relation_sort_key)
    selected = relation_refs_all[:safe_limit]
    omitted = relation_refs_all[safe_limit:]
    graph_relation_refs = [str(item["relation_id"]) for item in selected]
    neighbor_entity_keys = _unique_sorted(
        str(item["neighbor_entity_key"])
        for item in selected
        if item.get("neighbor_entity_key")
    )
    return GraphNeighborExpansion(
        graph_relation_refs=graph_relation_refs,
        relation_refs=selected,
        seed_entity_keys=[str(entity["entity_key"]) for entity in sorted(seed_entities, key=_entity_sort_key)],
        neighbor_entity_keys=neighbor_entity_keys,
        omitted_relation_refs=[str(item["relation_id"]) for item in omitted],
        route_reason={
            "algorithm": "graph_context_v1",
            "input_entity_keys": normalized_keys,
            "seed_entity_keys": [str(entity["entity_key"]) for entity in sorted(seed_entities, key=_entity_sort_key)],
            "hops": safe_hops,
            "limit": safe_limit,
            "relation_filter": sorted(relation_types) if relation_types else [],
            "selected_count": len(selected),
            "omitted_count": len(omitted),
        },
    )


def _empty_result(entity_keys: list[str], hops: int, limit: int, relation_types: set[str]) -> GraphNeighborExpansion:
    return GraphNeighborExpansion(
        graph_relation_refs=[],
        relation_refs=[],
        seed_entity_keys=[],
        neighbor_entity_keys=[],
        omitted_relation_refs=[],
        route_reason={
            "algorithm": "graph_context_v1",
            "input_entity_keys": entity_keys,
            "seed_entity_keys": [],
            "hops": hops,
            "limit": limit,
            "relation_filter": sorted(relation_types) if relation_types else [],
            "selected_count": 0,
            "omitted_count": 0,
        },
    )


def _find_entity(repo: GraphStorageProvider, namespace: str, entity_key: str) -> dict[str, Any] | None:
    page = repo.list_records("entities", filters={"namespace": namespace, "entity_key": entity_key}, limit=1)
    items = page.get("items") or []
    return dict(items[0]) if items else None


def _relations_for_entity(
    repo: GraphStorageProvider,
    entity_id: str,
    relation_types: set[str],
    limit: int,
) -> list[tuple[str, dict[str, Any]]]:
    relations: list[tuple[str, dict[str, Any]]] = []
    for direction, field in (("outgoing", "source_entity_id"), ("incoming", "target_entity_id")):
        page = repo.list_records("relations", filters={field: entity_id}, limit=limit)
        for relation in page.get("items") or []:
            if relation_types and str(relation.get("relation_type") or "") not in relation_types:
                continue
            relations.append((direction, dict(relation)))
    relations.sort(key=lambda item: (str(item[1].get("relation_type") or ""), str(item[1].get("relation_id") or ""), item[0]))
    return relations


def _entity_for_relation(
    repo: GraphStorageProvider,
    relation: Mapping[str, Any],
    field: str,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    entity_id = str(relation.get(field) or "")
    if not entity_id:
        return None
    if entity_id not in cache:
        entity = repo.get_record("entities", entity_id)
        if entity:
            cache[entity_id] = dict(entity)
    return cache.get(entity_id)


def _summarize_relation(
    relation: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    target: Mapping[str, Any],
    neighbor: Mapping[str, Any],
    direction: str,
    depth: int,
) -> dict[str, Any]:
    return {
        "relation_id": str(relation.get("relation_id") or ""),
        "relation_type": str(relation.get("relation_type") or ""),
        "source_entity_key": str(source.get("entity_key") or ""),
        "source_entity_type": str(source.get("entity_type") or ""),
        "source_title": str(source.get("title") or ""),
        "target_entity_key": str(target.get("entity_key") or ""),
        "target_entity_type": str(target.get("entity_type") or ""),
        "target_title": str(target.get("title") or ""),
        "neighbor_entity_key": str(neighbor.get("entity_key") or ""),
        "neighbor_entity_type": str(neighbor.get("entity_type") or ""),
        "neighbor_title": str(neighbor.get("title") or ""),
        "neighbor_summary": _short_text(neighbor.get("summary")),
        "direction": direction,
        "depth": depth,
        "evidence_refs": list(relation.get("evidence_refs") or []),
        "confidence": relation.get("confidence"),
    }


def _relation_sort_key(item: Mapping[str, Any]) -> tuple[int, int, str, str, str, str]:
    direction_rank = 0 if str(item.get("direction") or "") == "outgoing" else 1
    return (
        int(item.get("depth") or 0),
        direction_rank,
        str(item.get("source_entity_key") or ""),
        str(item.get("target_entity_key") or ""),
        str(item.get("relation_type") or ""),
        str(item.get("relation_id") or ""),
    )


def _entity_sort_key(item: Mapping[str, Any]) -> str:
    return str(item.get("entity_key") or item.get("entity_id") or "")


def _normalize_relation_filter(value: Iterable[str] | str | None) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value} if value else set()
    return {str(item) for item in value if str(item)}


def _clean_key(value: Any) -> str:
    return str(value or "").strip()


def _unique_sorted(values: Iterable[str]) -> list[str]:
    return sorted({value for value in values if value})


def _short_text(value: Any, *, max_chars: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."

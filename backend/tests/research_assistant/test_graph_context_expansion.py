from __future__ import annotations

from typing import Any, Mapping

from backend.services.research_assistant.graph_context import expand_neighbors


class FakeGraphProvider:
    def __init__(self) -> None:
        self.entities: dict[str, dict[str, Any]] = {}
        self.relations: dict[str, dict[str, Any]] = {}

    def add_entity(self, entity_id: str, entity_key: str, entity_type: str = "module", **extra: Any) -> None:
        row = {
            "entity_id": entity_id,
            "namespace": "aistock",
            "entity_key": entity_key,
            "entity_type": entity_type,
            "title": entity_key,
            "summary": f"summary for {entity_key}",
            **extra,
        }
        self.entities[entity_id] = row

    def add_relation(self, relation_id: str, source_entity_id: str, target_entity_id: str, relation_type: str, **extra: Any) -> None:
        self.relations[relation_id] = {
            "relation_id": relation_id,
            "source_entity_id": source_entity_id,
            "target_entity_id": target_entity_id,
            "relation_type": relation_type,
            "evidence_refs": [f"evidence://{relation_id}"],
            "confidence": 0.9,
            **extra,
        }

    def list_records(
        self,
        kind: str,
        *,
        filters: Mapping[str, Any] | None = None,
        search: str | None = None,
        limit: int,
        offset: int = 0,
    ) -> dict[str, Any]:
        del search
        rows = list((self.entities if kind == "entities" else self.relations).values())
        for key, value in (filters or {}).items():
            rows = [row for row in rows if row.get(key) == value]
        rows = list(reversed(rows))  # provider order is intentionally unstable-looking; core must sort.
        return {"items": rows[offset : offset + limit], "total": len(rows)}

    def get_record(self, kind: str, record_id: str) -> dict[str, Any] | None:
        rows = self.entities if kind == "entities" else self.relations
        row = rows.get(record_id)
        return dict(row) if row else None


def _provider() -> FakeGraphProvider:
    provider = FakeGraphProvider()
    provider.add_entity("ent_alpha", "module.alpha")
    provider.add_entity("ent_beta", "api.beta", "api")
    provider.add_entity("ent_gamma", "mcp.gamma", "mcp_server")
    provider.add_entity("ent_delta", "process.delta", "process")
    provider.add_relation("rel_z_gamma_alpha", "ent_gamma", "ent_alpha", "feeds")
    provider.add_relation("rel_a_alpha_beta", "ent_alpha", "ent_beta", "uses")
    provider.add_relation("rel_m_alpha_delta", "ent_alpha", "ent_delta", "uses")
    return provider


def test_expand_neighbors_returns_true_neighbors_in_deterministic_order() -> None:
    result = expand_neighbors(["module.alpha"], repo=_provider(), hops=1, limit=10)

    assert result.graph_relation_refs == ["rel_a_alpha_beta", "rel_m_alpha_delta", "rel_z_gamma_alpha"]
    assert result.neighbor_entity_keys == ["api.beta", "mcp.gamma", "process.delta"]
    assert result.seed_entity_keys == ["module.alpha"]
    assert result.route_reason["algorithm"] == "graph_context_v1"
    assert result.route_reason["selected_count"] == 3

    expected_keys = {
        "relation_id",
        "relation_type",
        "source_entity_key",
        "source_entity_type",
        "source_title",
        "target_entity_key",
        "target_entity_type",
        "target_title",
        "neighbor_entity_key",
        "neighbor_entity_type",
        "neighbor_title",
        "neighbor_summary",
        "direction",
        "depth",
        "evidence_refs",
        "confidence",
    }
    assert set(result.relation_refs[0]) == expected_keys
    assert all("content_json" not in item and "created_at" not in item for item in result.relation_refs)


def test_expand_neighbors_applies_relation_filter_limit_and_empty_case() -> None:
    filtered = expand_neighbors(["module.alpha"], repo=_provider(), relation_filter={"uses"}, limit=1)

    assert filtered.graph_relation_refs == ["rel_a_alpha_beta"]
    assert filtered.omitted_relation_refs == ["rel_m_alpha_delta"]
    assert filtered.route_reason["relation_filter"] == ["uses"]

    empty = expand_neighbors(["personal.preference.response"], repo=_provider(), limit=5)
    assert empty.graph_relation_refs == []
    assert empty.relation_refs == []
    assert empty.route_reason["selected_count"] == 0

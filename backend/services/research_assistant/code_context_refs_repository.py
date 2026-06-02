"""Persistence boundary for Research Assistant code context refs."""

from __future__ import annotations

from typing import Any

from .models import CodeContextManifest, CodeContextRef, new_id


class CodeContextRefsRepository:
    """Store already-validated code refs without creating schema implicitly."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def persist_manifest(
        self,
        *,
        context_pack_id: str,
        task_id: str | None,
        query_text: str,
        manifest: CodeContextManifest,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for ref in manifest.refs:
            rows.append(
                self.repository.create_record(
                    "code_context_refs",
                    _row_from_ref(
                        context_pack_id=context_pack_id,
                        task_id=task_id,
                        query_text=query_text,
                        manifest=manifest,
                        ref=ref,
                    ),
                )
            )
        return rows


def _row_from_ref(
    *,
    context_pack_id: str,
    task_id: str | None,
    query_text: str,
    manifest: CodeContextManifest,
    ref: CodeContextRef,
) -> dict[str, Any]:
    if not ref.provenance:
        raise AssertionError("assistant_code_context_refs rows require provenance_json")
    if not ref.as_of:
        raise AssertionError("assistant_code_context_refs rows require as_of")
    return {
        "code_context_ref_id": new_id("ccref"),
        "context_pack_id": context_pack_id,
        "task_id": task_id,
        "query_text": query_text,
        "file_path": ref.file_path,
        "symbol": ref.symbol,
        "status": ref.status,
        "summary_ref": ref.summary_ref,
        "detail_ref": ref.detail_ref,
        "as_of": ref.as_of,
        "edge_refs_json": [dict(item) for item in ref.edge_refs],
        "affected_tests_json": [item.model_dump(exclude_none=True) for item in ref.affected_tests],
        "manifest_json": {
            "manifest_ref": ref.manifest_ref or manifest.manifest_ref,
            "provider": manifest.provider,
            "schema_version": manifest.schema_version,
            "source_refs": list(manifest.source_refs),
            "status": manifest.status,
        },
        "provenance_json": dict(ref.provenance),
    }

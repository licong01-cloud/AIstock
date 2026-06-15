from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_HISTORY_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "history"

RUN_SCHEMA = "aistock_validation_run_v1"
COVERAGE_SCHEMA = "aistock_validation_coverage_snapshot_v1"
EVIDENCE_SCHEMA = "aistock_validation_evidence_manifest_v1"
CODE_INTELLIGENCE_SCHEMAS = {
    "aistock_codegraph_freshness_v1": "codegraph_freshness",
    "aistock_codegraph_latest_freshness_v1": "codegraph_latest_freshness",
    "aistock_code_intelligence_summary_v1": "code_intelligence_summary",
    "aistock_code_intelligence_context_v1": "codegraph_context",
    "aistock_codegraph_affected_tests_v1": "codegraph_affected_tests",
    "aistock_understand_anything_summary_v1": "understand_anything_summary",
    "aistock_understand_anything_summary_manifest_v1": "understand_anything_manifest",
}
CODEGRAPH_ARTIFACT_TYPES = {"codegraph_freshness", "codegraph_latest_freshness"}
CODE_INTELLIGENCE_ARTIFACT_ROOTS = (
    Path("tmp") / "validation" / "code-intelligence",
    Path("tests") / "aistock_validation" / "history" / "code-intelligence",
)
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_MARKDOWN_BYTES = 512 * 1024
ARTIFACT_MARKDOWN_SUFFIXES = (
    "-guardrail-md.md",
    "-l0-guardrail.md",
)


class ValidationHistoryStore:
    """Read local validation run history without executing commands or touching DB."""

    def __init__(self, history_root: Path | None = None, repo_root: Path | None = None) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT).resolve()
        self.history_root = Path(history_root or DEFAULT_HISTORY_ROOT).resolve()

    def health(self) -> dict[str, Any]:
        return {
            "mode": "read_only",
            "history_root": self._repo_path(self.history_root),
            "exists": self.history_root.exists(),
            "run_count": len(self._markdown_files()),
            "coverage_snapshot_count": len(self.list_coverage_snapshots(limit=10000)["items"]),
            "evidence_manifest_count": len(self.list_evidence_manifests(limit=10000)["items"]),
        }

    def list_runs(
        self,
        *,
        module: str | None = None,
        level: str | None = None,
        status: str | None = None,
        search: str | None = None,
        include_markdown_only: bool = True,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        items = [self._run_summary_from_markdown(path) for path in self._markdown_files()]
        if not include_markdown_only:
            items = [item for item in items if not item.get("metadata_missing")]
        if module:
            module_l = module.lower()
            items = [
                item
                for item in items
                if module_l in str(item.get("module") or "").lower()
                or module_l in str(item.get("module_slug") or "").lower()
            ]
        if level:
            level_u = level.upper()
            items = [item for item in items if str(item.get("level") or "").upper() == level_u]
        if status:
            status_l = status.lower()
            items = [item for item in items if str(item.get("status") or "").lower() == status_l]
        if search:
            needle = search.lower()
            items = [
                item
                for item in items
                if needle in item["run_id"].lower()
                or needle in str(item.get("title") or "").lower()
                or needle in str(item.get("markdown_path") or "").lower()
            ]
        items.sort(key=lambda item: (str(item.get("started_at") or ""), item["run_id"]), reverse=True)
        return self._page(items, page=page, page_size=page_size)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        for path in self._markdown_files():
            summary = self._run_summary_from_markdown(path)
            if summary["run_id"] != run_id:
                continue
            detail = dict(summary)
            detail["markdown_text"] = self._read_markdown(path)
            detail["metadata"] = self._load_run_metadata(path)
            coverage = self._associated_coverage(path)
            evidence = self._associated_evidence(path)
            detail["coverage_snapshot"] = coverage
            detail["evidence_manifest"] = evidence
            detail["coverage_missing"] = coverage is None
            detail["evidence_missing"] = evidence is None
            return detail
        return None

    def list_coverage_snapshots(
        self,
        *,
        module: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        for path in self._json_files():
            payload, parse_error = self._read_json(path)
            if parse_error:
                continue
            if payload.get("schema_version") != COVERAGE_SCHEMA:
                continue
            items.append(self._coverage_summary(path, payload))
        if module:
            module_l = module.lower()
            items = [item for item in items if module_l in str(item.get("module") or "").lower()]
        if status:
            status_l = status.lower()
            items = [item for item in items if str(item.get("status") or "").lower() == status_l]
        items.sort(key=lambda item: (str(item.get("generated_at") or ""), item["snapshot_id"]), reverse=True)
        if limit is not None:
            items = items[:limit]
            return {"items": items, "total": len(items), "page": 1, "page_size": limit, "has_more": False}
        return self._page(items, page=page, page_size=page_size)

    def get_coverage_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        for path in self._json_files():
            payload, parse_error = self._read_json(path)
            if parse_error:
                continue
            if payload.get("schema_version") != COVERAGE_SCHEMA:
                continue
            summary = self._coverage_summary(path, payload)
            if summary["snapshot_id"] == snapshot_id:
                return {"summary": summary, "snapshot": payload}
        return None

    def list_evidence_manifests(
        self,
        *,
        module: str | None = None,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        for path in self._json_files():
            payload, parse_error = self._read_json(path)
            if parse_error:
                continue
            if payload.get("schema_version") != EVIDENCE_SCHEMA:
                continue
            items.append(self._evidence_summary(path, payload))
        if module:
            module_l = module.lower()
            items = [item for item in items if module_l in str(item.get("module") or "").lower()]
        items.sort(key=lambda item: (str(item.get("generated_at") or ""), item["manifest_id"]), reverse=True)
        if limit is not None:
            items = items[:limit]
            return {"items": items, "total": len(items), "page": 1, "page_size": limit, "has_more": False}
        return self._page(items, page=page, page_size=page_size)

    def get_evidence_manifest(self, manifest_id: str) -> dict[str, Any] | None:
        for path in self._json_files():
            payload, parse_error = self._read_json(path)
            if parse_error:
                continue
            if payload.get("schema_version") != EVIDENCE_SCHEMA:
                continue
            summary = self._evidence_summary(path, payload)
            if summary["manifest_id"] == manifest_id:
                return {"summary": summary, "manifest": payload}
        return None

    def summary(self) -> dict[str, Any]:
        runs = self.list_runs(page_size=10000)["items"]
        coverage = self.list_coverage_snapshots(limit=10000)["items"]
        by_module: dict[str, dict[str, Any]] = {}
        by_status: dict[str, int] = {}
        for item in runs:
            module = str(item.get("module") or item.get("module_slug") or "unknown")
            status = str(item.get("status") or "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            module_bucket = by_module.setdefault(module, {"module": module, "run_count": 0, "latest_run": None})
            module_bucket["run_count"] += 1
            if module_bucket["latest_run"] is None:
                module_bucket["latest_run"] = item
        latest_coverage = coverage[0] if coverage else None
        return {
            "history_root": self._repo_path(self.history_root),
            "run_count": len(runs),
            "coverage_snapshot_count": len(coverage),
            "evidence_manifest_count": len(self.list_evidence_manifests(limit=10000)["items"]),
            "runs_by_status": by_status,
            "modules": sorted(by_module.values(), key=lambda item: item["module"]),
            "latest_runs": runs[:10],
            "latest_coverage": latest_coverage,
            "code_intelligence": self.code_intelligence_summary(),
        }

    def code_intelligence_summary(self) -> dict[str, Any]:
        artifacts = self.list_code_intelligence_artifacts(limit=10000)["items"]
        latest_codegraph = next(
            (item for item in artifacts if item.get("artifact_type") in CODEGRAPH_ARTIFACT_TYPES),
            None,
        )
        latest_codegraph = self._codegraph_with_repo_metadata(latest_codegraph)
        latest_manifest = next(
            (item for item in artifacts if item.get("artifact_type") == "understand_anything_manifest"),
            None,
        )
        ua_summaries = [
            item for item in artifacts if item.get("artifact_type") == "understand_anything_summary"
        ]
        warnings: list[str] = []
        if not artifacts:
            warnings.append("No code-intelligence artifacts found under tmp/validation/code-intelligence.")
        if latest_codegraph is None:
            warnings.append("CodeGraph freshness artifact is missing.")
        else:
            codegraph_freshness = latest_codegraph.get("effective_freshness") or latest_codegraph.get("freshness")
            if codegraph_freshness != "fresh":
                warnings.append(f"CodeGraph freshness is {codegraph_freshness or 'unknown'}.")
            elif latest_codegraph.get("stale_metadata_warning"):
                warnings.append("CodeGraph metadata is stale but effective freshness is fresh.")
        if latest_manifest is None and not ua_summaries:
            warnings.append("Understand Anything summary artifacts are missing; this is non-blocking.")
        data_state = "complete" if artifacts and latest_codegraph else ("partial" if artifacts else "missing")
        return {
            "schema_version": "aistock_validation_code_intelligence_summary_v1",
            "data_state": data_state,
            "blocking_for_issue_workflow": False,
            "artifact_count": len(artifacts),
            "artifact_roots": [
                self._repo_path((self.repo_root / root).resolve())
                for root in CODE_INTELLIGENCE_ARTIFACT_ROOTS
            ],
            "codegraph": latest_codegraph,
            "understand_anything": {
                "manifest": latest_manifest,
                "summary_count": len(ua_summaries),
                "latest_summaries": ua_summaries[:10],
            },
            "artifacts": artifacts[:20],
            "warnings": warnings,
            "reason_codes": [self._reason_code(item) for item in warnings],
        }

    def list_code_intelligence_artifacts(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        for root in self._code_intelligence_roots():
            for path in self._json_files_under(root):
                payload, parse_error = self._read_json(path)
                if parse_error:
                    continue
                schema = payload.get("schema_version")
                artifact_type = CODE_INTELLIGENCE_SCHEMAS.get(str(schema))
                if not artifact_type:
                    continue
                items.append(self._code_intelligence_artifact_summary(path, payload, artifact_type))
        items.sort(
            key=lambda item: (str(item.get("generated_at") or ""), str(item.get("modified_at") or ""), item["artifact_id"]),
            reverse=True,
        )
        if limit is not None:
            items = items[:limit]
            return {"items": items, "total": len(items), "page": 1, "page_size": limit, "has_more": False}
        return self._page(items, page=page, page_size=page_size)

    def _markdown_files(self) -> list[Path]:
        if not self.history_root.exists():
            return []
        return sorted(
            path
            for path in self.history_root.rglob("*.md")
            if path.is_file() and self._is_run_markdown(path)
        )

    @staticmethod
    def _is_run_markdown(path: Path) -> bool:
        name = path.name.lower()
        return not any(name.endswith(suffix) for suffix in ARTIFACT_MARKDOWN_SUFFIXES)

    def _json_files(self) -> list[Path]:
        if not self.history_root.exists():
            return []
        return sorted(path for path in self.history_root.rglob("*.json") if path.is_file())

    def _code_intelligence_roots(self) -> list[Path]:
        return [(self.repo_root / root).resolve() for root in CODE_INTELLIGENCE_ARTIFACT_ROOTS]

    @staticmethod
    def _json_files_under(root: Path) -> list[Path]:
        if not root.exists():
            return []
        return sorted(path for path in root.rglob("*.json") if path.is_file())

    def _run_summary_from_markdown(self, markdown_path: Path) -> dict[str, Any]:
        metadata, metadata_error = self._read_json(self._metadata_path(markdown_path))
        markdown_title = self._markdown_title(markdown_path)
        module_slug = markdown_path.parent.name
        level = metadata.get("level") or self._level_from_name(markdown_path.name)
        status = metadata.get("status") or self._status_from_markdown(markdown_path)
        associated_coverage = self._associated_coverage(markdown_path)
        associated_evidence = self._associated_evidence(markdown_path)
        run_id = self._run_id(markdown_path)
        return {
            "run_id": run_id,
            "module": metadata.get("module") or module_slug,
            "module_slug": metadata.get("module_slug") or module_slug,
            "level": level,
            "title": metadata.get("title") or markdown_title,
            "status": status or "unknown",
            "git_commit": metadata.get("git_commit"),
            "operator": metadata.get("operator"),
            "started_at": metadata.get("started_at") or self._date_from_name(markdown_path.name),
            "finished_at": metadata.get("finished_at"),
            "markdown_path": self._repo_path(markdown_path),
            "metadata_path": self._repo_path(self._metadata_path(markdown_path))
            if self._metadata_path(markdown_path).exists()
            else None,
            "metadata_missing": not self._metadata_path(markdown_path).exists(),
            "metadata_parse_error": metadata_error,
            "source_type": "markdown_with_json" if metadata and not metadata_error else "markdown_only",
            "coverage": self._coverage_status(metadata, associated_coverage),
            "coverage_snapshot_id": associated_coverage.get("snapshot_id") if associated_coverage else None,
            "coverage_missing": associated_coverage is None and not metadata.get("coverage"),
            "evidence_manifest_id": associated_evidence.get("manifest_id") if associated_evidence else None,
            "evidence_missing": associated_evidence is None,
            "pass_scope": metadata.get("pass_scope"),
            "business_assertion": metadata.get("business_assertion"),
            "success_scope_recorded": bool(metadata.get("pass_scope") or metadata.get("business_assertion")),
            "quality_gates": metadata.get("quality_gates") or [],
            "parse_error": metadata_error,
        }

    def _load_run_metadata(self, markdown_path: Path) -> dict[str, Any] | None:
        metadata, parse_error = self._read_json(self._metadata_path(markdown_path))
        if parse_error or not metadata:
            return None
        return metadata

    def _metadata_path(self, markdown_path: Path) -> Path:
        return markdown_path.with_suffix(".json")

    def _read_json(self, path: Path) -> tuple[dict[str, Any], str | None]:
        if not path.exists():
            return {}, None
        try:
            if path.stat().st_size > MAX_JSON_BYTES:
                return {}, f"JSON file exceeds max size: {MAX_JSON_BYTES}"
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            return {}, f"invalid JSON: {exc}"
        except UnicodeDecodeError as exc:
            return {}, f"invalid UTF-8: {exc}"
        except OSError as exc:
            return {}, f"read error: {exc}"
        if not isinstance(payload, dict):
            return {}, "JSON root is not an object"
        return payload, None

    def _read_markdown(self, path: Path) -> str | None:
        try:
            if path.stat().st_size > MAX_MARKDOWN_BYTES:
                return None
            return path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            return None

    def _markdown_title(self, path: Path) -> str:
        text = self._read_markdown(path)
        if text:
            for line in text.splitlines():
                if line.startswith("# "):
                    return line[2:].strip()
        return path.stem

    def _status_from_markdown(self, path: Path) -> str | None:
        text = self._read_markdown(path)
        if not text:
            return None
        lowered = text.lower()
        if "final status: pass" in lowered or "final status: passed" in lowered:
            return "passed"
        if "final status: fail" in lowered or "final status: failed" in lowered:
            return "failed"
        return None

    @staticmethod
    def _level_from_name(name: str) -> str | None:
        match = re.search(r"(^|_)(l[0-5])(_|-)", name.lower())
        return match.group(2).upper() if match else None

    @staticmethod
    def _date_from_name(name: str) -> str | None:
        match = re.match(r"(\d{8})(?:_(\d{6}))?", name)
        if not match:
            return None
        raw = match.group(1) + (match.group(2) or "000000")
        try:
            return datetime.strptime(raw, "%Y%m%d%H%M%S").isoformat(timespec="seconds")
        except ValueError:
            return None

    def _associated_coverage(self, markdown_path: Path) -> dict[str, Any] | None:
        for candidate in self._associated_candidates(markdown_path, suffixes=("-snapshot.json", ".snapshot.json")):
            payload, parse_error = self._read_json(candidate)
            if parse_error or payload.get("schema_version") != COVERAGE_SCHEMA:
                continue
            return self._coverage_summary(candidate, payload)
        return None

    def _associated_evidence(self, markdown_path: Path) -> dict[str, Any] | None:
        for candidate in self._associated_candidates(markdown_path, suffixes=("-evidence.json", ".evidence.json")):
            payload, parse_error = self._read_json(candidate)
            if parse_error or payload.get("schema_version") != EVIDENCE_SCHEMA:
                continue
            return self._evidence_summary(candidate, payload)
        return None

    @staticmethod
    def _associated_candidates(markdown_path: Path, *, suffixes: tuple[str, ...]) -> list[Path]:
        stems = [markdown_path.stem]
        if markdown_path.stem.endswith("-validation"):
            stems.append(markdown_path.stem[: -len("-validation")])
        candidates: list[Path] = []
        for stem in stems:
            for suffix in suffixes:
                candidates.append(markdown_path.with_name(stem + suffix))
        return candidates

    def _coverage_status(
        self,
        metadata: dict[str, Any],
        associated_coverage: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        metadata_coverage = metadata.get("coverage")
        if isinstance(metadata_coverage, dict) and metadata_coverage:
            return metadata_coverage
        if associated_coverage:
            return {
                "schema_version": COVERAGE_SCHEMA,
                "status": associated_coverage.get("status"),
                "line": (associated_coverage.get("totals") or {}).get("line_percent"),
                "branch": (associated_coverage.get("totals") or {}).get("branch_percent"),
                "diff_line": (associated_coverage.get("diff") or {}).get("line_percent"),
                "snapshot_path": associated_coverage.get("snapshot_path"),
                "quality_gates": associated_coverage.get("quality_gates") or [],
            }
        return None

    def _coverage_summary(self, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "snapshot_id": self._id_for_path(path),
            "schema_version": payload.get("schema_version"),
            "module": payload.get("module"),
            "level": payload.get("level"),
            "title": payload.get("title"),
            "run_id": payload.get("run_id"),
            "generated_at": payload.get("generated_at"),
            "git_commit": payload.get("git_commit"),
            "status": payload.get("status"),
            "snapshot_path": self._repo_path(path),
            "totals": payload.get("totals") or {},
            "diff": payload.get("diff") or {},
            "quality_gates": payload.get("quality_gates") or [],
            "failed_gates": payload.get("failed_gates") or [],
        }

    def _evidence_summary(self, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        evidence = payload.get("evidence") or []
        return {
            "manifest_id": self._id_for_path(path),
            "schema_version": payload.get("schema_version"),
            "module": payload.get("module"),
            "level": payload.get("level"),
            "title": payload.get("title"),
            "run_id": payload.get("run_id"),
            "generated_at": payload.get("generated_at"),
            "git_commit": payload.get("git_commit"),
            "manifest_path": self._repo_path(path),
            "missing_count": payload.get("missing_count") or 0,
            "evidence_count": len(evidence) if isinstance(evidence, list) else 0,
            "missing": payload.get("missing") or [],
        }

    def _code_intelligence_artifact_summary(
        self,
        path: Path,
        payload: dict[str, Any],
        artifact_type: str,
    ) -> dict[str, Any]:
        stat = path.stat()
        effective = payload.get("effective") if isinstance(payload.get("effective"), dict) else {}
        latest = payload.get("latest") if isinstance(payload.get("latest"), dict) else {}
        source_payload = effective if artifact_type == "codegraph_latest_freshness" and effective else payload
        summary_ref = source_payload.get("summary_ref") or payload.get("summary_ref")
        summary_path = self.repo_root / str(summary_ref) if summary_ref else None
        freshness = (
            source_payload.get("freshness")
            or payload.get("effective_freshness")
            or payload.get("freshness")
        )
        status = (
            source_payload.get("status")
            or source_payload.get("workflow_gate")
            or payload.get("status")
            or payload.get("workflow_gate")
        )
        effective_status = source_payload.get("status") or source_payload.get("workflow_gate")
        return {
            "artifact_id": self._id_for_repo_path(path),
            "schema_version": payload.get("schema_version"),
            "artifact_type": artifact_type,
            "provider": source_payload.get("provider") or payload.get("provider") or payload.get("graph_provider"),
            "status": status,
            "freshness": freshness,
            "effective_freshness": freshness,
            "effective_status": effective_status,
            "effective_source": payload.get("effective_source") or payload.get("persisted_from"),
            "stale_metadata_warning": bool(payload.get("stale_metadata_warning")),
            "freshness_basis": source_payload.get("freshness_basis") or payload.get("freshness_basis"),
            "current_git_commit": payload.get("current_git_commit"),
            "latest_git_commit": latest.get("git_commit") if latest else None,
            "module": source_payload.get("module") or payload.get("module"),
            "generated_at": source_payload.get("generated_at") or payload.get("generated_at"),
            "git_commit": source_payload.get("git_commit") or payload.get("git_commit") or payload.get("graph_commit"),
            "artifact_path": self._repo_path(path),
            "summary_ref": str(summary_ref) if summary_ref else None,
            "summary_exists": bool(summary_path and summary_path.exists()),
            "blocking_for_issue_workflow": bool(
                payload.get("blocking_for_issue_workflow") or source_payload.get("blocking_for_issue_workflow")
            ),
            "warnings": payload.get("warnings") or source_payload.get("warnings") or [],
            "notes": payload.get("notes") or source_payload.get("notes") or [],
            "index_summary": source_payload.get("index_summary") or payload.get("index_summary") or {},
            "summary_refs": source_payload.get("summary_refs") or payload.get("summary_refs") or [],
            "node_count": source_payload.get("node_count") or payload.get("node_count"),
            "edge_count": source_payload.get("edge_count") or payload.get("edge_count"),
            "nodes_used": source_payload.get("nodes_used") or payload.get("nodes_used"),
            "edges_used": source_payload.get("edges_used") or payload.get("edges_used"),
            "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(timespec="seconds"),
        }

    def _codegraph_with_repo_metadata(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        if item is None:
            return None
        enriched = dict(item)
        current_commit = enriched.get("current_git_commit") or self._current_git_commit()
        latest_commit = enriched.get("latest_git_commit") or enriched.get("git_commit")
        if current_commit:
            enriched["current_git_commit"] = current_commit
        if latest_commit:
            enriched["latest_git_commit"] = latest_commit
        index_summary = enriched.get("index_summary") if isinstance(enriched.get("index_summary"), dict) else {}
        stale_but_usable = bool(
            current_commit
            and latest_commit
            and str(current_commit) != str(latest_commit)
            and enriched.get("freshness") == "fresh"
            and (
                index_summary.get("up_to_date") is True
                or enriched.get("freshness_basis") == "live_codegraph_status"
                or enriched.get("effective_source")
            )
        )
        if stale_but_usable:
            enriched["stale_metadata_warning"] = True
            enriched.setdefault("effective_source", "artifact_metadata_stale")
            enriched.setdefault("effective_freshness", "fresh")
        return enriched

    def _current_git_commit(self) -> str | None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.repo_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except Exception:
            return None
        if result.returncode != 0:
            return None
        return result.stdout.strip() or None

    def _id_for_path(self, path: Path) -> str:
        relative = self._relative_to_history(path)
        digest = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:10]
        readable = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(relative).with_suffix("").as_posix())
        return f"{readable}__{digest}"

    def _id_for_repo_path(self, path: Path) -> str:
        relative = self._repo_path(path)
        digest = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:10]
        readable = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(relative).with_suffix("").as_posix())
        return f"{readable}__{digest}"

    def _run_id(self, markdown_path: Path) -> str:
        return self._id_for_path(markdown_path)

    def _relative_to_history(self, path: Path) -> str:
        return path.resolve().relative_to(self.history_root).as_posix()

    def _repo_path(self, path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(self.repo_root).as_posix()
        except ValueError:
            return str(resolved)

    @staticmethod
    def _reason_code(warning: str) -> str:
        text = re.sub(r"[^a-z0-9]+", "_", warning.lower()).strip("_")
        return text[:80] or "code_intelligence_warning"

    @staticmethod
    def _page(items: list[dict[str, Any]], *, page: int, page_size: int) -> dict[str, Any]:
        safe_page = max(page, 1)
        safe_page_size = max(page_size, 1)
        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size
        return {
            "items": items[start:end],
            "total": len(items),
            "page": safe_page,
            "page_size": safe_page_size,
            "has_more": end < len(items),
        }

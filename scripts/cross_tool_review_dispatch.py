"""Cross-tool review dispatch CLI (T-PIPE-5.3).

Pipeline-foundation Stage 5 deliverable.

Workflow
--------
A reviewer (Claude Code Lead session, Codex App, or human) finishes a code
review against a target branch and produces a ``findings.json`` describing
what they found. This CLI:

1. Reads the findings file and validates schema_version.
2. For each finding at or above the severity filter, calls
   ``aistock_mcp_server.report_bug`` (in-process import; no MCP transport
   round-trip needed because we already share the filesystem). Bugs are
   deduped on (module, title, reproduce_command) fingerprint.
3. Composes a single ``[REVIEW]`` cross-tool drawer summarizing the
   bug_ids created (or referenced if deduplicated), the reviewer, the
   target branch, and the commit list reviewed.
4. Optionally writes the drawer via the MemPalace HTTP server if the
   user has it running, otherwise prints the drawer body for manual
   filing.

The default mode is ``--dry-run`` which performs steps 1-3 in memory and
prints the plan + drawer body without writing anything.

Findings JSON schema
--------------------
::

    {
      "schema_version": "aistock_cross_tool_review_findings_v1",
      "reviewer": "claude_code" | "codex_app" | "human",
      "target_branch": "origin/...",
      "commit_list": ["abc1234", "def5678"],
      "findings": [
        {
          "title": "...",
          "severity": "P0" | "P1" | "P2" | "P3",
          "module": "<module_id>",
          "files": ["backend/..."],
          "reproduce_command": "...",
          "expected": "...",
          "actual": "...",
          "fix_owner": "claude_code" | "codex_app" | null,
          "comments": ["..."]
        }
      ]
    }

Usage
-----
::

    python scripts/cross_tool_review_dispatch.py \
        --findings-json review_2026-05-11.json \
        --target-tool codex \
        --severity-filter P0,P1

    # Apply (write bugs + post drawer)
    python scripts/cross_tool_review_dispatch.py \
        --findings-json review_2026-05-11.json \
        --target-tool codex \
        --severity-filter P0,P1 \
        --apply
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = "aistock_cross_tool_review_findings_v1"
DEFAULT_TARGET_TOOL = "codex"
TARGET_TOOL_VALUES = {"codex", "claude_code", "human"}
DEFAULT_SEVERITY_FILTER = ("P0", "P1")
DRAWER_WING = "cross-tool"
DRAWER_ROOM = "codex-claude-coord"


@dataclass
class FindingResult:
    finding_index: int
    title: str
    severity: str
    module: str
    bug_id: str | None
    deduplicated: bool
    fingerprint: str | None
    skipped_reason: str | None = None


def _import_mcp_server():
    """Import scripts.aistock_mcp_server lazily so this script can run alone.

    Returns the module so callers can swap it out in tests via monkeypatch.
    """
    import importlib

    return importlib.import_module("scripts.aistock_mcp_server")


def parse_findings(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"findings file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("findings JSON root must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported findings schema_version: {payload.get('schema_version')!r}; "
            f"expected {SCHEMA_VERSION!r}"
        )
    findings = payload.get("findings")
    if not isinstance(findings, list):
        raise ValueError("findings JSON must include a list 'findings'")
    return payload


def filter_findings(
    findings: Sequence[dict[str, Any]],
    severity_filter: Sequence[str],
) -> list[tuple[int, dict[str, Any]]]:
    sev_set = {s.upper() for s in severity_filter}
    return [
        (i, f) for i, f in enumerate(findings)
        if str(f.get("severity") or "").upper() in sev_set
    ]


def dispatch_one(
    mcp_module,
    finding: dict[str, Any],
    *,
    related_drawer: str | None,
    fix_owner_default: str | None,
) -> dict[str, Any]:
    """Call mcp_module.report_bug for a single finding and return its result."""
    return mcp_module.report_bug(
        title=finding["title"],
        severity=finding["severity"],
        module=finding["module"],
        files=list(finding.get("files") or []),
        reproduce_command=finding["reproduce_command"],
        expected=finding.get("expected", ""),
        actual=finding.get("actual", ""),
        fix_owner=finding.get("fix_owner") or fix_owner_default,
        related_drawer=related_drawer,
        comments=list(finding.get("comments") or []),
    )


def compose_drawer_body(
    *,
    payload: dict[str, Any],
    target_tool: str,
    results: Sequence[FindingResult],
) -> str:
    reviewer = payload.get("reviewer") or "unknown"
    branch = payload.get("target_branch") or "unspecified"
    commits = payload.get("commit_list") or []
    new_bugs = [r for r in results if r.bug_id and not r.deduplicated]
    deduped = [r for r in results if r.deduplicated]
    skipped = [r for r in results if r.skipped_reason]

    lines = [
        f"[REVIEW] Cross-tool review by {reviewer} -> {target_tool}",
        "",
        f"branch_reviewed={branch}",
        f"commits_reviewed={','.join(commits) if commits else 'unspecified'}",
        f"production_touched=no",
        "",
        "Summary:",
        f"- new bugs filed: {len(new_bugs)}",
        f"- deduplicated against existing bugs: {len(deduped)}",
        f"- skipped (under severity filter): {len(skipped)}",
        "",
    ]
    if new_bugs:
        lines.append("New BUG-NNN entries (status=open):")
        for r in new_bugs:
            lines.append(
                f"  {r.bug_id} [{r.severity}] {r.module} - {r.title}"
            )
        lines.append("")
    if deduped:
        lines.append("Deduplicated (already tracked):")
        for r in deduped:
            lines.append(
                f"  {r.bug_id} [{r.severity}] {r.module} - {r.title}"
            )
        lines.append("")
    if skipped:
        lines.append("Skipped:")
        for r in skipped:
            lines.append(
                f"  [{r.severity}] {r.module} - {r.title}  ({r.skipped_reason})"
            )
        lines.append("")
    lines.append(
        f"To start fix work, call MCP get_bug_agent_context(bug_id) for the\n"
        f"context (reproduce_command, allowed_write_scope, required_verification,\n"
        f"closure_requirements). Coordinate fix + verify per\n"
        f"docs/process/dual_party_verify_20260510.md."
    )
    lines.append("")
    lines.append(f"-- {reviewer} (cross_tool_review_dispatch.py)")
    return "\n".join(lines)


def post_drawer_via_mempalace(body: str) -> str | None:
    """Try to post the drawer via the MemPalace MCP / HTTP API.

    Returns the drawer ID on success or ``None`` when no MemPalace endpoint
    is reachable; the caller should fall back to printing the body for
    manual filing.

    This intentionally does NOT block on a missing MemPalace -- the CLI is
    expected to be useful even when the MemPalace daemon is not running.
    Tests stub this function out via monkeypatch.
    """
    try:
        import os

        import httpx
    except ImportError:
        return None
    base = os.environ.get("MEMPALACE_HTTP_BASE")
    if not base:
        return None
    try:
        r = httpx.post(
            f"{base.rstrip('/')}/drawers",
            json={"wing": DRAWER_WING, "room": DRAWER_ROOM, "content": body},
            timeout=10.0,
        )
        if r.status_code >= 400:
            return None
        payload = r.json()
        return payload.get("drawer_id")
    except (httpx.HTTPError, ValueError, RuntimeError):
        return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--findings-json", required=True, type=Path)
    p.add_argument(
        "--target-tool",
        default=DEFAULT_TARGET_TOOL,
        choices=sorted(TARGET_TOOL_VALUES),
    )
    p.add_argument(
        "--severity-filter",
        default=",".join(DEFAULT_SEVERITY_FILTER),
        help="Comma-separated severities to dispatch (default 'P0,P1').",
    )
    p.add_argument(
        "--related-drawer",
        default=None,
        help="Drawer ID to attach to created bugs as evidence.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually write bugs + post drawer. Default is dry run.",
    )
    p.add_argument(
        "--fix-owner-default",
        default=None,
        help="fix_owner used when the finding does not name one.",
    )
    return p.parse_args(argv)


def run(args: argparse.Namespace, mcp_module=None, drawer_poster=None) -> dict[str, Any]:
    """Pure orchestration; returns a result dict for callers / tests."""
    if mcp_module is None:
        mcp_module = _import_mcp_server()
    if drawer_poster is None:
        drawer_poster = post_drawer_via_mempalace

    payload = parse_findings(args.findings_json)
    severity_filter = [s.strip() for s in args.severity_filter.split(",") if s.strip()]
    matched = filter_findings(payload["findings"], severity_filter)
    skipped_indices = {
        i for i, _ in enumerate(payload["findings"]) if i not in {idx for idx, _ in matched}
    }

    results: list[FindingResult] = []

    if args.apply:
        for idx, finding in matched:
            response = dispatch_one(
                mcp_module,
                finding,
                related_drawer=args.related_drawer,
                fix_owner_default=args.fix_owner_default,
            )
            if response.get("deduplicated"):
                existing = response.get("existing") or {}
                results.append(
                    FindingResult(
                        finding_index=idx,
                        title=finding["title"],
                        severity=str(finding.get("severity") or "").upper(),
                        module=finding["module"],
                        bug_id=existing.get("bug_id"),
                        deduplicated=True,
                        fingerprint=response.get("fingerprint"),
                    )
                )
            else:
                results.append(
                    FindingResult(
                        finding_index=idx,
                        title=finding["title"],
                        severity=str(finding.get("severity") or "").upper(),
                        module=finding["module"],
                        bug_id=response.get("bug_id"),
                        deduplicated=False,
                        fingerprint=response.get("fingerprint"),
                    )
                )
    else:
        # Dry run: simulate dedup probe via MCP without writing.
        for idx, finding in matched:
            results.append(
                FindingResult(
                    finding_index=idx,
                    title=finding["title"],
                    severity=str(finding.get("severity") or "").upper(),
                    module=finding["module"],
                    bug_id="(dry-run)",
                    deduplicated=False,
                    fingerprint=None,
                )
            )

    for idx in skipped_indices:
        finding = payload["findings"][idx]
        results.append(
            FindingResult(
                finding_index=idx,
                title=finding.get("title", ""),
                severity=str(finding.get("severity") or "").upper(),
                module=str(finding.get("module") or ""),
                bug_id=None,
                deduplicated=False,
                fingerprint=None,
                skipped_reason=f"severity below filter ({severity_filter})",
            )
        )

    body = compose_drawer_body(payload=payload, target_tool=args.target_tool, results=results)

    drawer_id: str | None = None
    if args.apply:
        drawer_id = drawer_poster(body)

    return {
        "results": results,
        "drawer_body": body,
        "drawer_id": drawer_id,
        "applied": args.apply,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    outcome = run(args)
    print(outcome["drawer_body"])
    print()
    if not outcome["applied"]:
        print(
            f"DRY RUN. No bugs written, no drawer posted. "
            f"Pass --apply to dispatch {len([r for r in outcome['results'] if not r.skipped_reason])} finding(s)."
        )
        return 0
    print(f"Applied. drawer_id={outcome['drawer_id'] or 'NOT POSTED -- file the body manually'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

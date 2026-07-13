
from __future__ import annotations

from pathlib import Path


def test_issue_candidates_page_describes_validation_source_and_blocks_ra_github_dry_run() -> None:
    path = Path("frontend/src/app/research-assistant/issue-candidates/page.tsx")
    if not path.exists():
        # Retired RA pages cannot expose stale draft-table source-of-truth UI.
        assert not path.exists()
        return
    text = path.read_text(encoding="utf-8")

    assert "source_of_truth: Validation issue candidates" in text
    assert "RA retired assistant_issue_candidates" in text
    assert "cannot substitute for Validation facts" in text
    assert "githubSyncIssueCandidate" not in text
    assert "GitHub dry-run" not in text
    assert "Use standard workflow" in text
    assert "validation_issue_fact_source_unavailable" in text


def test_streams_page_labels_discovery_as_derived_view_without_ra_report_fact_source() -> None:
    path = Path("frontend/src/app/research-assistant/streams/page.tsx")
    if not path.exists():
        # Retired RA pages cannot expose stale discovery-report source-of-truth UI.
        assert not path.exists()
        return
    text = path.read_text(encoding="utf-8")

    assert "derived_from_validation_candidates" in text
    assert "RA retired assistant_validation_discovery_reports and cannot substitute drafts for Validation/Nightly facts" in text
    assert "Validation fact source unavailable" in text
    assert "source_type / source_plan_key / active_discovery_reason" in text
    assert "assistant_validation_discovery_reports\"" not in text

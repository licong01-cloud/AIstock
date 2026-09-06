"""Retired MemPalace cross-tool review dispatcher entry point.

The active AIstock workflow uses GitHub Issues/PRs, BUG JSON, and durable
validation receipts.  This tombstone remains only so stale automation fails
with an explicit migration message instead of a misleading file-not-found or
silent fallback.
"""

from __future__ import annotations

import sys

RETIREMENT_MESSAGE = (
    "cross_tool_review_dispatch is retired; use aistock_issue_workflow.py "
    "and GitHub Issue/PR review evidence"
)


def main() -> int:
    print(RETIREMENT_MESSAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Thin CLI entry for the phased AIstock MCP gateway."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from backend.mcp.gateway import DEFAULT_BASE_URL, run_gateway


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AIstock MCP gateway")
    parser.add_argument("--profile", default="research", help="Phase 0-5 allows only: research")
    parser.add_argument("--modules", default=None, help="Comma-separated modules; Phase 0-5 allows only: research")
    parser.add_argument(
        "--base-url",
        default=None,
        help=f"Loopback backend API base URL; default is AISTOCK_MCP_BASE_URL or {DEFAULT_BASE_URL}",
    )
    parser.add_argument("--transport", default="stdio", help="FastMCP transport, default: stdio")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_gateway(
        profile=args.profile,
        modules=args.modules,
        base_url=args.base_url,
        transport_name=args.transport,
    )


if __name__ == "__main__":
    main()

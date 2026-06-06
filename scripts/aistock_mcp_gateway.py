"""Thin CLI entry for the unified AIstock MCP gateway."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from backend.mcp.gateway import (
    DEFAULT_BASE_URL,
    list_profiles_payload,
    list_tools_payload,
    run_gateway,
    self_check_payload,
    startup_summary_payload,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AIstock MCP gateway")
    parser.add_argument(
        "--profile",
        default="lite",
        help="Gateway profile, e.g. lite, research, data, qe, validation, factor, trading_ops, full",
    )
    parser.add_argument("--modules", default=None, help="Comma-separated gateway modules, e.g. research,factor_library,model_registry")
    parser.add_argument(
        "--base-url",
        default=None,
        help=f"Loopback backend API base URL; default is AISTOCK_MCP_BASE_URL or {DEFAULT_BASE_URL}",
    )
    parser.add_argument("--transport", default="stdio", help="FastMCP transport, default: stdio")
    parser.add_argument("--list-profiles", action="store_true", help="Print profile metadata as JSON and exit")
    parser.add_argument("--list-tools", action="store_true", help="Print selected tool metadata as JSON and exit")
    parser.add_argument("--startup-summary", action="store_true", help="Print structured startup summary JSON and exit")
    parser.add_argument("--self-check", action="store_true", help="Print gateway readiness JSON and exit")
    parser.add_argument("--check-backend", action="store_true", help="When used with --self-check, try a short backend health request")
    parser.add_argument("--no-startup-summary", action="store_true", help="Do not emit startup summary to stderr before MCP transport starts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profile = None if args.modules else args.profile
    if args.list_profiles:
        print(json.dumps(list_profiles_payload(), ensure_ascii=False, indent=2))
        return
    if args.list_tools:
        print(json.dumps(list_tools_payload(profile=profile, modules=args.modules), ensure_ascii=False, indent=2))
        return
    if args.startup_summary:
        payload = startup_summary_payload(profile=profile, modules=args.modules, base_url=args.base_url, transport_name=args.transport)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        raise SystemExit(0 if payload.get("status") == "pass" else 2)
    if args.self_check:
        payload = self_check_payload(profile=profile, modules=args.modules, base_url=args.base_url, check_backend=args.check_backend)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        raise SystemExit(0 if payload.get("status") == "pass" else 2)
    run_gateway(
        profile=profile,
        modules=args.modules,
        base_url=args.base_url,
        transport_name=args.transport,
        emit_startup_summary=not args.no_startup_summary,
    )


if __name__ == "__main__":
    main()

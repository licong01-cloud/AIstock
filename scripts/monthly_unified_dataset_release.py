#!/usr/bin/env python
"""Operate the unified monthly dataset release through its single backend API."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request


TOKEN_HEADER = "X-Dataset-Release-Operator-Token"
OPERATION_ID_RE = re.compile(r"^dmr_[0-9a-f]{32}$")
AUTHORIZATION_REF_RE = re.compile(r"^dsauth_[0-9a-f]{32}$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-root",
        default=os.getenv("AISTOCK_BACKEND_API_ROOT", "http://127.0.0.1:8001/api/v1"),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("bind-repairs", "bind-quality"):
        bind = commands.add_parser(name)
        bind.add_argument("--operation-id", required=True)
        bind.add_argument("--inputs", type=Path, required=True)
    for name in ("plan", "run"):
        command = commands.add_parser(name)
        command.add_argument("--cutoff", type=date.fromisoformat, required=True)
        command.add_argument("--profile", choices=("qe_hmm_full_v2",), default="qe_hmm_full_v2")
        command.add_argument("--idempotency-key", required=True)
        command.add_argument("--repair-authorization-ref", action="append", default=[])
        if name == "run":
            command.add_argument("--activate", action="store_true")
            command.add_argument("--authorization-ref")
    for name in ("status", "receipts", "resume", "cancel"):
        command = commands.add_parser(name)
        command.add_argument("--operation-id", required=True)
    for name in ("activate", "rollback"):
        command = commands.add_parser(name)
        command.add_argument("--operation-id", required=True)
        command.add_argument("--authorization-ref", required=True)
    adopt = commands.add_parser("adopt")
    adopt.add_argument("--cutoff", type=date.fromisoformat, required=True)
    adopt.add_argument("--profile", choices=("qe_hmm_full_v2",), default="qe_hmm_full_v2")
    adopt.add_argument("--idempotency-key", required=True)
    adopt.add_argument("--candidate-root", required=True)
    adopt.add_argument("--profile-candidate", required=True)
    adopt.add_argument("--predecessor-profile-sha256", required=True)
    adopt.add_argument("--target-profile-sha256", required=True)
    adopt.add_argument("--dataset-manifest-sha256", required=True)
    adopt.add_argument("--dataset-manifest-file-sha256", required=True)
    adopt.add_argument("--node-manifest", action="append", required=True)
    adopt.add_argument("--evidence-ref", action="append", required=True)
    authorize = commands.add_parser("authorize")
    authorize.add_argument("--operation-id", required=True)
    authorize.add_argument("--action", choices=("ACTIVATE",), default="ACTIVATE")
    return parser


def _token() -> str:
    configured = str(os.getenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE") or "").strip()
    if not configured:
        raise RuntimeError("DATASET_RELEASE_OPERATOR_TOKEN_FILE is not configured")
    path = Path(configured)
    if not path.is_absolute() or not path.is_file():
        raise RuntimeError("dataset release operator token path is invalid")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        is_junction = getattr(current, "is_junction", None)
        if current.is_symlink() or bool(is_junction and is_junction()):
            raise RuntimeError("dataset release operator token path is linked")
    token = path.read_text(encoding="utf-8").strip()
    if len(token) < 32 or len(token) > 4096 or any(ord(character) < 33 or ord(character) == 127 for character in token):
        raise RuntimeError("dataset release operator token is invalid")
    return token


def _api_url(root: str, suffix: str) -> str:
    parsed = urllib.parse.urlsplit(root)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("api-root must be an absolute HTTP(S) URL without query or fragment")
    return root.rstrip("/") + "/qlib/monthly-releases" + suffix


def _call(
    *,
    root: str,
    method: str,
    suffix: str,
    body: Mapping[str, Any] | None,
    idempotency: str | None,
) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {TOKEN_HEADER: _token(), "Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if idempotency is not None:
        headers["Idempotency-Key"] = idempotency
    request = urllib.request.Request(
        _api_url(root, suffix),
        data=payload,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            value = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"monthly release API failed HTTP {exc.code}: {detail}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"monthly release API is unavailable: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError("monthly release API returned a non-object")
    return value


def _validate_args(args: argparse.Namespace) -> None:
    if hasattr(args, "operation_id") and OPERATION_ID_RE.fullmatch(args.operation_id) is None:
        raise ValueError("operation-id format is invalid")
    if hasattr(args, "authorization_ref") and args.authorization_ref is not None:
        if AUTHORIZATION_REF_RE.fullmatch(args.authorization_ref) is None:
            raise ValueError("authorization-ref format is invalid")
    if hasattr(args, "repair_authorization_ref"):
        refs = args.repair_authorization_ref
        if any(AUTHORIZATION_REF_RE.fullmatch(value) is None for value in refs):
            raise ValueError("repair-authorization-ref format is invalid")
        if len(refs) != len(set(refs)):
            raise ValueError("repair-authorization-ref values are duplicated")
    if args.command == "run":
        if args.activate != (args.authorization_ref is not None):
            raise ValueError("--activate and --authorization-ref must be supplied together")
    if args.command == "adopt":
        hashes = (
            args.predecessor_profile_sha256,
            args.target_profile_sha256,
            args.dataset_manifest_sha256,
            args.dataset_manifest_file_sha256,
        )
        if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in hashes):
            raise ValueError("adoption identity hash is invalid")
        _parse_node_manifests(args.node_manifest)
        _parse_evidence_refs(args.evidence_ref)


def _parse_node_manifests(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        node, separator, sha256 = value.partition("=")
        if (
            not separator
            or node not in {"controller", "wsl2-5080", "rdagent-node1"}
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            or node in parsed
        ):
            raise ValueError("node-manifest must uniquely bind NODE=SHA256")
        parsed[node] = sha256
    if set(parsed) != {"controller", "wsl2-5080", "rdagent-node1"}:
        raise ValueError("node-manifest must cover all release nodes")
    return parsed


def _parse_evidence_refs(values: Sequence[str]) -> list[dict[str, str]]:
    parsed: list[dict[str, str]] = []
    paths: set[str] = set()
    for value in values:
        parts = value.split("|", maxsplit=2)
        if (
            len(parts) != 3
            or not parts[0]
            or parts[0] in paths
            or re.fullmatch(r"[0-9a-f]{64}", parts[1]) is None
            or not parts[2]
        ):
            raise ValueError("evidence-ref must uniquely bind PATH|SHA256|SCHEMA")
        paths.add(parts[0])
        parsed.append(
            {"relative_path": parts[0], "sha256": parts[1], "schema_version": parts[2]}
        )
    if len(parsed) < 2:
        raise ValueError("at least two adoption evidence refs are required")
    return parsed


def _release_body(args: argparse.Namespace) -> dict[str, Any]:
    activate = bool(getattr(args, "activate", False))
    return {
        "schema_version": "aistock_monthly_release_request_v1",
        "target_cutoff": args.cutoff.isoformat(),
        "product_profile": args.profile,
        "activation_mode": "activate_when_ready" if activate else "prepare_only",
        "activation_authorization_ref": args.authorization_ref if activate else None,
        "repair_authorization_refs": args.repair_authorization_ref,
    }


def _adoption_body(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "schema_version": "aistock_monthly_existing_successor_adoption_v1",
        "target_cutoff": args.cutoff.isoformat(),
        "product_profile": args.profile,
        "candidate_root": args.candidate_root,
        "profile_candidate": args.profile_candidate,
        "predecessor_profile_sha256": args.predecessor_profile_sha256,
        "target_profile_sha256": args.target_profile_sha256,
        "dataset_manifest_sha256": args.dataset_manifest_sha256,
        "dataset_manifest_file_sha256": args.dataset_manifest_file_sha256,
        "node_manifest_file_sha256": _parse_node_manifests(args.node_manifest),
        "evidence_refs": _parse_evidence_refs(args.evidence_ref),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _validate_args(args)
    if args.command in {"bind-repairs", "bind-quality"}:
        if not args.inputs.is_absolute() or not args.inputs.is_file() or args.inputs.stat().st_size > 1024 * 1024:
            raise ValueError("repair input file must be a bounded absolute regular file")
        for path in (args.inputs, *args.inputs.parents):
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise ValueError("repair input file must not be linked")
        before = args.inputs.stat()
        raw = args.inputs.read_bytes()
        after = args.inputs.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("repair input file changed during read")
        inputs = json.loads(raw)
        if not isinstance(inputs, dict):
            raise ValueError("repair input file must contain an object")
        binding = "repair-inputs" if args.command == "bind-repairs" else "source-quality-inputs"
        result = _call(root=args.api_root, method="POST",
            suffix=f"/{urllib.parse.quote(args.operation_id, safe='')}/{binding}",
            body={"schema_version": "aistock_monthly_repair_inputs_request_v1", "inputs": inputs}, idempotency=None)
    elif args.command in {"plan", "run"}:
        result = _call(
            root=args.api_root,
            method="POST",
            suffix="/plan" if args.command == "plan" else "",
            body=_release_body(args),
            idempotency=args.idempotency_key,
        )
    elif args.command == "adopt":
        result = _call(
            root=args.api_root,
            method="POST",
            suffix="/adopt",
            body=_adoption_body(args),
            idempotency=args.idempotency_key,
        )
    elif args.command == "authorize":
        result = _call(
            root=args.api_root,
            method="POST",
            suffix=f"/{urllib.parse.quote(args.operation_id, safe='')}/authorizations",
            body={
                "schema_version": "aistock_monthly_authorization_issue_request_v1",
                "action": args.action,
            },
            idempotency=None,
        )
    elif args.command == "status":
        result = _call(
            root=args.api_root,
            method="GET",
            suffix=f"/{urllib.parse.quote(args.operation_id, safe='')}",
            body=None,
            idempotency=None,
        )
    elif args.command == "receipts":
        result = _call(
            root=args.api_root,
            method="GET",
            suffix=f"/{urllib.parse.quote(args.operation_id, safe='')}/receipts",
            body=None,
            idempotency=None,
        )
    elif args.command in {"resume", "cancel"}:
        result = _call(
            root=args.api_root,
            method="POST",
            suffix=f"/{urllib.parse.quote(args.operation_id, safe='')}/{args.command}",
            body={"schema_version": "dataset_release_command_request_v1"},
            idempotency=None,
        )
    else:
        result = _call(
            root=args.api_root,
            method="POST",
            suffix=f"/{urllib.parse.quote(args.operation_id, safe='')}/{args.command}",
            body={
                "schema_version": "aistock_monthly_release_action_v1",
                "authorization_ref": args.authorization_ref,
            },
            idempotency=None,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

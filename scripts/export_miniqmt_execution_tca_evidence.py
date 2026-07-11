"""Export canonical, pseudonymized MiniQMT TCA evidence from a read-only snapshot."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Callable, Sequence

from backend.services.qmt_strategy_ledger.tca_read_service import TcaReadError
from backend.services.simulation_runtime.tca_read_api import (
    ExecutionTcaReadService,
    TCA_EVIDENCE_EXPORT_VERSION,
    render_canonical_evidence_export,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export canonical pseudonymized MiniQMT execution TCA evidence.")
    parser.add_argument("--binding-id", required=True)
    parser.add_argument("--trade-date", required=True, help="ISO YYYY-MM-DD")
    parser.add_argument("--output", required=True, help="target JSON or NDJSON path")
    parser.add_argument("--format", default="json", help="json or ndjson")
    parser.add_argument("--evidence-version", default=TCA_EVIDENCE_EXPORT_VERSION)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    service_factory: Callable[[], ExecutionTcaReadService] = ExecutionTcaReadService,
) -> int:
    args = build_parser().parse_args(argv)
    output = Path(args.output)
    try:
        service = service_factory()
        export = service.export_execution_evidence(
            binding_id=args.binding_id,
            trade_date=args.trade_date,
            evidence_version=args.evidence_version,
        )
        rendered = render_canonical_evidence_export(export, output_format=args.format)
        _write_export(output, rendered, overwrite=bool(args.overwrite))
    except TcaReadError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 2
    except OSError as exc:
        payload = {
            "error_code": "ADAPTIVE_IS_TCA_EXPORT_WRITE_FAILED",
            "message": "unable to write evidence export",
            "context": {"output": str(output), "error_type": type(exc).__name__},
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 3
    print(
        json.dumps(
            {
                "ok": True,
                "output": str(output),
                "manifest_sha256": export.manifest["manifest_sha256"],
                "records_sha256": export.manifest["records_sha256"],
                "record_count": export.manifest["record_count"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def _write_export(output: Path, rendered: str, *, overwrite: bool) -> None:
    if output.exists() and not overwrite:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_EXPORT_OUTPUT_EXISTS",
            "refusing to overwrite an existing evidence export without --overwrite",
            http_status=409,
            stage="TCA_EXPORT",
            context={"output": str(output)},
        )
    if not output.parent.exists():
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_EXPORT_OUTPUT_DIRECTORY_MISSING",
            "evidence export output directory does not exist",
            http_status=400,
            stage="TCA_EXPORT",
            context={"output_parent": str(output.parent)},
        )
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    except Exception as write_error:
        try:
            temporary.unlink(missing_ok=True)
        except OSError as cleanup_error:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_EXPORT_TEMP_CLEANUP_FAILED",
                "evidence export failed and its temporary file could not be removed",
                http_status=500,
                stage="TCA_EXPORT",
                context={"temporary": str(temporary), "write_error_type": type(write_error).__name__},
            ) from cleanup_error
        raise


if __name__ == "__main__":
    raise SystemExit(main())

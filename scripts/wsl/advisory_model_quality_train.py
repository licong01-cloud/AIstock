from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one isolated M5A stage inside WSL rdagent-gpu.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage_a = subparsers.add_parser("stage-a")
    stage_a.add_argument("--request", required=True)
    prepare = subparsers.add_parser("prepare-test")
    prepare.add_argument("--train-request", required=True)
    prepare.add_argument("--projection-receipt", required=True)
    prepare.add_argument("--winner-receipt")
    prepare.add_argument("--test-request-output", required=True)
    stage_b = subparsers.add_parser("stage-b")
    stage_b.add_argument("--request", required=True)
    stage_b.add_argument("--train-request", required=True)
    publish = subparsers.add_parser("publish")
    publish.add_argument("--test-request", required=True)
    publish.add_argument("--train-request", required=True)
    publish.add_argument("--model-root", required=True)
    return parser.parse_args()


def main() -> int:
    repository_root = Path(__file__).resolve().parents[2]
    if str(repository_root) not in sys.path:
        sys.path.insert(0, str(repository_root))
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

    args = parse_args()
    try:
        result = _execute(args)
    except AdvisoryModelFirstError as exc:
        _write_failure_receipt(args, exc.as_dict())
        print(json.dumps(exc.as_dict(), ensure_ascii=True, sort_keys=True), file=sys.stderr, flush=True)
        return 2
    except Exception as exc:
        payload = {
            "status": "failed",
            "reason_code": "ADVISORY_M5_UNEXPECTED_ERROR",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__, "stage": args.command},
        }
        _write_failure_receipt(args, payload)
        print(
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
        raise
    print(json.dumps(result, ensure_ascii=True, sort_keys=True), flush=True)
    return 0


def _write_failure_receipt(args: argparse.Namespace, error: dict[str, object]) -> None:
    request_path_value = getattr(args, "request", None) or getattr(args, "train_request", None)
    if not request_path_value:
        return
    try:
        request_payload = json.loads(Path(request_path_value).read_text(encoding="utf-8"))
        output_root = Path(str(request_payload["output_root"]))
        identity = str(request_payload.get("request_id") or request_payload.get("evaluation_id") or "unresolved")
        target = output_root / "quality_failures" / f"{identity}_{args.command}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": "advisory_reranker_quality_failure_receipt_v1",
            "stage": args.command,
            "identity": identity,
            "failed_at": datetime.now(UTC).isoformat(),
            **error,
        }
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            temporary.write_text(
                json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": "ADVISORY_M5_FAILURE_RECEIPT_WRITE_FAILED",
                    "message": "M5A structured failure receipt could not be written",
                    "context": {"stage": args.command, "error_type": type(exc).__name__},
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _execute(args: argparse.Namespace) -> dict[str, object]:
    from backend.services.advisory_model_first.quality_bundle import publish_quality_model_bundle
    from backend.services.advisory_model_first.quality_contracts import (
        AdvisoryRerankerQualityTestRequestV1,
        AdvisoryRerankerQualityTrainRequestV1,
        QualityProjectionDescriptor,
        QualityWinnerReceiptV1,
    )
    from backend.services.advisory_model_first.quality_pipeline import (
        create_quality_test_request,
        run_quality_stage_a,
        run_quality_stage_b,
    )

    if args.command == "stage-a":
        winner = run_quality_stage_a(args.request)
        return {"status": winner.status, "winner_receipt_id": winner.receipt_id}
    if args.command == "prepare-test":
        train = AdvisoryRerankerQualityTrainRequestV1.model_validate_json(
            Path(args.train_request).read_text(encoding="utf-8")
        )
        projection_receipt = json.loads(Path(args.projection_receipt).read_text(encoding="utf-8"))
        test_projection = QualityProjectionDescriptor.model_validate(projection_receipt["test_projection"])
        winner_receipt = args.winner_receipt or str(
            Path(train.output_root) / "quality_runs" / train.request_id / "winner_receipt.json"
        )
        test_request = create_quality_test_request(
            train_request=train,
            winner_receipt_path=winner_receipt,
            test_projection=test_projection,
            output_root=train.output_root,
        )
        test_request.write_json(args.test_request_output)
        return {"status": "ready", "evaluation_id": test_request.evaluation_id}
    if args.command == "stage-b":
        result = run_quality_stage_b(args.request, train_request_path=args.train_request)
        return {
            "status": "SUCCEEDED",
            "evaluation_id": result["report"]["evaluation_id"],
            "idempotent": result["idempotent"],
        }
    test_request = AdvisoryRerankerQualityTestRequestV1.model_validate_json(
        Path(args.test_request).read_text(encoding="utf-8")
    )
    train = AdvisoryRerankerQualityTrainRequestV1.model_validate_json(
        Path(args.train_request).read_text(encoding="utf-8")
    )
    winner = QualityWinnerReceiptV1.model_validate_json(
        Path(test_request.winner_receipt_path).read_text(encoding="utf-8")
    )
    if winner.winner.model_weight == 0.0:
        return {"status": "NO_VALIDATION_MODEL_LIFT_OBSERVED", "bundle_published": False}
    test_report = (
        Path(test_request.output_root) / "quality_evaluations" / test_request.evaluation_id / "test_report.json"
    )
    bundle_id, bundle_path, _manifest = publish_quality_model_bundle(
        model_root=args.model_root,
        train_request=train,
        winner_receipt=winner,
        test_report_path=test_report,
    )
    return {
        "status": "SUCCEEDED",
        "bundle_published": True,
        "bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "shadow_binding_activated": False,
    }


if __name__ == "__main__":
    raise SystemExit(main())

"""One approved held-VIX reference, no model fit/inference or QE task."""

from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_holding_vix_reference as study  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import validate_output_location, write_once  # noqa: E402
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.run_risk_l2_loss_severity import guard  # noqa: E402
from scripts.hmm_risk.run_risk_l2_rolling import clean_head  # noqa: E402


@contextmanager
def zero_compute():
    def forbidden(*args, **kwargs):
        raise ValueError("held-VIX reference forbids model inference/target reconstruction")

    with guard(allow_ridge=False), ExitStack() as stack:
        for target in (
            "sklearn.linear_model.Ridge.predict",
            "sklearn.linear_model.LogisticRegression.predict_proba",
            "backend.services.hmm_risk.risk_l2_member_loss.FrozenMemberSource.targets",
        ):
            stack.enter_context(patch(target, forbidden))
        yield


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = validate_output_location(args.output)
    failure = output.with_name(output.name + ".failure.json")
    study.require(not output.exists() and not failure.exists(), "output/failure exists; no overwrite")
    request = study.member.pinned_json(args.request, args.request_sha256)
    features = study.member.pinned_json(request["features"], request["features_file_sha256"])
    validate_output_location(output, dataset_root=Path(features["source"]["candidate_root"]))
    # Also protect independent shared VIX and all frozen input parents.
    for root in [Path(request["vix_root"]), *(Path(request[k]).parent for k in (*study.PINS, "features"))]:
        study.require(output != root and root not in output.parents, "output overlaps frozen input")
    head = clean_head()
    try:
        with zero_compute():
            result = study.execute(request)
        study.require(clean_head() == head, "source changed during reference")
        result = receipt(result | dict(executor_commit=head, request_file_sha256=args.request_sha256))
        write_once(output, result)
        print(
            f"COMPLETED reference_only=true fits=0 paired={result['summary']['common_paired_episodes']} "
            f"sha256={result['receipt_sha256']}"
        )
        return 0
    except Exception as exc:
        write_once(
            failure,
            receipt(
                dict(
                    status="FAILED",
                    executor_commit=head,
                    contract=study.CONTRACT,
                    request_file_sha256=args.request_sha256,
                    error=str(exc),
                    fits=0,
                    new_inference_calls=0,
                    database_write=False,
                    runtime_action=False,
                )
            ),
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())

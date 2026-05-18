"""Generate a read-only MiniQMT strategy-ledger reconstruction report.

This script consumes a local JSON fixture or snapshot file. It never connects to
MiniQMT, never submits orders, and never writes to the database.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.qmt_strategy_ledger.reconstruct import reconstruct_ledger


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        default="backend/tests/qmt_strategy_ledger/fixtures/miniqmt_poc_20260518_summary.json",
        help="Path to a JSON file with metadata, orders, and trades.",
    )
    parser.add_argument("--out", default=None, help="Optional JSON report output path.")
    parser.add_argument("--markdown-out", default=None, help="Optional Markdown report output path.")
    args = parser.parse_args()

    fixture_path = Path(args.fixture)
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    metadata = payload.get("metadata") or {}
    snapshot = reconstruct_ledger(
        orders=payload.get("orders") or [],
        trades=payload.get("trades") or [],
        account_id=metadata.get("account_id"),
        trade_date=metadata.get("trade_date"),
    )
    report = snapshot.to_dict()
    report["source_fixture"] = str(fixture_path)
    report["metadata"] = metadata

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.markdown_out:
        md_path = Path(args.markdown_out)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(_to_markdown(report), encoding="utf-8")

    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    return 0


def _to_markdown(report: dict) -> str:
    lines = [
        "# MiniQMT Strategy Ledger Reconstruction Report",
        "",
        f"- account_id: `{report.get('account_id')}`",
        f"- trade_date: `{report.get('trade_date')}`",
        f"- source_fixture: `{report.get('source_fixture')}`",
        "",
        "## Summary",
        "",
    ]
    for key, value in (report.get("summary") or {}).items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Positions", "", "| strategy | symbol | quantity | avg_cost | cost_amount | lots |", "| --- | --- | ---: | ---: | ---: | ---: |"])
    for item in report.get("positions") or []:
        lines.append(
            "| {strategy_name} | {symbol} | {quantity} | {avg_cost:.6f} | {cost_amount:.2f} | {lot_count} |".format(**item)
        )
    lines.extend(["", "## Anomalies", "", "| type | severity | order_id | strategy | symbol | remark |", "| --- | --- | --- | --- | --- | --- |"])
    for item in report.get("anomalies") or []:
        lines.append(
            "| {anomaly_type} | {severity} | {order_id} | {strategy_name} | {symbol} | {order_remark} |".format(**item)
        )
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())

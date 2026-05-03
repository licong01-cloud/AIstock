from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.execution_algos.v25_core import (  # noqa: E402
    V25MarketAction,
    classify_v25_minute_market_state,
)


FIELDS = [
    "$open",
    "$close",
    "$volume",
    "$factor",
    "$prev_close",
    "$up_limit_price",
    "$down_limit_price",
    "$limit_up",
    "$limit_down",
]


def _split_codes(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip().upper() for part in value.replace(";", ",").split(",") if part.strip()]


def _flatten_qlib(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lstrip("$") for col in out.columns]
    if not isinstance(out.index, pd.MultiIndex) or out.index.nlevels != 2:
        raise RuntimeError(f"expected Qlib MultiIndex, got {type(out.index).__name__}")
    frame = out.index.to_frame(index=False)
    names = list(out.index.names)
    if "datetime" in names and "instrument" in names:
        dt_col = names.index("datetime")
        inst_col = names.index("instrument")
    else:
        parsed0 = pd.to_datetime(frame.iloc[:, 0], errors="coerce")
        parsed1 = pd.to_datetime(frame.iloc[:, 1], errors="coerce")
        dt_col, inst_col = (0, 1) if parsed0.notna().sum() >= parsed1.notna().sum() else (1, 0)
    out.index = pd.MultiIndex.from_frame(
        pd.DataFrame(
            {
                "datetime": pd.to_datetime(frame.iloc[:, dt_col]),
                "instrument": frame.iloc[:, inst_col].astype(str).str.upper(),
            }
        ),
        names=["datetime", "instrument"],
    )
    return out.sort_index()


def _read_instruments(provider_uri: Path, limit: int) -> list[str]:
    path = provider_uri / "instruments" / "all.txt"
    if not path.exists():
        raise FileNotFoundError(path)
    out: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        parts = raw.strip().split()
        if parts:
            out.append(parts[0].upper())
        if len(out) >= limit:
            break
    return out


def run_v25_limit_state_smoke(
    *,
    provider_uri: Path,
    day_provider_uri: str,
    start: str,
    end: str,
    codes: Sequence[str],
    num_stocks: int,
) -> dict[str, Any]:
    import qlib
    from qlib.config import C
    from qlib.data import D

    selected = list(codes) or _read_instruments(provider_uri, num_stocks)
    if not selected:
        raise RuntimeError("no instruments available for V25 limit-state smoke")

    C["kernels"] = 1
    qlib.init(
        provider_uri={"day": day_provider_uri, "1min": str(provider_uri)},
        region="cn",
        dataset_cache=None,
        expression_cache=None,
    )
    raw = D.features(
        selected,
        FIELDS,
        start_time=f"{start} 09:30:00",
        end_time=f"{end} 15:00:00",
        freq="1min",
    )
    df = _flatten_qlib(raw).dropna(how="all")
    if df.empty:
        raise RuntimeError("Qlib returned no minute rows")

    data_errors: list[dict[str, Any]] = []
    state_counts: dict[str, int] = {}
    flag_mismatches: list[dict[str, Any]] = []
    rows_checked = 0
    for (dt_value, instrument), row in df.iterrows():
        factor = row.get("factor")
        close_adj = row.get("close")
        open_adj = row.get("open")
        if not np.isfinite(factor) or float(factor) <= 0:
            data_errors.append({"instrument": instrument, "datetime": str(dt_value), "reason": "invalid_factor", "factor": _safe(row.get("factor"))})
            continue
        close_raw = float(close_adj) / float(factor) if np.isfinite(close_adj) else np.nan
        open_raw = float(open_adj) / float(factor) if np.isfinite(open_adj) else np.nan
        for side, price in (("BUY", close_raw), ("SELL", close_raw)):
            state = classify_v25_minute_market_state(
                side=side,
                price=price,
                volume=row.get("volume"),
                prev_close=row.get("prev_close"),
                limit_up=row.get("up_limit_price"),
                limit_down=row.get("down_limit_price"),
                price_basis="raw",
                limit_price_basis="raw",
            )
            state_counts[state.reason] = state_counts.get(state.reason, 0) + 1
            if state.action == V25MarketAction.DATA_ERROR:
                data_errors.append(
                    {
                        "instrument": instrument,
                        "datetime": str(dt_value),
                        "side": side,
                        "reason": state.reason,
                        "detail": state.detail,
                    }
                )
        up = row.get("up_limit_price")
        down = row.get("down_limit_price")
        if np.isfinite(close_raw) and np.isfinite(up):
            expected_up = float(close_raw) >= float(up) - 1e-4
            actual_up = bool(float(row.get("limit_up", 0.0)) >= 0.5)
            if expected_up != actual_up:
                flag_mismatches.append(
                    {
                        "instrument": instrument,
                        "datetime": str(dt_value),
                        "close_raw": close_raw,
                        "up_limit": _safe(up),
                        "actual_limit_up": actual_up,
                        "expected_limit_up": expected_up,
                    }
                )
        if np.isfinite(close_raw) and np.isfinite(down):
            expected_down = float(close_raw) <= float(down) + 1e-4
            actual_down = bool(float(row.get("limit_down", 0.0)) >= 0.5)
            if expected_down != actual_down:
                flag_mismatches.append(
                    {
                        "instrument": instrument,
                        "datetime": str(dt_value),
                        "close_raw": close_raw,
                        "down_limit": _safe(down),
                        "actual_limit_down": actual_down,
                        "expected_limit_down": expected_down,
                    }
                )
        if not np.isfinite(open_raw):
            data_errors.append({"instrument": instrument, "datetime": str(dt_value), "reason": "invalid_open_raw"})
        rows_checked += 1

    result = {
        "ok": not data_errors and not flag_mismatches,
        "provider_uri": str(provider_uri),
        "codes": selected,
        "start": start,
        "end": end,
        "rows_checked": rows_checked,
        "state_counts": dict(sorted(state_counts.items())),
        "data_error_count": len(data_errors),
        "data_errors": data_errors[:50],
        "flag_mismatch_count": len(flag_mismatches),
        "flag_mismatches": flag_mismatches[:50],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    return result


def _safe(value: Any) -> Any:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return number


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify V25 can read Qlib 1min limit/pre-close fields on raw basis.")
    parser.add_argument("--provider-uri", required=True)
    parser.add_argument("--day-provider-uri", default="/home/lc999/data/qlib_bin")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--codes", default=None)
    parser.add_argument("--num-stocks", type=int, default=5)
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    result = run_v25_limit_state_smoke(
        provider_uri=Path(args.provider_uri),
        day_provider_uri=args.day_provider_uri,
        start=args.start,
        end=args.end,
        codes=_split_codes(args.codes),
        num_stocks=args.num_stocks,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

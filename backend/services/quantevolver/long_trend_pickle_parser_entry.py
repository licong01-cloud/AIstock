"""Isolated parser for trusted artifacts of one archived QE Recorder.

The supervisor invokes this module in a subprocess with no database, callback,
resource-session, or artifact-store credentials.  Each artifact is normalized
independently so one malformed optional pickle cannot erase other evidence.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import pickle
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

PARSER_RECEIPT_SCHEMA = "qe_long_trend_pickle_parser_receipt_v1"
FORBIDDEN_ENV_PREFIXES = (
    "DATABASE_",
    "POSTGRES_",
    "PG",
    "QE_RESOURCE_",
    "AISTOCK_PREDICTION_STORE_",
)
ALLOWED_INPUTS = frozenset(
    {
        "prediction",
        "label",
        "positions",
        "portfolio_report",
        "indicator_summary",
        "indicator_object",
        "orders",
        "trades",
    }
)


class ParserContractError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    try:
        request_path = Path(args.request).resolve()
        output_dir = Path(args.output_dir).resolve()
        request = _read_json(request_path)
        receipt = parse_artifacts(request=request, output_dir=output_dir)
        _atomic_json(output_dir / "parser_receipt.json", receipt)
        return 0
    except Exception as exc:
        failure = {
            "schema_version": PARSER_RECEIPT_SCHEMA,
            "status": "failed",
            "reason_code": "QELT_PICKLE_PARSER_FAILED",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        try:
            _atomic_json(Path(args.output_dir).resolve() / "parser_receipt.json", failure)
        except Exception as write_exc:
            print(
                json.dumps(
                    {
                        "reason_code": "QELT_PICKLE_PARSER_FAILED",
                        "error": str(exc),
                        "receipt_write_error": f"{type(write_exc).__name__}: {write_exc}",
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
        return 2


def parse_artifacts(*, request: Mapping[str, Any], output_dir: Path) -> dict[str, Any]:
    _reject_secrets()
    if str(request.get("schema_version") or "") != "qe_long_trend_parser_request_v1":
        raise ParserContractError("unsupported parser request schema")
    evaluation_id = str(request.get("evaluation_id") or "")
    allowed_root = Path(str(request.get("allowed_root") or "")).resolve()
    if not evaluation_id.startswith("qelt_") or not allowed_root.is_dir():
        raise ParserContractError("parser request identity or allowed_root is invalid")
    inputs = request.get("inputs")
    if not isinstance(inputs, Mapping):
        raise ParserContractError("parser request inputs must be an object")
    unknown = sorted(set(inputs) - ALLOWED_INPUTS)
    if unknown:
        raise ParserContractError(f"unsupported parser inputs: {unknown}")
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}
    for name in sorted(ALLOWED_INPUTS):
        source_value = inputs.get(name)
        if source_value in (None, ""):
            results[name] = {"status": "missing", "reason_code": f"QELT_{name.upper()}_MISSING"}
            continue
        obj: Any = None
        normalized: Any = None
        try:
            source = Path(str(source_value)).resolve(strict=True)
            source.relative_to(allowed_root)
            if not source.is_file() or source.is_symlink():
                raise ParserContractError(f"input is not a regular file: {name}")
            obj = _load_pickle(source)
            normalized = _normalize_object(name, obj)
            output_path = output_dir / f"{name}.parquet"
            normalized.to_parquet(output_path, index=True)
            digest, size = _sha256_file(output_path)
            results[name] = {
                "status": "parsed",
                "relative_path": output_path.name,
                "sha256": digest,
                "size_bytes": size,
                "row_count": int(len(normalized)),
                "columns": [str(value) for value in normalized.columns],
                "source_relative_path": source.relative_to(allowed_root).as_posix(),
                "source_sha256": _sha256_file(source)[0],
                "source_size_bytes": int(source.stat().st_size),
            }
        except Exception as exc:
            results[name] = {
                "status": "failed",
                "reason_code": "QELT_PICKLE_PARSER_FAILED",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        finally:
            # Recorder pickle cannot be streamed safely, so the isolated
            # parser processes exactly one allowlisted artifact at a time,
            # records source bytes/output rows in its receipt, and releases
            # the object before the next family is loaded.
            obj = None
            normalized = None
            gc.collect()
    return {
        "schema_version": PARSER_RECEIPT_SCHEMA,
        "status": "completed_with_artifact_statuses",
        "evaluation_id": evaluation_id,
        "artifacts": results,
    }


def _reject_secrets() -> None:
    leaked = sorted(
        key
        for key, value in os.environ.items()
        if value and any(key.upper().startswith(prefix) for prefix in FORBIDDEN_ENV_PREFIXES)
    )
    if leaked:
        raise ParserContractError(f"parser environment contains forbidden credentials: {leaked}")


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as handle:
        return pickle.Unpickler(handle).load()  # noqa: S301 - exact trusted QE Recorder boundary.


def _normalize_object(name: str, obj: Any):
    import pandas as pd

    if name == "positions":
        return _normalize_positions(obj)
    if name == "indicator_object":
        return _normalize_indicator_object(obj)
    if isinstance(obj, pd.Series):
        frame = obj.to_frame(name=obj.name or ("score" if name == "prediction" else "value"))
    elif isinstance(obj, pd.DataFrame):
        frame = obj.copy(deep=False)
    elif name in {"orders", "trades"} and isinstance(obj, (list, tuple)):
        frame = pd.DataFrame.from_records(obj)
    else:
        raise ParserContractError(f"unsupported {name} pickle type: {type(obj).__name__}")
    if name == "prediction" and "score" not in frame.columns:
        if len(frame.columns) != 1:
            raise ParserContractError("prediction requires a score column or exactly one value column")
        frame = frame.rename(columns={frame.columns[0]: "score"})
    return frame.sort_index()


def _normalize_positions(obj: Any):
    import pandas as pd

    if isinstance(obj, pd.DataFrame):
        return obj.sort_index()
    if not isinstance(obj, Mapping):
        raise ParserContractError("positions artifact must be a DataFrame or date-to-Position mapping")
    rows: list[dict[str, Any]] = []
    for date_value, position in sorted(obj.items(), key=lambda item: str(item[0])):
        if isinstance(position, Mapping):
            stocks = [key for key in position if str(key).lower() not in {"cash", "now_account_value"}]
            amounts = position
        elif hasattr(position, "get_stock_list") and hasattr(position, "get_stock_amount"):
            stocks = list(position.get_stock_list())
            amounts = None
        else:
            raise ParserContractError(f"unsupported Position value type: {type(position).__name__}")
        for stock in stocks:
            rows.append(
                {
                    "datetime": pd.Timestamp(date_value).normalize(),
                    "instrument": str(stock),
                    "amount": float(amounts[stock] if amounts is not None else position.get_stock_amount(stock)),
                }
            )
    frame = pd.DataFrame.from_records(rows, columns=["datetime", "instrument", "amount"])
    return frame.set_index(["datetime", "instrument"]).sort_index() if not frame.empty else frame


def _normalize_indicator_object(obj: Any):
    import pandas as pd

    if isinstance(obj, pd.DataFrame):
        required = {"amount", "deal_amount", "ffr"}
        if not required.issubset(obj.columns):
            raise ParserContractError("indicator object DataFrame lacks amount/deal_amount/ffr")
        return obj.sort_index()
    history = getattr(obj, "order_indicator_his", None)
    if not isinstance(history, Mapping):
        raise ParserContractError("indicator object lacks authoritative order_indicator_his")
    rows: list[dict[str, Any]] = []
    for date_value, indicator in sorted(history.items(), key=lambda item: str(item[0])):
        data = getattr(indicator, "data", None)
        if not isinstance(data, Mapping):
            raise ParserContractError("order_indicator_his entry lacks data mapping")
        columns: dict[str, dict[Any, Any]] = {}
        for key in ("amount", "deal_amount", "ffr", "trade_dir", "base_price", "trade_value"):
            value = data.get(key)
            if value is not None:
                if not hasattr(value, "to_dict"):
                    raise ParserContractError(f"indicator field {key} has no to_dict contract")
                columns[key] = dict(value.to_dict())
        if not {"amount", "deal_amount", "ffr"}.issubset(columns):
            raise ParserContractError("indicator entry lacks amount/deal_amount/ffr")
        instruments = sorted(set().union(*(mapping.keys() for mapping in columns.values())), key=str)
        for instrument in instruments:
            trade_dir = columns.get("trade_dir", {}).get(instrument)
            rows.append(
                {
                    "datetime": pd.Timestamp(date_value).normalize(),
                    "instrument": str(instrument),
                    "side": "buy" if trade_dir is None or float(trade_dir) > 0 else "sell",
                    **{key: mapping.get(instrument) for key, mapping in columns.items()},
                }
            )
    frame = pd.DataFrame.from_records(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=["datetime", "instrument", "side", "amount", "deal_amount", "ffr"]
        )
    return frame.set_index(["datetime", "instrument"]).sort_index()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ParserContractError("parser request must be a JSON object")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    raise SystemExit(main())

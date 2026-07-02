"""xtquant adapter for AIstock data service.

This module should wrap xtquant APIs for:
- 历史 K 线
- 实时 snapshot
- 行情订阅 (push)

All functions are thin wrappers returning pandas.DataFrame or simple
Python dataclasses that are further normalized by the public API layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterator, List, Optional

import queue
import logging
import threading
import time

import pandas as pd


logger = logging.getLogger(__name__)


_xtdata_run_lock = threading.Lock()
_xtdata_run_started = False


def _ensure_xtdata_run_started(xtdata) -> None:  # type: ignore[no-untyped-def]
    global _xtdata_run_started
    if _xtdata_run_started:
        return
    with _xtdata_run_lock:
        if _xtdata_run_started:
            return

        def _run() -> None:
            try:
                xtdata.run()
            except Exception:
                return

        t = threading.Thread(target=_run, name="xtdata-run", daemon=True)
        t.start()
        _xtdata_run_started = True


@dataclass
class XtQuoteBatch:
    """Internal quote batch structure for xtquant streaming quotes."""

    timestamp: datetime
    data: pd.DataFrame  # index: instrument, columns: normalized quote fields


def fetch_history_window_xt(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
    adj: str = "front",
) -> pd.DataFrame:
    """Fetch historical window from xtquant.

    Implementation notes:
    - Uses ``xtquant.xtdata.get_market_data`` for level1 K-line data;
    - Supports typical periods such as ``"1d"`` and ``"1m"``;
    - Returns a ``MultiIndex(datetime, instrument)`` DataFrame with
      columns like ``open/high/low/close/volume/amount``;
    - If *fields* is provided, columns are filtered accordingly;
    - If *bars* is provided, the result is trimmed per instrument to the
      latest N bars.
    - *adj* supports: 'front' (前复权), 'back' (后复权), 'none' (不复权).
    """

    try:
        from xtquant import xtdata  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover - environment specific
        raise RuntimeError("xtquant is not available in current environment") from exc

    if not universe:
        return pd.DataFrame()

    # Map AIstock adj to xtquant dividend_type
    div_map = {
        "none": "none",
        "front": "front",
        "back": "back",
    }
    dividend_type = div_map.get(adj, "front")

    # Normalize requested fields; fall back to a standard OHLCV set.
    default_fields = ["open", "high", "low", "close", "volume", "amount"]
    field_list: List[str]
    if fields:
        # 保留与默认字段的交集，避免请求不存在字段导致报错
        field_list = [f for f in fields if f in default_fields] or default_fields
    else:
        field_list = default_fields

    # Map datetime parameters to xtdata's (start_time, end_time, count).
    if end is not None:
        end_str = end.strftime("%Y%m%d%H%M%S")
    else:
        end_str = ""

    if bars is not None and bars > 0:
        # 仅使用 count 控制条数，让 xtdata 以 end_time 为基准向前取
        count = bars
        # start_time 为空表示从最早到 end_time，配合 count 限制条数
        start_str = ""
    else:
        count = -1
        start_str = start.strftime("%Y%m%d%H%M%S") if start is not None else ""

    def _load_market_data() -> dict:
        return xtdata.get_market_data(
            field_list=field_list,
            stock_list=universe,
            period=freq,
            start_time=start_str,
            end_time=end_str,
            count=count,
            dividend_type=dividend_type,
            fill_data=True,
        )

    data = _load_market_data()

    # 若首次获取为空或全部字段为空，尝试先下载历史数据再重试一次
    def _is_all_empty(d: dict) -> bool:
        if not d:
            return True
        for v in d.values():
            try:
                if v is not None and not getattr(v, "empty", False):
                    return False
            except Exception:
                continue
        return True

    if _is_all_empty(data):
        try:
            # best-effort 补齐本地历史数据缓存
            for code in universe:
                xtdata.download_history_data(code, freq)
            data = _load_market_data()
        except Exception:
            # 补数据失败时保持 data 为首次结果，由后续逻辑返回空框架
            pass

    if _is_all_empty(data):
        return pd.DataFrame()

    # For K-line periods, xtdata returns {field: DataFrame(index=stock, columns=time)}.
    frames: list[pd.Series] = []
    for field_name, df_field in data.items():
        if df_field is None or df_field.empty:
            continue
        
        # 将 (instrument, time) 转置并堆叠
        # xtdata 返回的是 index=stocks, columns=times
        s = df_field.stack().rename(field_name)
        s.index = s.index.set_names(["instrument", "datetime"])
        frames.append(s)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1)
    
    # 转换为 Qlib 要求的 MultiIndex(datetime, instrument) 顺序并排序
    combined = combined.reorder_levels(["datetime", "instrument"]).sort_index()
    
    # 强制将索引 datetime 转换为 pd.Timestamp 格式，防止 Qlib 报错
    combined.index = combined.index.set_levels(
        pd.to_datetime(combined.index.levels[0]), level="datetime"
    )

    # Optional trim per instrument if bars is specified but we could not
    # rely solely on xtdata's count semantics (e.g. multiple symbols).
    if bars is not None and bars > 0:
        combined = (
            combined.groupby(level="instrument", group_keys=True)
            .tail(bars)
            .sort_index()
        )

    return combined


def fetch_realtime_snapshot_xt(
    universe: List[str],
    *,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
) -> pd.DataFrame:
    """Fetch realtime snapshot from xtquant for given universe.

    Implementation notes:
    - Uses ``xtdata.get_full_tick`` to obtain latest tick snapshot for
      each instrument in *universe*;
    - Maps xtquant tick fields into a compact snapshot schema with
      columns such as ``close/open/high/low/volume/amount/last_close``;
    - Returns a DataFrame indexed by instrument.
    """

    try:
        from xtquant import xtdata  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover - environment specific
        raise RuntimeError("xtquant is not available in current environment") from exc

    if not universe:
        return pd.DataFrame()

    raw = xtdata.get_full_tick(universe)
    if not raw:
        return pd.DataFrame()

    records: list[dict] = []
    for code, q in raw.items():
        if not isinstance(q, dict):
            continue
        ts = q.get("time")
        # xtquant 文档中 time 为毫秒时间戳
        dt: Optional[datetime]
        try:
            if ts is not None:
                dt = datetime.fromtimestamp(int(ts) / 1000)
            else:
                dt = None
        except Exception:  # pragma: no cover - 容错
            dt = None

        records.append(
            {
                "instrument": str(code),
                "datetime": dt,
                # 价格相关字段
                "close": q.get("lastPrice"),
                "open": q.get("open"),
                "high": q.get("high"),
                "low": q.get("low"),
                "last_close": q.get("lastClose"),
                # 量额
                "volume": q.get("volume"),
                "amount": q.get("amount"),
                # 其他可选字段
                "stock_status": q.get("stockStatus"),
            }
        )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records).set_index("instrument")

    if fields is not None and len(fields) > 0:
        # 始终保留 datetime 列，方便外层使用
        cols = [c for c in ["datetime", *fields] if c in df.columns]
        if cols:
            df = df[cols]

    return df


def subscribe_quotes_xt(
    universe: List[str],
    *,
    fields: Optional[List[str]] = None,
    freq: str = "tick",
    on_batch: Optional[Callable[[XtQuoteBatch], None]] = None,
) -> Callable[[], None]:
    """Subscribe quotes via xtquant and forward them as XtQuoteBatch.

    当前实现采用对 ``get_full_tick`` 的定期轮询，将结果封装为
    ``XtQuoteBatch`` 传递给回调，避免依赖复杂的推送接口，同时不对
    数据做任何近似或合成。

    返回一个可调用对象，用于停止轮询并清理后台线程。
    """

    if on_batch is None:
        raise ValueError("on_batch callback must be provided for subscribe_quotes_xt")

    try:
        from xtquant import xtdata  # type: ignore[import-not-found]
    except Exception:
        # Fall back to polling if xtquant is not available.
        xtdata = None

    if xtdata is not None:
        try:
            _ensure_xtdata_run_started(xtdata)

            want_fields = fields or ["close", "open", "high", "low", "volume", "amount"]

            def _normalize(datas: dict) -> Optional[pd.DataFrame]:
                if not datas:
                    return None
                records: list[dict] = []
                for code, q in datas.items():
                    if not isinstance(q, dict):
                        continue
                    records.append(
                        {
                            "instrument": str(code),
                            "close": q.get("lastPrice"),
                            "open": q.get("open"),
                            "high": q.get("high"),
                            "low": q.get("low"),
                            "volume": q.get("volume"),
                            "amount": q.get("amount"),
                        }
                    )

                if not records:
                    return None

                df = pd.DataFrame.from_records(records).set_index("instrument")
                cols = [c for c in want_fields if c in df.columns]
                if cols:
                    df = df[cols]
                return df

            def _on_quote(datas: dict) -> None:  # type: ignore[no-untyped-def]
                try:
                    df = _normalize(datas)
                    if df is None or df.empty:
                        return
                    on_batch(XtQuoteBatch(timestamp=datetime.now(), data=df))
                except Exception:
                    logger.warning("xtquant push quote callback failed", exc_info=True)
                    return

            seq = xtdata.subscribe_whole_quote(code_list=universe, callback=_on_quote)
            if not isinstance(seq, int) or seq <= 0:
                raise RuntimeError(f"xtdata.subscribe_whole_quote failed: {seq}")

            def _stop_push() -> None:
                try:
                    xtdata.unsubscribe_quote(seq)
                except Exception:
                    logger.warning("xtquant unsubscribe_quote failed", exc_info=True)

            return _stop_push
        except Exception:
            logger.warning("xtquant push subscription unavailable; falling back to polling", exc_info=True)

    stop_event = threading.Event()

    def _worker_poll() -> None:
        while not stop_event.is_set():
            try:
                df = fetch_realtime_snapshot_xt(universe, fields=fields, freq=freq)
                if df is not None and not df.empty:
                    batch = XtQuoteBatch(timestamp=datetime.now(), data=df)
                    on_batch(batch)
            except Exception:
                logger.warning("xtquant polling quote fetch failed", exc_info=True)
            time.sleep(1.0)

    thread = threading.Thread(target=_worker_poll, name="xtquant_quote_subscriber", daemon=True)
    thread.start()

    def _stop_poll() -> None:
        stop_event.set()
        thread.join(timeout=5)

    return _stop_poll


def stream_quotes_xt(
    universe: List[str],
    *,
    fields: Optional[List[str]] = None,
    freq: str = "tick",
) -> Iterator[XtQuoteBatch]:
    """Convenience generator built on top of subscribe_quotes_xt.

    使用一个线程安全队列缓存 ``XtQuoteBatch``，在首次迭代时启动
    订阅线程，在生成器结束时自动取消订阅。
    """

    q: "queue.Queue[XtQuoteBatch]" = queue.Queue()

    def _on_batch(batch: XtQuoteBatch) -> None:
        try:
            q.put_nowait(batch)
        except Exception:
            # 若队列已满/关闭，静默丢弃，由上层控制消费速度。
            pass

    stop = subscribe_quotes_xt(universe, fields=fields, freq=freq, on_batch=_on_batch)

    try:
        while True:
            try:
                batch = q.get(timeout=5.0)
            except queue.Empty:
                # 若长时间无数据，继续等待，直到上层主动终止迭代。
                continue
            yield batch
    finally:
        stop()

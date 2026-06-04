from __future__ import annotations
import logging

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ..repositories.watchlist_repo_impl import watchlist_repo
from ..core.data_source_manager_impl import data_source_manager
from ..data_service import xtquant_adapter

logger = logging.getLogger("aistock.watchlist")


REALTIME_FIELDS = {
    "last": "最新价",
    "pct_change": "涨幅%",
    "open": "开盘",
    "prev_close": "昨收",
    "high": "最高",
    "low": "最低",
    "volume_hand": "成交量(手)",
    "amount": "成交额",
}


def _fetch_quotes(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """并行获取实时行情数据，使用线程池提高性能。"""
    from concurrent.futures import ThreadPoolExecutor

    def _fetch_single(code: str) -> tuple[str, Dict[str, Any]]:
        base = code
        if "." in str(code):
            try:
                base = data_source_manager._convert_from_ts_code(code)  # type: ignore[attr-defined]
            except Exception:
                base = code
        try:
            q = data_source_manager.get_realtime_quotes(base)
        except Exception:
            q = {}
        return code, q or {}

    out: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(_fetch_single, codes))
        for code, quote in results:
            out[code] = quote
    return out


def _get_entry_price_strict(ts_code: str) -> float:
    """获取加入价格：TDX 优先，失败回退 miniQMT(xtquant)。

    返回值必须 > 0，否则抛错。
    """

    ts_code = (ts_code or "").strip().upper()
    if not ts_code:
        raise ValueError("code 不能为空")

    base = ts_code
    if "." in ts_code:
        try:
            base = data_source_manager._convert_from_ts_code(ts_code)  # type: ignore[attr-defined]
        except Exception:
            base = ts_code.split(".", 1)[0]

    # 1) TDX 优先（通过 data_source_manager.get_realtime_quotes）
    try:
        q = data_source_manager.get_realtime_quotes(base)
    except Exception:
        q = {}
    if isinstance(q, dict):
        p = q.get("price")
        if isinstance(p, (int, float)) and float(p) > 0:
            return float(p)

    # 2) xtquant/miniQMT 兜底
    try:
        snap = xtquant_adapter.fetch_realtime_snapshot_xt([ts_code], fields=["close"], freq="1d")
    except Exception as exc:
        raise ValueError(f"行情获取失败: {ts_code}") from exc

    if snap is None or snap.empty:
        raise ValueError(f"行情为空: {ts_code}")
    try:
        row = snap.loc[ts_code]
    except Exception as exc:
        raise ValueError(f"行情缺失: {ts_code}") from exc

    p2 = None
    try:
        p2 = row.get("close") if hasattr(row, "get") else None
    except Exception:
        p2 = None
    if isinstance(p2, (int, float)) and float(p2) > 0:
        return float(p2)
    raise ValueError(f"行情价格无效: {ts_code}")


def _optional_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    value_float = float(value)
    return value_float if value_float > 0 else None


def _parse_entry_date(value: Any) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None
    return None


def _normalize_adjustment_code(code: Any) -> Optional[str]:
    text = str(code or "").strip().upper()
    if not text:
        return None
    if "." in text:
        return text
    converted = data_source_manager._convert_to_ts_code(text)  # type: ignore[attr-defined]
    return str(converted).strip().upper() if converted else None


def _qfq_adjust_entry_price(
    entry_price: Optional[float],
    *,
    entry_adj_factor: Any,
    latest_adj_factor: Any,
) -> Optional[float]:
    price = _optional_float(entry_price)
    entry_factor = _optional_float(entry_adj_factor)
    latest_factor = _optional_float(latest_adj_factor)
    if price is None or entry_factor is None or latest_factor is None:
        return None
    return price * entry_factor / latest_factor


def _fetch_qfq_entry_adjustments(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Fetch per-item qfq entry-price adjustment factors from local DB.

    The calculation keeps current realtime prices on the latest raw basis and
    converts the stored entry price to the same qfq basis:
    entry_price_qfq = entry_price_raw * entry_adj_factor / latest_adj_factor.
    """

    normalized: Dict[str, tuple[str, date, float]] = {}
    for item in items:
        original_code = str(item.get("code") or "")
        code = _normalize_adjustment_code(original_code)
        entry_price = _optional_float(item.get("entry_price"))
        entry_date = _parse_entry_date(item.get("entry_as_of")) or _parse_entry_date(item.get("created_at"))
        if code and entry_price is not None and entry_date:
            normalized[code] = (original_code, entry_date, entry_price)
    if not normalized:
        return {}

    codes = list(normalized.keys())
    dates = [normalized[code][1] for code in codes]
    sql = """
        WITH input AS (
            SELECT *
            FROM unnest(%s::text[], %s::date[]) AS t(ts_code, entry_date)
        )
        SELECT
            input.ts_code,
            input.entry_date,
            entry_factor.adj_factor AS entry_adj_factor,
            entry_factor.trade_date AS entry_adj_factor_date,
            latest_factor.adj_factor AS latest_adj_factor,
            latest_factor.trade_date AS latest_adj_factor_date
        FROM input
        LEFT JOIN LATERAL (
            SELECT adj_factor, trade_date
            FROM market.adj_factor
            WHERE ts_code = input.ts_code AND trade_date <= input.entry_date
            ORDER BY trade_date DESC
            LIMIT 1
        ) entry_factor ON TRUE
        LEFT JOIN LATERAL (
            SELECT adj_factor, trade_date
            FROM market.adj_factor
            WHERE ts_code = input.ts_code
            ORDER BY trade_date DESC
            LIMIT 1
        ) latest_factor ON TRUE
    """
    try:
        from ..db.pg_pool import get_conn

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (codes, dates))
                rows = cur.fetchall()
    except Exception as exc:
        logger.warning("watchlist qfq entry adjustment query failed: %s", exc)
        return {
            original_code: {
                "entry_price_basis": "raw_fallback_adjustment_query_failed",
                "entry_price_adjusted": None,
                "entry_adjustment_factor": None,
                "entry_adj_factor_date": None,
                "latest_adj_factor_date": None,
            }
            for original_code, _, _ in normalized.values()
        }

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ts_code, _, entry_adj_factor, entry_factor_date, latest_adj_factor, latest_factor_date = row
        original_code, _, entry_price = normalized.get(str(ts_code), (str(ts_code), date.today(), 0.0))
        adjusted = _qfq_adjust_entry_price(
            entry_price,
            entry_adj_factor=entry_adj_factor,
            latest_adj_factor=latest_adj_factor,
        )
        if adjusted is None:
            out[original_code] = {
                "entry_price_basis": "raw_fallback_missing_adj_factor",
                "entry_price_adjusted": None,
                "entry_adjustment_factor": None,
                "entry_adj_factor_date": entry_factor_date.isoformat() if entry_factor_date else None,
                "latest_adj_factor_date": latest_factor_date.isoformat() if latest_factor_date else None,
            }
            continue
        adjustment_factor = float(entry_adj_factor) / float(latest_adj_factor)
        out[original_code] = {
            "entry_price_basis": "qfq_adjusted",
            "entry_price_adjusted": adjusted,
            "entry_adjustment_factor": adjustment_factor,
            "entry_adj_factor_date": entry_factor_date.isoformat() if entry_factor_date else None,
            "latest_adj_factor_date": latest_factor_date.isoformat() if latest_factor_date else None,
        }
    for original_code, _, _ in normalized.values():
        out.setdefault(
            original_code,
            {
                "entry_price_basis": "raw_fallback_missing_adj_factor",
                "entry_price_adjusted": None,
                "entry_adjustment_factor": None,
                "entry_adj_factor_date": None,
                "latest_adj_factor_date": None,
            },
        )
    return out


def _compute_realtime_fields(
    q: Dict[str, Any],
    entry_price: Optional[float] = None,
    entry_price_for_return: Optional[float] = None,
) -> Dict[str, Optional[float]]:
    price = q.get("price")
    pre_close = q.get("pre_close")
    open_ = q.get("open")
    high = q.get("high")
    low = q.get("low")
    volume = q.get("volume")
    amount = q.get("amount")

    # 停牌/盘前: price=0 但 pre_close 有效 → 用 pre_close 作为有效价格
    effective_price = None
    if isinstance(price, (int, float)) and float(price) > 0:
        effective_price = float(price)
    elif isinstance(pre_close, (int, float)) and float(pre_close) > 0:
        effective_price = float(pre_close)

    pct = None
    if effective_price is not None and isinstance(pre_close, (int, float)) and float(pre_close) > 0:
        try:
            pct = (effective_price - pre_close) / pre_close * 100.0
        except Exception:
            pct = None

    # 计算加入以来涨幅
    pct_since_entry = None
    basis_entry_price = _optional_float(entry_price_for_return) or _optional_float(entry_price)
    if effective_price is not None and basis_entry_price is not None:
        try:
            pct_since_entry = (effective_price - basis_entry_price) / basis_entry_price * 100.0
        except Exception:
            pct_since_entry = None

    volume_hand = volume / 100.0 if isinstance(volume, (int, float)) else None

    return {
        "last": effective_price,
        "pct_change": pct,
        "pct_since_entry": pct_since_entry,
        "open": open_,
        "prev_close": pre_close,
        "high": high,
        "low": low,
        "volume_hand": volume_hand,
        "amount": amount,
    }


def _normalize_code_for_storage(code: str) -> Optional[str]:
    """将外部输入的股票代码规范为内部存储格式（优先 ts_code）。

    行为与根目录 watchlist_ui._normalize_code_for_storage 保持一致：
    - 已含 '.' 的视为 ts_code，直接返回大写形式；
    - 否则尝试通过 data_source_manager._convert_to_ts_code 转为 ts_code；
    - 失败返回 None。
    """

    code = (code or "").strip().upper()
    if not code:
        return None
    if "." in code:
        return code
    try:
        return data_source_manager._convert_to_ts_code(code)  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return None


def _get_stock_name(code: str) -> Optional[str]:
    """根据代码获取股票名称，优先使用 TDX 基本信息。

    code 可以是 ts_code 或 6 位代码。
    """

    base = code.strip()
    if not base:
        return None
    if "." in base:
        try:
            base = data_source_manager._convert_from_ts_code(base)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            base = base.split(".", 1)[0]
    try:
        info = data_source_manager.get_stock_basic_info(base)
    except Exception:  # noqa: BLE001
        info = {}
    if isinstance(info, dict):
        name = info.get("name") or info.get("stock_name")
        if name and name not in {"-", "未知", "None"}:
            return str(name)
    return None


# -------------------- 分类相关 --------------------


def list_categories() -> List[Dict[str, Any]]:
    return watchlist_repo.list_categories()


def create_category(name: str, description: Optional[str] = None) -> int:
    return watchlist_repo.create_category(name, description)


def rename_category(category_id: int, new_name: str, new_desc: Optional[str] = None) -> bool:
    return watchlist_repo.rename_category(category_id, new_name, new_desc)


def delete_category(category_id: int) -> bool:
    return watchlist_repo.delete_category(category_id)


# -------------------- 自选条目与实时行情 --------------------


def list_items_with_quotes(
    category_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "updated_at",
    sort_dir: str = "desc",
) -> Dict[str, Any]:
    base = watchlist_repo.list_items(
        category_id=category_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    items: List[Dict[str, Any]] = base.get("items", [])
    codes = [str(it.get("code")) for it in items if it.get("code")]
    quotes_raw = _fetch_quotes(codes)
    entry_adjustments = _fetch_qfq_entry_adjustments(items)

    enriched: List[Dict[str, Any]] = []
    for it in items:
        code = str(it.get("code"))
        entry_price = it.get("entry_price")
        entry_adjustment = entry_adjustments.get(code) or {}
        entry_price_for_return = entry_adjustment.get("entry_price_adjusted")
        q = quotes_raw.get(code, {})
        rt = _compute_realtime_fields(q, entry_price=entry_price, entry_price_for_return=entry_price_for_return)
        row = dict(it)
        for k, v in rt.items():
            row[k] = v
        row["entry_price_basis"] = entry_adjustment.get("entry_price_basis") or (
            "raw_fallback_missing_adj_factor" if entry_price else "missing_entry_price"
        )
        row["entry_price_adjusted"] = entry_adjustment.get("entry_price_adjusted")
        row["entry_adjustment_factor"] = entry_adjustment.get("entry_adjustment_factor")
        row["entry_adj_factor_date"] = entry_adjustment.get("entry_adj_factor_date")
        row["latest_adj_factor_date"] = entry_adjustment.get("latest_adj_factor_date")
        enriched.append(row)

    return {"total": base.get("total", len(enriched)), "items": enriched}


def add_items_bulk(codes: List[str], category_id: int, on_conflict: str = "ignore") -> Dict[str, int]:
    names_map: Dict[str, str] = {}
    for c in codes:
        base = c
        if "." in str(c):
            try:
                base = data_source_manager._convert_from_ts_code(c)  # type: ignore[attr-defined]
            except Exception:
                base = c
        try:
            info = data_source_manager.get_stock_basic_info(base)
        except Exception:
            info = {}
        name = None
        if isinstance(info, dict):
            name = info.get("name") or info.get("stock_name")
        names_map[c] = name or c
    return watchlist_repo.add_items_bulk(codes, category_id, on_conflict=on_conflict, names=names_map)


def delete_items(ids: List[int]) -> int:
    return watchlist_repo.delete_items(ids)


def add_single_item(
    code: str,
    category_id: int,
    name: Optional[str] = None,
    note: Optional[str] = None,
    extra_category_ids: Optional[List[int]] = None,
    entry_price: Optional[float] = None,
    entry_rank: Optional[int] = None,
    entry_source: Optional[str] = None,
    entry_task_id: Optional[str] = None,
    entry_loop_id: Optional[int] = None,
    entry_as_of: Optional[str] = None,
) -> int:
    """单只添加到自选股票池，带分类管理。

    - 代码会按旧版 UI 逻辑标准化为 ts_code；
    - 名称为空时，会通过 data_source_manager 查询；
    - 支持备注 (note)；
    - 支持额外分类 ID 列表，用于多分类映射。
    - 支持 entry_price (加入价格) 记录。
    返回创建/更新后的 item_id。
    """

    ts_code = _normalize_code_for_storage(code)
    if not ts_code:
        raise ValueError("无法识别的股票代码")

    display_name = name or _get_stock_name(ts_code) or ts_code

    # 如果没传 entry_price，严格获取最新价（TDX 优先 -> miniQMT 兜底），并要求 >0
    if entry_price is None:
        entry_price = _get_entry_price_strict(ts_code)
    if not isinstance(entry_price, (int, float)) or float(entry_price) <= 0:
        raise ValueError("加入价格无效，必须 > 0")

    entry_as_of_date = None
    if entry_as_of:
        try:
            entry_as_of_date = date.fromisoformat(str(entry_as_of))
        except Exception:
            entry_as_of_date = None

    item_id = watchlist_repo.add_item(
        ts_code,
        display_name,
        category_id,
        note=note,
        entry_price=float(entry_price),
        entry_rank=entry_rank,
        entry_source=entry_source,
        entry_task_id=entry_task_id,
        entry_loop_id=entry_loop_id,
        entry_as_of=entry_as_of_date,
    )

    if extra_category_ids:
        valid_extra = [cid for cid in extra_category_ids if isinstance(cid, int)]
        if valid_extra:
            watchlist_repo.add_categories_to_items([item_id], valid_extra)

    return item_id


def _get_close_price_by_date(ts_codes: List[str], trade_date: str) -> Dict[str, float]:
    """从 market.kline_daily_raw 查询指定日期的收盘价（复权）。"""
    if not ts_codes:
        return {}
    from ..db.pg_pool import get_conn
    placeholders = ",".join(["%s"] * len(ts_codes))
    sql = f"""
        SELECT ts_code, close_li
        FROM market.kline_daily_raw
        WHERE ts_code IN ({placeholders}) AND trade_date = %s
    """
    results: Dict[str, float] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*ts_codes, trade_date])
            for row in cur.fetchall():
                ts_code, close_li = row
                if close_li and int(close_li) > 0:
                    results[ts_code] = float(close_li) / 1000.0
    if results:
        logger.info(f"指定日期({trade_date})收盘价查询: {len(results)}/{len(ts_codes)} 只")
    return results


def _get_entry_price_bulk(ts_codes: List[str]) -> Dict[str, float]:
    """批量获取加入价格：TDX 优先 → miniQMT → DB 最近收盘价。

    返回结果字典 {ts_code: price}，仅包含成功的项。
    """
    if not ts_codes:
        return {}

    results: Dict[str, float] = {}
    # 标准化代码用于查询
    code_to_ts = {}
    remaining_base_codes = []

    for c in ts_codes:
        if not c: continue
        base = c
        if "." in c:
            try:
                base = data_source_manager._convert_from_ts_code(c)
            except Exception:
                base = c.split(".", 1)[0]
        code_to_ts[base] = c
        remaining_base_codes.append(base)

    # 1) 尝试批量获取 TDX 行情
    from concurrent.futures import ThreadPoolExecutor

    def _fetch_single_tdx(base):
        try:
            q = data_source_manager.get_realtime_quotes(base)
            if isinstance(q, dict) and q.get("source") == "tdx":
                p = q.get("price")
                if isinstance(p, (int, float)) and float(p) > 0:
                    return base, float(p)
        except Exception:
            pass
        return base, None

    if remaining_base_codes:
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_results = list(executor.map(_fetch_single_tdx, remaining_base_codes))
            for base, p in future_results:
                if p is not None:
                    ts_code = code_to_ts[base]
                    results[ts_code] = p
                    if base in remaining_base_codes:
                        remaining_base_codes.remove(base)

    if not remaining_base_codes:
        return results

    # 2) xtquant/miniQMT 批量兜底
    remaining_ts_codes = [code_to_ts[b] for b in remaining_base_codes]
    try:
        snap = xtquant_adapter.fetch_realtime_snapshot_xt(remaining_ts_codes, fields=["close"], freq="1d")
        if snap is not None and not snap.empty:
            for ts_code in remaining_ts_codes:
                try:
                    if ts_code in snap.index:
                        p2 = snap.loc[ts_code].get("close")
                        if isinstance(p2, (int, float)) and float(p2) > 0:
                            results[ts_code] = float(p2)
                except Exception:
                    pass
    except Exception as exc:
        logger.warning(f"xtquant 批量行情获取失败: {exc}")

    # 3) DB kline_daily_raw 最近收盘价兜底（非交易时段 TDX/xtquant 均不可用时）
    still_missing = [c for c in ts_codes if c not in results]
    if still_missing:
        try:
            from ..db.pg_pool import get_conn
            placeholders = ",".join(["%s"] * len(still_missing))
            sql = f"""
                SELECT DISTINCT ON (ts_code)
                       ts_code, close_li
                FROM market.kline_daily_raw
                WHERE ts_code IN ({placeholders})
                ORDER BY ts_code, trade_date DESC
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, still_missing)
                    for row in cur.fetchall():
                        ts_code, close_li = row
                        if close_li and int(close_li) > 0:
                            results[ts_code] = float(close_li) / 1000.0
            filled = [c for c in still_missing if c in results]
            if filled:
                logger.info(f"DB收盘价兜底: {len(filled)}/{len(still_missing)} 只股票")
        except Exception as exc:
            logger.warning(f"DB收盘价兜底失败: {exc}")

    return results


def add_items_bulk_from_task_selection(
    *,
    items: List[Dict[str, Any]],
    category_id: int,
    on_conflict: str = "ignore",
    entry_source: str = "rdagent_task",
    entry_price_date: Optional[str] = None,
) -> Dict[str, Any]:
    """从选股结果批量加入自选池（支持 rdagent_task / qe_experiment / qe_evolution）。

    items: [{code, rank, name?, entry_price?, task_id?, loop_id?, as_of?}]
    - entry_price: 选股时 DB 中的收盘价，优先使用；未提供时才走实时行情获取。
    - entry_price_date: 指定以该日期收盘价覆盖所有 entry_price（用于"下一交易日收盘价入场"）。
    - task_id: task_run_id 或 experiment_id，统一写入 entry_task_id
    - loop_id: rdagent loop_id，写入 entry_loop_id
    - 写入 entry_rank/entry_source/entry_task_id/entry_loop_id/entry_as_of。
    """

    if not items:
        return {"ok": True, "added": 0, "skipped": 0, "moved": 0, "errors": [], "item_ids_by_code": {}}

    # 1. 标准化代码并去重
    code_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        raw_code = str((it or {}).get("code") or "").strip()
        ts_code = _normalize_code_for_storage(raw_code)
        if ts_code:
            code_map[ts_code] = it

    all_ts_codes = list(code_map.keys())

    # 2. 价格获取：若指定了 entry_price_date，从 DB 查该日收盘价覆盖所有
    price_map: Dict[str, float] = {}
    if entry_price_date:
        price_map = _get_close_price_by_date(all_ts_codes, entry_price_date)
        need_price_codes = [c for c in all_ts_codes if c not in price_map]
    else:
        # 分离：已有价格 vs 需要获取价格
        need_price_codes: List[str] = []
        for ts_code in all_ts_codes:
            it = code_map[ts_code]
            ep = it.get("entry_price")
            if isinstance(ep, (int, float)) and float(ep) > 0:
                price_map[ts_code] = float(ep)
            else:
                need_price_codes.append(ts_code)

    # 仅对未提供价格的股票获取实时行情
    if need_price_codes:
        fetched = _get_entry_price_bulk(need_price_codes)
        price_map.update(fetched)

    prepared: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for ts_code in all_ts_codes:
        it = code_map[ts_code]
        p = price_map.get(ts_code)
        
        if p is None or p <= 0:
            errors.append({"code": ts_code, "error": "无法获取实时行情或价格无效"})
            continue

        display_name = it.get("name") or _get_stock_name(ts_code) or ts_code

        # entry_as_of: 若未提供则使用当天
        as_of_raw = it.get("as_of")
        if not as_of_raw:
            as_of_raw = date.today().isoformat()
        try:
            as_of = date.fromisoformat(str(as_of_raw))
        except Exception:
            as_of = date.today()

        prepared.append(
            {
                "code": ts_code,
                "name": display_name,
                "note": it.get("note"),
                "entry_price": float(p),
                "entry_rank": it.get("rank"),
                "entry_source": it.get("entry_source") or entry_source,
                "entry_task_id": it.get("task_id"),
                "entry_loop_id": it.get("loop_id"),
                "entry_as_of": as_of,
                "lifecycle_status": "ENTERED",
                "planned_entry_price": float(p),
                "actual_entry_price": float(p),
                "actual_entry_date": as_of,
                "advisory_enabled": True,
            }
        )

    if not prepared:
        return {"ok": False, "added": 0, "skipped": 0, "moved": 0, "errors": errors, "item_ids_by_code": {}}

    try:
        res = watchlist_repo.add_items_bulk_with_meta(
            category_id=category_id,
            items=prepared,
            on_conflict=on_conflict,
        )
        return {
            "ok": True,
            "added": res.get("added", 0),
            "skipped": res.get("skipped", 0),
            "moved": res.get("moved", 0),
            "errors": errors,
            "item_ids_by_code": res.get("item_ids_by_code", {}),
        }
    except Exception as exc:
        logger.error(f"批量加入自选数据库操作失败: {exc}")
        return {"ok": False, "added": 0, "skipped": 0, "moved": 0, "errors": [{"error": str(exc)}], "item_ids_by_code": {}}


def update_items_category(ids: List[int], new_category_id: int) -> int:
    """批量替换指定条目的分类为单一分类（原分类全部清空）。"""

    return watchlist_repo.update_item_category(ids, new_category_id)


def add_categories_to_items(ids: List[int], category_ids: List[int]) -> int:
    """为一批自选条目追加多个分类映射。"""

    return watchlist_repo.add_categories_to_items(ids, category_ids)


def remove_categories_from_items(ids: List[int], category_ids: List[int]) -> int:
    """从一批自选条目上移除给定分类映射。"""

    return watchlist_repo.remove_categories_from_items(ids, category_ids)

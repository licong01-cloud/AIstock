from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..repositories.watchlist_repo_impl import watchlist_repo
from ..core.data_source_manager_impl import data_source_manager


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
    out: Dict[str, Dict[str, Any]] = {}
    for code in codes:
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
        out[code] = q or {}
    return out


def _compute_realtime_fields(q: Dict[str, Any]) -> Dict[str, Optional[float]]:
    price = q.get("price")
    pre_close = q.get("pre_close")
    open_ = q.get("open")
    high = q.get("high")
    low = q.get("low")
    volume = q.get("volume")
    amount = q.get("amount")

    pct = None
    if isinstance(price, (int, float)) and isinstance(pre_close, (int, float)) and pre_close not in (0, None):
        try:
            pct = (price - pre_close) / pre_close * 100.0
        except Exception:
            pct = None

    volume_hand = volume / 100.0 if isinstance(volume, (int, float)) else None

    return {
        "last": price,
        "pct_change": pct,
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

    enriched: List[Dict[str, Any]] = []
    for it in items:
        code = str(it.get("code"))
        q = quotes_raw.get(code, {})
        rt = _compute_realtime_fields(q)
        row = dict(it)
        for k, v in rt.items():
            row[k] = v
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
    extra_category_ids: Optional[List[int]] = None,
) -> int:
    """单只添加到自选股票池，带分类管理。

    - 代码会按旧版 UI 逻辑标准化为 ts_code；
    - 名称为空时，会通过 data_source_manager 查询；
    - 支持额外分类 ID 列表，用于多分类映射。
    返回创建/更新后的 item_id。
    """

    ts_code = _normalize_code_for_storage(code)
    if not ts_code:
        raise ValueError("无法识别的股票代码")

    display_name = name or _get_stock_name(ts_code) or ts_code

    item_id = watchlist_repo.add_item(ts_code, display_name, category_id)

    if extra_category_ids:
        valid_extra = [cid for cid in extra_category_ids if isinstance(cid, int)]
        if valid_extra:
            watchlist_repo.add_categories_to_items([item_id], valid_extra)

    return item_id


def update_items_category(ids: List[int], new_category_id: int) -> int:
    """批量替换指定条目的分类为单一分类（原分类全部清空）。"""

    return watchlist_repo.update_item_category(ids, new_category_id)


def add_categories_to_items(ids: List[int], category_ids: List[int]) -> int:
    """为一批自选条目追加多个分类映射。"""

    return watchlist_repo.add_categories_to_items(ids, category_ids)


def remove_categories_from_items(ids: List[int], category_ids: List[int]) -> int:
    """从一批自选条目上移除给定分类映射。"""

    return watchlist_repo.remove_categories_from_items(ids, category_ids)

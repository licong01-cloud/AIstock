"""
通达信板块管理服务 — 基于 TdxQuant API

通过 TdxQuant (tqcenter.py) 与本机通达信客户端进程通信，管理自定义板块。
不再直接操作 .blk / blocknew.cfg 文件。

前提: 通达信客户端必须运行并登录。
"""

import logging
import os
import sys
from typing import Any, Dict

import psycopg2.extras

from backend.db.pg_pool import get_conn

logger = logging.getLogger(__name__)

# ─── TdxQuant 初始化 ──────────────────────────────────────────────────────────

_TDX_CLIENT_PATH = os.getenv("TDX_CLIENT_PATH", "").strip()
_tq = None


def _ensure_tq():
    """懒加载 TdxQuant 单例，返回 tq 对象。失败抛异常。"""
    global _tq
    if _tq is not None:
        return _tq

    if not _TDX_CLIENT_PATH:
        raise RuntimeError("TDX_CLIENT_PATH 环境变量未配置")

    pyplugins_user = os.path.join(_TDX_CLIENT_PATH, "PYPlugins", "user")
    if not os.path.isdir(pyplugins_user):
        raise RuntimeError(f"TdxQuant PYPlugins 目录不存在: {pyplugins_user}")

    sys.path.insert(0, pyplugins_user)
    from tqcenter import tq as _tq_module  # noqa: E402

    # initialize 需要通达信客户端目录下的路径才能正确连接 DLL 管道
    init_path = os.path.join(_TDX_CLIENT_PATH, "PYPlugins", "user", "__aistock__.py")
    _tq_module.initialize(init_path)
    run_id = _tq_module._get_run_id()
    if run_id <= 0:
        raise RuntimeError(f"TdxQuant 连接失败 (run_id={run_id})，请确认通达信客户端已运行并登录")

    _tq = _tq_module
    logger.info("TdxQuant 初始化成功, run_id=%s", run_id)
    return _tq


def is_available() -> bool:
    """TDX 板块管理功能是否可用（通达信客户端运行+登录）"""
    try:
        tq = _ensure_tq()
        return tq._get_run_id() > 0
    except Exception:
        return False


# ─── 板块 CRUD ─────────────────────────────────────────────────────────────────

def list_blocks() -> list[dict]:
    """列出所有通达信自定义板块"""
    tq = _ensure_tq()
    sectors = tq.get_user_sector() or []
    blocks = []
    for sector in sectors:
        # get_user_sector 返回的元素结构由 DLL 决定
        if isinstance(sector, dict):
            block_code = sector.get("block_code") or sector.get("code") or ""
            block_name = sector.get("block_name") or sector.get("name") or ""
        else:
            block_code = str(sector)
            block_name = str(sector)

        # 获取板块内股票数
        stocks = tq.get_stock_list_in_sector(block_code) or []
        blocks.append({
            "name": block_code,
            "display_name": block_name,
            "count": len(stocks),
        })
    return blocks


def get_block_stocks(name: str) -> list[dict]:
    """读取板块内股票列表"""
    tq = _ensure_tq()
    stocks_raw = tq.get_stock_list_in_sector(name) or []
    result = []
    for item in stocks_raw:
        if isinstance(item, dict):
            code = item.get("code", "")
        else:
            code = str(item)
        if code:
            result.append({"code": code, "market": ""})
    return result


def create_block(name: str, display_name: str, stocks: list[str]) -> dict:
    """创建新板块并添加股票"""
    tq = _ensure_tq()
    tq.create_sector(block_code=name, block_name=display_name)
    if stocks:
        tq.send_user_block(block_code=name, stocks=stocks)
    return {
        "name": name,
        "display_name": display_name,
        "count": len(stocks),
    }


def add_stocks(name: str, stocks: list[str]) -> dict:
    """增量添加股票到板块"""
    tq = _ensure_tq()
    tq.send_user_block(block_code=name, stocks=stocks)
    return {"added": len(stocks), "skipped": 0}


def remove_stocks(name: str, stocks: list[str]) -> dict:
    """从板块移除股票"""
    tq = _ensure_tq()
    # TdxQuant 无直接移除 API，需清空后重新添加剩余
    current_raw = tq.get_stock_list_in_sector(name) or []
    current_codes = set()
    for item in current_raw:
        if isinstance(item, dict):
            c = item.get("code", "")
        else:
            c = str(item)
        if c:
            current_codes.add(c)

    remove_set = set(stocks)
    remaining = [c for c in current_codes if c not in remove_set]
    removed = len(current_codes) - len(remaining)

    if removed > 0:
        tq.clear_sector(block_code=name)
        if remaining:
            tq.send_user_block(block_code=name, stocks=remaining)

    return {"removed": removed}


def delete_block(name: str) -> dict:
    """删除整个板块"""
    tq = _ensure_tq()
    tq.delete_sector(block_code=name)
    return {"deleted": True, "unregistered": True}


# ─── 从自选分类同步 ────────────────────────────────────────────────────────────

def sync_from_category(category_name: str) -> Dict[str, Any]:
    """将自选分类中的股票同步到通达信板块。

    1. DB 查询分类下的所有股票代码
    2. 构造 block_code: AIstock_<category_id>
    3. clear_sector 清空旧数据
    4. send_user_block 写入新数据
    5. 返回 { name, display_name, count, codes }
    """
    tq = _ensure_tq()

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT c.id AS category_id, c.name AS display_name, i.code
                FROM app.watchlist_items i
                JOIN app.watchlist_item_categories w ON w.item_id = i.id
                JOIN app.watchlist_categories c ON c.id = w.category_id
                WHERE c.name = %s
                ORDER BY i.code
                """,
                (category_name,),
            )
            rows = cur.fetchall()

    if not rows:
        raise ValueError(f"自选分类不存在或无股票: {category_name!r}")

    category_id = rows[0]["category_id"]
    display_name = rows[0]["display_name"]
    codes = [r["code"] for r in rows]  # DB 中已是 "600519.SH" 格式

    # 验证代码格式 (API 要求 CODE.SUFFIX)
    valid_codes = [c for c in codes if "." in c and len(c.split(".")) == 2]
    if not valid_codes:
        raise ValueError(f"分类 {category_name!r} 中无有效股票代码")

    block_code = f"AIstock_{category_id}"

    # 尝试创建板块（已存在则忽略错误）
    try:
        tq.create_sector(block_code=block_code, block_name=display_name)
    except Exception:
        pass  # 板块已存在，忽略

    # 清空后写入
    try:
        tq.clear_sector(block_code=block_code)
    except Exception:
        pass  # 板块可能为空

    tq.send_user_block(block_code=block_code, stocks=valid_codes)

    logger.info(
        "sync_from_category: 分类=%r → 板块=%s, %d 只股票",
        category_name, block_code, len(valid_codes),
    )

    return {
        "name": block_code,
        "display_name": display_name,
        "count": len(valid_codes),
        "codes": valid_codes,
    }

"""
通达信 .blk 板块文件读写服务

读写通达信客户端的自选股/自定义板块文件 (.blk) 和板块配置 (blocknew.cfg)。

.blk 格式: 每行 = MCCCCCC\\r\\n (纯文本, CRLF)
  M = 市场标识: 0=深圳, 1=上海, 2=北交所
  CCCCCC = 6位股票代码
  例: 贵州茅台 = 1600519\\r\\n

blocknew.cfg 格式: 每条记录 120 字节
  前 50 字节: 显示名 (GBK, null-padded)
  后 70 字节: 文件名 (ASCII, null-padded)
"""

import logging
import os
import shutil
from typing import Any, Dict, Optional

import psycopg2.extras

from backend.db.pg_pool import get_conn

logger = logging.getLogger(__name__)

# ─── 市场编码 ─────────────────────────────────────────────────────────────────
MARKET_SH = "1"   # 上海
MARKET_SZ = "0"   # 深圳
MARKET_BJ = "2"   # 北交所

CODE_MARKET_MAP = {
    "6": MARKET_SH, "9": MARKET_SH, "5": MARKET_SH,   # 沪市
    "0": MARKET_SZ, "3": MARKET_SZ, "1": MARKET_SZ,   # 深市
    "2": MARKET_BJ, "4": MARKET_BJ, "8": MARKET_BJ,    # 北交所
}

SUFFIX_MAP = {
    MARKET_SH: ".SH",
    MARKET_SZ: ".SZ",
    MARKET_BJ: ".BJ",
}

MARKET_NAME = {
    MARKET_SH: "上海",
    MARKET_SZ: "深圳",
    MARKET_BJ: "北交所",
}

# blocknew.cfg 记录格式
CFG_RECORD_SIZE = 120
CFG_NAME_LEN = 50
CFG_FILE_LEN = 70


# ─── 配置 ─────────────────────────────────────────────────────────────────────

def _block_dir() -> Optional[str]:
    """获取通达信板块文件目录路径, 未配置或不存在返回 None"""
    d = os.getenv("TDX_BLOCK_DIR", "").strip()
    if not d:
        return None
    if not os.path.isdir(d):
        logger.warning("TDX_BLOCK_DIR 目录不存在: %s", d)
        return None
    return d


def is_available() -> bool:
    """TDX 板块管理功能是否可用"""
    return _block_dir() is not None


# ─── 代码转换 ──────────────────────────────────────────────────────────────────

def normalize_code(raw: str) -> Optional[str]:
    """blk 内部格式 → 标准格式: 1600519 → 600519.SH"""
    raw = raw.strip()
    if not raw or len(raw) != 7:
        return None
    market = raw[0]
    code6 = raw[1:]
    suffix = SUFFIX_MAP.get(market)
    if not suffix:
        return None
    return f"{code6}{suffix}"


def denormalize_code(code: str) -> Optional[str]:
    """标准格式 → blk 内部格式: 600519.SH → 1600519"""
    code = code.strip().upper()
    # 已经是 7 位数字
    if len(code) == 7 and code.isdigit():
        return code
    # 无后缀, 根据代码推断市场
    if "." not in code:
        if len(code) != 6 or not code.isdigit():
            return None
        market = CODE_MARKET_MAP.get(code[0])
        if not market:
            return None
        return f"{market}{code}"
    # 解析 CODE.MKT
    parts = code.split(".")
    if len(parts) != 2 or len(parts[0]) != 6 or not parts[0].isdigit():
        return None
    mkt_upper = parts[1].upper()
    market = {".SH": MARKET_SH, ".SZ": MARKET_SZ, ".BJ": MARKET_BJ}.get(f".{mkt_upper}")
    if not market:
        return None
    return f"{market}{parts[0]}"


# ─── blk 文件读写 ─────────────────────────────────────────────────────────────

def _blk_path(name: str) -> str:
    suffix = ".blk" if not name.endswith(".blk") else ""
    return os.path.join(_block_dir(), f"{name}{suffix}")


def read_blk(name: str) -> list[str]:
    """读取 .blk 文件, 返回标准格式代码列表 [600519.SH, ...]"""
    path = _blk_path(name)
    if not os.path.exists(path):
        return []
    codes = []
    with open(path, "r", encoding="ascii", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            normalized = normalize_code(line)
            if normalized:
                codes.append(normalized)
    return codes


def _write_blk(name: str, raw_codes: list[str]) -> None:
    """写入 .blk 文件 (去重, CRLF)"""
    path = _blk_path(name)
    seen = set()
    unique = []
    for rc in raw_codes:
        if rc not in seen:
            seen.add(rc)
            unique.append(rc)
    with open(path, "w", encoding="ascii", newline="") as f:
        for rc in unique:
            f.write(rc + "\r\n")
    logger.info("写入 %s: %d 只股票", path, len(unique))


# ─── blocknew.cfg 读写 ───────────────────────────────────────────────────────

def _cfg_path() -> str:
    return os.path.join(_block_dir(), "blocknew.cfg")


def read_blocknew_cfg() -> list[tuple[str, str]]:
    """解析 blocknew.cfg, 返回 [(display_name, blk_filename), ...]"""
    path = _cfg_path()
    if not os.path.exists(path):
        return []
    entries = []
    with open(path, "rb") as f:
        data = f.read()
    for offset in range(0, len(data), CFG_RECORD_SIZE):
        record = data[offset:offset + CFG_RECORD_SIZE]
        if len(record) < CFG_RECORD_SIZE:
            break
        name_raw = record[:CFG_NAME_LEN].rstrip(b"\x00")
        file_raw = record[CFG_NAME_LEN:].rstrip(b"\x00")
        if not name_raw and not file_raw:
            continue
        try:
            display_name = name_raw.decode("gbk")
        except UnicodeDecodeError:
            display_name = name_raw.decode("ascii", errors="replace")
        blk_filename = file_raw.decode("ascii", errors="replace")
        entries.append((display_name, blk_filename))
    return entries


def _write_blocknew_cfg(entries: list[tuple[str, str]]) -> None:
    path = _cfg_path()
    # 备份
    if os.path.exists(path):
        shutil.copy2(path, path + ".bak")
    data = bytearray()
    for display_name, blk_filename in entries:
        record = bytearray(CFG_RECORD_SIZE)
        name_bytes = display_name.encode("gbk")
        record[:min(len(name_bytes), CFG_NAME_LEN)] = name_bytes[:CFG_NAME_LEN]
        file_bytes = blk_filename.encode("ascii")
        start = CFG_NAME_LEN
        record[start:start + min(len(file_bytes), CFG_FILE_LEN)] = file_bytes[:CFG_FILE_LEN]
        data.extend(record)
    with open(path, "wb") as f:
        f.write(bytes(data))


def _register_in_cfg(blk_name: str, display_name: str) -> bool:
    """在 blocknew.cfg 中注册新板块, 返回 True=新注册"""
    entries = read_blocknew_cfg()
    for _, fn in entries:
        if fn.upper() == blk_name.upper():
            return False
    entries.append((display_name, blk_name))
    _write_blocknew_cfg(entries)
    return True


def _unregister_from_cfg(blk_name: str) -> bool:
    """从 blocknew.cfg 中移除板块注册"""
    entries = read_blocknew_cfg()
    new_entries = [(dn, fn) for dn, fn in entries if fn.upper() != blk_name.upper()]
    if len(new_entries) == len(entries):
        return False
    _write_blocknew_cfg(new_entries)
    return True


# ─── 业务 API ─────────────────────────────────────────────────────────────────

def list_blocks() -> list[dict]:
    """列出所有通达信板块 (合并 cfg 显示名 + blk 股票数)"""
    d = _block_dir()
    if not d:
        return []

    # 读取 cfg 显示名
    cfg_entries = read_blocknew_cfg()
    display_map = {}
    for dn, fn in cfg_entries:
        display_map[fn.upper().replace(".BLK", "")] = dn

    blocks = []
    for fname in sorted(os.listdir(d)):
        if not fname.endswith(".blk"):
            continue
        fpath = os.path.join(d, fname)
        base = fname[:-4]
        with open(fpath, "r", encoding="ascii", errors="replace") as f:
            count = sum(1 for line in f if line.strip())
        blocks.append({
            "name": base,
            "display_name": display_map.get(base.upper(), base),
            "count": count,
            "size": os.path.getsize(fpath),
        })
    return blocks


def get_block_stocks(name: str) -> list[dict]:
    """读取板块内股票列表, 返回 [{code, market}, ...]"""
    codes = read_blk(name)
    result = []
    for code in codes:
        raw = denormalize_code(code)
        market_ch = MARKET_NAME.get(raw[0], "未知") if raw else "未知"
        result.append({"code": code, "market": market_ch})
    return result


def create_block(name: str, display_name: str, stocks: list[str]) -> dict:
    """创建新板块 (blk + cfg 注册)"""
    # 验证名称合法性
    safe_name = name.strip().upper()
    if not safe_name or not all(c.isalnum() or c == "_" for c in safe_name):
        raise ValueError(f"板块名称不合法: {name!r} (仅支持字母数字下划线)")

    # 检查是否已存在
    path = _blk_path(safe_name)
    if os.path.exists(path):
        raise FileExistsError(f"板块文件已存在: {path}")

    # 转换并写入 blk
    raw_codes = []
    for code in stocks:
        raw = denormalize_code(code)
        if raw:
            raw_codes.append(raw)
        else:
            logger.warning("无法转换代码: %r", code)

    _write_blk(safe_name, raw_codes)

    # 注册到 cfg
    _register_in_cfg(safe_name, display_name or safe_name)

    return {
        "name": safe_name,
        "display_name": display_name or safe_name,
        "count": len(raw_codes),
    }


def add_stocks(name: str, stocks: list[str]) -> dict:
    """增量添加股票到板块"""
    existing = read_blk(name)
    existing_raw = {denormalize_code(c) for c in existing}

    added = 0
    skipped = 0
    to_add_raw = []
    for code in stocks:
        raw = denormalize_code(code)
        if not raw:
            logger.warning("无法转换代码: %r", code)
            continue
        if raw in existing_raw:
            skipped += 1
        else:
            to_add_raw.append(raw)
            added += 1

    if added > 0:
        path = _blk_path(name)
        with open(path, "a", encoding="ascii", newline="") as f:
            for rc in to_add_raw:
                f.write(rc + "\r\n")

    return {"added": added, "skipped": skipped}


def remove_stocks(name: str, stocks: list[str]) -> dict:
    """从板块移除股票"""
    existing = read_blk(name)
    remove_raw = {denormalize_code(c) for c in stocks if denormalize_code(c)}
    new_codes = [c for c in existing if denormalize_code(c) not in remove_raw]
    removed = len(existing) - len(new_codes)

    if removed > 0:
        new_raw = [denormalize_code(c) for c in new_codes if denormalize_code(c)]
        _write_blk(name, new_raw)

    return {"removed": removed}


def delete_block(name: str) -> dict:
    """删除整个板块 (blk 文件 + cfg 注册)"""
    # 从 cfg 注销
    unregistered = _unregister_from_cfg(name)

    # 删除 blk 文件
    path = _blk_path(name)
    deleted = False
    if os.path.exists(path):
        os.remove(path)
        deleted = True

    if not unregistered and not deleted:
        raise FileNotFoundError(f"板块不存在: {name}")

    return {"deleted": deleted, "unregistered": unregistered}


def sync_from_category(category_name: str) -> Dict[str, Any]:
    """将自选分类中的股票同步到通达信板块文件 (.blk)。

    1. DB 查询分类下的所有股票代码
    2. 以 category ID 构造稳定文件名 AIstock_cat_<id>.blk
    3. 调用 _write_blk 覆盖写入
    4. 调用 _register_in_cfg 幂等注册到 blocknew.cfg
    """
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
    codes = [r["code"] for r in rows]

    # 转换为 blk 内部格式
    raw_codes = []
    for code in codes:
        raw = denormalize_code(code)
        if raw:
            raw_codes.append(raw)
        else:
            logger.warning("sync_from_category: 无法转换代码 %r", code)

    if not raw_codes:
        raise ValueError(f"分类 {category_name!r} 中无有效股票代码")

    block_name = f"AIstock_cat_{category_id}"
    _write_blk(block_name, raw_codes)
    _register_in_cfg(block_name, display_name)

    logger.info(
        "sync_from_category: 分类=%r → 板块=%s, %d 只股票",
        category_name, block_name, len(raw_codes),
    )

    return {
        "name": block_name,
        "display_name": display_name,
        "count": len(raw_codes),
        "codes": codes,
    }

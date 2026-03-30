"""SOTA 因子 Catalog 入库服务。

在 Task 同步（sync_task_from_log）成功后自动调用，将 SOTA 因子元数据
写入 aistock_factor_catalog 表，包括因子名称、回测指标、代码路径等。

数据来源（全部通过 API 获取，禁止直接文件访问）：
- v2_alignment_preview: SOTA 因子名称列表
- /tasks/{tid}/loops: 每轮 loop 的回测指标和测试因子
- sota_factor_anchor: 因子代码 file key（用于记录 source_code_origin）
- download_task_asset_bytes: 下载因子代码（用于提取表达式和去重哈希）
"""
from __future__ import annotations

import ast
import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..db.pg_pool import get_conn
from .rdagent_results_api_client import RDAgentResultsApiClient

logger = logging.getLogger("aistock.factor_catalog_sync")

JsonDict = Dict[str, Any]


def _normalize_factor_name_static(name: str) -> str:
    """从因子文件名中提取归一化的英文名。

    处理中文名（含英文括号）的情况，例如：
      '主力资金净流入强度5日滚动（MainNetAmtRatio_5D）' -> 'MainNetAmtRatio_5D'
      '主力资金净流入强度5日滚动(MainNetAmtRatio_5D)' -> 'MainNetAmtRatio_5D'
      'MainNetAmtRatio_5D' -> 'MainNetAmtRatio_5D'
    """
    # 尝试从中文/全角括号中提取英文名
    for pattern in [r'[（(]([A-Za-z_]\w*)[）)]', r'[（(]([A-Za-z_][\w.]*)[）)]']:
        m = re.search(pattern, name)
        if m:
            return m.group(1)
    return name

_rdagent_client = RDAgentResultsApiClient()


def _decode_code_bytes(data: bytes) -> str:
    """尝试多种编码解码因子代码字节，优先 UTF-8，回退 GBK。"""
    for enc in ("utf-8", "gbk", "gb2312", "latin-1"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return data.decode("utf-8", errors="replace")


@dataclass
class FactorSyncResult:
    """因子入库结果。"""
    ok: bool
    task_id: str
    total_sota_factors: int
    inserted: int
    updated: int
    dedup_skipped: int
    errors: List[str]


# ---------------------------------------------------------------------------
# 因子代码解析
# ---------------------------------------------------------------------------

def _extract_factor_computation_code(factor_code: str, factor_name: str) -> str:
    """从 factor.py 代码中提取指定因子的核心计算代码。

    查找 calculate_{factor_name} 函数体中 BEGIN/END FACTOR COMPUTATION AREA
    之间的代码。如果没有标记，则提取整个函数体。
    """
    # 尝试提取 BEGIN/END 标记之间的代码
    pattern = r'#\s*=+\s*BEGIN FACTOR COMPUTATION AREA\s*=+\s*\n(.*?)#\s*=+\s*END FACTOR COMPUTATION AREA'
    matches = re.findall(pattern, factor_code, re.DOTALL)
    if matches:
        # 如果有多个 BEGIN/END 块，尝试找到属于目标因子的那个
        func_pattern = rf'def\s+calculate_{re.escape(factor_name)}\s*\('
        func_match = re.search(func_pattern, factor_code)
        if func_match:
            func_start = func_match.start()
      # 找到函数之后最近的 BEGIN/END 块
            for m in re.finditer(pattern, factor_code, re.DOTALL):
                if m.start() > func_start:
                    return m.group(1).strip()
        # 回退：返回第一个匹配
        return matches[0].strip()

    # 没有标记，尝试提取整个 calculate_ 函数体
    func_pattern = rf'def\s+calculate_{re.escape(factor_name)}\s*\([^)]*\).*?:\s*\n'
    func_match = re.search(func_pattern, factor_code)
    if func_match:
        func_start = func_match.end()
        # 找到函数体结束（下一个 def 或文件末尾）
        next_def = re.search(r'\ndef\s+\w+\s*\(', factor_code[func_start:])
        if next_def:
            func_body = factor_code[func_start:func_start + next_def.start()]
        else:
            func_body = factor_code[func_start:]
        return func_body.strip()

    return ""


def _extract_factor_expression(factor_code: str, factor_name: str) -> Optional[str]:
    """从因子代码中提取表达式描述。

    优先从核心计算区域的注释提取有意义的描述，
    过滤掉模板 docstring（如"根据给定因子定义计算因子值"）。
    """
    # 模板 docstring 黑名单（这些不是因子表达式）
    _TEMPLATE_DOCSTRINGS = [
        "根据给定因子定义计算因子值",
        "并写入 result.h5",
        "Calculate factor values",
    ]

    # 1. 优先从核心计算区域的注释提取
    comp_code = _extract_factor_computation_code(factor_code, factor_name)
    if comp_code:
        comments = []
        for line in comp_code.split('\n'):
            line = line.strip()
            if line.startswith('#') and len(line) > 3:
                comment_text = line.lstrip('# ').strip()
                # 过滤掉模板注释和标记行
                if comment_text and not any(t in comment_text for t in _TEMPLATE_DOCSTRINGS):
                    if not comment_text.startswith('====') and not comment_text.startswith('---'):
                        comments.append(comment_text)
        if comments:
            # 返回前3行有意义的注释作为表达式描述
            return '; '.join(comments[:3])

    # 2. 尝试从 docstring 提取（过滤模板文本）
    func_pattern = rf'def\s+calculate_{re.escape(factor_name)}\s*\([^)]*\).*?:\s*\n\s*"""(.*?)"""'
    match = re.search(func_pattern, factor_code, re.DOTALL)
    if match:
        doc = match.group(1).strip()
        if doc and not any(t in doc for t in _TEMPLATE_DOCSTRINGS):
            return doc

    # 3. 如果都没有，尝试匹配任意 calculate_ 函数的 docstring
    generic_match = re.search(r'def\s+calculate_\w+\s*\([^)]*\).*?:\s*\n\s*"""(.*?)"""', factor_code, re.DOTALL)
    if generic_match:
        doc = generic_match.group(1).strip()
        if doc and not any(t in doc for t in _TEMPLATE_DOCSTRINGS):
            return doc

    return None


# ---------------------------------------------------------------------------
# 去重：阶段 1 — 代码哈希
# ---------------------------------------------------------------------------

def _normalize_code_for_dedup(code: str) -> str:
    """对因子代码进行归一化处理，用于去重哈希计算。

    1. 移除注释和空行
    2. 移除字符串常量
    3. 保留数值常量（窗口大小等是因子逻辑关键部分）
    4. 变量名不做替换（AST 级别归一化复杂度过高，先用文本级别）
    """
    lines = []
    for line in code.split('\n'):
        stripped = line.strip()
        # 跳过空行和纯注释行
        if not stripped or stripped.startswith('#'):
            continue
        # 移除行内注释
        comment_idx = stripped.find('#')
        if comment_idx > 0:
            stripped = stripped[:comment_idx].rstrip()
        lines.append(stripped)
    return '\n'.join(lines)


def compute_factor_dedup_hash(factor_code: str, factor_name: str) -> str:
    """计算因子核心代码的去重哈希。"""
    comp_code = _extract_factor_computation_code(factor_code, factor_name)
    if not comp_code:
        # 无法提取核心代码，使用因子名称作为哈希（不会误判重复）
        return hashlib.sha256(f"name_only:{factor_name}".encode()).hexdigest()
    normalized = _normalize_code_for_dedup(comp_code)
    return hashlib.sha256(normalized.encode()).hexdigest()


def check_factor_dedup(
    dedup_hash: str,
    factor_name: str,
) -> Optional[Dict[str, Any]]:
    """检查因子是否与已有因子重复（阶段 1：精确哈希匹配）。

    Returns:
        None 表示无重复；dict 包含匹配的因子信息。
    """
    sql = """
        SELECT factor_name, source, source_task_id, dedup_group_id
        FROM aistock_factor_catalog
        WHERE dedup_hash = %s AND factor_name != %s
        LIMIT 1
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (dedup_hash, factor_name))
            row = cur.fetchone()
            if row:
                return {
                    "matched_factor_name": row[0],
                    "matched_source": row[1],
                    "matched_task_id": row[2],
                    "dedup_group_id": row[3],
                }
    return None


# ---------------------------------------------------------------------------
# 从 loops API 获取回测指标
# ---------------------------------------------------------------------------

def _fetch_loops_metrics(task_id: str) -> Dict[str, JsonDict]:
    """从 /tasks/{tid}/loops API 获取所有 loop 的回测指标。

    Returns:
        dict: factor_name -> {loop_id, annualized_return, max_drawdown, information_ratio, ...}
        每个因子取其首次出现的 SOTA loop 的指标。
    """
    base_url = _rdagent_client.base_url.rstrip("/")
    url = f"{base_url}/tasks/{task_id}/loops"
    try:
        resp = requests.get(url, timeout=300.0)
        resp.raise_for_status()
        loops_data = resp.json()
    except Exception as e:
        raise RuntimeError(f"[{task_id}] 获取 loops 失败: {e}") from e

    loops = loops_data.get("loops", [])
    factor_metrics: Dict[str, JsonDict] = {}

    for loop in loops:
        is_sota = loop.get("is_sota", False)
        if not is_sota:
            continue

        loop_id = loop.get("loop_id")
        ann_ret = loop.get("annualized_return")
        max_dd = loop.get("max_drawdown")
        info_ratio = loop.get("information_ratio")
        ic_val = loop.get("valid_score")  # IC 值
        tested_factors = loop.get("tested_factors", [])

        metrics = {
            "loop_id": loop_id,
            "annualized_return": ann_ret,
            "max_drawdown": max_dd,
            "information_ratio": info_ratio,
            "ic": ic_val,
            "is_sota": True,
        }

        for fname in tested_factors:
            if isinstance(fname, str) and fname not in factor_metrics:
                factor_metrics[fname] = metrics

    return factor_metrics


# ---------------------------------------------------------------------------
# 核心入库函数
# ---------------------------------------------------------------------------

def sync_factors_from_task(
    task_id: str,
    v2_preview_data: JsonDict,
    anchor_resp: JsonDict,
    task_dir: str,
) -> FactorSyncResult:
    """从 Task 同步数据中提取 SOTA 因子并入库到 aistock_factor_catalog。

    Args:
        task_id: Task ID
        v2_preview_data: v2_alignment_preview API 返回数据
        anchor_resp: sota_factor_anchor API 返回数据
        task_dir: AIstock 侧 task 资产目录路径
    """
    sota_factor_names = v2_preview_data.get("sota_factors", [])
    if not sota_factor_names:
        return FactorSyncResult(
            ok=True, task_id=task_id, total_sota_factors=0,
            inserted=0, updated=0, dedup_skipped=0, errors=[]
        )

    logger.info(f"[{task_id}] 开始因子入库: {len(sota_factor_names)} 个 SOTA 因子")

    # 1. 获取 loops 回测指标 + 建立因子名→loop→代码文件的映射
    factor_metrics = _fetch_loops_metrics(task_id)
    logger.info(f"[{task_id}] 获取到 {len(factor_metrics)} 个因子的回测指标")

    # 2. 建立因子名 → 代码文件映射
    # 仅对最后 SOTA loop 的因子使用主 factor.py（API 明确标记）
    # 不再推测 based_entries 与 SOTA loop 的对应关系
    based_entries = anchor_resp.get("based_factor_entries", [])
    last_sota_idx = anchor_resp.get("last_sota_factor_index")
    resolved_factor_key = anchor_resp.get("resolved_factor_entry_key")

    # 因子名 → 代码文件信息
    factor_code_map: Dict[str, Dict[str, Any]] = {}

    # 从 loops API 获取最后 SOTA loop 的 tested_factors
    base_url = _rdagent_client.base_url.rstrip("/")
    try:
        resp = requests.get(f"{base_url}/tasks/{task_id}/loops", timeout=300.0)
        resp.raise_for_status()
        all_loops = resp.json().get("loops", [])

        # 只处理最后 SOTA loop 对应主 factor.py（API 明确标记）
        if last_sota_idx is not None:
            for lp in all_loops:
                if lp.get("loop_id") == last_sota_idx:
                    tested = lp.get("tested_factors", [])
                    for fname in tested:
                        if isinstance(fname, str):
                            factor_code_map[fname] = {
                                "relpath": "factor.py",
                                "origin": resolved_factor_key,
                                "is_main": True,
                            }
                    break

        # based_entries 有明确的 resolved_factor_entry_key，
        # 但无法确定哪些因子名属于哪个 based_entry，不做推测映射
        # 仅记录 based_entries 的代码下载路径供后续使用
        for entry in based_entries:
            based_idx = entry.get("based_index")
            entry_key = entry.get("resolved_factor_entry_key")
            if entry_key and based_idx is not None:
                logger.debug(
                    f"[{task_id}] based_entry[{based_idx}] 有代码 key={entry_key}，"
                    f"但无法确定对应因子名，跳过映射"
                )
    except Exception as e:
        raise RuntimeError(f"[{task_id}] 获取 loops 构建因子代码映射失败: {e}") from e

    logger.info(f"[{task_id}] 因子代码映射: {len(factor_code_map)}/{len(sota_factor_names)} 个因子有代码文件")

    # 3. 下载因子代码（通过 API）用于去重哈希和表达式提取
    downloaded_codes: Dict[str, str] = {}  # relpath -> code_text

    if resolved_factor_key:
        try:
            code_bytes = _rdagent_client.download_task_asset_bytes(task_id, resolved_factor_key)
            if code_bytes:
                downloaded_codes["factor.py"] = _decode_code_bytes(code_bytes)
                logger.info(f"[{task_id}] 下载主因子代码成功: {len(code_bytes)} bytes")
        except Exception as e:
            raise RuntimeError(f"[{task_id}] 下载主因子代码失败: {e}") from e

    for entry in based_entries:
        idx = entry.get("based_index")
        key = entry.get("resolved_factor_entry_key")
        relpath = f"based_factors/based_factor_{idx}.py"
        if key and idx is not None and relpath not in downloaded_codes:
            try:
                code_bytes = _rdagent_client.download_task_asset_bytes(task_id, key)
                if code_bytes:
                    downloaded_codes[relpath] = _decode_code_bytes(code_bytes)
            except Exception as e:
                raise RuntimeError(f"[{task_id}] 下载 based_factor_{idx} 失败: {e}") from e

    # 4. 从 RDAgent aligned API 获取每个因子的 formulation/description/variables/source_code
    # aligned API 返回每个因子独立的完整代码（source_code），比从共享 factor.py 提取更准确
    aligned_factor_details: Dict[str, Dict[str, Any]] = {}  # factor_name -> {formulation, description, variables, source_code}
    try:
        aligned_resp = _rdagent_client.get_aligned_sota_factors(task_id)
        if aligned_resp and aligned_resp.get("success"):
            aligned_factors = aligned_resp.get("aligned_factors", {})
            for fname, info in aligned_factors.items():
                aligned_factor_details[fname] = {
                    "factor_formulation": info.get("factor_formulation", ""),
                    "factor_description": info.get("factor_description", ""),
                    "variables": info.get("variables"),
                    "source_code": info.get("source_code", ""),
                }
            logger.info(f"[{task_id}] 从 aligned API 获取到 {len(aligned_factor_details)} 个因子的 formulation/description/source_code")
        else:
            raise RuntimeError(f"[{task_id}] aligned API 返回失败: {aligned_resp.get('error', 'unknown') if aligned_resp else 'empty'}")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"[{task_id}] 调用 aligned API 获取因子详情失败: {e}") from e

    # 5. 构造入库数据并执行 UPSERT
    now_utc = datetime.now(timezone.utc)
    inserted = 0
    updated = 0
    dedup_skipped = 0
    errors: List[str] = []

    for i, fname in enumerate(sota_factor_names):
        try:
            # 确定因子代码来源
            # 优先使用 aligned API 返回的每个因子独立的 source_code（每个因子有自己的完整代码文件）
            # 只有在 aligned API 没有返回 source_code 时才回退到共享 factor.py
            code_for_factor = ""
            source_code_relpath = None
            source_code_origin = None

            aligned_info = aligned_factor_details.get(fname)

            # 优先：从 aligned API 获取该因子独立的 source_code
            if aligned_info and aligned_info.get("source_code"):
                code_for_factor = aligned_info["source_code"]
                source_code_relpath = f"aligned/{fname}.py"
                source_code_origin = "aligned_api"
            else:
                # 回退：从 factor_code_map（共享 factor.py / based_factor_*.py）获取
                code_info = factor_code_map.get(fname)
                if code_info:
                    source_code_relpath = code_info.get("relpath")
                    source_code_origin = code_info.get("origin")
                    code_for_factor = downloaded_codes.get(source_code_relpath, "")

            # 从文件系统读取完整因子源代码（权威数据源）
            # task_dir/factors/{fname}.py 是 task 同步时保存的原始完整源代码文件
            full_code_from_file: str = ""
            asset_path_value: str = ""
            if task_dir:
                factor_file = Path(task_dir) / "factors" / f"{fname}.py"
                if not factor_file.exists():
                    # 模糊匹配：parquet 名可能是英文，文件名可能是中文（含英文括号）
                    factors_dir = Path(task_dir) / "factors"
                    if factors_dir.exists():
                        normalized = _normalize_factor_name_static(fname)
                        candidates = [f for f in factors_dir.glob("*.py")
                                      if normalized in f.stem or fname in f.stem]
                        if candidates:
                            factor_file = candidates[0]
                            logger.info(f"[{task_id}] 因子 {fname} 精确匹配失败，模糊匹配到: {factor_file.name}")

                if factor_file.exists():
                    full_code_from_file = factor_file.read_text(encoding="utf-8")
                    # 存储相对于 AIstock 项目根目录的相对路径（跨平台兼容）
                    # task_dir 结构: {aistock_root}/rdagent_assets/rdagent_tasks/{task_id}
                    # 所以需要 parent.parent.parent 才能得到 aistock_root
                    try:
                        aistock_root = Path(task_dir).parent.parent.parent
                        asset_path_value = factor_file.relative_to(aistock_root).as_posix()
                    except ValueError:
                        asset_path_value = str(factor_file)
                    logger.info(f"[{task_id}] 因子 {fname} 从文件系统读取完整源码: {len(full_code_from_file)} chars, path={asset_path_value}")
                else:
                    logger.warning(f"[{task_id}] 因子 {fname} 文件不存在（精确+模糊均未匹配）: {factor_file}")

            # 提取表达式（仅用于 expression 字段，不影响 code_text）
            core_code = ""
            expression = None

            # 优先：从 aligned API 获取 factor_formulation（权威数据）
            if aligned_info:
                formulation = aligned_info.get("factor_formulation", "")
                description = aligned_info.get("factor_description", "")
                if formulation and description:
                    expression = f"{formulation} | {description}"
                elif formulation:
                    expression = formulation
                elif description:
                    expression = description

            if code_for_factor:
                # 提取核心代码（仅用于去重哈希和表达式提取，不再用于 code_text）
                core_code = _extract_factor_computation_code(code_for_factor, fname)
                # 如果 aligned API 没有返回表达式，回退到代码注释提取
                if not expression:
                    expression = _extract_factor_expression(code_for_factor, fname)
                # 如果因子名不匹配（通用函数名），尝试提取第一个 calculate_ 函数
                if not core_code:
                    func_match = re.search(r'def\s+calculate_(\w+)\s*\(', code_for_factor)
                    if func_match:
                        generic_name = func_match.group(1)
                        core_code = _extract_factor_computation_code(code_for_factor, generic_name)
                        if not expression:
                            expression = _extract_factor_expression(code_for_factor, generic_name)

            # code_text 优先使用文件系统中的完整源代码；文件不存在时回退到 API 返回的代码
            code_text_value = full_code_from_file if full_code_from_file else code_for_factor
            if not code_text_value:
                raise ValueError(f"[{task_id}] 因子 {fname} 无任何代码来源（文件系统和 API 均为空）")

            # 计算去重哈希
            dedup_hash = compute_factor_dedup_hash(code_for_factor, fname) if code_for_factor else None

            # 去重检查
            dedup_group_id = None
            is_dedup_primary = True
            if dedup_hash:
                dedup_match = check_factor_dedup(dedup_hash, fname)
                if dedup_match:
                    dedup_group_id = dedup_match.get("dedup_group_id") or str(uuid.uuid4())[:12]
                    is_dedup_primary = False
                    logger.info(f"[{task_id}] 因子 {fname} 与 {dedup_match['matched_factor_name']} 代码哈希重复，标记为非主因子")
                else:
                    dedup_group_id = str(uuid.uuid4())[:12]

            # 获取回测指标
            metrics = factor_metrics.get(fname, {})
            ann_ret = metrics.get("annualized_return")
            max_dd = metrics.get("max_drawdown")
            info_ratio = metrics.get("information_ratio")
            ic_val = metrics.get("ic")
            loop_id = metrics.get("loop_id")

            # UPSERT 到 aistock_factor_catalog
            sql = """
                INSERT INTO aistock_factor_catalog (
                    factor_name, source, catalog_version, generated_at_utc, catalog_source,
                    expression, is_sota_factor, first_sota_task_id,
                    source_task_id, source_code_relpath, source_code_origin,
                    source_loop_tag, source_index,
                    ic, annualized_return, max_drawdown, sharpe,
                    performance_metrics, best_performance_ann_ret, best_performance_sharpe,
                    dedup_hash, dedup_group_id, is_dedup_primary, code_text, asset_path
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (factor_name, source) DO UPDATE SET
                    catalog_version = EXCLUDED.catalog_version,
                    generated_at_utc = EXCLUDED.generated_at_utc,
                    expression = COALESCE(EXCLUDED.expression, aistock_factor_catalog.expression),
                    source_task_id = EXCLUDED.source_task_id,
                    source_code_relpath = EXCLUDED.source_code_relpath,
                    source_code_origin = EXCLUDED.source_code_origin,
                    source_loop_tag = EXCLUDED.source_loop_tag,
                    source_index = EXCLUDED.source_index,
                    ic = EXCLUDED.ic,
                    annualized_return = EXCLUDED.annualized_return,
                    max_drawdown = EXCLUDED.max_drawdown,
                    sharpe = EXCLUDED.sharpe,
                    performance_metrics = EXCLUDED.performance_metrics,
                    best_performance_ann_ret = EXCLUDED.best_performance_ann_ret,
                    best_performance_sharpe = EXCLUDED.best_performance_sharpe,
                    first_sota_task_id = COALESCE(aistock_factor_catalog.first_sota_task_id, EXCLUDED.first_sota_task_id),
                    dedup_hash = COALESCE(EXCLUDED.dedup_hash, aistock_factor_catalog.dedup_hash),
                    dedup_group_id = COALESCE(EXCLUDED.dedup_group_id, aistock_factor_catalog.dedup_group_id),
                    is_dedup_primary = EXCLUDED.is_dedup_primary,
                    code_text = COALESCE(EXCLUDED.code_text, aistock_factor_catalog.code_text),
                    asset_path = COALESCE(EXCLUDED.asset_path, aistock_factor_catalog.asset_path)
            """

            perf_metrics_json = json.dumps(metrics, ensure_ascii=False) if metrics else None

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        fname, "rdagent_task_sync", "task_sync_v1", now_utc, "rdagent_task_sync",
                        expression, True, task_id,
                        task_id, source_code_relpath, source_code_origin,
                        str(loop_id) if loop_id is not None else None, i,
                        ic_val, ann_ret, max_dd, info_ratio,
                        perf_metrics_json, ann_ret, info_ratio,
                        dedup_hash, dedup_group_id, is_dedup_primary,
                        code_text_value,
                        asset_path_value or None,
                    ))
                    # rowcount: 1 for insert, 1 for update
                    inserted += 1

            logger.info(f"[{task_id}] 因子 {fname} 入库成功 (loop={loop_id}, ic={ic_val}, ann_ret={ann_ret}, sharpe={info_ratio})")

        except Exception as e:
            err_msg = f"因子 {fname} 入库失败: {e}"
            errors.append(err_msg)
            logger.error(f"[{task_id}] {err_msg}")
            continue

    logger.info(f"[{task_id}] 因子入库完成: {inserted} 入库, {dedup_skipped} 去重跳过, {len(errors)} 错误")

    return FactorSyncResult(
        ok=len(errors) == 0,
        task_id=task_id,
        total_sota_factors=len(sota_factor_names),
        inserted=inserted,
        updated=updated,
        dedup_skipped=dedup_skipped,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Loop 因子手动同步
# ---------------------------------------------------------------------------

def sync_factors_from_loop(
    task_id: str,
    loop_id: int,
    loop_factors_data: JsonDict,
    task_dir: Optional[str] = None,
) -> FactorSyncResult:
    """将指定 Loop 的因子入库到 aistock_factor_catalog。

    与 sync_factors_from_task 复用相同的去重和代码解析逻辑，
    但数据源来自 RDAgent API 的 extract_loop_factors 返回值。

    Args:
        task_id: Task ID
        loop_id: Loop 索引 (0-based)
        loop_factors_data: RDAgent API /v2/{task_id}/loops/{loop_id}/factors 响应
        task_dir: AIstock 侧 task 资产目录路径 (可选)
    """
    errors: List[str] = []
    factors = loop_factors_data.get("factors", {})
    loop_metrics = loop_factors_data.get("loop_metrics", {})

    if not factors:
        return FactorSyncResult(
            ok=True, task_id=task_id, total_sota_factors=0,
            inserted=0, updated=0, dedup_skipped=0, errors=[],
        )

    factor_names = list(factors.keys())
    logger.info(
        f"[{task_id}] Loop {loop_id} 手动同步: {len(factor_names)} 个因子"
    )

    now_utc = datetime.now(timezone.utc)
    inserted = 0
    updated = 0
    dedup_skipped = 0
    source_loop_tag = f"loop_{loop_id}_manual_sync"

    for i, fname in enumerate(factor_names):
        try:
            finfo = factors[fname]
            code_for_factor = finfo.get("source_code", "") or ""
            formulation = finfo.get("factor_formulation", "")
            description = finfo.get("factor_description", "")

            # 构建 expression
            expression = None
            if formulation and description:
                expression = f"{formulation} | {description}"
            elif formulation:
                expression = formulation
            elif description:
                expression = description

            # 如果没有从 API 获得表达式，尝试从代码提取
            if not expression and code_for_factor:
                expression = _extract_factor_expression(code_for_factor, fname)

            # 计算去重哈希
            dedup_hash = compute_factor_dedup_hash(code_for_factor, fname) if code_for_factor else None

            # 去重检查
            dedup_group_id = None
            is_dedup_primary = True
            if dedup_hash:
                dedup_match = check_factor_dedup(dedup_hash, fname)
                if dedup_match:
                    dedup_group_id = dedup_match.get("dedup_group_id") or str(uuid.uuid4())[:12]
                    is_dedup_primary = False
                    logger.info(
                        f"[{task_id}] Loop {loop_id} 因子 {fname} 与 "
                        f"{dedup_match['matched_factor_name']} 代码哈希重复"
                    )
                else:
                    dedup_group_id = str(uuid.uuid4())[:12]

            # 回测指标来自 Loop 整体 metrics
            ann_ret = loop_metrics.get("annualized_return")
            max_dd = loop_metrics.get("max_drawdown")
            info_ratio = loop_metrics.get("information_ratio")
            ic_val = loop_metrics.get("ic")

            # 保存因子代码文件到 task_dir (如果提供)
            code_text_value = code_for_factor or None
            asset_path_value = None
            if task_dir and code_for_factor:
                factors_dir = Path(task_dir) / "factors"
                factors_dir.mkdir(parents=True, exist_ok=True)
                factor_file = factors_dir / f"{fname}.py"
                factor_file.write_text(code_for_factor, encoding="utf-8")
                try:
                    aistock_root = Path(task_dir).parent.parent.parent
                    asset_path_value = factor_file.relative_to(aistock_root).as_posix()
                except ValueError:
                    asset_path_value = str(factor_file)
                logger.info(f"[{task_id}] Loop {loop_id} 因子 {fname} 代码已保存: {factor_file}")

            perf_metrics_json = json.dumps(loop_metrics, ensure_ascii=False) if loop_metrics else None

            # UPSERT 到 aistock_factor_catalog
            sql = """
                INSERT INTO aistock_factor_catalog (
                    factor_name, source, catalog_version, generated_at_utc, catalog_source,
                    expression, is_sota_factor, first_sota_task_id,
                    source_task_id, source_code_relpath, source_code_origin,
                    source_loop_tag, source_index,
                    ic, annualized_return, max_drawdown, sharpe,
                    performance_metrics, best_performance_ann_ret, best_performance_sharpe,
                    dedup_hash, dedup_group_id, is_dedup_primary, code_text, asset_path
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (factor_name, source) DO UPDATE SET
                    catalog_version = EXCLUDED.catalog_version,
                    generated_at_utc = EXCLUDED.generated_at_utc,
                    expression = COALESCE(EXCLUDED.expression, aistock_factor_catalog.expression),
                    source_task_id = EXCLUDED.source_task_id,
                    source_code_relpath = EXCLUDED.source_code_relpath,
                    source_code_origin = EXCLUDED.source_code_origin,
                    source_loop_tag = EXCLUDED.source_loop_tag,
                    source_index = EXCLUDED.source_index,
                    ic = EXCLUDED.ic,
                    annualized_return = EXCLUDED.annualized_return,
                    max_drawdown = EXCLUDED.max_drawdown,
                    sharpe = EXCLUDED.sharpe,
                    performance_metrics = EXCLUDED.performance_metrics,
                    best_performance_ann_ret = EXCLUDED.best_performance_ann_ret,
                    best_performance_sharpe = EXCLUDED.best_performance_sharpe,
                    first_sota_task_id = COALESCE(aistock_factor_catalog.first_sota_task_id, EXCLUDED.first_sota_task_id),
                    dedup_hash = COALESCE(EXCLUDED.dedup_hash, aistock_factor_catalog.dedup_hash),
                    dedup_group_id = COALESCE(EXCLUDED.dedup_group_id, aistock_factor_catalog.dedup_group_id),
                    is_dedup_primary = EXCLUDED.is_dedup_primary,
                    code_text = COALESCE(EXCLUDED.code_text, aistock_factor_catalog.code_text),
                    asset_path = COALESCE(EXCLUDED.asset_path, aistock_factor_catalog.asset_path)
            """

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        fname, "rdagent_task_sync", "loop_sync_v1", now_utc, "rdagent_loop_manual_sync",
                        expression, True, task_id,
                        task_id, f"factors/{fname}.py", "loop_manual_sync",
                        source_loop_tag, i,
                        ic_val, ann_ret, max_dd, info_ratio,
                        perf_metrics_json, ann_ret, info_ratio,
                        dedup_hash, dedup_group_id, is_dedup_primary,
                        code_text_value,
                        asset_path_value,
                    ))
                    inserted += 1

            logger.info(
                f"[{task_id}] Loop {loop_id} 因子 {fname} 入库成功 "
                f"(ic={ic_val}, ann_ret={ann_ret})"
            )

        except Exception as e:
            err_msg = f"Loop {loop_id} 因子 {fname} 入库失败: {e}"
            errors.append(err_msg)
            logger.error(f"[{task_id}] {err_msg}")

    # 更新 aistock_task_catalog.sota_factors_count
    if inserted > 0:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE aistock_task_catalog
                        SET sota_factors_count = COALESCE(sota_factors_count, 0) + %s
                        WHERE task_id = %s
                    """, (inserted, task_id))
                    if cur.rowcount > 0:
                        logger.info(f"[{task_id}] sota_factors_count += {inserted}")
                    else:
                        logger.warning(f"[{task_id}] aistock_task_catalog 中未找到该 task")
        except Exception as e:
            errors.append(f"更新 sota_factors_count 失败: {e}")
            logger.error(f"[{task_id}] 更新 sota_factors_count 失败: {e}")

    logger.info(
        f"[{task_id}] Loop {loop_id} 因子同步完成: "
        f"{inserted} 入库, {dedup_skipped} 去重跳过, {len(errors)} 错误"
    )

    return FactorSyncResult(
        ok=len(errors) == 0,
        task_id=task_id,
        total_sota_factors=len(factor_names),
        inserted=inserted,
        updated=updated,
        dedup_skipped=dedup_skipped,
        errors=errors,
    )

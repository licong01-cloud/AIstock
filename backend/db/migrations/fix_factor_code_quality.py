#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量修复因子改造代码质量问题：
1. 移除 23 个因子中的 os.path.exists 文件检查残留
2. 解包 92 个因子中的 try-except 违规块
3. 清理孤立的 import os
4. 同步更新数据库 realtime_code_text 和磁盘 qe_code_path 文件

运行: conda run -n AIstock python backend/db/migrations/fix_factor_code_quality.py [--dry-run]
"""
import os
import re
import sys
import json
from datetime import datetime

# 项目根目录
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"))

import psycopg2
from backend.db.pg_pool import _db_cfg

DRY_RUN = "--dry-run" in sys.argv


# ── 修复函数 ──

def remove_os_path_exists_block(code: str) -> tuple[str, int]:
    """移除 os.path.exists/isfile 文件检查块。返回 (cleaned_code, removed_count)。"""
    # 匹配: [import os\n] if not os.path.exists("static_factors.parquet"): \n    raise ...
    pat = re.compile(
        r"^([ \t]*)(?:import\s+os\s*\n\1)?"
        r"if\s+not\s+os\.path\.(?:exists|isfile)\s*\(\s*['\"](?:\./)?[^'\"]+['\"]\s*\)\s*:\s*\n"
        r"(?:\1[ \t]+[^\n]*\n)*?"      # if 体内容（可能多行）
        r"\1[ \t]+[^\n]*\n?"           # if 体最后一行
        r"\s*\n?",                     # 尾随空行
        re.MULTILINE,
    )
    count = len(pat.findall(code))
    if count > 0:
        code = pat.sub("\n", code)
    return code, count


def unwrap_try_except(code: str) -> tuple[str, int]:
    """解包所有 try-except 块：保留 try 体（去一级缩进），移除 except/finally 块。"""
    lines = code.split("\n")
    result = []
    i = 0
    unwrap_count = 0
    while i < len(lines):
        stripped = lines[i].lstrip()
        if stripped == "try:" or stripped.startswith("try:  ") or stripped.startswith("try: #"):
            try_indent = len(lines[i]) - len(lines[i].lstrip())
            try_indent_str = lines[i][:try_indent]
            body_lines = []
            i += 1
            # 收集 try 体
            while i < len(lines):
                if lines[i].strip() == "":
                    body_lines.append("")
                    i += 1
                    continue
                cur_indent = len(lines[i]) - len(lines[i].lstrip())
                cur_stripped = lines[i].lstrip()
                if cur_indent == try_indent and (
                    cur_stripped.startswith("except") or cur_stripped.startswith("finally")
                ):
                    break
                body_lines.append(lines[i])
                i += 1
            # 跳过 except/finally 块
            if i < len(lines):
                handler_indent = try_indent
                i += 1  # 跳过 except/finally 行
                while i < len(lines):
                    if lines[i].strip() == "":
                        i += 1
                        continue
                    cur_indent = len(lines[i]) - len(lines[i].lstrip())
                    if cur_indent <= handler_indent:
                        break
                    i += 1
            # try 体去一级缩进
            for bl in body_lines:
                if bl == "":
                    result.append("")
                elif bl.startswith(try_indent_str + "    "):
                    result.append(try_indent_str + bl[try_indent + 4:])
                elif bl.startswith(try_indent_str + "\t"):
                    result.append(try_indent_str + bl[try_indent + 1:])
                else:
                    result.append(bl)
            unwrap_count += 1
        else:
            result.append(lines[i])
            i += 1
    return "\n".join(result), unwrap_count


def clean_orphan_import_os(code: str) -> tuple[str, bool]:
    """移除孤立的 import os（当代码中不再有其他 os. 引用时）。"""
    if not re.search(r"^\s*import\s+os\s*$", code, re.MULTILINE):
        return code, False
    code_without_import = re.sub(r"^\s*import\s+os\s*$", "", code, flags=re.MULTILINE)
    if not re.search(r"\bos\.", code_without_import):
        code = re.sub(r"^\s*import\s+os\s*\n", "", code, flags=re.MULTILINE)
        return code, True
    return code, False


def clean_excess_blank_lines(code: str) -> str:
    """连续超过2个空行合并为2个。"""
    return re.sub(r"\n{4,}", "\n\n\n", code)


def fix_factor_code(code: str) -> tuple[str, dict]:
    """应用所有修复。返回 (fixed_code, stats)。"""
    stats = {"os_path_removed": 0, "try_except_unwrapped": 0, "import_os_cleaned": False}

    code, n = remove_os_path_exists_block(code)
    stats["os_path_removed"] = n

    code, n = unwrap_try_except(code)
    stats["try_except_unwrapped"] = n

    code, cleaned = clean_orphan_import_os(code)
    stats["import_os_cleaned"] = cleaned

    code = clean_excess_blank_lines(code)
    return code, stats


# ── 主流程 ──

def main():
    conn = psycopg2.connect(**_db_cfg())
    conn.autocommit = False
    cur = conn.cursor()

    print(f"{'[DRY RUN] ' if DRY_RUN else ''}因子代码质量批量修复")
    print("=" * 70)

    # 查询所有需要修复的因子
    cur.execute("""
        SELECT factor_name, source, realtime_code_text, qe_code_path
        FROM aistock_factor_catalog
        WHERE transformation_status = 'SUCCESS'
          AND realtime_code_text IS NOT NULL
          AND (
              realtime_code_text LIKE '%%os.path%%'
              OR realtime_code_text ~ '\\mexcept\\s'
          )
        ORDER BY factor_name
    """)
    rows = cur.fetchall()
    print(f"找到 {len(rows)} 个需要检查的因子\n")

    total_fixed = 0
    total_os_path = 0
    total_try_except = 0
    total_import_os = 0
    db_updated = 0
    file_updated = 0
    file_failed = []
    fixed_factors = []

    for factor_name, source, code, qe_code_path in rows:
        fixed_code, stats = fix_factor_code(code)

        # 跳过无变化的因子
        if fixed_code == code:
            continue

        changes = []
        if stats["os_path_removed"] > 0:
            changes.append(f"os.path×{stats['os_path_removed']}")
            total_os_path += stats["os_path_removed"]
        if stats["try_except_unwrapped"] > 0:
            changes.append(f"try-except×{stats['try_except_unwrapped']}")
            total_try_except += stats["try_except_unwrapped"]
        if stats["import_os_cleaned"]:
            changes.append("import_os")
            total_import_os += 1

        total_fixed += 1
        fixed_factors.append(factor_name)
        change_str = ", ".join(changes)
        print(f"  [{total_fixed:3d}] {factor_name:50s} {change_str}")

        if DRY_RUN:
            continue

        # 更新数据库
        cur.execute("""
            UPDATE aistock_factor_catalog
            SET realtime_code_text = %s,
                last_transformation_at = NOW()
            WHERE factor_name = %s AND source = %s
        """, (fixed_code, factor_name, source))
        db_updated += cur.rowcount

        # 更新磁盘文件
        if qe_code_path:
            # qe_code_path 可能是相对路径或绝对路径
            fpath = qe_code_path if os.path.isabs(qe_code_path) else os.path.join(ROOT, qe_code_path)
            if os.path.isfile(fpath):
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(fixed_code)
                file_updated += 1
            else:
                # 尝试标准路径
                alt_path = os.path.join(ROOT, "rdagent_assets", "qe_factors", source, f"{factor_name}.py")
                if os.path.isfile(alt_path):
                    with open(alt_path, "w", encoding="utf-8") as f:
                        f.write(fixed_code)
                    file_updated += 1
                else:
                    file_failed.append((factor_name, qe_code_path))

    if not DRY_RUN:
        conn.commit()

    # ── 修复后验证 ──
    if not DRY_RUN and total_fixed > 0:
        print(f"\n{'=' * 70}")
        print("修复后验证...")
        cur.execute("""
            SELECT COUNT(*) FROM aistock_factor_catalog
            WHERE transformation_status = 'SUCCESS'
              AND realtime_code_text IS NOT NULL
              AND realtime_code_text LIKE '%%os.path%%'
        """)
        remaining_os = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM aistock_factor_catalog
            WHERE transformation_status = 'SUCCESS'
              AND realtime_code_text IS NOT NULL
              AND realtime_code_text ~ '\\mexcept\\s'
        """)
        remaining_except = cur.fetchone()[0]

        print(f"  残留 os.path 因子: {remaining_os}")
        print(f"  残留 try-except 因子: {remaining_except}")

    # ── 汇总 ──
    print(f"\n{'=' * 70}")
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}修复汇总:")
    print(f"  修复因子数: {total_fixed}")
    print(f"  os.path 块移除: {total_os_path}")
    print(f"  try-except 解包: {total_try_except}")
    print(f"  import os 清理: {total_import_os}")
    print(f"  数据库更新: {db_updated}")
    print(f"  文件更新: {file_updated}")
    if file_failed:
        print(f"  文件更新失败: {len(file_failed)}")
        for fn, path in file_failed[:10]:
            print(f"    {fn}: {path}")

    cur.close()
    conn.close()
    return total_fixed


if __name__ == "__main__":
    main()

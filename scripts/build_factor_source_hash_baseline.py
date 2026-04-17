"""一次性脚本：为现有因子缓存建立 source_hash 基线。

用途：
- 遍历 rdagent_assets/qe_factors/*.py 所有因子代码
- 对每个因子计算稳定 hash（AST 归一化：去除注释/docstring/空行/空白差异）
- 合并写回 rdagent_assets/factor_values/_meta.json 的 source_hash 字段

运行：
  conda activate qlib
  python scripts/build_factor_source_hash_baseline.py
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CODE_DIR = REPO_ROOT / "rdagent_assets" / "qe_factors"
CACHE_DIR = REPO_ROOT / "rdagent_assets" / "factor_values"
SINGLE_DIR = CACHE_DIR / "single"
META_PATH = CACHE_DIR / "_meta.json"


def compute_source_hash(code_path: Path) -> tuple[str, str]:
    """用 AST 归一化 + sha256 计算稳定 hash。

    Returns:
        (ast_hash, raw_hash): AST 归一化后的 hash 和原始源码 hash
    """
    raw = code_path.read_text(encoding="utf-8")
    raw_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    try:
        tree = ast.parse(raw)
        # 去除所有 docstring (Str 首节点在 Module/FunctionDef/ClassDef)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if (node.body and isinstance(node.body[0], ast.Expr)
                        and isinstance(node.body[0].value, ast.Constant)
                        and isinstance(node.body[0].value.value, str)):
                    node.body = node.body[1:] if len(node.body) > 1 else [ast.Pass()]
        normalized = ast.unparse(tree)
        # 标准化空白
        normalized = "\n".join(ln.rstrip() for ln in normalized.splitlines() if ln.strip())
        ast_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
    except SyntaxError as e:
        print(f"  [WARN] AST parse 失败 ({e}), 使用 raw hash", file=sys.stderr)
        ast_hash = raw_hash

    return ast_hash, raw_hash


def main():
    print("=" * 60)
    print("Factor Source Hash Baseline Builder")
    print("=" * 60)

    # 扫描因子代码
    if not CODE_DIR.exists():
        print(f"[ERROR] 代码目录不存在: {CODE_DIR}", file=sys.stderr)
        sys.exit(1)

    code_files = {f.stem: f for f in CODE_DIR.glob("*.py")}
    print(f"  代码文件: {len(code_files)} 个")

    # 扫描缓存
    if not SINGLE_DIR.exists():
        print(f"[ERROR] 缓存目录不存在: {SINGLE_DIR}", file=sys.stderr)
        sys.exit(1)
    cached_names = {f.stem for f in SINGLE_DIR.glob("*.parquet")}
    print(f"  缓存文件: {len(cached_names)} 个")

    # 读取现有 meta
    meta = {}
    if META_PATH.exists():
        meta = json.loads(META_PATH.read_text(encoding="utf-8"))
    factors_meta = meta.get("factors", {})
    print(f"  现有 meta: {len(factors_meta)} 条")
    print()

    # 处理每个因子
    updated = 0
    added_missing_meta = 0
    no_code = 0
    for name in sorted(cached_names):
        code_path = code_files.get(name)
        if code_path is None:
            no_code += 1
            continue

        ast_hash, raw_hash = compute_source_hash(code_path)

        entry = factors_meta.get(name)
        if entry is None:
            # meta 中缺失：补一条最小 meta
            parquet_path = SINGLE_DIR / f"{name}.parquet"
            entry = {
                "computed_at": datetime.fromtimestamp(
                    parquet_path.stat().st_mtime
                ).isoformat(),
                "rows": None,
                "date_range": "unknown",
                "as_of_date": None,
                "elapsed_sec": None,
            }
            added_missing_meta += 1

        entry["source_hash"] = ast_hash
        entry["source_hash_raw"] = raw_hash
        entry["source_hash_updated_at"] = datetime.now().isoformat()
        factors_meta[name] = entry
        updated += 1

    # 清理：meta 有但 parquet 没有的
    meta_only = set(factors_meta.keys()) - cached_names
    for name in meta_only:
        # 保留但标记
        factors_meta[name]["_status"] = "parquet_missing"

    # 写回 meta
    meta["factors"] = factors_meta
    meta["_source_hash_baseline_built_at"] = datetime.now().isoformat()
    META_PATH.write_text(
        json.dumps(meta, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("=" * 60)
    print("完成:")
    print(f"  更新 source_hash 的因子: {updated}")
    print(f"  补齐 meta 的因子: {added_missing_meta}")
    print(f"  缓存无对应代码 (跳过): {no_code}")
    print(f"  meta 有但 parquet 缺失 (标记): {len(meta_only)}")
    print(f"  baseline 写入: {META_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()

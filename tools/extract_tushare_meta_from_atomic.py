from __future__ import annotations

"""从 tushare_atomic_api.py 中提取接口元数据到 CSV。

- 使用 AST 解析 TushareAtomicClient 类下的各个方法
- 从方法名、docstring 中收集：
  - api_name（方法名，等于 pro.query 的 api 名）
  - doc_url（docstring 中以 "文档:" 开头的行）
  - doc_id（从 doc_url 中解析 ?doc_id=XXX）
  - description（docstring 第一行）
  - params_text（docstring 中以 "参数:" 开头的行）
  - returns_text（docstring 中以 "返回:" 开头的行）

输出: docs/tushare_apis_from_atomic.csv (UTF-8 BOM)
"""

from pathlib import Path
import ast
import csv
import re
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_FILE = PROJECT_ROOT / "tushare_atomic_api.py"
OUT_CSV = PROJECT_ROOT / "docs" / "tushare_apis_from_atomic.csv"

DOC_ID_RE = re.compile(r"doc_id=(\d+)")


def extract_from_docstring(doc: str) -> Dict[str, str]:
    lines = [ln.strip() for ln in doc.splitlines() if ln.strip()]
    description = ""
    doc_url = ""
    params = ""
    returns = ""
    for ln in lines:
        if not description and not ln.startswith("文档:"):
            # 第一行非空且不是文档行，视为描述
            description = ln
        if ln.startswith("文档:"):
            doc_url = ln.split("文档:", 1)[1].strip()
        elif ln.startswith("参数:"):
            params = ln.split("参数:", 1)[1].strip()
        elif ln.startswith("返回:"):
            returns = ln.split("返回:", 1)[1].strip()
    m = DOC_ID_RE.search(doc_url)
    doc_id = m.group(1) if m else ""
    return {
        "description": description,
        "doc_url": doc_url,
        "doc_id": doc_id,
        "params_text": params,
        "returns_text": returns,
    }


def extract_meta() -> List[Dict[str, str]]:
    if not SRC_FILE.exists():
        raise SystemExit(f"源文件不存在: {SRC_FILE}")
    src = SRC_FILE.read_text(encoding="utf-8", errors="ignore")
    tree = ast.parse(src, filename=str(SRC_FILE))

    records: List[Dict[str, str]] = []

    class_name = "TushareAtomicClient"
    target_class = None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            target_class = node
            break

    if target_class is None:
        raise SystemExit(f"未找到类 {class_name}")

    for item in target_class.body:
        if not isinstance(item, ast.FunctionDef):
            continue
        method_name = item.name
        # 跳过构造和工具方法
        if method_name in {"__init__", "call", "compose"}:
            continue
        docstring = ast.get_docstring(item) or ""
        info = extract_from_docstring(docstring) if docstring else {
            "description": "",
            "doc_url": "",
            "doc_id": "",
            "params_text": "",
            "returns_text": "",
        }
        records.append(
            {
                "api_name": method_name,
                "method_name": method_name,
                "doc_url": info["doc_url"],
                "doc_id": info["doc_id"],
                "description": info["description"],
                "params_text": info["params_text"],
                "returns_text": info["returns_text"],
            }
        )

    return records


def main() -> None:
    records = extract_meta()
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "api_name",
                "method_name",
                "doc_url",
                "doc_id",
                "description",
                "params_text",
                "returns_text",
            ],
        )
        writer.writeheader()
        writer.writerows(records)
    print(f"wrote {len(records)} records to {OUT_CSV}")


if __name__ == "__main__":
    main()

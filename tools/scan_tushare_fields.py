from __future__ import annotations

import ast
import csv
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TushareCallVisitor(ast.NodeVisitor):
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.calls: List[Dict[str, str]] = []

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        api_name = None
        if isinstance(func, ast.Attribute):
            if isinstance(func.value, ast.Name) and func.value.id in {"pro", "ts", "tushare_pro"}:
                api_name = func.attr
        if api_name:
            self.calls.append(
                {
                    "file": self.filename,
                    "lineno": str(node.lineno),
                    "api_object": getattr(func.value, "id", ""),
                    "api_name": api_name,
                }
            )
        self.generic_visit(node)


def scan_tushare_calls(root: Path) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    for path in root.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            continue
        visitor = TushareCallVisitor(str(path.relative_to(root)))
        visitor.visit(tree)
        results.extend(visitor.calls)
    return results


def write_csv(rows: List[Dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["file", "lineno", "api_object", "api_name"]
    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    rows = scan_tushare_calls(PROJECT_ROOT)
    out_path = PROJECT_ROOT / "docs" / "tushare_api_calls.csv"
    write_csv(rows, out_path)


if __name__ == "__main__":
    main()

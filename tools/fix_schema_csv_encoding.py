from __future__ import annotations

"""Rewrite schema CSV files with UTF-8 BOM encoding so Excel shows Chinese correctly.

This script rewrites a fixed list of CSV files using utf-8-sig.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FILES = [
    PROJECT_ROOT / "docs" / "data_schema_fields.csv",
    PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv",
]


def rewrite_utf8_sig(path: Path) -> None:
    if not path.exists():
        print(f"skip (not exists): {path}")
        return
    # 先按当前假定的 UTF-8 读取，再用 utf-8-sig 写回
    text = path.read_text(encoding="utf-8", errors="ignore")
    path.write_text(text, encoding="utf-8-sig", newline="")
    print(f"rewritten as utf-8-sig: {path}")


def main() -> None:
    for p in FILES:
        rewrite_utf8_sig(p)


if __name__ == "__main__":
    main()

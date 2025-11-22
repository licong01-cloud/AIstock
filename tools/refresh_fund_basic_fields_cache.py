from __future__ import annotations

from pathlib import Path
import sys

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

import tushare_docs_cache as tcache


def main() -> None:
    # 强制刷新 fund_basic(doc_id=43) 的字段信息到 tushare_fields_cache.json
    fields = tcache.get_interface_fields("fund_basic", doc_id=43, force_refresh=True)
    print(f"loaded fund_basic fields: {len(fields)}")


if __name__ == "__main__":
    main()

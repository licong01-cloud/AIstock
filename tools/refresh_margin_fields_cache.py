from __future__ import annotations

from pathlib import Path
import sys

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

import tushare_docs_cache as tcache


def main() -> None:
    # 融资融券汇总 margin(doc_id=58)
    fields_margin = tcache.get_interface_fields("margin", doc_id=58, force_refresh=True)
    print(f"loaded margin fields: {len(fields_margin)}")
    # 融资融券明细 margin_detail(doc_id=59)
    fields_detail = tcache.get_interface_fields("margin_detail", doc_id=59, force_refresh=True)
    print(f"loaded margin_detail fields: {len(fields_detail)}")


if __name__ == "__main__":
    main()

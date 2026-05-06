from __future__ import annotations

import ast
from pathlib import Path


def test_backfill_factor_cache_eligible_index_uses_tasks_dir_constant() -> None:
    source_path = Path("scripts/backfill_factor_cache.py")
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))

    loaded_names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }

    assert "TASK_DIR" not in loaded_names
    assert 'TASKS_DIR / f"{task_id}.eligible_index.parquet"' in source

"""Patch train.py: log oracle_gap + early stopping based on oracle_gap"""
import pathlib

path = pathlib.Path('/mnt/f/Dev/AIstock/scripts/rl_execution/train.py')
src = path.read_text()

# 1. 修改 validation 日志输出
old1 = '            logger.info(
                f"  Validation: avg_pa={val_result[\'avg_pa\']:.2f} bps, "
                f"n_eval={val_result[\'n_eval\']}"
            )'
new1 = '            logger.info(
                f"  Validation: avg_pa={val_result[\'avg_pa\']:.2f} bps, "
                f"oracle_gap={val_result[\'avg_oracle_gap\']:.2f} bps, "
                f"n_eval={val_result[\'n_eval\']}"
            )'

# 2. 初始化 best_oracle_gap
old2 = '    best_val_pa = -float("inf")'
new2 = '    best_val_pa = -float("inf")\n    best_oracle_gap = float("inf")  # oracle_gap 越小越好'

# 3. early stopping 判断
old3 = '            if val_result["avg_pa"] > best_val_pa:'
new3 = '            cur_oracle_gap = val_result["avg_oracle_gap"]\n            best_val_pa = max(best_val_pa, val_result["avg_pa"])\n            if cur_oracle_gap < best_oracle_gap:'

old4 = '                best_val_pa = val_result["avg_pa"]\n                patience_counter = 0'
new4 = '                best_oracle_gap = cur_oracle_gap\n                patience_counter = 0'

old5 = '                    "val_pa": best_val_pa,'
new5 = '                    "val_pa": val_result["avg_pa"],\n                    "oracle_gap": best_oracle_gap,'

old6 = '                logger.info(f"  New best model saved (PA={best_val_pa:.2f} bps)")'
new6 = '                logger.info(f"  New best model saved (oracle_gap={best_oracle_gap:.2f} bps, PA={val_result[\'avg_pa\']:.2f} bps)")'

for old, new, label in [
    (old1, new1, 'log output'),
    (old2, new2, 'best_oracle_gap init'),
    (old3, new3, 'early stop condition'),
    (old4, new4, 'best update'),
    (old5, new5, 'checkpoint save'),
    (old6, new6, 'log best model'),
]:
    if old not in src:
        print(f'ERROR: pattern not found: {label}')
        print(repr(old[:80]))
    else:
        src = src.replace(old, new, 1)
        print(f'OK: {label}')

path.write_text(src)
print('Done')

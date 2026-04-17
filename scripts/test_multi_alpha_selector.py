"""Test the Multi-Alpha Factor Selector."""
import os, sys
os.environ.setdefault("TDX_DB_PASSWORD", "lc78080808")
sys.path.insert(0, '/mnt/f/Dev/AIstock')

from backend.services.quantevolver.multi_alpha_selector import MultiAlphaFactorSelector

selector = MultiAlphaFactorSelector()

# Auto select with B+ grade, IC>0.02
config = selector.auto_select(min_grade="B", min_ic=0.02, execution_mode="distributed")

print("=== Auto-Selected Multi-Alpha Config ===")
print(f"Groups: {len(config.alpha_groups)}")
print(f"Meta: {config.meta_model.method}")
print(f"Execution: {config.execution_mode}")
print()

total_factors = 0
for g in config.alpha_groups:
    total_factors += len(g.factor_names)
    print(f"  {g.group_name}: {len(g.factor_names)} factors")
    print(f"    model={g.model_id}, ds={g.dataset_type}, device={g.compute_resource}")
    shown = g.factor_names[:5]
    extra = len(g.factor_names) - 5
    print(f"    factors: {shown}" + (f" + {extra} more" if extra > 0 else ""))
    print()

print(f"Total: {len(config.alpha_groups)} groups, {total_factors} factors")

# Preview
preview = selector.preview(config)
print(f"\nPreview: {preview['total_groups']} groups, {preview['total_factors']} factors")

# Also test with C grade / lower IC threshold
config2 = selector.auto_select(min_grade="C", min_ic=0.015)
total2 = sum(len(g.factor_names) for g in config2.alpha_groups)
print(f"\nWith C grade + IC>0.015: {len(config2.alpha_groups)} groups, {total2} factors")

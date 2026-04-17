"""Quick test for classify_data_source with real Qlib $ syntax."""
import sys
sys.path.insert(0, '/mnt/f/Dev/AIstock')
from backend.services.quantevolver.factor_analyst import (
    classify_data_source, _extract_fields_from_code
)

# Test 1: Qlib $ syntax only
expr = '(Rank($close) - Rank(Ref($close, 20))) / Ref($close, 20)'
fields = _extract_fields_from_code(None, expr)
print(f"Test 1 Qlib expr fields: {sorted(fields)}")
print(f"  → data_source_group: {classify_data_source(expression=expr, factor_name='mom_20')}")

# Test 2: mixed
expr2 = '($mf_main_net_amt - $volume) * Rank($close)'
fields2 = _extract_fields_from_code(None, expr2)
print(f"\nTest 2 mixed fields: {sorted(fields2)}")
print(f"  → data_source_group: {classify_data_source(expression=expr2)}")

# Test 3: Python with df indexing
code3 = '''
def factor(pv_df, mf_df):
    main_flow = mf_df["mf_main_net_amt"]
    vol = pv_df["volume"]
    return main_flow / vol
'''
fields3 = _extract_fields_from_code(code3, None)
print(f"\nTest 3 Python df fields: {sorted(fields3)}")
print(f"  → data_source_group: {classify_data_source(code_text=code3)}")

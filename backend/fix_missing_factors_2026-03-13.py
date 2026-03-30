#!/usr/bin/env python3
"""补录 Task 2026-03-13_12-46-42-151164 缺失的 4 个因子到数据库。"""
import sys
import os
sys.path.insert(0, 'F:/Dev/AIstock/backend')
os.chdir('F:/Dev/AIstock/backend')

from pathlib import Path
from dotenv import load_dotenv
load_dotenv('F:/Dev/AIstock/.env')

# 使用模块导入避免相对导入问题
import importlib.util
spec = importlib.util.spec_from_file_location("sync", "F:/Dev/AIstock/backend/services/rdagent_factor_catalog_sync.py")
sync_module = importlib.util.module_from_spec(spec)
sys.modules['sync'] = sync_module
spec.loader.exec_module(sync_module)

from services.rdagent_results_api_client import RDAgentResultsApiClient
sync_factors_from_task = sync_module.sync_factors_from_task

task_id = '2026-03-13_12-46-42-151164'
task_dir = f'F:/Dev/AIstock/rdagent_assets/rdagent_tasks/{task_id}'

print(f'=== 补录 Task {task_id} 缺失因子 ===\n')

# 1. 获取 v2_alignment_preview 数据
client = RDAgentResultsApiClient()
v2_preview = client.get_v2_alignment_preview(task_id)

if not v2_preview or not v2_preview.get('success'):
    print(f'❌ 获取 v2_alignment_preview 失败')
    sys.exit(1)

print(f'✓ v2_alignment_preview: {len(v2_preview.get("sota_factors", []))} 个 SOTA 因子')

# 2. 获取 sota_factor_anchor 数据
anchor_resp = client.get_sota_factor_anchor(task_id)

if not anchor_resp or not anchor_resp.get('ok'):
    print(f'❌ 获取 sota_factor_anchor 失败')
    sys.exit(1)

print(f'✓ sota_factor_anchor: resolved_factor_entry_key={anchor_resp.get("resolved_factor_entry_key")}')

# 3. 执行同步
print(f'\n开始同步...\n')
result = sync_factors_from_task(
    task_id=task_id,
    v2_preview_data=v2_preview,
    anchor_resp=anchor_resp,
    task_dir=task_dir,
)

# 4. 输出结果
print(f'\n=== 同步结果 ===')
print(f'状态: {"✓ 成功" if result.ok else "❌ 失败"}')
print(f'总 SOTA 因子: {result.total_sota_factors}')
print(f'入库/更新: {result.inserted}')
print(f'去重跳过: {result.dedup_skipped}')

if result.errors:
    print(f'\n错误 ({len(result.errors)} 个):')
    for err in result.errors:
        print(f'  - {err}')
else:
    print('\n✓ 无错误')

print(f'\n完成。')

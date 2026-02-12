"""
[已废弃] AIstock侧 - SOTA因子同步模块 (V1)

此模块已废弃。所有SOTA因子同步现在通过 rdagent_task_sync_service.py 中的
V2 API (v2_alignment_preview / complete_assets) 实现。

废弃原因：
1. V1 extractor API (/api/extractors/sota_factors/{task_id}) 存在幽灵因子问题
2. V1 逻辑依赖本地 model_meta.json，违反 API-only 通信原则
3. V2 API 基于 parquet schema 提供权威的因子顺序，数据一致性更高

保留此文件仅作为历史记录。请勿重新启用。
"""

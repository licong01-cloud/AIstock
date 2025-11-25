"""DeepAR 模型（训练与推理脚本），仅在 next_app.backend 目录下使用。

当前阶段：
- 提供日级 / 60m 频率的 DeepAR 风格 RNN 训练与推理入口；
- 只依赖 TimescaleDB 中已有的行情与高频聚合表；
- 不修改任何旧程序文件。
"""
from __future__ import annotations

__all__ = []

"""
HMM 数据源数据模型

所有数据模型使用 Pydantic 定义，提供运行时类型验证。
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class DataSourceConfig(BaseModel):
    """数据源配置"""

    mode: str = Field(..., description="数据源模式: backtest 或 realtime")

    # 回测模式参数
    base_loop_ref: Optional[str] = Field(None, description="QE loop 引用")
    cache_dir: str = Field("tmp/hmm_evolution_cache/", description="缓存目录")

    # 实时模式参数
    candidate_id: Optional[str] = Field(None, description="显式 HMM 研究候选 ID")
    snapshot_id: Optional[str] = Field(
        "latest",
        description="兼容字段；实时预测不得隐式解析 latest",
    )
    as_of_date: Optional[date] = Field(None, description="分析口径日期")
    lag_days: int = Field(1, ge=1, description="完成交易日延迟数")
    max_query_days: int = Field(730, ge=1, description="单次查询最大天数")

    @field_validator('mode')
    @classmethod
    def validate_mode(cls, v):
        if v not in ['backtest', 'realtime']:
            raise ValueError(f"mode must be 'backtest' or 'realtime', got '{v}'")
        return v

    @model_validator(mode="after")
    def validate_backtest_config(self):
        if self.mode == 'backtest' and not self.base_loop_ref:
            raise ValueError("base_loop_ref is required for backtest mode")
        if self.mode == 'realtime' and self.candidate_id is None:
            self.candidate_id = self.snapshot_id
        return self


class PredictionRecord(BaseModel):
    """单条预测记录"""

    trade_date: date
    symbol: str = Field(..., description="股票代码（含后缀）")
    score: float = Field(..., description="预测分数")
    rank: Optional[int] = Field(None, description="排名")


class LabelRecord(BaseModel):
    """单条标签记录"""

    trade_date: date = Field(..., description="T日")
    symbol: str
    horizon_days: int = Field(..., description="未来窗口（天）")
    future_return: float = Field(..., description="未来收益率")
    label_date: date = Field(..., description="标签日期（T+horizon）")


class SectorMapping(BaseModel):
    """板块映射"""

    trade_date: date
    mapping: dict[str, str] = Field(..., description="{symbol: sector_code}")


class DateRange(BaseModel):
    """日期范围"""

    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_date_order(self):
        if self.end_date < self.start_date:
            raise ValueError(f"end_date {self.end_date} must be >= start_date {self.start_date}")
        return self


class CacheInfo(BaseModel):
    """缓存信息"""

    loop_ref: str
    cache_dir: str
    artifacts: dict[str, dict]
    total_size: int = Field(..., description="总大小（bytes）")

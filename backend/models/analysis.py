from typing import Any, Dict, List, Literal

from datetime import datetime

from pydantic import BaseModel


class StockAnalysisRequest(BaseModel):
    ts_code: str
    start_date: str | None = None  # YYYY-MM-DD，可选
    end_date: str | None = None  # YYYY-MM-DD，可选
    holding_price: float | None = None
    holding_ratio: float | None = None
    # 前端可选传入启用的分析师配置；如不传则使用后端默认配置
    enabled_analysts: Dict[str, bool] | None = None


class AgentOpinion(BaseModel):
    name: str
    summary: str
    score: float | None = None


class StockAnalysisResponse(BaseModel):
    ts_code: str
    agents: List[AgentOpinion]
    conclusion: str
    # 追加原始多智能体分析结果，便于前端渲染详细报告
    agents_raw: Dict[str, Any] | None = None
    discussion: str | None = None
    final_decision: Dict[str, Any] | None = None
    data_fetch_diagnostics: Dict[str, Any] | None = None
    technical_indicators: Dict[str, Any] | None = None
    # 与旧版 pg_stock_analysis_repo 对齐的持久化信息
    record_id: int | None = None
    saved_to_db: bool | None = None


class StockKlineSeries(BaseModel):
    dates: List[str]
    open: List[float | None]
    high: List[float | None]
    low: List[float | None]
    close: List[float | None]


class StockQuote(BaseModel):
    symbol: str
    name: str
    current_price: float | None = None
    change_percent: float | None = None
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    pre_close: float | None = None
    volume: float | None = None
    amount: float | None = None
    quote_source: str | None = None
    quote_timestamp: datetime | None = None
    week52_high: float | None = None
    week52_low: float | None = None


class StockContextResponse(BaseModel):
    ts_code: str
    name: str
    quote: StockQuote | None = None
    kline: StockKlineSeries | None = None


class BatchStockAnalysisRequest(BaseModel):
    """批量分析请求，复用单股分析的大部分参数。

    - ts_codes: 待分析的股票代码列表（6位代码或 ts_code 均可，由后端统一转换）；
    - start_date/end_date: 全局时间范围，可选；
    - enabled_analysts: 分析师启用配置，在整个批次内共享；
    - batch_mode: sequential 或 parallel，对应旧版“顺序分析”和“多线程并行”。
    """

    ts_codes: List[str]
    start_date: str | None = None
    end_date: str | None = None
    enabled_analysts: Dict[str, bool] | None = None
    batch_mode: str = "sequential"


class BatchStockAnalysisItemResult(BaseModel):
    ts_code: str
    success: bool
    error: str | None = None
    analysis: StockAnalysisResponse | None = None


class BatchStockAnalysisResponse(BaseModel):
    total: int
    success_count: int
    failed_count: int
    results: List[BatchStockAnalysisItemResult]


class TrendPredictionScenario(BaseModel):
    """Single price-move scenario for a given horizon.

    Used by the stock trend analysis module to describe a possible
    future return range and its probability.
    """

    direction: Literal["up", "down", "flat"]
    magnitude_min_pct: float
    magnitude_max_pct: float
    probability: float
    label: str
    narrative: str


class TrendPredictionHorizon(BaseModel):
    """Prediction for a specific time horizon (1d / 1w / 1m / long)."""

    horizon: Literal["1d", "1w", "1m", "long"]
    scenarios: List[TrendPredictionScenario]
    base_expectation_pct: float | None = None


class TrendAnalystResult(BaseModel):
    """Single analyst's trend analysis report and structured conclusion."""

    name: str
    role: str
    raw_text: str
    conclusion_json: Dict[str, Any]
    created_at: datetime


class PredictionStep(BaseModel):
    """Snapshot of predictions after each analyst's adjustment.

    This is used to visualise how forecasts evolve step‑by‑step on the
    frontend charts.
    """

    step: int
    analyst_key: str
    analyst_name: str
    description: str
    horizons: List[TrendPredictionHorizon]
    created_at: datetime


class StockTrendAnalysisRequest(BaseModel):
    """Request payload for the stock trend analysis service.

    This is intentionally separate from StockAnalysisRequest so that the
    new trend module can evolve independently while reusing the same
    data access layer.
    """

    ts_code: str
    start_date: str | None = None
    end_date: str | None = None
    analysis_date: str | None = None
    enabled_analysts: Dict[str, bool] | None = None
    mode: Literal["realtime", "backtest"] = "realtime"


class StockTrendAnalysisResponse(BaseModel):
    """Response payload returned by the stock trend analysis service."""

    ts_code: str
    analysis_date: str
    mode: Literal["realtime", "backtest"]
    horizons: List[TrendPredictionHorizon]
    analysts: List[TrendAnalystResult]
    risk_report: TrendAnalystResult | None = None
    prediction_evolution: List[PredictionStep]
    record_id: int | None = None
    data_fetch_diagnostics: Dict[str, Any] | None = None
    technical_indicators: Dict[str, Any] | None = None
    rating: str | None = None


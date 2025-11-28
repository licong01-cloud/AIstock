from __future__ import annotations

from datetime import datetime, timedelta
import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..db.pg_pool import get_conn
from ..quant_datasets.lstm_dataset import (
    LSTMDatasetConfig,
    load_lstm_timeseries_for_symbol,
)
from ..quant_datasets.deepar_dataset import (
    DeepARDatasetConfig,
    load_deepar_daily_for_symbol,
    load_deepar_60m_for_symbol,
)


class DataScope(BaseModel):
    intraday_history_days: int = Field(..., ge=0)
    daily_history_years: float = Field(..., ge=0)
    need_high_freq_features: bool = True


class QuantAnalystRequest(BaseModel):
    symbol: str
    as_of_time: datetime
    horizons: List[str]
    frequencies: List[str]
    data_scope: DataScope
    use_models: Optional[List[str]] = None
    include_raw_model_outputs: bool = True


class UnifiedSignal(BaseModel):
    frequency: str
    horizon: str
    direction: Optional[str] = None
    prob_up: Optional[float] = None
    prob_down: Optional[float] = None
    prob_flat: Optional[float] = None
    confidence: Optional[float] = None
    expected_return: Optional[float] = None
    expected_volatility: Optional[float] = None
    risk_score: Optional[float] = None
    regime: Optional[str] = None
    liquidity_label: Optional[str] = None
    microstructure_label: Optional[str] = None
    anomaly_flags: Optional[Dict[str, Any]] = None
    suggested_position_delta: Optional[float] = None
    suggested_t0_action: Optional[str] = None
    model_votes: Optional[Dict[str, Any]] = None
    ensemble_method: Optional[str] = None
    data_coverage: Optional[Dict[str, Any]] = None
    model_versions: Optional[Dict[str, Any]] = None
    quality_flags: Optional[Dict[str, Any]] = None


class ARIMAHorizonForecast(BaseModel):
    horizon: str
    expected_return: float
    direction: Optional[str] = None


class ARIMAOutput(BaseModel):
    """Structured ARIMA forecast output for a single symbol.

    Matches quant_analyst_design.md §9.5 中对 ARIMA 的角色：
    - 为下一个/若干个时间步提供基线预测，用于方向与期望收益的集成。
    """

    symbol: str
    as_of_time: datetime
    freq: str
    forecasts: List[ARIMAHorizonForecast]
    meta: Optional[Dict[str, Any]] = None


class HMMOutput(BaseModel):
    """Structured HMM regime output for a single symbol.

    对应设计文档中 HMM 识别行情状态/regime 的角色，用于 UnifiedSignal 中的
    `regime` 与部分风险信息。
    """

    symbol: str
    as_of_time: datetime
    freq: str
    regime: Optional[str] = None
    regime_probabilities: Optional[Dict[str, float]] = None
    meta: Optional[Dict[str, Any]] = None


class LSTMHorizonForecast(BaseModel):
    horizon: str
    expected_return: float
    direction: Optional[str] = None
    prob_up: Optional[float] = None
    prob_down: Optional[float] = None
    prob_flat: Optional[float] = None


class LSTMOutput(BaseModel):
    """Structured per-symbol LSTM forecast.

    对应 §9.5 `infer_lstm_for_symbol` / `LSTMOutput`：
    - 以 5m 为主频率（可扩展到 15m/60m），
    - 针对若干 horizon（如 next_5m / next_3x5m）给出方向与期望收益预测。
    """

    symbol: str
    as_of_time: datetime
    freq: str
    forecasts: List[LSTMHorizonForecast]
    meta: Optional[Dict[str, Any]] = None


class DeepARHorizonForecast(BaseModel):
    horizon: str
    expected_return: float
    expected_volatility: float
    direction: Optional[str] = None
    prob_up: Optional[float] = None
    prob_down: Optional[float] = None
    prob_flat: Optional[float] = None


class DeepAROutput(BaseModel):
    """Structured DeepAR forecast output.

    对应 §9.6 `infer_deepar_for_symbol` / `DeepAROutput`：
    - 日级/60m 粒度，
    - 输出多 horizon 的 (mu, sigma) 及派生方向/概率信息。
    """

    symbol: str
    as_of_time: datetime
    freq: str
    forecasts: List[DeepARHorizonForecast]
    meta: Optional[Dict[str, Any]] = None


class QuantAnalystResponse(BaseModel):
    symbol: str
    as_of_time: datetime
    signals: List[UnifiedSignal]
    raw_model_outputs: Optional[Dict[str, Any]] = None


def run_quant_analyst(request: QuantAnalystRequest) -> QuantAnalystResponse:
    if not request.horizons:
        raise ValueError("horizons must not be empty")
    if not request.frequencies:
        raise ValueError("frequencies must not be empty")

    # Overall flow mirrors quant_analyst_design.md §9.9:
    # 1) 数据准备；2) 调用各模型；3) 构建统一信号列表；4) 组装输出。

    data_bundle = load_data_for_quant(request)

    arima_out = run_arima_if_enabled(request, data_bundle)
    hmm_out = run_hmm_if_enabled(request, data_bundle)
    lstm_out = run_lstm_if_enabled(request, data_bundle)
    deepar_out = run_deepar_if_enabled(request, data_bundle)

    signals: List[UnifiedSignal] = []
    for freq in request.frequencies:
        for horizon in request.horizons:
            sig = build_unified_signal(
                symbol=request.symbol,
                as_of_time=request.as_of_time,
                freq=freq,
                horizon=horizon,
                arima_out=arima_out,
                hmm_out=hmm_out,
                lstm_out=lstm_out,
                deepar_out=deepar_out,
            )
            signals.append(sig)

    raw_outputs: Optional[Dict[str, Any]]
    if request.include_raw_model_outputs:
        raw_outputs = {
            "ARIMA": arima_out,
            "HMM": hmm_out,
            "LSTM": lstm_out,
            "DEEPAR": deepar_out,
        }
    else:
        raw_outputs = None

    return QuantAnalystResponse(
        symbol=request.symbol,
        as_of_time=request.as_of_time,
        signals=signals,
        raw_model_outputs=raw_outputs,
    )


def load_data_for_quant(request: QuantAnalystRequest) -> Dict[str, Any]:
    """Load all data required by Quant Analyst according to the design doc.

    实现要点（对应 quant_analyst_design.md §9.1–9.3）:
    - 基于 data_scope.intraday_history_days / daily_history_years 计算历史窗口；
    - 对 5m 频率，复用 LSTMDataset 构建 5 分钟高频特征序列；
    - 对 1d/60m 频率，复用 DeepARDataset 构建日级/60m 特征序列；
    - 是否包含高频特征由 data_scope.need_high_freq_features 控制。

    返回的数据包是一个结构化的 dict，供后续各模型使用：
    {
      "ts_code": "600000.SH",
      "as_of_time": datetime,
      "lstm_5m": DataFrame | None,
      "deepar_daily": DataFrame | None,
      "deepar_60m": DataFrame | None,
    }
    """

    if request.data_scope is None:
        raise ValueError("data_scope must be provided in QuantAnalystRequest")

    ts_code = _normalize_symbol_to_ts_code(request.symbol)
    as_of_time = request.as_of_time

    result: Dict[str, Any] = {
        "ts_code": ts_code,
        "as_of_time": as_of_time,
    }

    scope = request.data_scope
    intraday_days = int(scope.intraday_history_days or 0)
    daily_years = float(scope.daily_history_years or 0.0)
    need_hf = bool(scope.need_high_freq_features)

    freqs = {f.lower() for f in request.frequencies}

    # 1) 5m LSTM 输入：使用 LSTMDataset 构建 5m 时序特征
    if "5m" in freqs and intraday_days > 0:
        start_dt = as_of_time - timedelta(days=intraday_days)
        if start_dt >= as_of_time:
            # 确保窗口为正
            start_dt = as_of_time - timedelta(days=1)

        lstm_cfg = LSTMDatasetConfig(include_trade_agg=need_hf)
        lstm_df = load_lstm_timeseries_for_symbol(
            ts_code=ts_code,
            start=start_dt,
            end=as_of_time,
            config=lstm_cfg,
        )
        result["lstm_5m"] = lstm_df

    # 2) 日级 DeepAR 输入
    if any(f in freqs for f in {"1d", "d", "daily"}) and daily_years > 0:
        days = int(daily_years * 365)
        if days <= 0:
            days = 365
        start_date = (as_of_time.date() - timedelta(days=days))
        end_date = as_of_time.date()

        deepar_cfg = DeepARDatasetConfig(include_hf_factors=need_hf)
        daily_df = load_deepar_daily_for_symbol(
            ts_code=ts_code,
            start=start_date,
            end=end_date,
            config=deepar_cfg,
        )
        result["deepar_daily"] = daily_df

    # 3) 60m DeepAR 输入（可选）
    if any(f in freqs for f in {"60m", "1h"}) and daily_years > 0:
        days_60m = int(daily_years * 365)
        if days_60m <= 0:
            days_60m = 365
        start_dt_60m = as_of_time - timedelta(days=days_60m)
        if start_dt_60m >= as_of_time:
            start_dt_60m = as_of_time - timedelta(days=1)

        deepar_cfg_60m = DeepARDatasetConfig(include_hf_factors=need_hf)
        df_60m = load_deepar_60m_for_symbol(
            ts_code=ts_code,
            start=start_dt_60m,
            end=as_of_time,
            config=deepar_cfg_60m,
        )
        result["deepar_60m"] = df_60m

    return result


def _normalize_symbol_to_ts_code(symbol: str) -> str:
    """Convert symbol like SH600000/SZ000001 to ts_code like 600000.SH.

    若传入的已经是 ts_code 形式（包含点号，如 600000.SH），则直接返回。
    """

    text = (symbol or "").strip().upper()
    if not text:
        raise ValueError("symbol must not be empty")

    if "." in text:
        return text

    if len(text) != 8:
        raise ValueError(f"unsupported symbol format: {symbol!r}")

    prefix = text[:2]
    code = text[2:]
    if prefix not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"unsupported symbol prefix in {symbol!r}")

    suffix = prefix  # SH/SZ/BJ -> SH/SZ/BJ
    return f"{code}.{suffix}"


def _model_enabled(name: str, request: QuantAnalystRequest) -> bool:
    models = request.use_models
    if not models:
        return True
    upper = {m.upper() for m in models}
    return name.upper() in upper


def run_arima_if_enabled(
    request: QuantAnalystRequest,
    data_bundle: Dict[str, Any],
) -> Optional[ARIMAOutput]:
    """Run ARIMA model if enabled in request.use_models.

    The actual ARIMA implementation will be integrated later; this helper
    currently only defines the call surface and raises NotImplementedError
    when the model is enabled.
    """

    if not _model_enabled("ARIMA", request):
        return None
    raise NotImplementedError(
        "run_arima_if_enabled is not implemented yet; "
        "integrate ARIMA inference according to quant_analyst_design.md."
    )


def run_hmm_if_enabled(
    request: QuantAnalystRequest,
    data_bundle: Dict[str, Any],
) -> Optional[HMMOutput]:
    """Run HMM model if enabled in request.use_models.

    The concrete HMM implementation will be added later; for now this
    function only defines the structure and raises NotImplementedError
    when HMM is requested.
    """

    if not _model_enabled("HMM", request):
        return None
    raise NotImplementedError(
        "run_hmm_if_enabled is not implemented yet; "
        "integrate HMM-based regime detection according to the design doc."
    )


def run_lstm_if_enabled(
    request: QuantAnalystRequest,
    data_bundle: Dict[str, Any],
) -> Optional[LSTMOutput]:
    """Run LSTM models (per-stock/shared) if enabled.

    This helper is expected to dispatch to the appropriate LSTM inference
    scripts under backend.quant_models.lstm.* based on model configuration
    and data_bundle. For now it only defines the API surface and raises
    NotImplementedError when LSTM is requested.
    """

    if not _model_enabled("LSTM", request):
        return None
    raise NotImplementedError(
        "run_lstm_if_enabled is not implemented yet; "
        "wire it to backend.quant_models.lstm.infer_* as per the design."
    )


def run_deepar_if_enabled(
    request: QuantAnalystRequest,
    data_bundle: Dict[str, Any],
) -> Optional[DeepAROutput]:
    """Run DeepAR model if enabled.

    The actual DeepAR inference (daily/60m) will be integrated later; this
    function currently only declares the interface and raises
    NotImplementedError when DeepAR is requested.
    """

    if not _model_enabled("DEEPAR", request):
        return None
    raise NotImplementedError(
        "run_deepar_if_enabled is not implemented yet; "
        "wire it to backend.quant_models.deepar.infer according to the design."
    )


def build_unified_signal(
    symbol: str,
    as_of_time: datetime,
    freq: str,
    horizon: str,
    arima_out: Any,
    hmm_out: Any,
    lstm_out: Any,
    deepar_out: Any,
) -> UnifiedSignal:
    """Aggregate model outputs into a UnifiedSignal.

    The aggregation logic (direction/probabilities/expected_return/risk)
    should follow quant_analyst_design.md §9.7, using helper functions like
    aggregate_direction/aggregate_return_vol/extract_regime_and_risk and
    build_model_votes.

    To avoid any simplified placeholder logic, this function currently
    raises NotImplementedError and will be implemented together with the
    full model integration.
    """

    raise NotImplementedError(
        "build_unified_signal is not implemented yet; "
        "implement aggregation according to quant_analyst_design.md §9.7."
    )


def _json_or_none(value: Optional[Dict[str, Any]]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def persist_unified_signals(
    symbol: str,
    as_of_time: datetime,
    signals: List[UnifiedSignal],
) -> None:
    """Upsert a batch of UnifiedSignal rows into app.quant_unified_signal.

    Schema is defined in backend.db.init_quant_schema.DDL and mirrors
    quant_analyst_design.md §8.1.
    """

    if not signals:
        return

    sql = """
        INSERT INTO app.quant_unified_signal (
            symbol,
            as_of_time,
            frequency,
            horizon,
            direction,
            prob_up,
            prob_down,
            prob_flat,
            confidence,
            expected_return,
            expected_volatility,
            risk_score,
            regime,
            liquidity_label,
            microstructure_label,
            anomaly_flags,
            suggested_position_delta,
            suggested_t0_action,
            model_votes,
            ensemble_method,
            data_coverage,
            model_versions,
            quality_flags
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (symbol, as_of_time, frequency, horizon)
        DO UPDATE SET
            direction = EXCLUDED.direction,
            prob_up = EXCLUDED.prob_up,
            prob_down = EXCLUDED.prob_down,
            prob_flat = EXCLUDED.prob_flat,
            confidence = EXCLUDED.confidence,
            expected_return = EXCLUDED.expected_return,
            expected_volatility = EXCLUDED.expected_volatility,
            risk_score = EXCLUDED.risk_score,
            regime = EXCLUDED.regime,
            liquidity_label = EXCLUDED.liquidity_label,
            microstructure_label = EXCLUDED.microstructure_label,
            anomaly_flags = EXCLUDED.anomaly_flags,
            suggested_position_delta = EXCLUDED.suggested_position_delta,
            suggested_t0_action = EXCLUDED.suggested_t0_action,
            model_votes = EXCLUDED.model_votes,
            ensemble_method = EXCLUDED.ensemble_method,
            data_coverage = EXCLUDED.data_coverage,
            model_versions = EXCLUDED.model_versions,
            quality_flags = EXCLUDED.quality_flags,
            updated_at = NOW()
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sig in signals:
                cur.execute(
                    sql,
                    (
                        symbol,
                        as_of_time,
                        sig.frequency,
                        sig.horizon,
                        sig.direction,
                        sig.prob_up,
                        sig.prob_down,
                        sig.prob_flat,
                        sig.confidence,
                        sig.expected_return,
                        sig.expected_volatility,
                        sig.risk_score,
                        sig.regime,
                        sig.liquidity_label,
                        sig.microstructure_label,
                        _json_or_none(sig.anomaly_flags),
                        sig.suggested_position_delta,
                        sig.suggested_t0_action,
                        _json_or_none(sig.model_votes),
                        sig.ensemble_method,
                        _json_or_none(sig.data_coverage),
                        _json_or_none(sig.model_versions),
                        _json_or_none(sig.quality_flags),
                    ),
                )


def load_unified_signals(
    symbol: str,
    limit: int = 200,
) -> List[UnifiedSignal]:
    """Load recent unified signals for a symbol from app.quant_unified_signal.

    This helper focuses on storage semantics; higher-level selection logic
    (by frequency/horizon/as_of_time) should be handled by callers.
    """

    sql = """
        SELECT
            as_of_time,
            frequency,
            horizon,
            direction,
            prob_up,
            prob_down,
            prob_flat,
            confidence,
            expected_return,
            expected_volatility,
            risk_score,
            regime,
            liquidity_label,
            microstructure_label,
            anomaly_flags,
            suggested_position_delta,
            suggested_t0_action,
            model_votes,
            ensemble_method,
            data_coverage,
            model_versions,
            quality_flags
        FROM app.quant_unified_signal
        WHERE symbol = %s
        ORDER BY as_of_time DESC
        LIMIT %s
    """

    results: List[UnifiedSignal] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (symbol, limit))
            rows = cur.fetchall()
    for row in rows:
        (
            as_of_time,
            frequency,
            horizon,
            direction,
            prob_up,
            prob_down,
            prob_flat,
            confidence,
            expected_return,
            expected_volatility,
            risk_score,
            regime,
            liquidity_label,
            microstructure_label,
            anomaly_flags,
            suggested_position_delta,
            suggested_t0_action,
            model_votes,
            ensemble_method,
            data_coverage,
            model_versions,
            quality_flags,
        ) = row
        results.append(
            UnifiedSignal(
                frequency=frequency,
                horizon=horizon,
                direction=direction,
                prob_up=prob_up,
                prob_down=prob_down,
                prob_flat=prob_flat,
                confidence=confidence,
                expected_return=float(expected_return) if expected_return is not None else None,
                expected_volatility=float(expected_volatility) if expected_volatility is not None else None,
                risk_score=risk_score,
                regime=regime,
                liquidity_label=liquidity_label,
                microstructure_label=microstructure_label,
                anomaly_flags=anomaly_flags,
                suggested_position_delta=float(suggested_position_delta)
                if suggested_position_delta is not None
                else None,
                suggested_t0_action=suggested_t0_action,
                model_votes=model_votes,
                ensemble_method=ensemble_method,
                data_coverage=data_coverage,
                model_versions=model_versions,
                quality_flags=quality_flags,
            )
        )
    return results

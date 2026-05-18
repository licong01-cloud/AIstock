"""
Data Preprocessor Module

REQ-PREPROC-P3-001: Implement data preprocessing module for AIstock inference.
This module provides data preprocessing capabilities to ensure consistency
between RD-Agent model training and AIstock online inference.

预计算因子服务：
负责计算所有预计算派生字段，对齐RDAgent侧 generate_static_factors_bundle.py 的计算逻辑。

字段清单：
- 估值因子: value_pe_inv, value_pb_inv
- 规模因子: size_log_mv
- 流动性因子: liquidity_turnover, liquidity_vol_ratio
- 资金流净值: mf_total_net_amt, mf_total_net_vol, mf_main_net_amt, mf_main_net_vol, mf_elg_net_amt, mf_elg_net_vol
- 资金流强度: mf_*_ratio
- 滚动聚合: mf_*_5d, mf_*_20d
- 价格动量: PriceStrength_10D
"""

import logging
import re
from typing import Any, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# 预计算因子相关常量和函数
# ============================================================

# 必需的预计算字段列表
REQUIRED_PRECOMPUTED_FIELDS = [
    # 估值/规模/流动性
    'value_pe_inv', 'value_pb_inv', 'size_log_mv',
    'liquidity_turnover', 'liquidity_vol_ratio',
    # 资金流净值
    'mf_total_net_amt', 'mf_total_net_vol',
    'mf_main_net_amt', 'mf_main_net_vol',
    'mf_elg_net_amt', 'mf_elg_net_vol',
    # 资金流强度
    'mf_total_net_amt_ratio', 'mf_total_net_vol_ratio',
    'mf_main_net_amt_ratio', 'mf_main_net_vol_ratio',
    'mf_elg_net_amt_ratio', 'mf_elg_net_vol_ratio',
    'mf_elg_share_in_main_amt', 'mf_elg_share_in_main_vol',
    # 滚动聚合
    'mf_total_net_amt_5d', 'mf_total_net_amt_20d',
    'mf_main_net_amt_5d', 'mf_main_net_amt_20d',
    'mf_elg_net_amt_5d', 'mf_elg_net_amt_20d',
    'mf_total_net_amt_ratio_5d', 'mf_total_net_amt_ratio_20d',
    'mf_main_net_amt_ratio_5d', 'mf_main_net_amt_ratio_20d',
    'mf_elg_net_amt_ratio_5d', 'mf_elg_net_amt_ratio_20d',
    # 价格动量
    'PriceStrength_10D',
]


def _safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """安全除法，分母为0或NaN时返回NaN"""
    denom_safe = denom.replace(0, np.nan)
    return numer / denom_safe


def _rolling_sum_by_instrument(s: pd.Series, window: int) -> pd.Series:
    """按股票分组计算滚动和，保持MultiIndex对齐"""
    if s.empty:
        return s
    return (
        s.groupby(level='instrument')
        .rolling(window=window, min_periods=window)
        .sum()
        .reset_index(level=0, drop=True)
    )


def compute_precomputed_factors(
    df_fund_raw: pd.DataFrame,
    df_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    计算所有预计算因子字段

    对齐RDAgent侧 generate_static_factors_bundle.py 和 precompute_daily_basic_factors.py 的计算逻辑。

    Args:
        df_fund_raw: 从数据库获取的原始基本面+资金流数据
                     索引: MultiIndex(datetime, instrument)
                     列: db_*, mf_* 原始字段
        df_history: 从数据库获取的OHLCV行情数据
                    索引: MultiIndex(datetime, instrument)
                    列: open, high, low, close, volume, amount

    Returns:
        df_fund: 包含原始字段 + 所有预计算派生字段的DataFrame
    """
    if df_fund_raw.empty:
        logger.warning("df_fund_raw 为空，跳过预计算字段计算")
        return df_fund_raw

    df = df_fund_raw.copy()

    # 确保索引对齐
    if not df.index.equals(df_history.index):
        # 取交集
        common_idx = df.index.intersection(df_history.index)
        if len(common_idx) == 0:
            logger.error("df_fund_raw 和 df_history 索引无交集")
            return df
        df = df.loc[common_idx]
        df_history_aligned = df_history.loc[common_idx]
    else:
        df_history_aligned = df_history

    # 获取行情数据中的 amount 和 volume
    amount = df_history_aligned['amount'] if 'amount' in df_history_aligned.columns else None
    volume = df_history_aligned['volume'] if 'volume' in df_history_aligned.columns else None
    close = df_history_aligned['close'] if 'close' in df_history_aligned.columns else None

    # ========== 1. 估值因子 ==========
    # 对齐 precompute_daily_basic_factors.py:45-51
    if 'db_pe_ttm' in df.columns:
        df['value_pe_inv'] = 1.0 / df['db_pe_ttm'].replace(0, np.nan)
        logger.debug("✓ 已计算 value_pe_inv（基于 db_pe_ttm）")
    elif 'db_pe' in df.columns:
        df['value_pe_inv'] = 1.0 / df['db_pe'].replace(0, np.nan)
        logger.debug("✓ 已计算 value_pe_inv（基于 db_pe）")

    if 'db_pb' in df.columns:
        df['value_pb_inv'] = 1.0 / df['db_pb'].replace(0, np.nan)
        logger.debug("✓ 已计算 value_pb_inv")

    # ========== 2. 规模因子 ==========
    # 对齐 precompute_daily_basic_factors.py:54-60
    mv_col = None
    for c in ['db_circ_mv', 'db_total_mv']:
        if c in df.columns:
            mv_col = c
            break
    if mv_col is not None:
        df['size_log_mv'] = np.log(df[mv_col].where(df[mv_col] > 0)).replace(-np.inf, np.nan)
        logger.debug(f"✓ 已计算 size_log_mv（基于 {mv_col}）")

    # ========== 3. 流动性因子 ==========
    # 对齐 precompute_daily_basic_factors.py:63-67
    if 'db_turnover_rate' in df.columns:
        df['liquidity_turnover'] = df['db_turnover_rate']
        logger.debug("✓ 已计算 liquidity_turnover")

    if 'db_volume_ratio' in df.columns:
        df['liquidity_vol_ratio'] = df['db_volume_ratio']
        logger.debug("✓ 已计算 liquidity_vol_ratio")

    # ========== 4. 资金流净值字段 ==========
    # 对齐 generate_static_factors_bundle.py:164-179

    # 全档净流入
    if 'mf_net_amt' in df.columns:
        df['mf_total_net_amt'] = df['mf_net_amt']
    if 'mf_net_vol' in df.columns:
        df['mf_total_net_vol'] = df['mf_net_vol']

    # 主力净流入（大单+特大单）
    lg_buy_amt = df.get('mf_lg_buy_amt', pd.Series(0, index=df.index))
    lg_sell_amt = df.get('mf_lg_sell_amt', pd.Series(0, index=df.index))
    elg_buy_amt = df.get('mf_elg_buy_amt', pd.Series(0, index=df.index))
    elg_sell_amt = df.get('mf_elg_sell_amt', pd.Series(0, index=df.index))

    lg_buy_vol = df.get('mf_lg_buy_vol', pd.Series(0, index=df.index))
    lg_sell_vol = df.get('mf_lg_sell_vol', pd.Series(0, index=df.index))
    elg_buy_vol = df.get('mf_elg_buy_vol', pd.Series(0, index=df.index))
    elg_sell_vol = df.get('mf_elg_sell_vol', pd.Series(0, index=df.index))

    df['mf_main_net_amt'] = (lg_buy_amt + elg_buy_amt) - (lg_sell_amt + elg_sell_amt)
    df['mf_main_net_vol'] = (lg_buy_vol + elg_buy_vol) - (lg_sell_vol + elg_sell_vol)

    # 特大单净流入
    df['mf_elg_net_amt'] = elg_buy_amt - elg_sell_amt
    df['mf_elg_net_vol'] = elg_buy_vol - elg_sell_vol

    logger.debug("✓ 已计算资金流净值字段")

    # ========== 5. 资金流强度字段 ==========
    # 对齐 generate_static_factors_bundle.py:184-198

    if amount is not None:
        df['mf_total_net_amt_ratio'] = _safe_div(df['mf_total_net_amt'], amount)
        df['mf_main_net_amt_ratio'] = _safe_div(df['mf_main_net_amt'], amount)
        df['mf_elg_net_amt_ratio'] = _safe_div(df['mf_elg_net_amt'], amount)

    if volume is not None:
        df['mf_total_net_vol_ratio'] = _safe_div(df['mf_total_net_vol'], volume)
        df['mf_main_net_vol_ratio'] = _safe_div(df['mf_main_net_vol'], volume)
        df['mf_elg_net_vol_ratio'] = _safe_div(df['mf_elg_net_vol'], volume)

    # 特大单占主力比例
    df['mf_elg_share_in_main_amt'] = _safe_div(df['mf_elg_net_amt'], df['mf_main_net_amt'])
    df['mf_elg_share_in_main_vol'] = _safe_div(df['mf_elg_net_vol'], df['mf_main_net_vol'])

    logger.debug("✓ 已计算资金流强度字段")

    # ========== 6. 滚动聚合字段（5D/20D）==========
    # 对齐 generate_static_factors_bundle.py:200-208
    # 关键：先求和再算比率，而不是先算比率再求和

    for w in [5, 20]:
        suffix = f'{w}d'

        # 净流入金额滚动和
        df[f'mf_total_net_amt_{suffix}'] = _rolling_sum_by_instrument(df['mf_total_net_amt'], w)
        df[f'mf_main_net_amt_{suffix}'] = _rolling_sum_by_instrument(df['mf_main_net_amt'], w)
        df[f'mf_elg_net_amt_{suffix}'] = _rolling_sum_by_instrument(df['mf_elg_net_amt'], w)

        # 成交额滚动和（用于计算强度）
        if amount is not None:
            amount_w = _rolling_sum_by_instrument(amount, w)

            # 强度 = 滚动净流入 / 滚动成交额（先求和再算比率）
            df[f'mf_total_net_amt_ratio_{suffix}'] = _safe_div(df[f'mf_total_net_amt_{suffix}'], amount_w)
            df[f'mf_main_net_amt_ratio_{suffix}'] = _safe_div(df[f'mf_main_net_amt_{suffix}'], amount_w)
            df[f'mf_elg_net_amt_ratio_{suffix}'] = _safe_div(df[f'mf_elg_net_amt_{suffix}'], amount_w)

    logger.debug("✓ 已计算滚动聚合字段")

    # ========== 7. 价格动量字段 ==========
    if close is not None:
        df['PriceStrength_10D'] = close.groupby(level='instrument').pct_change(10)
        logger.debug("✓ 已计算 PriceStrength_10D")

    logger.info(f"预计算因子计算完成，共 {len(df.columns)} 列")

    return df


def validate_precomputed_factors(df: pd.DataFrame) -> tuple[bool, List[str]]:
    """
    验证预计算字段是否完整

    Args:
        df: 包含预计算字段的DataFrame

    Returns:
        (is_valid, missing_fields): 是否有效，缺失的字段列表
    """
    missing = [f for f in REQUIRED_PRECOMPUTED_FIELDS if f not in df.columns]
    is_valid = len(missing) == 0

    if not is_valid:
        logger.warning(f"预计算字段缺失 {len(missing)} 个: {missing}")
    else:
        logger.info("预计算字段验证通过")

    return is_valid, missing


# Alpha158因子的窗口需求。ROC60 需要 t 和 t-60 两端价格，因此是 61 个交易日。
_ALPHA158_FACTOR_WINDOWS = {
    "ROC60": 61,
    "RSQR60": 60,
    "CORR60": 60,
    "CORD60": 60,
    "WVMA60": 60,
    "RESI60": 60,
}

# 动态因子常见命名：m_turnover_percentile_250d、PriceStrength_120D、roc120d。
_LOOKBACK_DAY_PATTERN = re.compile(r"(?<!\d)(\d{1,4})[dD](?![A-Za-z0-9])")


def infer_factor_lookback_days(factor_name: str) -> int:
    """从因子名称推断该因子需要的最小交易日窗口。"""

    factor_text = str(factor_name or "").strip()
    factor_upper = factor_text.upper()
    for alpha_name, window in _ALPHA158_FACTOR_WINDOWS.items():
        if alpha_name in factor_upper:
            return window

    lookbacks = [int(match.group(1)) for match in _LOOKBACK_DAY_PATTERN.finditer(factor_text)]
    if lookbacks:
        return max(20, max(lookbacks))

    return 20


def get_required_data_window(factor_order: Optional[List[str]] = None) -> int:
    """
    根据因子列表计算所需的最小数据窗口（交易日数）

    Args:
        factor_order: 因子名称列表，如果为None则返回默认值

    Returns:
        所需的最小交易日数
    """
    # 如果没有指定因子列表，保持历史默认值，覆盖已知 Alpha158 最大窗口。
    if not factor_order:
        return 61

    return max(infer_factor_lookback_days(factor) for factor in factor_order)


def check_data_window_sufficient(
    df_history: pd.DataFrame,
    required_window: int,
    buffer_days: int = 5,
) -> tuple[bool, int, str]:
    """
    检查数据窗口是否足够

    Args:
        df_history: 历史数据DataFrame
        required_window: 所需的最小交易日数
        buffer_days: 安全余量天数

    Returns:
        (is_sufficient, actual_days, message): 是否足够，实际天数，提示信息
    """
    if df_history.empty:
        return False, 0, "历史数据为空"

    # 计算实际交易日数
    dates = df_history.index.get_level_values('datetime').unique()
    actual_days = len(dates)

    required_with_buffer = required_window + buffer_days
    is_sufficient = actual_days >= required_with_buffer

    if is_sufficient:
        message = f"数据窗口充足：实际 {actual_days} 天 >= 所需 {required_with_buffer} 天"
    else:
        message = (
            f"⚠️ 数据窗口不足：实际 {actual_days} 天 < 所需 {required_with_buffer} 天 "
            f"(因子需要 {required_window} 天 + 安全余量 {buffer_days} 天)。"
            f"建议按交易日历加载至少 {required_with_buffer} 个交易日的数据。"
        )
        logger.warning(message)

    return is_sufficient, actual_days, message


# ============================================================
# 原有的 DataPreprocessor 类
# ============================================================


class DataPreprocessor:
    """Data preprocessing module for AIstock inference.

    This module provides data preprocessing capabilities to ensure consistency
    between RD-Agent model training and AIstock online inference.

    REQ-PREPROC-P3-001: Implement data preprocessing module.
    """

    def apply_model_preprocess(
        self,
        df: pd.DataFrame,
        preprocess_config: dict[str, Any]
    ) -> pd.DataFrame:
        """Apply model training preprocessing configuration to data.

        Args:
            df: Input DataFrame with features
            preprocess_config: Preprocessing configuration from model_meta.json

        Returns:
            Preprocessed DataFrame

        Example:
            >>> config = {
            ...     "normalize": "zscore",
            ...     "fillna": "forward_fill",
            ...     "clip": [-3, 3],
            ...     "standardize_features": True
            ... }
            >>> preprocessor = DataPreprocessor()
            >>> df_processed = preprocessor.apply_model_preprocess(df, config)
        """
        if not preprocess_config:
            logger.warning("No preprocessing config provided, returning original data")
            return df.copy()

        df_result = df.copy()

        # Step 1: Fill missing values
        fillna_method = preprocess_config.get("fillna", "forward_fill")
        df_result = self._apply_fillna(df_result, fillna_method)
        logger.info(f"Applied fillna method: {fillna_method}")

        # Step 2: Normalize features
        normalize_method = preprocess_config.get("normalize", "none")
        if normalize_method != "none":
            df_result = self._apply_normalize(df_result, normalize_method)
            logger.info(f"Applied normalization method: {normalize_method}")

        # Step 3: Clip outliers
        clip_range = preprocess_config.get("clip")
        if clip_range and isinstance(clip_range, (list, tuple)) and len(clip_range) == 2:
            df_result = self._apply_clip(df_result, clip_range)
            logger.info(f"Applied clip range: {clip_range}")

        # Step 4: Standardize features if requested
        standardize = preprocess_config.get("standardize_features", False)
        if standardize:
            df_result = self._standardize_features(df_result)
            logger.info("Applied feature standardization")

        return df_result

    def validate_factor_input(
        self,
        df: pd.DataFrame,
        input_schema: dict[str, Any]
    ) -> bool:
        """Validate factor input data completeness.

        REQ-VALIDATION-P3-001: Validate input data before factor calculation.

        Args:
            df: Input DataFrame with market data
            input_schema: Input schema from factor_meta.json

        Returns:
            True if validation passes

        Raises:
            ValueError: If required fields are missing or data is invalid

        Example:
            >>> schema = {
            ...     "required_fields": ["open", "high", "low", "close", "volume"],
            ...     "optional_fields": ["amount", "pct_chg"],
            ...     "lookback_days": 10
            ... }
            >>> preprocessor = DataPreprocessor()
            >>> preprocessor.validate_factor_input(df, schema)
        """
        if not input_schema:
            logger.warning("No input schema provided, skipping validation")
            return True

        required_fields = input_schema.get("required_fields", [])
        if not required_fields:
            logger.warning("No required fields specified in schema")
            return True

        # Check for missing required fields
        available_fields = df.columns.tolist()
        missing_fields = [f for f in required_fields if f not in available_fields]

        if missing_fields:
            raise ValueError(
                f"因子输入数据缺少必需字段: {missing_fields}. "
                f"当前可用字段: {available_fields}"
            )

        # Check for empty DataFrame
        if df.empty:
            raise ValueError("因子输入数据为空")

        # Check lookback window
        lookback_days = input_schema.get("lookback_days", 0)
        if lookback_days > 0:
            unique_dates = df.index.get_level_values(0).unique() if isinstance(df.index, pd.MultiIndex) else df.index.unique()
            if len(unique_dates) < lookback_days:
                logger.warning(
                    f"数据窗口长度 ({len(unique_dates)}) 小于要求的回溯天数 ({lookback_days})"
                )

        logger.info(f"Factor input validation passed: {len(required_fields)} required fields present")
        return True

    def apply_feature_standardization(
        self,
        df: pd.DataFrame,
        feature_schema: list[str] | None = None
    ) -> pd.DataFrame:
        """Apply feature standardization based on feature schema.

        Args:
            df: Input DataFrame with features
            feature_schema: List of feature names to standardize

        Returns:
            Standardized DataFrame
        """
        if feature_schema:
            # Only standardize features in the schema
            features_to_standardize = [f for f in feature_schema if f in df.columns]
            if features_to_standardize:
                df_result = df.copy()
                for feature in features_to_standardize:
                    mean = df_result[feature].mean()
                    std = df_result[feature].std()
                    if std != 0:
                        df_result[feature] = (df_result[feature] - mean) / std
                logger.info(f"Standardized {len(features_to_standardize)} features")
                return df_result
        else:
            # Standardize all numeric columns
            df_result = df.copy()
            numeric_cols = df_result.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                mean = df_result[col].mean()
                std = df_result[col].std()
                if std != 0:
                    df_result[col] = (df_result[col] - mean) / std
            logger.info(f"Standardized {len(numeric_cols)} numeric columns")
            return df_result

        return df.copy()

    def detect_and_handle_outliers(
        self,
        df: pd.DataFrame,
        clip_range: tuple[float, float] | None = None
    ) -> pd.DataFrame:
        """Detect and handle outliers in the data.

        Args:
            df: Input DataFrame
            clip_range: Tuple of (min, max) values for clipping

        Returns:
            DataFrame with outliers handled
        """
        if clip_range:
            return self._apply_clip(df, clip_range)

        # If no clip range provided, use IQR method
        df_result = df.copy()
        numeric_cols = df_result.select_dtypes(include=['number']).columns

        for col in numeric_cols:
            Q1 = df_result[col].quantile(0.25)
            Q3 = df_result[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers = (df_result[col] < lower_bound) | (df_result[col] > upper_bound)
            outlier_count = outliers.sum()
            if outlier_count > 0:
                logger.warning(
                    f"Detected {outlier_count} outliers in column '{col}', "
                    f"clipping to [{lower_bound:.2f}, {upper_bound:.2f}]"
                )
                df_result[col] = df_result[col].clip(lower_bound, upper_bound)

        return df_result

    def check_data_quality(
        self,
        df: pd.DataFrame
    ) -> dict[str, Any]:
        """Check data quality and return a report.

        Args:
            df: Input DataFrame

        Returns:
            Dictionary containing quality metrics
        """
        quality_report = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "missing_ratio": df.isnull().sum().to_dict(),
            "outlier_count": self._detect_outliers(df),
            "data_type_consistency": self._check_dtypes(df),
            "duplicate_rows": df.duplicated().sum(),
        }

        logger.info(f"Data quality check completed: {quality_report['total_rows']} rows, "
                    f"{quality_report['total_columns']} columns")

        return quality_report

    def _apply_normalize(
        self,
        df: pd.DataFrame,
        method: str
    ) -> pd.DataFrame:
        """Apply normalization to DataFrame.

        Args:
            df: Input DataFrame
            method: Normalization method (zscore, minmax, none)

        Returns:
            Normalized DataFrame
        """
        df_result = df.copy()
        numeric_cols = df_result.select_dtypes(include=['number']).columns

        if method == "zscore":
            for col in numeric_cols:
                mean = df_result[col].mean()
                std = df_result[col].std()
                if std != 0:
                    df_result[col] = (df_result[col] - mean) / std
        elif method == "minmax":
            for col in numeric_cols:
                min_val = df_result[col].min()
                max_val = df_result[col].max()
                if max_val != min_val:
                    df_result[col] = (df_result[col] - min_val) / (max_val - min_val)
        else:
            logger.warning(f"Unknown normalization method: {method}")

        return df_result

    def _apply_fillna(
        self,
        df: pd.DataFrame,
        method: str
    ) -> pd.DataFrame:
        """Apply missing value filling to DataFrame.

        Args:
            df: Input DataFrame
            method: Fill method (forward_fill, backward_fill, mean, zero, none)

        Returns:
            DataFrame with missing values filled
        """
        df_result = df.copy()

        if method == "forward_fill":
            df_result = df_result.fillna(method="ffill")
        elif method == "backward_fill":
            df_result = df_result.fillna(method="bfill")
        elif method == "mean":
            numeric_cols = df_result.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                df_result[col] = df_result[col].fillna(df_result[col].mean())
        elif method == "zero":
            df_result = df_result.fillna(0)
        elif method == "none":
            pass
        else:
            logger.warning(f"Unknown fillna method: {method}")

        return df_result

    def _apply_clip(
        self,
        df: pd.DataFrame,
        clip_range: tuple[float, float]
    ) -> pd.DataFrame:
        """Apply clipping to DataFrame.

        Args:
            df: Input DataFrame
            clip_range: Tuple of (min, max) values

        Returns:
            Clipped DataFrame
        """
        df_result = df.copy()
        numeric_cols = df_result.select_dtypes(include=['number']).columns

        for col in numeric_cols:
            df_result[col] = df_result[col].clip(*clip_range)

        return df_result

    def _standardize_features(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """Standardize features to zero mean and unit variance.

        Args:
            df: Input DataFrame

        Returns:
            Standardized DataFrame
        """
        df_result = df.copy()
        numeric_cols = df_result.select_dtypes(include=['number']).columns

        for col in numeric_cols:
            mean = df_result[col].mean()
            std = df_result[col].std()
            if std != 0:
                df_result[col] = (df_result[col] - mean) / std

        return df_result

    def _detect_outliers(
        self,
        df: pd.DataFrame
    ) -> dict[str, int]:
        """Detect outliers using IQR method.

        Args:
            df: Input DataFrame

        Returns:
            Dictionary mapping column names to outlier counts
        """
        outlier_counts = {}
        numeric_cols = df.select_dtypes(include=['number']).columns

        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_counts[col] = outliers.sum()

        return outlier_counts

    def _check_dtypes(
        self,
        df: pd.DataFrame
    ) -> dict[str, str]:
        """Check data type consistency.

        Args:
            df: Input DataFrame

        Returns:
            Dictionary mapping column names to data types
        """
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

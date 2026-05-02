"""
QE (QuantEvolver) 专用的自定义数据加载器

这个文件是 AIstock/QuantEvolver 项目独立维护的，不影响 RDAgent 的原有功能。
RDAgent 使用 rdagent/scenarios/qlib/experiment/custom_loaders.py
QE 使用这个独立的 qe_custom_loaders.py
"""
from typing import Optional
import pandas as pd


_LABEL_FIELDS = {
    "close": "$close",
    "open": "$open",
    "vwap": "$vwap",
}
_ALLOWED_LABEL_HORIZONS = {1, 3, 5, 10, 20}


class DynamicFactorsOnlyLoader:
    """仅加载动态因子 parquet 的数据加载器（QE 专用版本）。
    
    用于 disable_alpha158=True 时，仅使用自定义因子进行回测。
    与 StaticDataLoader 不同，此类忽略 instruments 参数，
    直接加载 parquet 中所有数据，避免 KeyError: 'all' 错误。
    
    同时从 QLib provider 按 label_type / label_horizon 加载 label 数据，
    确保返回的 DataFrame 包含 feature 和 label 列。
    """
    
    def __init__(
        self,
        dynamic_path: str,
        enforce_instrument_format: bool = True,
        label_type: str = "close",
        label_horizon: int = 1,
    ) -> None:
        self.dynamic_path = dynamic_path
        self.enforce_instrument_format = bool(enforce_instrument_format)
        self.label_type = self._normalize_label_type(label_type)
        self.label_horizon = self._normalize_label_horizon(label_horizon)
        self.label_expr = self.build_label_expr(self.label_type, self.label_horizon)

    @staticmethod
    def _normalize_label_type(label_type: str) -> str:
        value = str(label_type or "close").strip().lower()
        if value not in _LABEL_FIELDS:
            raise ValueError(
                f"label_type={label_type!r} invalid, must be one of {sorted(_LABEL_FIELDS)}"
            )
        return value

    @staticmethod
    def _normalize_label_horizon(label_horizon: int) -> int:
        try:
            value = int(label_horizon)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"label_horizon={label_horizon!r} invalid, must be one of {sorted(_ALLOWED_LABEL_HORIZONS)}"
            ) from exc
        if value not in _ALLOWED_LABEL_HORIZONS:
            raise ValueError(
                f"label_horizon={label_horizon!r} invalid, must be one of {sorted(_ALLOWED_LABEL_HORIZONS)}"
            )
        return value

    @classmethod
    def build_label_expr(cls, label_type: str = "close", label_horizon: int = 1) -> str:
        label_type = cls._normalize_label_type(label_type)
        label_horizon = cls._normalize_label_horizon(label_horizon)
        label_field = _LABEL_FIELDS[label_type]
        return f"Ref({label_field}, -{label_horizon + 1}) / Ref({label_field}, -1) - 1"
    
    @staticmethod
    def _ensure_datetime_instrument_index(df: pd.DataFrame) -> pd.DataFrame:
        """确保索引为 MultiIndex(datetime, instrument)"""
        if df is None:
            return df
        
        if not isinstance(df.index, pd.MultiIndex) or set(df.index.names) != {"datetime", "instrument"}:
            if {"datetime", "instrument"}.issubset(df.columns):
                df = df.copy()
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df = df.set_index(["datetime", "instrument"])
        
        if isinstance(df.index, pd.MultiIndex):
            names = list(df.index.names)
            if set(names) == {"datetime", "instrument"} and names != ["datetime", "instrument"]:
                df = df.swaplevel("datetime", "instrument")
            df = df.sort_index()
        
        return df
    
    @staticmethod
    def _ensure_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
        """确保列为 MultiIndex(feature, ...)"""
        if df is None:
            return df
        
        if not isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = pd.MultiIndex.from_product([["feature"], df.columns.astype(str)])
            return df
        
        if df.columns.nlevels == 1:
            df = df.copy()
            df.columns = pd.MultiIndex.from_product([["feature"], df.columns.astype(str)])
            return df
        
        level0 = df.columns.get_level_values(0).astype(str)
        if not level0.isin(["feature", "label"]).all():
            df = df.copy()
            df.columns = pd.MultiIndex.from_product([["feature"], df.columns.get_level_values(-1).astype(str)])
        return df
    
    @staticmethod
    def _validate_instrument_format(instruments: pd.Index) -> None:
        """验证 instrument 格式为 '000001.SZ' 或 '600000.SH'"""
        if instruments is None:
            return
        s = pd.Index(instruments.astype(str))
        ok = s.str.match(r"^\d{6}\.(SZ|SH)$")
        if not bool(ok.all()):
            bad = s[~ok][:20].tolist()
            raise ValueError(
                "Invalid instrument format detected in dynamic factors. "
                "Expected like '000001.SZ' or '600000.SH'. "
                f"Examples: {bad}"
            )
    
    def load(
        self,
        instruments: Optional[object] = None,
        start_time: Optional[object] = None,
        end_time: Optional[object] = None,
    ) -> pd.DataFrame:
        """加载动态因子数据并添加label列。
        
        注意：忽略 instruments 参数，直接加载 parquet 中所有数据。
        这是为了避免 StaticDataLoader 的 KeyError: 'all' 问题。
        同时从 QLib provider 加载 label 数据。
        """
        # 1. 加载因子 parquet
        df = pd.read_parquet(self.dynamic_path)
        
        # 确保索引格式正确
        df = self._ensure_datetime_instrument_index(df)
        df = self._ensure_feature_columns(df)
        
        if df is None or df.empty:
            raise ValueError("Dynamic factors parquet is empty.")
        
        if not isinstance(df.index, pd.MultiIndex) or set(df.index.names) != {"datetime", "instrument"}:
            raise ValueError(
                "Dynamic factors parquet must be indexed by MultiIndex(datetime, instrument). "
                "Either write parquet with the correct MultiIndex, or include 'datetime' and 'instrument' columns."
            )
        
        # 验证 instrument 格式
        if self.enforce_instrument_format:
            self._validate_instrument_format(df.index.get_level_values("instrument"))
        
        # 时间范围过滤 - 使用 boolean mask 避免 IndexSlice 字符串切片的 InvalidIndexError
        if start_time is not None or end_time is not None:
            dt_level = df.index.get_level_values("datetime")
            mask = pd.Series(True, index=df.index)
            if start_time is not None:
                start_dt = pd.to_datetime(str(start_time))
                mask = mask & (dt_level >= start_dt)
            if end_time is not None:
                end_dt = pd.to_datetime(str(end_time))
                mask = mask & (dt_level <= end_dt)
            df = df.loc[mask.values]
        
        # 2. 从 QLib provider 加载 label 数据
        # Label 定义: Ref($close, -2) / Ref($close, -1) - 1
        try:
            from qlib.data import D
            
            # 获取所有唯一的 instruments
            unique_instruments = df.index.get_level_values("instrument").unique().tolist()
            
            # 使用 QLib 的 D.features 加载与配置一致的训练 label
            label_expr = f"({self.label_expr})"
            
            label_df = D.features(
                instruments=unique_instruments,
                fields=[label_expr],
                start_time=start_time,
                end_time=end_time,
            )
            
            # 确保 label_df 的 datetime 索引格式与 df 一致
            # QLib 返回的是 pd.Timestamp，需要统一为 pd.Timestamp
            if isinstance(label_df.index, pd.MultiIndex):
                # 确保 datetime 层级是 pd.Timestamp 类型
                dt_level = label_df.index.get_level_values("datetime")
                if not isinstance(dt_level, pd.DatetimeIndex):
                    # 转换为 DatetimeIndex
                    label_df = label_df.reset_index()
                    label_df["datetime"] = pd.to_datetime(label_df["datetime"])
                    label_df = label_df.set_index(["datetime", "instrument"])
            
            # 同样确保 df 的 datetime 索引是 pd.Timestamp 类型
            if isinstance(df.index, pd.MultiIndex):
                dt_level = df.index.get_level_values("datetime")
                if not isinstance(dt_level, pd.DatetimeIndex):
                    df = df.reset_index()
                    df["datetime"] = pd.to_datetime(df["datetime"])
                    df = df.set_index(["datetime", "instrument"])
            
            # 重命名列为 label
            if isinstance(label_df.columns, pd.MultiIndex):
                # 如果已经是 MultiIndex，修改第一层为 'label'
                label_df.columns = pd.MultiIndex.from_product([["label"], ["LABEL0"]])
            else:
                # 否则创建 MultiIndex
                label_df.columns = pd.MultiIndex.from_product([["label"], ["LABEL0"]])
            
            # 3. 合并 feature 和 label
            # 关键修复：使用 DataFrame.join() 而不是 pd.concat()
            # join() 方法会保持左侧 DataFrame 的索引结构不变，避免 MultiIndex 丢失
            # 使用 how='left' 确保保留所有 factor 数据，label 数据按索引对齐
            df = df.join(label_df, how='left')
            
        except Exception as e:
            raise RuntimeError(
                "Failed to load label data from QLib provider for "
                f"label_type={self.label_type!r}, label_horizon={self.label_horizon}, "
                f"label_expr={self.label_expr!r}"
            ) from e
        
        return df.sort_index()

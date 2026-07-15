"""
QE (QuantEvolver) 专用的自定义数据加载器

这个文件是 AIstock/QuantEvolver 项目独立维护的，不影响 RDAgent 的原有功能。
RDAgent 使用 rdagent/scenarios/qlib/experiment/custom_loaders.py
QE 使用这个独立的 qe_custom_loaders.py
"""
from typing import Optional
import pandas as pd

try:
    from qlib.data.dataset.processor import Processor as _QlibProcessor
except ModuleNotFoundError as exc:  # Windows API/unit-test process does not install Qlib.
    if not str(exc.name or "").startswith("qlib"):
        raise

    class _QlibProcessor:  # type: ignore[no-redef]
        """Import-only stand-in; experiment workers always load Qlib's Processor."""


_LABEL_FIELDS = {
    "close": "$close",
    "open": "$open",
    "vwap": "$vwap",
}
_ALLOWED_LABEL_HORIZONS = {1, 3, 5, 10, 20, 30, 40, 60, 120, 180}
_LONG_HORIZON_MIN = 30


class CSRightTailBinaryLabel(_QlibProcessor):
    """Convert a mature forward-return label into a daily right-tail target.

    The transformation is learn-only and uses only labels on the same signal
    date.  Missing/immature labels remain missing so ``DropnaLabel`` retains
    the existing maturity semantics.  The exact percentile and tie policy are
    stored on the processor and therefore become part of the Qlib task config.
    """

    def __init__(self, *, quantile: float = 0.99) -> None:
        try:
            parsed = float(quantile)
        except (TypeError, ValueError) as exc:
            raise ValueError("CSRightTailBinaryLabel.quantile must be numeric") from exc
        if not 0.0 < parsed < 1.0:
            raise ValueError("CSRightTailBinaryLabel.quantile must be strictly between 0 and 1")
        self.quantile = parsed
        self.transform_summary: dict[str, object] = {}

    @staticmethod
    def _label_columns(df: pd.DataFrame) -> list[object]:
        if isinstance(df.columns, pd.MultiIndex):
            return [column for column in df.columns if str(column[0]).lower() == "label"]
        return [column for column in df.columns if str(column).lower().startswith("label")]

    def is_for_infer(self) -> bool:
        return False

    def readonly(self) -> bool:
        return False

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            raise ValueError("CSRightTailBinaryLabel received empty learning data")
        if not isinstance(df.index, pd.MultiIndex) or "datetime" not in df.index.names:
            raise ValueError("CSRightTailBinaryLabel requires MultiIndex containing datetime")
        label_columns = self._label_columns(df)
        if not label_columns:
            raise ValueError("CSRightTailBinaryLabel found no label columns")

        datetimes = pd.DatetimeIndex(
            pd.to_datetime(df.index.get_level_values("datetime"))
        ).normalize()
        result = df.copy()
        valid_rows = 0
        positive_rows = 0
        per_column: dict[str, dict[str, int]] = {}
        for column in label_columns:
            numeric = pd.to_numeric(result[column], errors="coerce")
            original_non_null = result[column].notna()
            invalid = original_non_null & numeric.isna()
            if bool(invalid.any()):
                raise ValueError(
                    f"CSRightTailBinaryLabel found non-numeric label values in {column!r}"
                )
            percentile_rank = numeric.groupby(datetimes, sort=False).rank(
                method="average",
                pct=True,
            )
            valid = numeric.notna()
            transformed = pd.Series(float("nan"), index=result.index, dtype="float64")
            transformed.loc[valid] = (percentile_rank.loc[valid] > self.quantile).astype("float64")
            result.loc[:, column] = transformed
            column_valid = int(valid.sum())
            column_positive = int(transformed.eq(1.0).sum())
            valid_rows += column_valid
            positive_rows += column_positive
            per_column[str(column)] = {
                "valid_rows": column_valid,
                "positive_rows": column_positive,
            }

        self.transform_summary = {
            "objective": "cs_top_quantile_return",
            "quantile": self.quantile,
            "tie_policy": "daily_average_percentile_rank_strictly_greater_than_quantile",
            "valid_rows": valid_rows,
            "positive_rows": positive_rows,
            "per_column": per_column,
        }
        return result


class LongHorizonLabelMaturityPurge(_QlibProcessor):
    """Remove immature learning labels without truncating inference signals.

    Qlib keeps separate inference and learning frames.  This processor is
    deliberately learn-only: it masks the last ``label_horizon + 1`` trading
    observations of each train/valid/test segment, then the following
    ``DropnaLabel`` processor removes those rows from label-dependent model
    fitting and metric calculation.  The inference frame remains untouched, so
    predictions and portfolio backtests still cover the complete test segment.
    """

    def __init__(
        self,
        *,
        label_horizon: int,
        train_start: str,
        train_end: str,
        valid_start: str,
        valid_end: str,
        test_start: str,
        test_end: str,
    ) -> None:
        self.label_horizon = DynamicFactorsOnlyLoader._normalize_label_horizon(label_horizon)
        if self.label_horizon < _LONG_HORIZON_MIN:
            raise ValueError(
                "LongHorizonLabelMaturityPurge is reserved for label_horizon >= "
                f"{_LONG_HORIZON_MIN}, got {self.label_horizon}"
            )
        self.segment_bounds = {
            "train": (self._date(train_start, "train_start"), self._date(train_end, "train_end")),
            "valid": (self._date(valid_start, "valid_start"), self._date(valid_end, "valid_end")),
            "test": (self._date(test_start, "test_start"), self._date(test_end, "test_end")),
        }
        self._validate_segment_bounds()
        self.purge_summary: dict[str, dict[str, object]] = {}

    @staticmethod
    def _date(value: str, field_name: str) -> pd.Timestamp:
        try:
            parsed = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be a valid date, got {value!r}") from exc
        if pd.isna(parsed):
            raise ValueError(f"{field_name} must be a valid date, got {value!r}")
        return parsed.normalize()

    def _validate_segment_bounds(self) -> None:
        for name, (start, end) in self.segment_bounds.items():
            if start > end:
                raise ValueError(f"{name}_start must not be later than {name}_end")
        if self.segment_bounds["train"][1] >= self.segment_bounds["valid"][0]:
            raise ValueError("train_end must be earlier than valid_start")
        if self.segment_bounds["valid"][1] >= self.segment_bounds["test"][0]:
            raise ValueError("valid_end must be earlier than test_start")

    @staticmethod
    def _label_columns(df: pd.DataFrame) -> list[object]:
        if isinstance(df.columns, pd.MultiIndex):
            return [column for column in df.columns if str(column[0]).lower() == "label"]
        return [column for column in df.columns if str(column).lower().startswith("label")]

    def is_for_infer(self) -> bool:
        return False

    def readonly(self) -> bool:
        return False

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            raise ValueError("LongHorizonLabelMaturityPurge received empty learning data")
        if not isinstance(df.index, pd.MultiIndex) or "datetime" not in df.index.names:
            raise ValueError(
                "LongHorizonLabelMaturityPurge requires MultiIndex containing datetime"
            )
        label_columns = self._label_columns(df)
        if not label_columns:
            raise ValueError("LongHorizonLabelMaturityPurge found no label columns")

        datetimes = pd.DatetimeIndex(
            pd.to_datetime(df.index.get_level_values("datetime"))
        ).normalize()
        calendar = pd.DatetimeIndex(datetimes.unique()).sort_values()
        reference_offset = self.label_horizon + 1
        summary: dict[str, dict[str, object]] = {}

        for name, (start, end) in self.segment_bounds.items():
            end_pos = int(calendar.searchsorted(end, side="right")) - 1
            cutoff_pos = end_pos - reference_offset
            if end_pos < 0 or cutoff_pos < 0:
                raise ValueError(
                    f"{name} segment has insufficient trading history for "
                    f"label_horizon={self.label_horizon}"
                )
            cutoff = calendar[cutoff_pos]
            if cutoff < start:
                segment_days = int(((calendar >= start) & (calendar <= end)).sum())
                raise ValueError(
                    f"{name} segment has {segment_days} trading days, fewer than the "
                    f"{reference_offset + 1} required for a mature "
                    f"label_horizon={self.label_horizon} sample"
                )
            immature = (datetimes >= start) & (datetimes <= end) & (datetimes > cutoff)
            masked_rows = int(immature.sum())
            if masked_rows:
                df.loc[immature, label_columns] = float("nan")
            summary[name] = {
                "segment_start": start.date().isoformat(),
                "segment_end": end.date().isoformat(),
                "last_mature_feature_date": cutoff.date().isoformat(),
                "masked_rows": masked_rows,
            }

        self.purge_summary = summary
        return df


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

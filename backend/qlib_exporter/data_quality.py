"""数据质量增强模块.

提供数据校验、缺失处理、统计报告功能。
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json

import pandas as pd
import numpy as np


@dataclass
class ColumnStats:
    """单列统计信息."""
    name: str
    dtype: str
    count: int
    null_count: int
    null_rate: float
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    mean_val: Optional[float] = None
    std_val: Optional[float] = None
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "count": self.count,
            "null_count": self.null_count,
            "null_rate": round(self.null_rate, 6),
            "min": self.min_val,
            "max": self.max_val,
            "mean": round(self.mean_val, 4) if self.mean_val else None,
            "std": round(self.std_val, 4) if self.std_val else None,
        }


@dataclass
class AnomalyRecord:
    """异常记录."""
    datetime: str
    instrument: str
    column: str
    value: float
    reason: str
    
    def to_dict(self) -> dict:
        return {
            "datetime": self.datetime,
            "instrument": self.instrument,
            "column": self.column,
            "value": self.value,
            "reason": self.reason,
        }


@dataclass
class ValidationReport:
    """数据校验报告."""
    row_count: int
    date_range: Tuple[str, str]
    instrument_count: int
    column_count: int
    null_counts: Dict[str, int]
    duplicate_count: int
    column_dtypes: Dict[str, str]
    
    def to_dict(self) -> dict:
        return {
            "row_count": self.row_count,
            "date_range": list(self.date_range),
            "instrument_count": self.instrument_count,
            "column_count": self.column_count,
            "null_counts": self.null_counts,
            "duplicate_count": self.duplicate_count,
            "column_dtypes": self.column_dtypes,
        }


@dataclass
class ExportStatistics:
    """导出数据统计摘要."""
    # 元信息
    export_time: str
    snapshot_id: str
    data_type: str
    file_path: str
    
    # 基础统计
    total_rows: int
    total_instruments: int
    date_range: Tuple[str, str]
    trading_days: int
    
    # 数据覆盖率
    coverage_rate: float
    
    # 各列统计
    column_stats: List[ColumnStats] = field(default_factory=list)
    
    # 异常检测
    price_anomalies: List[AnomalyRecord] = field(default_factory=list)
    volume_anomalies: List[AnomalyRecord] = field(default_factory=list)
    
    # 数据质量评分
    quality_score: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "export_time": self.export_time,
            "snapshot_id": self.snapshot_id,
            "data_type": self.data_type,
            "file_path": self.file_path,
            "summary": {
                "total_rows": self.total_rows,
                "total_instruments": self.total_instruments,
                "date_range": list(self.date_range),
                "trading_days": self.trading_days,
                "coverage_rate": round(self.coverage_rate, 4),
            },
            "column_stats": [cs.to_dict() for cs in self.column_stats],
            "anomalies": {
                "price_anomaly_count": len(self.price_anomalies),
                "volume_anomaly_count": len(self.volume_anomalies),
                "price_anomalies": [a.to_dict() for a in self.price_anomalies[:100]],  # 最多100条
                "volume_anomalies": [a.to_dict() for a in self.volume_anomalies[:100]],
            },
            "quality_score": round(self.quality_score, 2),
        }
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)


class DataValidator:
    """数据校验器."""
    
    def validate_dataframe(self, df: pd.DataFrame) -> ValidationReport:
        """校验 DataFrame."""
        if df.empty:
            return ValidationReport(
                row_count=0,
                date_range=("", ""),
                instrument_count=0,
                column_count=0,
                null_counts={},
                duplicate_count=0,
                column_dtypes={},
            )
        
        # 获取日期范围
        if isinstance(df.index, pd.MultiIndex):
            dt_level = df.index.get_level_values("datetime")
            date_min = str(dt_level.min().date()) if hasattr(dt_level.min(), 'date') else str(dt_level.min())
            date_max = str(dt_level.max().date()) if hasattr(dt_level.max(), 'date') else str(dt_level.max())
            instrument_count = df.index.get_level_values("instrument").nunique()
        else:
            date_min = ""
            date_max = ""
            instrument_count = 0
        
        return ValidationReport(
            row_count=len(df),
            date_range=(date_min, date_max),
            instrument_count=instrument_count,
            column_count=len(df.columns),
            null_counts=df.isnull().sum().to_dict(),
            duplicate_count=int(df.index.duplicated().sum()),
            column_dtypes={col: str(df[col].dtype) for col in df.columns},
        )
    
    def validate_hdf5(self, h5_path: Path, key: str = "data") -> ValidationReport:
        """校验 HDF5 文件."""
        df = pd.read_hdf(h5_path, key=key)
        return self.validate_dataframe(df)
    
    def compare_reports(
        self, before: ValidationReport, after: ValidationReport
    ) -> Tuple[bool, List[str]]:
        """对比导出前后报告.
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []
        
        if before.row_count != after.row_count:
            errors.append(f"行数不一致: 导出前 {before.row_count}, 导出后 {after.row_count}")
        
        if before.date_range != after.date_range:
            errors.append(f"日期范围不一致: 导出前 {before.date_range}, 导出后 {after.date_range}")
        
        if before.instrument_count != after.instrument_count:
            errors.append(f"股票数量不一致: 导出前 {before.instrument_count}, 导出后 {after.instrument_count}")
        
        if after.duplicate_count > 0:
            errors.append(f"存在重复索引: {after.duplicate_count} 条")
        
        return len(errors) == 0, errors


class MissingDataHandler:
    """缺失数据处理器."""
    
    def detect_missing_dates(
        self,
        df: pd.DataFrame,
        calendar: Optional[pd.DatetimeIndex] = None,
    ) -> pd.DataFrame:
        """检测缺失交易日.
        
        Returns:
            DataFrame with columns: instrument, missing_dates
        """
        if df.empty or not isinstance(df.index, pd.MultiIndex):
            return pd.DataFrame(columns=["instrument", "missing_dates"])
        
        dt_level = df.index.get_level_values("datetime")
        
        # 如果没有提供交易日历，使用数据中的所有日期
        if calendar is None:
            if hasattr(dt_level[0], 'date'):
                calendar = pd.DatetimeIndex(sorted(set(d.date() for d in dt_level)))
            else:
                calendar = pd.DatetimeIndex(sorted(dt_level.unique()))
        
        results = []
        instruments = df.index.get_level_values("instrument").unique()
        
        for inst in instruments:
            inst_dates = df.loc[df.index.get_level_values("instrument") == inst].index.get_level_values("datetime")
            if hasattr(inst_dates[0], 'date'):
                inst_dates = pd.DatetimeIndex([d.date() for d in inst_dates])
            missing = calendar.difference(inst_dates)
            if len(missing) > 0:
                results.append({
                    "instrument": inst,
                    "missing_count": len(missing),
                    "missing_dates": [str(d) for d in missing[:10]],  # 最多显示10个
                })
        
        return pd.DataFrame(results)
    
    def fill_suspension(
        self,
        df: pd.DataFrame,
        price_method: str = "ffill",
        volume_method: str = "zero",
    ) -> pd.DataFrame:
        """停牌数据填充.
        
        Args:
            df: 输入数据
            price_method: 价格填充方法 (ffill/bfill/none)
            volume_method: 成交量填充方法 (zero/ffill/none)
        
        Returns:
            填充后的 DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # 价格列
        price_cols = [c for c in df.columns if c in ["open", "high", "low", "close", "$open", "$high", "$low", "$close"]]
        # 成交量列
        volume_cols = [c for c in df.columns if c in ["volume", "amount", "$volume"]]
        
        if price_method == "ffill":
            for col in price_cols:
                df[col] = df.groupby(level="instrument")[col].ffill()
        elif price_method == "bfill":
            for col in price_cols:
                df[col] = df.groupby(level="instrument")[col].bfill()
        
        if volume_method == "zero":
            for col in volume_cols:
                df[col] = df[col].fillna(0)
        elif volume_method == "ffill":
            for col in volume_cols:
                df[col] = df.groupby(level="instrument")[col].ffill()
        
        return df
    
    def add_suspension_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加停牌标记列."""
        if df.empty:
            return df
        
        df = df.copy()
        # 成交量为0或NaN视为停牌
        vol_col = "volume" if "volume" in df.columns else "$volume" if "$volume" in df.columns else None
        if vol_col:
            df["is_suspended"] = (df[vol_col].isna()) | (df[vol_col] == 0)
        else:
            df["is_suspended"] = False
        
        return df


class DataReporter:
    """数据统计报告生成器."""
    
    def __init__(self):
        self.validator = DataValidator()
    
    def generate_column_stats(self, df: pd.DataFrame) -> List[ColumnStats]:
        """生成各列统计信息."""
        stats = []
        for col in df.columns:
            series = df[col]
            null_count = int(series.isnull().sum())
            
            cs = ColumnStats(
                name=col,
                dtype=str(series.dtype),
                count=len(series),
                null_count=null_count,
                null_rate=null_count / len(series) if len(series) > 0 else 0,
            )
            
            # 数值列额外统计
            if pd.api.types.is_numeric_dtype(series):
                cs.min_val = float(series.min()) if not series.isnull().all() else None
                cs.max_val = float(series.max()) if not series.isnull().all() else None
                cs.mean_val = float(series.mean()) if not series.isnull().all() else None
                cs.std_val = float(series.std()) if not series.isnull().all() else None
            
            stats.append(cs)
        
        return stats
    
    def detect_price_anomalies(
        self,
        df: pd.DataFrame,
        threshold: float = 0.21,
        max_records: int = 1000,
    ) -> List[AnomalyRecord]:
        """检测价格异常.
        
        检测内容：
        1. 价格为 0 或负数（前复权计算问题）
        2. 涨跌幅超过阈值（可选，因为早期无涨跌停、新股上市等情况）
        
        Args:
            df: 输入数据，需要有 close 列
            threshold: 涨跌幅阈值，默认 21%（高于科创板/创业板 20% 涨跌停）
            max_records: 最大记录数
        
        Note:
            - 主板涨跌停 10%
            - ST 股涨跌停 5%
            - 科创板/创业板涨跌停 20%
            - 1996年前无涨跌停限制
            - 新股上市首日无涨跌停限制
            - 前复权可能导致早期价格为负数
        """
        anomalies = []
        
        close_col = "close" if "close" in df.columns else "$close" if "$close" in df.columns else None
        if close_col is None or df.empty:
            return anomalies
        
        # 1. 检测价格为 0 或负数（这是真正的数据问题）
        invalid_price_mask = df[close_col] <= 0
        invalid_prices = df[invalid_price_mask]
        
        for idx in invalid_prices.head(max_records // 2).index:
            dt_str = str(idx[0].date()) if hasattr(idx[0], 'date') else str(idx[0])
            val = df.loc[idx, close_col]
            anomalies.append(AnomalyRecord(
                datetime=dt_str,
                instrument=str(idx[1]),
                column=close_col,
                value=float(val),
                reason=f"价格异常: {val} (≤0，可能是前复权计算问题)",
            ))
        
        # 2. 检测涨跌幅异常（仅作参考，不一定是数据错误）
        # 排除价格为 0 或负数的记录，避免计算出 inf
        valid_df = df[df[close_col] > 0].copy()
        if not valid_df.empty:
            df_sorted = valid_df.sort_index()
            pct_change = df_sorted.groupby(level="instrument")[close_col].pct_change()
            
            # 找出涨跌幅异常（排除 inf 和 nan）
            mask = (pct_change.abs() > threshold) & (~pct_change.isna()) & (~np.isinf(pct_change))
            abnormal = pct_change[mask]
            
            remaining_slots = max_records - len(anomalies)
            for idx, val in abnormal.head(remaining_slots).items():
                dt_str = str(idx[0].date()) if hasattr(idx[0], 'date') else str(idx[0])
                anomalies.append(AnomalyRecord(
                    datetime=dt_str,
                    instrument=str(idx[1]),
                    column=close_col,
                    value=round(val * 100, 2),
                    reason=f"涨跌幅 {val*100:.2f}% (可能是早期数据/新股/ST复牌)",
                ))
        
        return anomalies
    
    def detect_volume_anomalies(
        self,
        df: pd.DataFrame,
        std_multiplier: float = 10,
        max_records: int = 1000,
    ) -> List[AnomalyRecord]:
        """检测成交量异常（超过均值 N 倍标准差）."""
        anomalies = []
        
        vol_col = "volume" if "volume" in df.columns else "$volume" if "$volume" in df.columns else None
        if vol_col is None or df.empty:
            return anomalies
        
        # 按股票计算均值和标准差
        grouped = df.groupby(level="instrument")[vol_col]
        mean_vol = grouped.transform("mean")
        std_vol = grouped.transform("std")
        
        # 找出异常（超过均值 + N倍标准差）
        upper_bound = mean_vol + std_multiplier * std_vol
        mask = df[vol_col] > upper_bound
        abnormal = df[mask][vol_col]
        
        for idx, val in abnormal.head(max_records).items():
            dt_str = str(idx[0].date()) if hasattr(idx[0], 'date') else str(idx[0])
            anomalies.append(AnomalyRecord(
                datetime=dt_str,
                instrument=str(idx[1]),
                column=vol_col,
                value=float(val),
                reason=f"成交量异常高",
            ))
        
        return anomalies
    
    def calculate_quality_score(self, stats: ExportStatistics) -> float:
        """计算数据质量评分 (0-100)."""
        score = 100.0
        
        # 覆盖率扣分（覆盖率每低1%扣1分）
        coverage_penalty = max(0, (1 - stats.coverage_rate) * 100)
        score -= coverage_penalty
        
        # 空值率扣分
        total_nulls = sum(cs.null_count for cs in stats.column_stats)
        total_cells = stats.total_rows * len(stats.column_stats) if stats.column_stats else 1
        null_rate = total_nulls / total_cells if total_cells > 0 else 0
        null_penalty = null_rate * 50  # 空值率每1%扣0.5分
        score -= null_penalty
        
        # 异常数据扣分
        anomaly_count = len(stats.price_anomalies) + len(stats.volume_anomalies)
        anomaly_rate = anomaly_count / stats.total_rows if stats.total_rows > 0 else 0
        anomaly_penalty = min(10, anomaly_rate * 1000)  # 最多扣10分
        score -= anomaly_penalty
        
        return max(0, min(100, score))
    
    def generate_report(
        self,
        df: pd.DataFrame,
        snapshot_id: str,
        data_type: str,
        file_path: str = "",
        detect_anomalies: bool = True,
    ) -> ExportStatistics:
        """生成完整统计报告."""
        if df.empty:
            # 使用本地时区时间，保证与前端展示时区一致
            local_now = datetime.now(timezone.utc).astimezone()
            return ExportStatistics(
                export_time=local_now.isoformat(),
                snapshot_id=snapshot_id,
                data_type=data_type,
                file_path=file_path,
                total_rows=0,
                total_instruments=0,
                date_range=("", ""),
                trading_days=0,
                coverage_rate=0,
            )
        
        # 基础统计
        dt_level = df.index.get_level_values("datetime")
        inst_level = df.index.get_level_values("instrument")
        
        date_min = dt_level.min()
        date_max = dt_level.max()
        date_min_str = str(date_min.date()) if hasattr(date_min, 'date') else str(date_min)
        date_max_str = str(date_max.date()) if hasattr(date_max, 'date') else str(date_max)
        
        trading_days = dt_level.nunique()
        total_instruments = inst_level.nunique()
        
        # 覆盖率 = 实际数据行数 / (股票数 × 交易日数)
        expected_rows = trading_days * total_instruments
        coverage_rate = len(df) / expected_rows if expected_rows > 0 else 0
        
        # 列统计
        column_stats = self.generate_column_stats(df)
        
        # 异常检测
        price_anomalies = []
        volume_anomalies = []
        if detect_anomalies:
            price_anomalies = self.detect_price_anomalies(df)
            volume_anomalies = self.detect_volume_anomalies(df)
        
        # 使用本地时区时间，保证与前端展示时区一致
        local_now = datetime.now(timezone.utc).astimezone()
        stats = ExportStatistics(
            export_time=local_now.isoformat(),
            snapshot_id=snapshot_id,
            data_type=data_type,
            file_path=file_path,
            total_rows=len(df),
            total_instruments=total_instruments,
            date_range=(date_min_str, date_max_str),
            trading_days=trading_days,
            coverage_rate=coverage_rate,
            column_stats=column_stats,
            price_anomalies=price_anomalies,
            volume_anomalies=volume_anomalies,
        )
        
        # 计算质量评分
        stats.quality_score = self.calculate_quality_score(stats)
        
        return stats
    
    def generate_report_from_hdf5(
        self,
        h5_path: Path,
        snapshot_id: str,
        data_type: str,
        key: str = "data",
    ) -> ExportStatistics:
        """从 HDF5 文件生成报告."""
        df = pd.read_hdf(h5_path, key=key)
        return self.generate_report(df, snapshot_id, data_type, str(h5_path))
    
    def save_report(
        self,
        stats: ExportStatistics,
        output_path: Path,
        format: str = "json",
    ) -> None:
        """保存报告到文件."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(stats.to_json())
        elif format == "md":
            md_content = self._generate_markdown(stats)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(md_content)
    
    def _generate_markdown(self, stats: ExportStatistics) -> str:
        """生成 Markdown 格式报告."""
        lines = [
            f"# 数据导出报告",
            f"",
            f"## 基本信息",
            f"- **导出时间**: {stats.export_time}",
            f"- **Snapshot ID**: {stats.snapshot_id}",
            f"- **数据类型**: {stats.data_type}",
            f"- **文件路径**: {stats.file_path}",
            f"",
            f"## 数据概览",
            f"| 指标 | 值 |",
            f"|------|-----|",
            f"| 总行数 | {stats.total_rows:,} |",
            f"| 股票数量 | {stats.total_instruments:,} |",
            f"| 日期范围 | {stats.date_range[0]} ~ {stats.date_range[1]} |",
            f"| 交易日数 | {stats.trading_days:,} |",
            f"| 数据覆盖率 | {stats.coverage_rate*100:.2f}% |",
            f"| **质量评分** | **{stats.quality_score:.1f}** |",
            f"",
            f"## 列统计",
            f"| 列名 | 类型 | 空值数 | 空值率 | 最小值 | 最大值 | 均值 |",
            f"|------|------|--------|--------|--------|--------|------|",
        ]
        
        for cs in stats.column_stats:
            min_str = f"{cs.min_val:.4f}" if cs.min_val is not None else "-"
            max_str = f"{cs.max_val:.4f}" if cs.max_val is not None else "-"
            mean_str = f"{cs.mean_val:.4f}" if cs.mean_val is not None else "-"
            lines.append(
                f"| {cs.name} | {cs.dtype} | {cs.null_count:,} | {cs.null_rate*100:.2f}% | {min_str} | {max_str} | {mean_str} |"
            )
        
        lines.extend([
            f"",
            f"## 异常检测",
            f"- 价格异常数: {len(stats.price_anomalies)}",
            f"- 成交量异常数: {len(stats.volume_anomalies)}",
        ])
        
        if stats.price_anomalies:
            lines.extend([
                f"",
                f"### 价格异常样例（前10条）",
                f"| 日期 | 股票 | 涨跌幅 | 说明 |",
                f"|------|------|--------|------|",
            ])
            for a in stats.price_anomalies[:10]:
                lines.append(f"| {a.datetime} | {a.instrument} | {a.value}% | {a.reason} |")
        
        return "\n".join(lines)

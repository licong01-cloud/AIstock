from __future__ import annotations

"""Qlib 导出相关 FastAPI 路由.

支持的 API：
- GET  /api/v1/qlib/config              获取当前配置
- GET  /api/v1/qlib/snapshots           罗列现有 Snapshot
- DELETE /api/v1/qlib/snapshots/{id}    删除指定 Snapshot
- POST /api/v1/qlib/snapshots/daily     日频全量导出
- POST /api/v1/qlib/snapshots/minute    分钟线全量导出
"""

import json
import os
import shutil
import io
import zipfile
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from ..infra.wsl_qlib_runner import QlibWSLConfigError, run_qlib_script_in_wsl, win_to_wsl_path
from .config import (
    DAILY_RAW_TABLE,
    FIELD_MAPPING_DB_DAILY,
    FIELD_MAPPING_DB_MINUTE,
    IPO_FILTER_DAYS,
    MINUTE_QFQ_TABLE,
    MONEYFLOW_TS_TABLE,
    QLIB_MARKET,
    QLIB_SNAPSHOT_ROOT,
)
from .exporter import (
    ExportResult,
    QlibBakBasicExporter,
    QlibCyqPerfExporter,
    QlibDailyBasicExporter,
    QlibDailyExporter,
    QlibMarginDetailExporter,
    QlibMinuteExporter,
    QlibMoneyflowExporter,
    QlibSectorDataExporter,
)
from .field_map_service import export_field_map_for_snapshot
from .data_quality import DataReporter, DataValidator
from .db_reader import DBReader
from .authoritative_bin_exporter import (
    MINUTE_FREQ_QLIB,
    export_stock_daily_csv,
    export_stock_minute_csv_chunked,
    normalize_stock_export_exchanges,
)


router = APIRouter()


class DailySnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    ts_codes: Optional[List[str]] = Field(
        None,
        description="可选，指定导出的 ts_code 列表；为空则导出全部 ts_code",
    )
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz'；北交所固定排除；为空默认 SH/SZ",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        """确保 snapshot_id 非空且无首尾空格."""
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _end_not_before_start(self):  # noqa: D401, N805
        """确保 end >= start."""
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class DailySnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "DailySnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


class MoneyflowSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz'；北交所固定排除；为空默认 SH/SZ",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _moneyflow_snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        """确保 snapshot_id 非空且无首尾空格."""
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _moneyflow_end_not_before_start(self):  # noqa: D401, N805
        """确保 end >= start."""
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class MoneyflowSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "MoneyflowSnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


class FieldMapExportRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID（目录名）")
    write_to_h5: bool = Field(
        True,
        description="是否将字段中文说明写入 snapshot 下的 daily_basic.h5/moneyflow.h5 的 HDF5 attrs",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2


class FieldMapExportResponse(BaseModel):
    snapshot_id: str
    csv_path: str
    rows: int
    written_h5: dict[str, int]
    has_daily_basic: bool
    has_moneyflow: bool
    has_bak_basic: bool
    has_margin_detail: bool = False
    has_cyq_perf: bool
    has_sector_data: bool = False


# NOTE: FastAPI builds request body TypeAdapters while registering routes.
# Ensure models are rebuilt BEFORE route decorators below.
try:
    DailySnapshotRequest.model_rebuild(force=True)
    MoneyflowSnapshotRequest.model_rebuild(force=True)
    FieldMapExportRequest.model_rebuild(force=True)
except Exception:
    pass


_daily_exporter = QlibDailyExporter()
_daily_basic_exporter = QlibDailyBasicExporter()
_minute_exporter = QlibMinuteExporter()
_moneyflow_exporter = QlibMoneyflowExporter()
_bak_basic_exporter = QlibBakBasicExporter()
_margin_detail_exporter = QlibMarginDetailExporter()
_cyq_perf_exporter = QlibCyqPerfExporter()
_sector_data_exporter = QlibSectorDataExporter()


@router.post("/api/v1/qlib/snapshots/daily", response_model=DailySnapshotResponse)
async def create_daily_snapshot(body: DailySnapshotRequest) -> DailySnapshotResponse:
    """触发一次日频前复权 Qlib Snapshot 导出."""

    try:
        result = _daily_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            ts_codes=body.ts_codes,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return DailySnapshotResponse.from_result(result)
    except ValueError as exc:
        # 参数或数据问题 → 400
        raise HTTPException(status_code=400, detail=str(exc))
    except NotImplementedError as exc:
        # 预留给未来增量导出等特性
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        import traceback as _tb_daily
        _tb_daily.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


class DailyBasicSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz'；北交所固定排除；为空默认 SH/SZ",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _daily_basic_snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _daily_basic_end_not_before_start(self) -> "DailyBasicSnapshotRequest":  # noqa: D401, N805
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class DailyBasicSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "DailyBasicSnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/snapshots/daily_basic", response_model=DailyBasicSnapshotResponse)
async def create_daily_basic_snapshot(body: DailyBasicSnapshotRequest) -> DailyBasicSnapshotResponse:
    """触发一次 Tushare daily_basic 指标 Snapshot 导出.

    生成的文件位于指定 snapshot 目录下的 daily_basic.h5，索引为
    MultiIndex(datetime, instrument)，列名为 db_* 系列字段。
    """

    try:
        result = _daily_basic_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        # 导出成功后自动触发字段映射生成
        try:
            export_field_map_for_snapshot(
                snapshot_id=body.snapshot_id,
                write_to_h5=True,
            )
        except Exception as e:
            # 字段映射生成失败不影响主导出流程
            print(f"[WARN] Auto field map generation failed: {e}")
        return DailyBasicSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/moneyflow", response_model=MoneyflowSnapshotResponse)
async def create_moneyflow_snapshot(body: MoneyflowSnapshotRequest) -> MoneyflowSnapshotResponse:
    """触发一次个股资金流向（moneyflow_ts） Snapshot 导出.

    生成的文件位于指定 snapshot 目录下的 moneyflow.h5，索引为
    MultiIndex(datetime, instrument)，列名为 mf_* 系列字段。
    """

    try:
        result = _moneyflow_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        # 导出成功后自动触发字段映射生成
        try:
            export_field_map_for_snapshot(
                snapshot_id=body.snapshot_id,
                write_to_h5=True,
            )
        except Exception as e:
            # 字段映射生成失败不影响主导出流程
            print(f"[WARN] Auto field map generation failed: {e}")
        return MoneyflowSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        # 打印完整堆栈，便于诊断内部错误（如 list index out of range 等）
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


class BakBasicSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz'；北交所固定排除；为空默认 SH/SZ",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _bak_basic_snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        """确保 snapshot_id 非空且无首尾空格."""
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _bak_basic_end_not_before_start(self):  # noqa: D401, N805
        """确保 end >= start."""
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class BakBasicSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "BakBasicSnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/snapshots/bak_basic", response_model=BakBasicSnapshotResponse)
async def create_bak_basic_snapshot(body: BakBasicSnapshotRequest) -> BakBasicSnapshotResponse:
    """触发一次 Tushare bak_basic 历史股票列表数据 Snapshot 导出.

    生成的文件位于指定 snapshot 目录下的 bak_basic.h5，索引为
    MultiIndex(datetime, instrument)，列名为 bb_* 系列字段。
    """

    import traceback as _tb

    try:
        result = _bak_basic_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        # 导出成功后自动触发字段映射生成
        print(f"[DEBUG] BakBasic export success for {body.snapshot_id}, auto-triggering field map...")
        try:
            from .field_map_service import export_field_map_for_snapshot
            fm_result = export_field_map_for_snapshot(
                snapshot_id=body.snapshot_id,
                write_to_h5=True,
            )
            print(f"[DEBUG] Field map generated: {fm_result.get('rows')} rows")
        except Exception as e:
            print(f"[WARN] Auto field map failed: {e}")
            _tb.print_exc()
        return BakBasicSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        _tb.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


class MarginDetailSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(None, description="可选，按交易所过滤")
    exclude_st: bool = Field(True, description="是否排除 ST 股票")
    exclude_delisted_or_paused: bool = Field(True, description="是否排除退市或暂停上市股票")

    @field_validator("snapshot_id")
    @classmethod
    def _margin_detail_snapshot_id_not_empty(cls, v: str) -> str:
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _margin_detail_end_not_before_start(self):
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class MarginDetailSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "MarginDetailSnapshotResponse":
        return cls(**result.__dict__)


@router.post("/api/v1/qlib/snapshots/margin_detail", response_model=MarginDetailSnapshotResponse)
async def create_margin_detail_snapshot(body: MarginDetailSnapshotRequest) -> MarginDetailSnapshotResponse:
    """全量导出 margin_detail 融资融券明细数据到 Snapshot."""

    import traceback as _tb

    try:
        result = _margin_detail_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        try:
            from .field_map_service import export_field_map_for_snapshot
            fm_result = export_field_map_for_snapshot(
                snapshot_id=body.snapshot_id,
                write_to_h5=True,
            )
            print(f"[DEBUG] Field map generated: {fm_result.get('rows')} rows")
        except Exception as e:
            print(f"[WARN] Auto field map failed: {e}")
            _tb.print_exc()
        return MarginDetailSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        _tb.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


class CyqPerfSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz'；北交所固定排除；为空默认 SH/SZ",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _cyq_perf_snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        """确保 snapshot_id 非空且无首尾空格."""
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _cyq_perf_end_not_before_start(self):  # noqa: D401, N805
        """确保 end >= start."""
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class CyqPerfSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "CyqPerfSnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/snapshots/cyq_perf", response_model=CyqPerfSnapshotResponse)
async def create_cyq_perf_snapshot(body: CyqPerfSnapshotRequest) -> CyqPerfSnapshotResponse:
    """触发一次 Tushare cyq_perf 每日筹码及胜率数据 Snapshot 导出.

    生成的文件位于指定 snapshot 目录下的 cyq_perf.h5，索引为
    MultiIndex(datetime, instrument)，列名为 cp_* 系列字段。
    """

    try:
        result = _cyq_perf_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        # 导出成功后自动触发字段映射生成
        print(f"[DEBUG] CyqPerf export success for {body.snapshot_id}, auto-triggering field map...")
        try:
            from .field_map_service import export_field_map_for_snapshot
            fm_result = export_field_map_for_snapshot(
                snapshot_id=body.snapshot_id,
                write_to_h5=True,
            )
            print(f"[DEBUG] Field map generated: {fm_result.get('rows')} rows")
        except Exception as e:
            import traceback
            print(f"[WARN] Auto field map failed: {e}")
            traceback.print_exc()
        return CyqPerfSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/field_map/export", response_model=FieldMapExportResponse)
async def export_field_map(body: FieldMapExportRequest) -> FieldMapExportResponse:
    """生成字段含义映射 CSV，并可选写入 HDF5 attrs.

    - 输出 CSV：AIstock/metadata/aistock_field_map.csv
    - 读取 DB 列 comment（market.daily_basic / market.moneyflow_ts）
    - 根据 snapshot 下 h5 的列名映射到最终导出列名（db_*/mf_*）
    - 写入 daily_basic.h5 / moneyflow.h5 的 storer.attrs.column_comments(_json)
    """

    try:
        payload = export_field_map_for_snapshot(
            snapshot_id=body.snapshot_id,
            write_to_h5=body.write_to_h5,
        )
        return FieldMapExportResponse(**payload)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# CSV → Qlib bin 导出 API（通过 WSL 调用 RD-Agent 脚本）
# =============================================================================


class BinExportRequest(BaseModel):
    """Qlib bin 导出请求（DB → CSV → bin）。"""

    snapshot_id: str = Field(..., description="bin Snapshot ID，作为 CSV/bin 目录名")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    freq: Literal["day", "1m", "1min", "5m", "15m"] = Field(
        "day",
        description="导出频率：日线 day 或分钟线 1m/5m/15m（当前仅实现 day 和 1m）",
    )
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，交易所过滤：sh, sz；为空表示全市场",
    )
    run_health_check: bool = Field(
        True,
        description="是否在 dump_bin 后运行 check_data_health.py",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )


class BinExportResponse(BaseModel):
    """Qlib bin 导出响应."""

    snapshot_id: str
    csv_dir: str
    bin_dir: str
    dump_bin_ok: bool
    check_ok: Optional[bool]
    stdout_dump: Optional[str] = None
    stderr_dump: Optional[str] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None


# Ensure Pydantic v2 fully rebuilds models for FastAPI TypeAdapter
try:
    DailySnapshotRequest.model_rebuild()
    DailyBasicSnapshotRequest.model_rebuild()
    BakBasicSnapshotRequest.model_rebuild()
    MarginDetailSnapshotRequest.model_rebuild()
    CyqPerfSnapshotRequest.model_rebuild()
    MoneyflowSnapshotRequest.model_rebuild()
    FieldMapExportRequest.model_rebuild()
    MinuteSnapshotRequest.model_rebuild()
    IncrementalExportRequest.model_rebuild()
    BinExportInfo.model_rebuild()
    BinExportListResponse.model_rebuild()
except Exception:
    # If rebuild fails, let runtime raise the original error for diagnosis.
    pass


_db_reader = DBReader()


def _export_daily_to_csv_for_dump_bin(
    snapshot_id: str,
    start: date,
    end: date,
    exchanges: Optional[List[str]],
    *,
    exclude_st: bool,
    exclude_delisted_or_paused: bool,
    basis_start: Optional[date] = None,
    basis_end: Optional[date] = None,
) -> Path:
    """Export authoritative per-stock daily CSV files for Qlib dump_bin.py."""

    csv_root = os.getenv("QLIB_CSV_ROOT_WIN")
    if not csv_root:
        raise HTTPException(status_code=500, detail="missing env QLIB_CSV_ROOT_WIN")
    try:
        stock_exchanges = normalize_stock_export_exchanges(exchanges)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    summary = export_stock_daily_csv(
        snapshot_id=snapshot_id,
        start=start,
        end=end,
        csv_root=Path(csv_root),
        exchanges=stock_exchanges,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
        basis_start=basis_start,
        basis_end=basis_end,
        strict_limit=True,
        overwrite_csv=True,
    )
    return Path(summary.csv_dir)


def _export_minute_to_csv_for_dump_bin(
    snapshot_id: str,
    start: date,
    end: date,
    exchanges: Optional[List[str]],
    *,
    exclude_st: bool,
    exclude_delisted_or_paused: bool,
    freq: str = "1min",
    basis_start: Optional[date] = None,
    basis_end: Optional[date] = None,
) -> Path:
    """Export authoritative per-stock 1min CSV files for Qlib dump_bin.py."""

    csv_root = os.getenv("QLIB_CSV_ROOT_WIN")
    if not csv_root:
        raise HTTPException(status_code=500, detail="missing env QLIB_CSV_ROOT_WIN")
    try:
        stock_exchanges = normalize_stock_export_exchanges(exchanges)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    dump_freq = MINUTE_FREQ_QLIB if freq in {"1m", "1min"} else freq
    if dump_freq != MINUTE_FREQ_QLIB:
        raise HTTPException(status_code=400, detail=f"unsupported minute freq for authoritative export: {freq}")

    summary = export_stock_minute_csv_chunked(
        snapshot_id=snapshot_id,
        start=start,
        end=end,
        csv_root=Path(csv_root),
        exchanges=stock_exchanges,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
        basis_start=basis_start,
        basis_end=basis_end,
        strict_limit=True,
        code_batch_size=100,
        chunk_months=3,
        overwrite_csv=True,
    )
    return Path(summary.csv_dir)


def _export_index_to_csv_for_dump_bin(
    snapshot_id: str,
    index_code: str,
    start: date,
    end: date,
    *,
    data_source: Literal["tushare", "tdx"] = "tushare",
) -> Path:
    """从 DB 导出单个指数日线为 CSV，供 dump_bin.py 使用。

    CSV 结构：date,symbol,open,high,low,close,volume,amount
    - date: YYYY-MM-DD
    - symbol: 指数 ts_code，例如 000300.SH
    """

    csv_root = os.getenv("QLIB_CSV_ROOT_WIN")
    if not csv_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_CSV_ROOT_WIN")

    csv_root_path = Path(csv_root)
    csv_dir = csv_root_path / snapshot_id / "index"
    csv_dir.mkdir(parents=True, exist_ok=True)

    if data_source == "tdx":
        df = _db_reader.load_index_daily_tdx(index_code, start, end)
    else:
        df = _db_reader.load_index_daily(index_code, start, end)
    if df.empty:
        raise HTTPException(status_code=400, detail="指定区间内无可导出的指数日线数据")

    # 构造 dump_bin.py 期望的列
    df_csv = pd.DataFrame()
    df_csv["date"] = pd.to_datetime(df["trade_date"]).astype("datetime64[ns]").dt.date.astype(str)
    df_csv["symbol"] = df["ts_code"].astype(str)
    df_csv["open"] = df["open"]
    df_csv["high"] = df["high"]
    df_csv["low"] = df["low"]
    df_csv["close"] = df["close"]
    df_csv["volume"] = df["volume"]
    # amount 统一输出为元：
    # - tushare(index_daily.amount) 单位为千元，需要 *1000
    # - tdx(index_daily_tdx.amount_li) 单位为厘，已在 DBReader 中 /1000 转为元
    if "amount" in df.columns:
        amt = pd.to_numeric(df["amount"], errors="coerce")
        df_csv["amount"] = amt * 1000.0 if data_source == "tushare" else amt

    csv_cols = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]

    missing_cols = [col for col in csv_cols if col not in df_csv.columns]
    if missing_cols:
        raise HTTPException(status_code=400, detail=f"index CSV missing required columns: {missing_cols}")
    df_csv = df_csv[csv_cols]
    if df_csv.isna().any().any():
        bad_cols = [col for col in csv_cols if df_csv[col].isna().any()]
        raise HTTPException(status_code=400, detail=f"index CSV has null required columns: {bad_cols}")

    csv_path = csv_dir / f"{index_code}.csv"
    df_csv.to_csv(csv_path, index=False)

    return csv_dir


@router.post("/api/v1/qlib/bin/export", response_model=BinExportResponse)
async def export_qlib_bin(body: BinExportRequest) -> BinExportResponse:
    """从 DB 导出 CSV，并通过 WSL 调用 dump_bin.py 生成 Qlib bin。

    根据 freq 参数决定导出日线还是分钟线：
    - day：使用日线宽表 CSV（兼容当前行为）
    - 1m：使用分钟线宽表 CSV（当前仅实现 1m，5m/15m 预留）
    """

    try:
        stock_exchanges = normalize_stock_export_exchanges(body.exchanges)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # 1. 导出 CSV（根据 freq 分支）
    if body.freq == "day":
        csv_dir = _export_daily_to_csv_for_dump_bin(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=stock_exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        dump_freq = "day"
    elif body.freq in {"1m", "1min"}:
        csv_dir = _export_minute_to_csv_for_dump_bin(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=stock_exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
            freq="1min",
        )
        dump_freq = MINUTE_FREQ_QLIB
    else:
        # 预留 5m/15m，将来有 DB 数据后再实现
        raise HTTPException(status_code=400, detail=f"暂不支持的 freq: {body.freq}（目前仅支持 'day' 和 '1m'）")

    # 2. 构造 bin 目录
    bin_root = os.getenv("QLIB_BIN_ROOT_WIN")
    if not bin_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_BIN_ROOT_WIN")

    bin_root_path = Path(bin_root)
    bin_dir = bin_root_path / body.snapshot_id
    bin_dir.mkdir(parents=True, exist_ok=True)

    # dump_bin.py 会维护 instruments/all.txt。
    # 需求：指数导出不应更新 all.txt（只维护 index.txt）。
    # 因此：
    # - 若 all.txt 原本存在：dump 后恢复原内容（避免指数写入影响股票 universe）
    # - 若 all.txt 原本不存在：dump 后删除新生成的 all.txt
    instruments_dir_for_all = bin_dir / "instruments"
    all_file = instruments_dir_for_all / "all.txt"
    all_file_existed = all_file.exists()
    all_file_backup: Optional[bytes] = None
    if all_file_existed:
        try:
            all_file_backup = all_file.read_bytes()
        except Exception:
            all_file_backup = None

    csv_dir_wsl = win_to_wsl_path(str(csv_dir))
    bin_dir_wsl = win_to_wsl_path(str(bin_dir))

    # 3. 调用 dump_bin.py
    # RD-Agent 的 dump_bin.py 使用 fire 定义子命令，需要指定子命令名称（dump_all/dump_fix/dump_update）
    dump_args = [
        "dump_all",
        "--data_path",
        csv_dir_wsl,
        "--qlib_dir",
        bin_dir_wsl,
        "--freq",
        dump_freq,
        "--date_field_name",
        "date",
        "--symbol_field_name",
        "symbol",
        "--exclude_fields",
        "date,symbol",
    ]

    dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)

    check_ok: Optional[bool] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None

    # 4. 可选：运行 check_data_health.py
    if body.run_health_check:
        check_args = [
            "--qlib_dir",
            bin_dir_wsl,
            "--freq",
            dump_freq,
        ]
        check_res = run_qlib_script_in_wsl("check_data_health.py", check_args)
        check_ok = check_res.ok
        stdout_check = check_res.stdout
        stderr_check = check_res.stderr

    # 5. 写出一次导出的 meta 信息，便于后续在 /api/v1/qlib/bin/exports 中展示。
    # meta 写入失败必须暴露给调用方，避免后续增量导出基于缺失元数据运行。
    dataset_key = "stock_daily" if dump_freq == "day" else f"stock_minute_{dump_freq}"
    meta = {
        "snapshot_id": body.snapshot_id,
        "start": body.start.isoformat(),
        "end": body.end.isoformat(),
        "basis_start": body.start.isoformat(),
        "basis_end": body.end.isoformat(),
        "exchanges": stock_exchanges,
        "exclude_st": body.exclude_st,
        "exclude_delisted_or_paused": True,
        "exclude_bj": True,
        "min_listed_days": IPO_FILTER_DAYS,
        "run_health_check": body.run_health_check,
        "freq_types": [
            "daily" if dump_freq == "day" else dump_freq,
        ],
        "last_end_dates": {
            dataset_key: body.end.isoformat(),
        },
    }
    meta_path = bin_dir / "meta_export.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return BinExportResponse(
        snapshot_id=body.snapshot_id,
        csv_dir=str(csv_dir),
        bin_dir=str(bin_dir),
        dump_bin_ok=dump_res.ok,
        check_ok=check_ok,
        stdout_dump=dump_res.stdout,
        stderr_dump=dump_res.stderr,
        stdout_check=stdout_check,
        stderr_check=stderr_check,
    )


# =============================================================================
# Qlib bin 目录列表 API
# =============================================================================


class BinExportInfo(BaseModel):
    """单个 Qlib bin 导出目录的信息."""

    snapshot_id: str = Field(..., description="bin Snapshot ID")
    bin_dir: str = Field(..., description="bin 目录绝对路径")
    created_at: Optional[datetime] = Field(
        None,
        description="目录创建时间（文件系统元数据，可能因平台而异）",
    )
    modified_at: Optional[datetime] = Field(
        None,
        description="目录最近修改时间",
    )
    start: Optional[date] = Field(None, description="导出开始日期（来自导出 meta）")
    end: Optional[date] = Field(None, description="导出结束日期（来自导出 meta）")
    exchanges: Optional[List[str]] = Field(None, description="导出时选择的交易所")
    exclude_st: Optional[bool] = Field(None, description="是否排除 ST 股票")
    exclude_delisted_or_paused: Optional[bool] = Field(
        None, description="是否排除退市 / 暂停上市股票"
    )
    freq_types: Optional[List[str]] = Field(
        None,
        description="bin 中包含的数据频率类型，例如 ['daily']，预留扩展分钟线等",
    )


class BinExportListResponse(BaseModel):
    """Qlib bin 导出目录列表响应."""

    items: List[BinExportInfo] = Field(..., description="bin 导出目录列表")
    total: int = Field(..., description="总数")


@router.get("/api/v1/qlib/bin/exports", response_model=BinExportListResponse)
async def list_bin_exports() -> BinExportListResponse:
    """罗列 Qlib bin 导出目录.

    该接口通过环境变量 ``QLIB_BIN_ROOT_WIN`` 查找 bin 根目录, 返回其下每个子目录
    作为一个 bin Snapshot。暂不深入解析 Qlib 目录结构, 仅提供基础信息供前端展示。
    """

    bin_root = os.getenv("QLIB_BIN_ROOT_WIN")
    if not bin_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_BIN_ROOT_WIN")

    root_path = Path(bin_root)
    if not root_path.exists():
        return BinExportListResponse(items=[], total=0)

    items: List[BinExportInfo] = []
    for child in root_path.iterdir():
        if not child.is_dir():
            continue

        try:
            stat = child.stat()
            created_at = datetime.fromtimestamp(stat.st_ctime)
            modified_at = datetime.fromtimestamp(stat.st_mtime)
        except Exception:
            created_at = None
            modified_at = None

        # 读取导出 meta（如有）
        meta_path = child / "meta_export.json"
        start: Optional[date] = None
        end: Optional[date] = None
        exchanges: Optional[List[str]] = None
        exclude_st: Optional[bool] = None
        exclude_delisted_or_paused: Optional[bool] = None
        freq_types: Optional[List[str]] = None

        if meta_path.exists():
            try:
                meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
                if "start" in meta_data:
                    start = date.fromisoformat(str(meta_data["start"]))
                if "end" in meta_data:
                    end = date.fromisoformat(str(meta_data["end"]))
                exchanges_val = meta_data.get("exchanges")
                if isinstance(exchanges_val, list):
                    exchanges = [str(x) for x in exchanges_val]
                if "exclude_st" in meta_data:
                    exclude_st = bool(meta_data["exclude_st"])
                if "exclude_delisted_or_paused" in meta_data:
                    exclude_delisted_or_paused = bool(meta_data["exclude_delisted_or_paused"])
                freq_val = meta_data.get("freq_types")
                if isinstance(freq_val, list):
                    freq_types = [str(x) for x in freq_val]
            except Exception:
                # meta 解析失败时忽略, 仅保留基础信息
                pass

        items.append(
            BinExportInfo(
                snapshot_id=child.name,
                bin_dir=str(child.resolve()),
                created_at=created_at,
                modified_at=modified_at,
                start=start,
                end=end,
                exchanges=exchanges,
                exclude_st=exclude_st,
                exclude_delisted_or_paused=exclude_delisted_or_paused,
                freq_types=freq_types,
            )
        )

    # 按修改时间倒序
    items.sort(key=lambda x: (x.modified_at or datetime.min), reverse=True)

    return BinExportListResponse(items=items, total=len(items))


# =============================================================================
# 指数列表 & 指数 bin 导出 API
# =============================================================================


class IndexMarketInfo(BaseModel):
    """指数市场信息."""

    market: str = Field(..., description="index_basic.market 字段")


class IndexMarketListResponse(BaseModel):
    """指数市场列表响应."""

    items: List[IndexMarketInfo]
    total: int


@router.get("/api/v1/qlib/index/markets", response_model=IndexMarketListResponse)
async def list_index_markets() -> IndexMarketListResponse:
    """罗列 index_basic.market 中已存在的市场列表."""

    markets = _db_reader.get_all_index_markets()
    items = [IndexMarketInfo(market=m) for m in markets]
    return IndexMarketListResponse(items=items, total=len(items))


class IndexInfo(BaseModel):
    """单个指数基础信息."""

    ts_code: str
    name: Optional[str]
    fullname: Optional[str]
    market: Optional[str]


class IndexListResponse(BaseModel):
    """指数列表响应."""

    items: List[IndexInfo]
    total: int


@router.get("/api/v1/qlib/index/list", response_model=IndexListResponse)
async def list_indices(
    markets: Optional[str] = None,
    data_source: Literal["tushare", "tdx"] = "tushare",
) -> IndexListResponse:
    """按 market 过滤罗列指数基础信息.

    Args:
        markets: 可选，逗号分隔的 market 列表，例如 "CSI,SSE,SZSE"。
        data_source: tushare 使用 index_basic；tdx 使用 index_daily_tdx 实际数据生成列表。
    """

    if data_source == "tdx":
        df = _db_reader.load_index_list_tdx()
        if df.empty:
            return IndexListResponse(items=[], total=0)
        items = [
            IndexInfo(
                ts_code=row["ts_code"],
                name=row.get("name"),
                fullname=row.get("fullname"),
                market=row.get("market"),
            )
            for _, row in df.iterrows()
        ]
        return IndexListResponse(items=items, total=len(items))

    market_list: Optional[List[str]]
    if markets:
        market_list = [m.strip().upper() for m in markets.split(",") if m.strip()]
        market_list = market_list or None
    else:
        market_list = None

    df = _db_reader.load_index_basic_by_markets(market_list)
    if df.empty:
        return IndexListResponse(items=[], total=0)

    items = [
        IndexInfo(
            ts_code=row["ts_code"],
            name=row.get("name"),
            fullname=row.get("fullname"),
            market=row.get("market"),
        )
        for _, row in df.iterrows()
    ]

    return IndexListResponse(items=items, total=len(items))


class IndexBinExportRequest(BaseModel):
    """单个指数 bin 导出请求（DB → CSV → bin）。"""

    snapshot_id: str = Field(..., description="bin Snapshot ID，作为 CSV/bin 目录名")
    index_code: str = Field(..., description="指数 ts_code，例如 000300.SH")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    data_source: Literal["tushare", "tdx"] = Field(
        "tushare",
        description="指数数据源：tushare=market.index_daily（amount=千元）；tdx=market.index_daily_tdx（价格/金额=厘）",
    )
    run_health_check: bool = Field(
        True,
        description="是否在 dump_bin 后运行 check_data_health.py（对整个日频 bin）",
    )


class IndexBinExportResponse(BaseModel):
    """单个指数 bin 导出响应."""

    snapshot_id: str
    index_code: str
    csv_dir: str
    bin_dir: str
    dump_bin_ok: bool
    check_ok: Optional[bool]
    stdout_dump: Optional[str]
    stderr_dump: Optional[str]
    stdout_check: Optional[str]
    stderr_check: Optional[str]


@router.post("/api/v1/qlib/index/bin/export", response_model=IndexBinExportResponse)
async def export_index_bin(body: IndexBinExportRequest) -> IndexBinExportResponse:
    """从 index_daily 表导出单个指数到 Qlib bin.

    步骤：
    1. 调用 DBReader.load_index_daily 加载日线；
    2. 写 CSV 至 QLIB_CSV_ROOT_WIN/<snapshot_id>/index/<index_code>.csv；
    3. 通过 WSL 调用 dump_bin.py，将该 CSV 写入 Qlib bin 目录；
    4. 维护 instruments/index.txt 中的指数代码。
    """

    if body.start > body.end:
        raise HTTPException(status_code=400, detail="end 日期不能早于 start")

    # 1. 生成指数 CSV
    csv_dir = _export_index_to_csv_for_dump_bin(
        snapshot_id=body.snapshot_id,
        index_code=body.index_code,
        start=body.start,
        end=body.end,
        data_source=body.data_source,
    )

    # 2. 构造 bin 目录
    bin_root = os.getenv("QLIB_BIN_ROOT_WIN")
    if not bin_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_BIN_ROOT_WIN")

    bin_root_path = Path(bin_root)
    bin_dir = bin_root_path / body.snapshot_id
    bin_dir.mkdir(parents=True, exist_ok=True)

    csv_dir_wsl = win_to_wsl_path(str(csv_dir))
    bin_dir_wsl = win_to_wsl_path(str(bin_dir))

    # 3. 调用 dump_bin.py，将该指数追加到 bin 中
    dump_args = [
        "dump_all",
        "--data_path",
        csv_dir_wsl,
        "--qlib_dir",
        bin_dir_wsl,
        "--freq",
        "day",
        "--date_field_name",
        "date",
        "--symbol_field_name",
        "symbol",
        "--exclude_fields",
        "date,symbol",
    ]

    dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)

    check_ok: Optional[bool] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None

    # 4. 可选：运行 check_data_health.py 对整个日频 bin 做健康检查
    if body.run_health_check:
        try:
            check_args = [
                "--qlib_dir",
                bin_dir_wsl,
                "--freq",
                "day",
            ]
            check_res = run_qlib_script_in_wsl("check_data_health.py", check_args)
            check_ok = check_res.ok
            stdout_check = check_res.stdout
            stderr_check = check_res.stderr
        except QlibWSLConfigError as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        except Exception as exc:  # noqa: BLE001
            check_ok = None
            stdout_check = None
            stderr_check = str(exc)

    # dump_bin.py 可能已修改/生成 instruments/all.txt；这里按策略恢复/删除
    try:
        if all_file_existed:
            if all_file_backup is not None:
                instruments_dir_for_all.mkdir(parents=True, exist_ok=True)
                all_file.write_bytes(all_file_backup)
        else:
            if all_file.exists():
                all_file.unlink()
    except Exception:
        # 不影响导出主流程
        pass

    # 5. 维护 instruments/index.txt
    instruments_dir = bin_dir / "instruments"
    instruments_dir.mkdir(parents=True, exist_ok=True)
    index_file = instruments_dir / "index.txt"
    # 写入策略：
    # - 若 index.txt 已存在且包含其它指数，则保留其它行；
    # - 若本次导出的指数已存在，则覆盖更新该行 start/end；
    # - 若本次导出的指数不存在，则追加新行。
    # - 读取兼容 1 列（仅 code）或 3 列（code\tstart\tend）格式；写回统一为 3 列 Tab 分隔。
    # start/end 使用本次导出区间内实际存在数据的 min/max(trade_date)，失败则回退到 body.start/body.end。
    try:
        if body.data_source == "tdx":
            idx_df = _db_reader.load_index_daily_tdx(body.index_code, body.start, body.end)
        else:
            idx_df = _db_reader.load_index_daily(body.index_code, body.start, body.end)
        if not idx_df.empty and "trade_date" in idx_df.columns:
            min_dt = min(idx_df["trade_date"])
            max_dt = max(idx_df["trade_date"])
            start_str = pd.Timestamp(min_dt).strftime("%Y-%m-%d")
            end_str = pd.Timestamp(max_dt).strftime("%Y-%m-%d")
        else:
            start_str = body.start.strftime("%Y-%m-%d")
            end_str = body.end.strftime("%Y-%m-%d")
    except Exception:
        start_str = body.start.strftime("%Y-%m-%d")
        end_str = body.end.strftime("%Y-%m-%d")

    records: list[tuple[str, str, str]] = []
    if index_file.exists():
        try:
            raw = index_file.read_text(encoding="utf-8")
            for line in raw.splitlines():
                s = line.strip()
                if not s:
                    continue
                parts = s.split("\t")
                if len(parts) >= 3:
                    code = parts[0].strip()
                    s0 = parts[1].strip()
                    e0 = parts[2].strip()
                else:
                    # 兼容 1 列（或空白分隔）
                    parts2 = s.split()
                    code = parts2[0].strip() if parts2 else ""
                    s0 = ""
                    e0 = ""
                if code:
                    records.append((code, s0, e0))
        except Exception:
            records = []

    updated = False
    new_records: list[tuple[str, str, str]] = []
    for code, s0, e0 in records:
        if code == body.index_code:
            new_records.append((code, start_str, end_str))
            updated = True
        else:
            # 保留原有 start/end（若为空则回退到请求区间）
            new_records.append((code, s0 or body.start.strftime("%Y-%m-%d"), e0 or body.end.strftime("%Y-%m-%d")))

    if not updated:
        new_records.append((body.index_code, start_str, end_str))

    content = "".join([f"{c}\t{s}\t{e}\n" for c, s, e in new_records])
    index_file.write_text(content, encoding="utf-8")

    return IndexBinExportResponse(
        snapshot_id=body.snapshot_id,
        index_code=body.index_code,
        csv_dir=str(csv_dir),
        bin_dir=str(bin_dir),
        dump_bin_ok=dump_res.ok,
        check_ok=check_ok,
        stdout_dump=dump_res.stdout,
        stderr_dump=dump_res.stderr,
        stdout_check=stdout_check,
        stderr_check=stderr_check,
    )


# =============================================================================
# 指数 bin 健康检查 API
# =============================================================================


class IndexHealthCheckRequest(BaseModel):
    """指数 bin 健康检查请求.

    当前基于 Qlib bin 目录进行检查：
    - 检查 instruments/index.txt 是否存在且非空；
    - 复用 check_data_health.py 对整个日频 bin 做一次数据健康检查。
    """

    snapshot_id: str = Field(..., description="bin Snapshot ID，对应 QLIB_BIN_ROOT_WIN 下的子目录")


class IndexHealthCheckResponse(BaseModel):
    """指数 bin 健康检查响应."""

    snapshot_id: str
    bin_dir: str
    has_index_file: bool
    index_count: int
    check_ok: Optional[bool]
    stdout_check: Optional[str]
    stderr_check: Optional[str]


@router.post("/api/v1/qlib/index/health_check", response_model=IndexHealthCheckResponse)
async def check_index_bin_health(body: IndexHealthCheckRequest) -> IndexHealthCheckResponse:
    """对指定 Snapshot 的指数 bin 进行健康检查.

    检查内容：
    1. instruments/index.txt 是否存在且至少包含 1 条指数代码；
    2. 复用 RD-Agent 的 check_data_health.py 脚本，对整个日频 bin 目录做一次数据健康检查。

    注意：目前 check_data_health.py 针对的是整个日频数据集（股票 + 指数），
    这里不对单个指数做精细化过滤，只作为整体健康的基线检查。
    """

    bin_root = os.getenv("QLIB_BIN_ROOT_WIN")
    if not bin_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_BIN_ROOT_WIN")

    bin_root_path = Path(bin_root)
    bin_dir = bin_root_path / body.snapshot_id
    if not bin_dir.exists() or not bin_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"bin Snapshot {body.snapshot_id} 不存在")

    # 1. 检查 instruments/index.txt
    instruments_dir = bin_dir / "instruments"
    index_file = instruments_dir / "index.txt"
    has_index_file = index_file.exists()
    index_count = 0
    if has_index_file:
        try:
            lines = [
                line.strip()
                for line in index_file.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            index_count = len(lines)
            has_index_file = index_count > 0
        except Exception:
            # 读取失败视作无效 index 文件
            has_index_file = False
            index_count = 0

    # 2. 通过 WSL 调用 check_data_health.py 对整个日频 bin 做一次健康检查
    check_ok: Optional[bool] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None

    try:
        bin_dir_wsl = win_to_wsl_path(str(bin_dir))
        check_args = [
            "--qlib_dir",
            bin_dir_wsl,
            "--freq",
            "day",
        ]
        check_res = run_qlib_script_in_wsl("check_data_health.py", check_args)
        check_ok = check_res.ok
        stdout_check = check_res.stdout
        stderr_check = check_res.stderr
    except QlibWSLConfigError as exc:  # 配置问题直接返回给前端
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        # 其余错误记录在 stderr_check 中返回
        check_ok = None
        stdout_check = None
        stderr_check = str(exc)

    return IndexHealthCheckResponse(
        snapshot_id=body.snapshot_id,
        bin_dir=str(bin_dir),
        has_index_file=has_index_file,
        index_count=index_count,
        check_ok=check_ok,
        stdout_check=stdout_check,
        stderr_check=stderr_check,
    )


class MinuteSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    ts_codes: Optional[List[str]] = Field(
        None,
        description="可选，指定导出的 ts_code 列表；为空则导出全部 ts_code（基于分钟线表）",
    )
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz'；北交所固定排除；为空默认 SH/SZ",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）",
    )
    freq: str = Field("1m", description="分钟线频率，当前固定为 1m")

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _end_not_before_start(self) -> "MinuteSnapshotRequest":  # noqa: D401, N805
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class MinuteSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "MinuteSnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/snapshots/minute", response_model=MinuteSnapshotResponse)
async def create_minute_snapshot(body: MinuteSnapshotRequest) -> MinuteSnapshotResponse:
    """触发一次分钟线 Qlib Snapshot 导出（目前支持 1m，按日期区间导出全天分钟线）。"""

    try:
        result = _minute_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            ts_codes=body.ts_codes,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
            freq=body.freq,
        )
        return MinuteSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except NotImplementedError as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# 配置与 Snapshot 管理 API
# =============================================================================


class QlibConfigResponse(BaseModel):
    """Qlib 导出配置响应."""

    snapshot_root: str = Field(..., description="Snapshot 根目录路径")
    market: str = Field(..., description="市场标识")
    daily_table: str = Field(..., description="日频前复权表名")
    minute_table: str = Field(..., description="分钟线表名")
    field_mapping_daily: Dict[str, str] = Field(..., description="日频字段映射")
    field_mapping_minute: Dict[str, str] = Field(..., description="分钟线字段映射")


@router.get("/api/v1/qlib/config", response_model=QlibConfigResponse)
async def get_qlib_config() -> QlibConfigResponse:
    """获取当前 Qlib 导出配置."""

    return QlibConfigResponse(
        snapshot_root=str(QLIB_SNAPSHOT_ROOT.absolute()),
        market=QLIB_MARKET,
        daily_table=DAILY_RAW_TABLE,
        minute_table=MINUTE_QFQ_TABLE,
        field_mapping_daily=FIELD_MAPPING_DB_DAILY,
        field_mapping_minute=FIELD_MAPPING_DB_MINUTE,
    )


class SnapshotInfo(BaseModel):
    """单个 Snapshot 的信息."""

    snapshot_id: str = Field(..., description="Snapshot ID")
    path: str = Field(..., description="Snapshot 目录路径")
    has_daily: bool = Field(..., description="是否包含日频数据")
    has_minute: bool = Field(..., description="是否包含分钟线数据")
    has_factor_data: bool = Field(False, description="是否包含 RD-Agent 因子数据")
    has_moneyflow: bool = Field(False, description="是否包含资金流向数据")
    has_daily_basic: bool = Field(False, description="是否包含 daily_basic 指标数据")
    has_bak_basic: bool = Field(False, description="是否包含 bak_basic 历史股票数据")
    has_margin_detail: bool = Field(False, description="是否包含 margin_detail 融资融券明细数据")
    has_cyq_perf: bool = Field(False, description="是否包含 cyq_perf 筹码胜率数据")
    has_sector_data: bool = Field(False, description="是否包含 sector_data 申万行业板块数据")
    has_static_factors: bool = Field(False, description="是否包含 static_factors.parquet")
    meta: Optional[Dict[str, Any]] = Field(None, description="meta.json 内容（如存在）")
    created_at: Optional[str] = Field(None, description="创建时间（从 meta.json 读取）")


class SnapshotListResponse(BaseModel):
    """Snapshot 列表响应."""

    snapshots: List[SnapshotInfo] = Field(..., description="Snapshot 列表")
    total: int = Field(..., description="总数")


@router.get("/api/v1/qlib/snapshots", response_model=SnapshotListResponse)
async def list_snapshots() -> SnapshotListResponse:
    """罗列现有 Snapshot 目录."""

    snapshots: List[SnapshotInfo] = []

    if not QLIB_SNAPSHOT_ROOT.exists():
        return SnapshotListResponse(snapshots=[], total=0)

    for item in QLIB_SNAPSHOT_ROOT.iterdir():
        if not item.is_dir():
            continue

        snapshot_id = item.name
        has_daily_pv = (item / "daily_pv.h5").exists()
        has_minute = (item / "minute_1min.h5").exists()
        has_moneyflow = (item / "moneyflow.h5").exists()
        has_daily_basic = (item / "daily_basic.h5").exists()
        has_bak_basic = (item / "bak_basic.h5").exists()
        has_margin_detail = (item / "margin_detail.h5").exists()
        has_cyq_perf = (item / "cyq_perf.h5").exists()
        has_sector_data = (item / "sector_data.h5").exists()
        has_static_factors = (item / "static_factors.parquet").exists()

        meta: Optional[Dict[str, Any]] = None
        created_at: Optional[str] = None
        meta_path = item / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                created_at = meta.get("generated_at")
            except Exception:
                pass

        snapshots.append(
            SnapshotInfo(
                snapshot_id=snapshot_id,
                path=str(item.absolute()),
                has_daily=has_daily_pv,
                has_minute=has_minute,
                has_factor_data=has_daily_pv,
                has_moneyflow=has_moneyflow,
                has_daily_basic=has_daily_basic,
                has_bak_basic=has_bak_basic,
                has_margin_detail=has_margin_detail,
                has_cyq_perf=has_cyq_perf,
                has_sector_data=has_sector_data,
                has_static_factors=has_static_factors,
                meta=meta,
                created_at=created_at,
            )
        )

    # 按创建时间倒序排列
    snapshots.sort(key=lambda x: x.created_at or "", reverse=True)

    return SnapshotListResponse(snapshots=snapshots, total=len(snapshots))


@router.get("/api/v1/qlib/snapshots/{snapshot_id}/export")
async def export_snapshot_zip(snapshot_id: str):
    """导出指定 Snapshot 目录为 zip 下载."""

    sid = (snapshot_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="snapshot_id 不能为空")

    root = QLIB_SNAPSHOT_ROOT.resolve()
    snap_dir = (root / sid).resolve()
    if root not in snap_dir.parents and snap_dir != root:
        raise HTTPException(status_code=400, detail="invalid snapshot_id")
    if not snap_dir.exists() or not snap_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"snapshot not found: {sid}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in snap_dir.rglob("*"):
            if p.is_dir():
                continue
            rel = p.relative_to(snap_dir)
            zf.write(p, arcname=str(Path(sid) / rel))

    buf.seek(0)
    filename = f"{sid}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buf, media_type="application/zip", headers=headers)


class DeleteSnapshotResponse(BaseModel):
    """删除 Snapshot 响应."""

    snapshot_id: str = Field(..., description="被删除的 Snapshot ID")
    deleted: bool = Field(..., description="是否成功删除")
    message: str = Field(..., description="操作结果消息")


@router.delete("/api/v1/qlib/snapshots/{snapshot_id}", response_model=DeleteSnapshotResponse)
async def delete_snapshot(snapshot_id: str) -> DeleteSnapshotResponse:
    """删除指定的 Snapshot 目录."""

    snapshot_path = QLIB_SNAPSHOT_ROOT / snapshot_id

    if not snapshot_path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot '{snapshot_id}' 不存在")

    if not snapshot_path.is_dir():
        raise HTTPException(status_code=400, detail=f"'{snapshot_id}' 不是有效的 Snapshot 目录")

    try:
        shutil.rmtree(snapshot_path)
        return DeleteSnapshotResponse(
            snapshot_id=snapshot_id,
            deleted=True,
            message=f"Snapshot '{snapshot_id}' 已成功删除",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"删除失败: {exc}")


# =============================================================================
# 增量导出 API
# =============================================================================


class IncrementalExportRequest(BaseModel):
    """增量导出请求（通用）."""

    snapshot_id: str = Field(..., description="Snapshot ID")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，交易所过滤（仅分钟线有效）：sh, sz",
    )
    exclude_st: bool = Field(
        True,
        description="是否排除所有在 stock_st 中出现过的股票（仅分钟线有效）",
    )
    exclude_delisted_or_paused: bool = Field(
        True,
        description="是否排除退市或当前暂停上市股票（仅分钟线有效；stock_basic.list_status in ('D','P')）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2


class IncrementalExportResponse(BaseModel):
    """增量导出响应."""

    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int
    is_incremental: bool = True

    @classmethod
    def from_result(cls, result: ExportResult) -> "IncrementalExportResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            ts_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/snapshots/minute/incremental", response_model=IncrementalExportResponse)
async def create_minute_snapshot_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出分钟线数据。从上次导出位置继续。"""

    try:
        result = _minute_exporter.export_incremental(
            snapshot_id=body.snapshot_id,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# Sector Data 全量 + 增量导出 API
# =============================================================================


class SectorDataSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(None, description="可选，交易所过滤")
    exclude_st: bool = Field(True)
    exclude_delisted_or_paused: bool = Field(True)

    @field_validator("snapshot_id")
    @classmethod
    def _sector_data_snapshot_id_not_empty(cls, v: str) -> str:
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _sector_data_end_not_before_start(self):
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class SectorDataSnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    ts_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "SectorDataSnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id, freq=result.freq,
            start=result.start, end=result.end,
            ts_codes=result.ts_codes, rows=result.rows,
        )


try:
    SectorDataSnapshotRequest.model_rebuild()
except Exception:
    pass


@router.post("/api/v1/qlib/snapshots/sector_data", response_model=SectorDataSnapshotResponse)
async def create_sector_data_snapshot(body: SectorDataSnapshotRequest) -> SectorDataSnapshotResponse:
    """全量导出申万行业板块 sector_data 到 Snapshot."""
    try:
        result = _sector_data_exporter.export_full(
            snapshot_id=body.snapshot_id, start=body.start, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return SectorDataSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/sector_data/incremental", response_model=IncrementalExportResponse)
async def create_sector_data_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出 sector_data 数据。"""
    try:
        result = _sector_data_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# 日频数据增量导出 API（daily, moneyflow, daily_basic, bak_basic, cyq_perf）
# =============================================================================


@router.post("/api/v1/qlib/snapshots/daily/incremental", response_model=IncrementalExportResponse)
async def create_daily_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出日频行情数据。"""
    try:
        result = _daily_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/moneyflow/incremental", response_model=IncrementalExportResponse)
async def create_moneyflow_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出个股资金流向数据。"""
    try:
        result = _moneyflow_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/daily_basic/incremental", response_model=IncrementalExportResponse)
async def create_daily_basic_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出每日指标数据。"""
    try:
        result = _daily_basic_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/bak_basic/incremental", response_model=IncrementalExportResponse)
async def create_bak_basic_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出历史股票列表数据。"""
    try:
        result = _bak_basic_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/margin_detail/incremental", response_model=IncrementalExportResponse)
async def create_margin_detail_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出融资融券明细数据。"""
    try:
        result = _margin_detail_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/cyq_perf/incremental", response_model=IncrementalExportResponse)
async def create_cyq_perf_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出每日筹码及胜率数据。"""
    try:
        result = _cyq_perf_exporter.export_incremental(
            snapshot_id=body.snapshot_id, end=body.end,
            exchanges=body.exchanges, exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# Static Factors 生成 API
# =============================================================================


class StaticFactorsResponse(BaseModel):
    snapshot_id: str
    rows: int
    columns: int
    parquet_path: str


@router.post("/api/v1/qlib/snapshots/{snapshot_id}/static_factors", response_model=StaticFactorsResponse)
async def build_static_factors_for_snapshot(snapshot_id: str) -> StaticFactorsResponse:
    """为指定 Snapshot 生成 static_factors.parquet."""
    sid = (snapshot_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="snapshot_id 不能为空")

    snap_dir = QLIB_SNAPSHOT_ROOT / sid
    h5_path = snap_dir / "daily_pv.h5"
    if not h5_path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot {sid} 中未找到 daily_pv.h5，请先导出日频数据")

    try:
        df_daily = pd.read_hdf(str(h5_path), key="data")
        instruments = df_daily.index.get_level_values("instrument").unique().tolist()
        dates = df_daily.index.get_level_values("datetime")
        start_date = dates.min().date()
        end_date = dates.max().date()

        from ..data_service.qe_data_service import build_static_factors
        df_sf = build_static_factors(instruments, start_date, end_date)

        parquet_path = snap_dir / "static_factors.parquet"
        df_sf.to_parquet(str(parquet_path))

        return StaticFactorsResponse(
            snapshot_id=sid,
            rows=len(df_sf),
            columns=len(df_sf.columns),
            parquet_path=str(parquet_path),
        )
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# 统一 Bin 导出 API（股票日线 + 指数一键导出）
# =============================================================================


class UnifiedBinExportRequest(BaseModel):
    """统一 Bin 导出请求：股票日线 + 可选指数。"""
    snapshot_id: str = Field(..., description="bin Snapshot ID")
    start: date = Field(..., description="开始日期")
    end: date = Field(..., description="结束日期")
    exchanges: Optional[List[str]] = Field(None, description="交易所过滤")
    exclude_st: bool = Field(True)
    exclude_delisted_or_paused: bool = Field(True)
    run_health_check: bool = Field(True)
    index_codes: Optional[List[str]] = Field(None, description="指数代码列表，如 ['000300.SH']")
    index_data_source: Literal["tushare", "tdx"] = Field("tushare")


class UnifiedBinExportResponse(BaseModel):
    snapshot_id: str
    stock_ok: bool
    stock_csv_dir: Optional[str] = None
    stock_bin_dir: Optional[str] = None
    stock_stdout: Optional[str] = None
    stock_stderr: Optional[str] = None
    index_results: List[Dict[str, Any]] = []
    check_ok: Optional[bool] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None


@router.post("/api/v1/qlib/bin/unified_export", response_model=UnifiedBinExportResponse)
async def unified_bin_export(body: UnifiedBinExportRequest) -> UnifiedBinExportResponse:
    """一键导出股票日线 bin + 指数 bin。"""
    try:
        stock_exchanges = normalize_stock_export_exchanges(body.exchanges)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # 1. 导出股票日线
    csv_dir = _export_daily_to_csv_for_dump_bin(
        snapshot_id=body.snapshot_id, start=body.start, end=body.end,
        exchanges=stock_exchanges,
        exclude_st=body.exclude_st,
        exclude_delisted_or_paused=body.exclude_delisted_or_paused,
    )

    bin_root = os.getenv("QLIB_BIN_ROOT_WIN")
    if not bin_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_BIN_ROOT_WIN")

    bin_dir = Path(bin_root) / body.snapshot_id
    bin_dir.mkdir(parents=True, exist_ok=True)

    csv_dir_wsl = win_to_wsl_path(str(csv_dir))
    bin_dir_wsl = win_to_wsl_path(str(bin_dir))

    dump_args = [
        "dump_all", "--data_path", csv_dir_wsl, "--qlib_dir", bin_dir_wsl,
        "--freq", "day", "--date_field_name", "date",
        "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
    ]
    dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)

    # 2. 导出指数（dump_bin 会覆盖 instruments/all.txt，需要先备份再还原，并将指数写入 index.txt）
    index_results = []
    if body.index_codes:
        inst_dir = bin_dir / "instruments"
        inst_file = inst_dir / "all.txt"
        # 备份股票 instruments
        stock_instruments_text = ""
        if inst_file.exists():
            stock_instruments_text = inst_file.read_text(encoding="utf-8")

        index_instrument_lines: list[str] = []
        for idx_code in body.index_codes:
            try:
                idx_csv_dir = _export_index_to_csv_for_dump_bin(
                    snapshot_id=body.snapshot_id, index_code=idx_code,
                    start=body.start, end=body.end,
                    data_source=body.index_data_source,
                )
                idx_csv_wsl = win_to_wsl_path(str(idx_csv_dir))
                idx_dump_args = [
                    "dump_all", "--data_path", idx_csv_wsl, "--qlib_dir", bin_dir_wsl,
                    "--freq", "day", "--date_field_name", "date",
                    "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
                ]
                idx_res = run_qlib_script_in_wsl("dump_bin.py", idx_dump_args)
                index_results.append({
                    "index_code": idx_code, "ok": idx_res.ok,
                    "stdout": idx_res.stdout, "stderr": idx_res.stderr,
                })
                # 收集指数 instrument 行（dump_bin 写入的 all.txt 内容）
                if idx_res.ok and inst_file.exists():
                    idx_lines = inst_file.read_text(encoding="utf-8").strip().splitlines()
                    index_instrument_lines.extend(idx_lines)
            except Exception as exc:
                index_results.append({
                    "index_code": idx_code, "ok": False, "error": str(exc),
                })

        # 还原股票 instruments/all.txt
        if stock_instruments_text:
            inst_file.write_text(stock_instruments_text, encoding="utf-8")

        # 将指数写入单独的 instruments/index.txt
        if index_instrument_lines:
            idx_inst_file = inst_dir / "index.txt"
            idx_inst_file.write_text("\n".join(index_instrument_lines) + "\n", encoding="utf-8")

    # 3. 可选健康检查
    check_ok = None
    stdout_check = None
    stderr_check = None
    if body.run_health_check:
        check_args = ["--qlib_dir", bin_dir_wsl, "--freq", "day"]
        check_res = run_qlib_script_in_wsl("check_data_health.py", check_args)
        check_ok = check_res.ok
        stdout_check = check_res.stdout
        stderr_check = check_res.stderr

    # 4. 写出 meta
    try:
        meta = {
            "snapshot_id": body.snapshot_id,
            "start": body.start.isoformat(), "end": body.end.isoformat(),
            "exchanges": stock_exchanges,
            "exclude_st": body.exclude_st,
            "exclude_delisted_or_paused": True,
            "exclude_bj": True,
            "min_listed_days": IPO_FILTER_DAYS,
            "freq_types": ["daily"],
            "index_codes": body.index_codes or [],
        }
        (bin_dir / "meta_export.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass

    return UnifiedBinExportResponse(
        snapshot_id=body.snapshot_id, stock_ok=dump_res.ok,
        stock_csv_dir=str(csv_dir), stock_bin_dir=str(bin_dir),
        stock_stdout=dump_res.stdout, stock_stderr=dump_res.stderr,
        index_results=index_results,
        check_ok=check_ok, stdout_check=stdout_check, stderr_check=stderr_check,
    )


# =============================================================================
# 统一 Bin 导出 V2（多选数据集 + 增量模式）
# =============================================================================


class BinDatasetStepResult(BaseModel):
    """V2 导出中每个数据集的结果。"""
    dataset: str
    ok: bool
    rows: Optional[int] = None
    error: Optional[str] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    mode_used: Optional[str] = None  # "dump_all" | "dump_update"


class UnifiedBinExportRequestV2(BaseModel):
    """V2 统一 Bin 导出请求：多选数据集 + 全量/增量模式。"""
    snapshot_id: str = Field(..., description="bin Snapshot ID")
    mode: Literal["full", "incremental"] = Field("full", description="导出模式")
    start: Optional[date] = Field(None, description="开始日期（full 模式必填）")
    end: date = Field(..., description="截止日期")
    datasets: List[str] = Field(..., description='数据集列表，如 ["stock_daily", "000300.SH"]')
    exchanges: Optional[List[str]] = Field(None, description="交易所过滤")
    exclude_st: bool = Field(True)
    exclude_delisted_or_paused: bool = Field(True)
    run_health_check: bool = Field(True)
    index_data_source: Literal["tushare", "tdx"] = Field("tushare")

    @model_validator(mode="after")
    def _validate_full_mode_start(self) -> "UnifiedBinExportRequestV2":
        if self.mode == "full" and self.start is None:
            raise ValueError("全量模式下 start 不能为空")
        return self


class UnifiedBinExportResponseV2(BaseModel):
    """V2 统一 Bin 导出响应。"""
    snapshot_id: str
    mode: str
    steps: List[BinDatasetStepResult] = []
    check_ok: Optional[bool] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None


def _backup_file(path: Path) -> Optional[bytes]:
    """读取文件内容用于备份，不存在返回 None。"""
    if path.exists():
        return path.read_bytes()
    return None


def _restore_file(path: Path, backup: Optional[bytes]) -> None:
    """从备份恢复文件，若原本不存在则删除当前文件。"""
    if backup is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(backup)
    elif path.exists():
        path.unlink()


def _load_bin_meta(bin_dir: Path) -> dict:
    """读取 meta_export.json，不存在返回空 dict。"""
    meta_path = bin_dir / "meta_export.json"
    if meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))
    return {}


def _get_incremental_start(meta: dict, dataset_key: str) -> Optional[date]:
    """从 meta.last_end_dates 推算增量起始日期 = last_end + 1 天。"""
    last_end_dates = meta.get("last_end_dates", {})
    last_end_str = last_end_dates.get(dataset_key)
    if last_end_str is None:
        return None
    last_end = date.fromisoformat(last_end_str)
    return last_end + timedelta(days=1)


def _resolve_stock_qfq_basis(
    *,
    meta: dict,
    mode: str,
    start: Optional[date],
    end: date,
) -> tuple[date, date]:
    """Resolve the qfq denominator window for stock day/1min bin export.

    dump_update cannot rewrite all historical factor-scaled OHLCV values. For
    stock datasets, extending beyond the original basis_end could make an
    incremental snapshot differ from a full authoritative rebuild, so this path
    fails fast instead of silently producing mixed qfq bases.
    """

    if mode == "full":
        if start is None:
            raise HTTPException(status_code=400, detail="full stock bin export requires start")
        return start, end

    basis_start_raw = meta.get("basis_start") or meta.get("start")
    basis_end_raw = meta.get("basis_end") or meta.get("end")
    if not basis_start_raw or not basis_end_raw:
        raise HTTPException(
            status_code=400,
            detail="incremental stock bin export requires meta_export.json basis_start/basis_end; run a full export first",
        )
    basis_start = date.fromisoformat(str(basis_start_raw))
    basis_end = date.fromisoformat(str(basis_end_raw))
    if end > basis_end:
        raise HTTPException(
            status_code=400,
            detail=(
                "incremental stock bin export would extend qfq basis_end and can differ from a full rebuild; "
                "run full export for stock_daily/stock_minute"
            ),
        )
    return basis_start, basis_end


def _update_index_instruments(bin_dir: Path, index_code: str, start_str: str, end_str: str) -> None:
    """将指数条目合并写入 instruments/index.txt（保留已有条目）。"""
    instruments_dir = bin_dir / "instruments"
    instruments_dir.mkdir(parents=True, exist_ok=True)
    index_file = instruments_dir / "index.txt"

    records: list[tuple[str, str, str]] = []
    if index_file.exists():
        raw = index_file.read_text(encoding="utf-8")
        for line in raw.splitlines():
            s = line.strip()
            if not s:
                continue
            parts = s.split("\t")
            if len(parts) >= 3:
                records.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
            else:
                parts2 = s.split()
                if parts2:
                    records.append((parts2[0].strip(), "", ""))

    updated = False
    new_records: list[tuple[str, str, str]] = []
    for code, s0, e0 in records:
        if code == index_code:
            # 合并日期范围：取 min(start), max(end)
            merged_start = min(s0, start_str) if s0 else start_str
            merged_end = max(e0, end_str) if e0 else end_str
            new_records.append((code, merged_start, merged_end))
            updated = True
        else:
            new_records.append((code, s0, e0))

    if not updated:
        new_records.append((index_code, start_str, end_str))

    content = "".join([f"{c}\t{s}\t{e}\n" for c, s, e in new_records])
    index_file.write_text(content, encoding="utf-8")


def _extract_index_entry_from_all_txt(all_txt_path: Path, index_code: str) -> Optional[tuple[str, str, str]]:
    """从 dump 后被覆盖的 all.txt 中提取指定指数的条目。"""
    if not all_txt_path.exists():
        return None
    raw = all_txt_path.read_text(encoding="utf-8")
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        parts = s.split("\t")
        if len(parts) >= 3:
            code = parts[0].strip()
            # dump_bin 会将 ts_code 转换为 Qlib 内部格式（小写前缀），如 000300.SH → sh000300
            # 也可能直接保留原格式，需要两种都匹配
            qlib_code = index_code.split(".")[0]  # "000300"
            suffix = index_code.split(".")[-1].lower() if "." in index_code else ""  # "sh"
            qlib_internal = f"{suffix}{qlib_code}"  # "sh000300"
            if code == index_code or code == qlib_internal:
                return (code, parts[1].strip(), parts[2].strip())
    return None


@router.post("/api/v1/qlib/bin/unified_export_v2", response_model=UnifiedBinExportResponseV2)
async def unified_bin_export_v2(body: UnifiedBinExportRequestV2) -> UnifiedBinExportResponseV2:
    """V2 统一导出：多选数据集 + 全量/增量模式 + instruments 双文件维护。"""

    bin_root = os.getenv("QLIB_BIN_ROOT_WIN")
    if not bin_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_BIN_ROOT_WIN")

    bin_dir = Path(bin_root) / body.snapshot_id
    bin_dir_wsl = win_to_wsl_path(str(bin_dir))
    all_txt = bin_dir / "instruments" / "all.txt"

    # 增量模式前置校验
    meta = _load_bin_meta(bin_dir)
    if body.mode == "incremental":
        if not bin_dir.exists() or not meta:
            raise HTTPException(status_code=400, detail="增量模式要求 bin 目录已存在且有 meta_export.json（请先执行全量导出）")

    bin_dir.mkdir(parents=True, exist_ok=True)

    steps: List[BinDatasetStepResult] = []
    last_end_dates: Dict[str, str] = meta.get("last_end_dates", {})
    has_stock_dataset = any(ds in body.datasets for ds in ("stock_daily", "stock_minute"))
    stock_exchanges: Optional[List[str]] = None
    if has_stock_dataset:
        try:
            stock_exchanges = normalize_stock_export_exchanges(body.exchanges)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    stock_basis_start: Optional[date] = None
    stock_basis_end: Optional[date] = None
    if has_stock_dataset:
        stock_basis_start, stock_basis_end = _resolve_stock_qfq_basis(
            meta=meta,
            mode=body.mode,
            start=body.start,
            end=body.end,
        )

    # ──────────────────────────────────────────────────────
    # 步骤 1：stock_daily
    # ──────────────────────────────────────────────────────
    if "stock_daily" in body.datasets:
        try:
            if body.mode == "incremental":
                inc_start = _get_incremental_start(meta, "stock_daily")
                if inc_start is None:
                    raise HTTPException(status_code=400, detail="增量模式下 meta 中缺少 stock_daily 的 last_end_dates 记录")
                if inc_start > body.end:
                    steps.append(BinDatasetStepResult(
                        dataset="stock_daily", ok=True, rows=0,
                        mode_used="dump_update",
                    ))
                else:
                    csv_dir = _export_daily_to_csv_for_dump_bin(
                        snapshot_id=body.snapshot_id, start=inc_start, end=body.end,
                        exchanges=stock_exchanges,
                        exclude_st=body.exclude_st,
                        exclude_delisted_or_paused=body.exclude_delisted_or_paused,
                        basis_start=stock_basis_start,
                        basis_end=stock_basis_end,
                    )
                    csv_dir_wsl = win_to_wsl_path(str(csv_dir))
                    dump_args = [
                        "dump_update", "--data_path", csv_dir_wsl, "--qlib_dir", bin_dir_wsl,
                        "--freq", "day", "--date_field_name", "date",
                        "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
                    ]
                    dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)
                    steps.append(BinDatasetStepResult(
                        dataset="stock_daily", ok=dump_res.ok,
                        stdout=dump_res.stdout, stderr=dump_res.stderr,
                        mode_used="dump_update",
                    ))
                    if dump_res.ok:
                        last_end_dates["stock_daily"] = body.end.isoformat()
            else:
                # full 模式
                assert body.start is not None
                csv_dir = _export_daily_to_csv_for_dump_bin(
                    snapshot_id=body.snapshot_id, start=body.start, end=body.end,
                    exchanges=stock_exchanges,
                    exclude_st=body.exclude_st,
                    exclude_delisted_or_paused=body.exclude_delisted_or_paused,
                    basis_start=stock_basis_start,
                    basis_end=stock_basis_end,
                )
                csv_dir_wsl = win_to_wsl_path(str(csv_dir))
                dump_args = [
                    "dump_all", "--data_path", csv_dir_wsl, "--qlib_dir", bin_dir_wsl,
                    "--freq", "day", "--date_field_name", "date",
                    "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
                ]
                dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)
                steps.append(BinDatasetStepResult(
                    dataset="stock_daily", ok=dump_res.ok,
                    stdout=dump_res.stdout, stderr=dump_res.stderr,
                    mode_used="dump_all",
                ))
                if dump_res.ok:
                    last_end_dates["stock_daily"] = body.end.isoformat()
        except HTTPException:
            raise
        except Exception as exc:
            steps.append(BinDatasetStepResult(
                dataset="stock_daily", ok=False, error=str(exc),
            ))

    # ──────────────────────────────────────────────────────
    # 步骤 2：逐个指数
    # ──────────────────────────────────────────────────────
    if "stock_minute" in body.datasets:
        try:
            dataset_key = f"stock_minute_{MINUTE_FREQ_QLIB}"
            if body.mode == "incremental":
                inc_start = _get_incremental_start(meta, dataset_key)
                if inc_start is None:
                    raise HTTPException(status_code=400, detail=f"澧為噺妯″紡涓?meta 涓己灏?{dataset_key} 鐨?last_end_dates 璁板綍")
                if inc_start > body.end:
                    steps.append(BinDatasetStepResult(
                        dataset="stock_minute", ok=True, rows=0,
                        mode_used="dump_update",
                    ))
                else:
                    csv_dir = _export_minute_to_csv_for_dump_bin(
                        snapshot_id=body.snapshot_id, start=inc_start, end=body.end,
                        exchanges=stock_exchanges,
                        exclude_st=body.exclude_st,
                        exclude_delisted_or_paused=body.exclude_delisted_or_paused,
                        freq=MINUTE_FREQ_QLIB,
                        basis_start=stock_basis_start,
                        basis_end=stock_basis_end,
                    )
                    csv_dir_wsl = win_to_wsl_path(str(csv_dir))
                    dump_args = [
                        "dump_update", "--data_path", csv_dir_wsl, "--qlib_dir", bin_dir_wsl,
                        "--freq", MINUTE_FREQ_QLIB, "--date_field_name", "date",
                        "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
                    ]
                    dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)
                    steps.append(BinDatasetStepResult(
                        dataset="stock_minute", ok=dump_res.ok,
                        stdout=dump_res.stdout, stderr=dump_res.stderr,
                        mode_used="dump_update",
                    ))
                    if dump_res.ok:
                        last_end_dates[dataset_key] = body.end.isoformat()
            else:
                assert body.start is not None
                csv_dir = _export_minute_to_csv_for_dump_bin(
                    snapshot_id=body.snapshot_id, start=body.start, end=body.end,
                    exchanges=stock_exchanges,
                    exclude_st=body.exclude_st,
                    exclude_delisted_or_paused=body.exclude_delisted_or_paused,
                    freq=MINUTE_FREQ_QLIB,
                    basis_start=stock_basis_start,
                    basis_end=stock_basis_end,
                )
                csv_dir_wsl = win_to_wsl_path(str(csv_dir))
                dump_args = [
                    "dump_all", "--data_path", csv_dir_wsl, "--qlib_dir", bin_dir_wsl,
                    "--freq", MINUTE_FREQ_QLIB, "--date_field_name", "date",
                    "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
                ]
                dump_res = run_qlib_script_in_wsl("dump_bin.py", dump_args)
                steps.append(BinDatasetStepResult(
                    dataset="stock_minute", ok=dump_res.ok,
                    stdout=dump_res.stdout, stderr=dump_res.stderr,
                    mode_used="dump_all",
                ))
                if dump_res.ok:
                    last_end_dates[dataset_key] = body.end.isoformat()
        except HTTPException:
            raise
        except Exception as exc:
            steps.append(BinDatasetStepResult(
                dataset="stock_minute", ok=False, error=str(exc),
            ))

    index_datasets = [ds for ds in body.datasets if ds not in {"stock_daily", "stock_minute"}]
    for idx_code in index_datasets:
        dataset_key = f"index_{idx_code}"
        try:
            # 确定起止日期
            if body.mode == "incremental":
                inc_start = _get_incremental_start(meta, dataset_key)
                if inc_start is None:
                    raise HTTPException(status_code=400, detail=f"增量模式下 meta 中缺少 {dataset_key} 的 last_end_dates 记录")
                if inc_start > body.end:
                    steps.append(BinDatasetStepResult(
                        dataset=idx_code, ok=True, rows=0,
                        mode_used="dump_update",
                    ))
                    continue
                idx_start = inc_start
                dump_subcmd = "dump_update"
            else:
                assert body.start is not None
                idx_start = body.start
                dump_subcmd = "dump_all"

            # BACKUP all.txt
            all_backup = _backup_file(all_txt)

            # 导出指数 CSV
            idx_csv_dir = _export_index_to_csv_for_dump_bin(
                snapshot_id=body.snapshot_id, index_code=idx_code,
                start=idx_start, end=body.end,
                data_source=body.index_data_source,
            )
            idx_csv_wsl = win_to_wsl_path(str(idx_csv_dir))

            # dump
            idx_dump_args = [
                dump_subcmd, "--data_path", idx_csv_wsl, "--qlib_dir", bin_dir_wsl,
                "--freq", "day", "--date_field_name", "date",
                "--symbol_field_name", "symbol", "--exclude_fields", "date,symbol",
            ]
            idx_res = run_qlib_script_in_wsl("dump_bin.py", idx_dump_args)

            # 从被覆盖的 all.txt 提取指数条目
            idx_entry = _extract_index_entry_from_all_txt(all_txt, idx_code)

            # RESTORE all.txt
            _restore_file(all_txt, all_backup)

            # 写入 instruments/index.txt
            if idx_entry:
                _update_index_instruments(bin_dir, idx_entry[0], idx_entry[1], idx_entry[2])
            else:
                # 没从 all.txt 提取到，用请求的日期区间
                _update_index_instruments(bin_dir, idx_code, idx_start.isoformat(), body.end.isoformat())

            steps.append(BinDatasetStepResult(
                dataset=idx_code, ok=idx_res.ok,
                stdout=idx_res.stdout, stderr=idx_res.stderr,
                mode_used=dump_subcmd,
            ))
            if idx_res.ok:
                last_end_dates[dataset_key] = body.end.isoformat()

        except HTTPException:
            raise
        except Exception as exc:
            # 确保 all.txt 被恢复
            try:
                _restore_file(all_txt, all_backup)  # type: ignore[possibly-undefined]
            except Exception:
                pass
            steps.append(BinDatasetStepResult(
                dataset=idx_code, ok=False, error=str(exc),
            ))

    # ──────────────────────────────────────────────────────
    # 步骤 3：可选健康检查
    # ──────────────────────────────────────────────────────
    check_ok: Optional[bool] = None
    stdout_check: Optional[str] = None
    stderr_check: Optional[str] = None
    if body.run_health_check:
        check_freqs: list[str] = []
        if "stock_daily" in body.datasets or index_datasets:
            check_freqs.append("day")
        if "stock_minute" in body.datasets:
            check_freqs.append(MINUTE_FREQ_QLIB)
        check_outputs: list[str] = []
        check_errors: list[str] = []
        check_results: list[bool] = []
        for check_freq in check_freqs:
            check_args = ["--qlib_dir", bin_dir_wsl, "--freq", check_freq]
            check_res = run_qlib_script_in_wsl("check_data_health.py", check_args)
            check_results.append(check_res.ok)
            check_outputs.append(f"===== check_data_health freq={check_freq} =====\n{check_res.stdout}")
            check_errors.append(f"===== check_data_health freq={check_freq} =====\n{check_res.stderr}")
        check_ok = all(check_results) if check_results else None
        stdout_check = "\n".join(check_outputs) if check_outputs else None
        stderr_check = "\n".join(check_errors) if check_errors else None

    # ──────────────────────────────────────────────────────
    # 步骤 4：更新 meta_export.json
    # ──────────────────────────────────────────────────────
    freq_types = set(meta.get("freq_types", []))
    if "stock_daily" in body.datasets or index_datasets:
        freq_types.add("daily")
    if "stock_minute" in body.datasets:
        freq_types.add(MINUTE_FREQ_QLIB)

    meta_update = {
        "snapshot_id": body.snapshot_id,
        "end": body.end.isoformat(),
        "exchanges": stock_exchanges if has_stock_dataset else (list(body.exchanges) if body.exchanges else None),
        "exclude_st": body.exclude_st,
        "exclude_delisted_or_paused": True,
        "exclude_bj": bool(has_stock_dataset),
        "min_listed_days": IPO_FILTER_DAYS if has_stock_dataset else None,
        "freq_types": sorted(freq_types),
        "index_codes": index_datasets,
        "last_end_dates": last_end_dates,
    }
    if stock_basis_start is not None and stock_basis_end is not None:
        meta_update["basis_start"] = stock_basis_start.isoformat()
        meta_update["basis_end"] = stock_basis_end.isoformat()
    else:
        if "basis_start" in meta:
            meta_update["basis_start"] = meta["basis_start"]
        if "basis_end" in meta:
            meta_update["basis_end"] = meta["basis_end"]
    if body.mode == "full" and body.start is not None:
        meta_update["start"] = body.start.isoformat()
    elif "start" in meta:
        meta_update["start"] = meta["start"]
    # 合并已有 index_codes
    existing_index_codes = set(meta.get("index_codes", []))
    existing_index_codes.update(index_datasets)
    meta_update["index_codes"] = sorted(existing_index_codes)

    meta_path = bin_dir / "meta_export.json"
    meta_path.write_text(json.dumps(meta_update, ensure_ascii=False, indent=2), encoding="utf-8")

    return UnifiedBinExportResponseV2(
        snapshot_id=body.snapshot_id,
        mode=body.mode,
        steps=steps,
        check_ok=check_ok,
        stdout_check=stdout_check,
        stderr_check=stderr_check,
    )


# =============================================================================
# 一键增量更新 API
# =============================================================================


class IncrementalAllResponse(BaseModel):
    snapshot_id: str
    results: Dict[str, Any]


@router.post("/api/v1/qlib/snapshots/{snapshot_id}/incremental_all", response_model=IncrementalAllResponse)
async def incremental_all(snapshot_id: str, body: IncrementalExportRequest) -> IncrementalAllResponse:
    """对已有 Snapshot 的所有数据集执行增量更新。"""
    sid = (snapshot_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="snapshot_id 不能为空")

    snap_dir = QLIB_SNAPSHOT_ROOT / sid
    if not snap_dir.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot {sid} 不存在")

    results: Dict[str, Any] = {}

    # 定义要增量更新的数据集及对应 exporter 和文件
    dataset_map = [
        ("daily", _daily_exporter, "daily_pv.h5"),
        ("moneyflow", _moneyflow_exporter, "moneyflow.h5"),
        ("daily_basic", _daily_basic_exporter, "daily_basic.h5"),
        ("bak_basic", _bak_basic_exporter, "bak_basic.h5"),
        ("margin_detail", _margin_detail_exporter, "margin_detail.h5"),
        ("cyq_perf", _cyq_perf_exporter, "cyq_perf.h5"),
        ("sector_data", _sector_data_exporter, "sector_data.h5"),
    ]

    for data_type, exporter, filename in dataset_map:
        # 仅对已存在的数据集执行增量
        if not (snap_dir / filename).exists():
            results[data_type] = {"skipped": True, "reason": "file_not_found"}
            continue

        try:
            result = exporter.export_incremental(
                snapshot_id=sid, end=body.end,
                exchanges=body.exchanges,
                exclude_st=body.exclude_st,
                exclude_delisted_or_paused=body.exclude_delisted_or_paused,
            )
            results[data_type] = {
                "rows": result.rows,
                "start": str(result.start),
                "end": str(result.end),
            }
        except Exception as exc:
            results[data_type] = {"error": str(exc)}

    return IncrementalAllResponse(snapshot_id=sid, results=results)


# ─────────────────────────────────────────────────────────────────────────────
# 数据质量报告 API
# ─────────────────────────────────────────────────────────────────────────────

_data_reporter = DataReporter()
_data_validator = DataValidator()


class QualityReportRequest(BaseModel):
    """数据质量报告请求."""
    data_type: str = Field(..., description="数据类型: daily, minute")
    detect_anomalies: bool = Field(True, description="是否检测异常数据")


class QualityReportResponse(BaseModel):
    """数据质量报告响应."""
    snapshot_id: str
    data_type: str
    total_rows: int
    total_instruments: int
    date_range: List[str]
    trading_days: int
    coverage_rate: float
    quality_score: float
    column_stats: List[dict]
    anomaly_summary: dict
    export_time: str


@router.get("/api/v1/qlib/snapshots/{snapshot_id}/quality")
async def get_snapshot_quality_report(
    snapshot_id: str,
    data_type: str = "daily",
    detect_anomalies: bool = True,
) -> QualityReportResponse:
    """获取 Snapshot 数据质量报告.
    
    Args:
        snapshot_id: Snapshot ID
        data_type: 数据类型 (daily, minute)
        detect_anomalies: 是否检测异常数据
    """
    snapshot_path = Path(QLIB_SNAPSHOT_ROOT) / snapshot_id
    if not snapshot_path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} 不存在")
    
    # 根据数据类型确定文件路径
    file_map = {
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
    }

    if data_type not in file_map:
        raise HTTPException(status_code=400, detail=f"不支持的数据类型: {data_type}")

    h5_file = snapshot_path / file_map[data_type]
    if not h5_file.exists():
        raise HTTPException(status_code=404, detail=f"数据文件不存在: {file_map[data_type]}")

    try:
        stats = _data_reporter.generate_report_from_hdf5(
            h5_file,
            snapshot_id=snapshot_id,
            data_type=data_type,
        )
        
        return QualityReportResponse(
            snapshot_id=snapshot_id,
            data_type=data_type,
            total_rows=stats.total_rows,
            total_instruments=stats.total_instruments,
            date_range=list(stats.date_range),
            trading_days=stats.trading_days,
            coverage_rate=stats.coverage_rate,
            quality_score=stats.quality_score,
            column_stats=[cs.to_dict() for cs in stats.column_stats],
            anomaly_summary={
                "price_anomaly_count": len(stats.price_anomalies),
                "volume_anomaly_count": len(stats.volume_anomalies),
            },
            export_time=stats.export_time,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/snapshots/{snapshot_id}/quality/report")
async def generate_quality_report_file(
    snapshot_id: str,
    body: QualityReportRequest,
    format: str = "json",
) -> dict:
    """生成并保存数据质量报告文件.
    
    Args:
        snapshot_id: Snapshot ID
        body: 请求体
        format: 报告格式 (json, md)
    
    Returns:
        报告文件路径
    """
    snapshot_path = Path(QLIB_SNAPSHOT_ROOT) / snapshot_id
    if not snapshot_path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} 不存在")
    
    file_map = {
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
    }

    if body.data_type not in file_map:
        raise HTTPException(status_code=400, detail=f"不支持的数据类型: {body.data_type}")

    h5_file = snapshot_path / file_map[body.data_type]
    if not h5_file.exists():
        raise HTTPException(status_code=404, detail=f"数据文件不存在: {file_map[body.data_type]}")
    
    try:
        stats = _data_reporter.generate_report_from_hdf5(
            h5_file,
            snapshot_id=snapshot_id,
            data_type=body.data_type,
        )
        
        # 保存报告
        report_dir = snapshot_path / "reports"
        report_dir.mkdir(exist_ok=True)
        
        ext = "json" if format == "json" else "md"
        report_file = report_dir / f"quality_report_{body.data_type}.{ext}"
        
        _data_reporter.save_report(stats, report_file, format=format)
        
        return {
            "success": True,
            "report_path": str(report_file),
            "quality_score": stats.quality_score,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/v1/qlib/snapshots/{snapshot_id}/validate")
async def validate_snapshot_data(
    snapshot_id: str,
    data_type: str = "daily",
) -> dict:
    """校验 Snapshot 数据完整性.
    
    Args:
        snapshot_id: Snapshot ID
        data_type: 数据类型
    
    Returns:
        校验结果
    """
    snapshot_path = Path(QLIB_SNAPSHOT_ROOT) / snapshot_id
    if not snapshot_path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} 不存在")
    
    file_map = {
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
    }

    if data_type not in file_map:
        raise HTTPException(status_code=400, detail=f"不支持的数据类型: {data_type}")

    h5_file = snapshot_path / file_map[data_type]
    if not h5_file.exists():
        raise HTTPException(status_code=404, detail=f"数据文件不存在: {file_map[data_type]}")
    
    try:
        report = _data_validator.validate_hdf5(h5_file)
        
        issues = []
        if report.duplicate_count > 0:
            issues.append(f"存在 {report.duplicate_count} 条重复索引")
        
        total_nulls = sum(report.null_counts.values())
        if total_nulls > 0:
            issues.append(f"存在 {total_nulls} 个空值")
        
        return {
            "snapshot_id": snapshot_id,
            "data_type": data_type,
            "is_valid": len(issues) == 0,
            "validation_report": report.to_dict(),
            "issues": issues,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# 数据库源数据检查 API
# =============================================================================

from .db_reader import DBReader

_db_reader = DBReader()


class DataCheckRequest(BaseModel):
    """数据检查请求."""
    ts_codes: Optional[List[str]] = Field(None, description="股票代码列表，为空则检查全部")
    start: date = Field(..., description="开始日期")
    end: date = Field(..., description="结束日期")
    exchanges: Optional[List[str]] = Field(None, description="交易所过滤")
    check_adj_factor: bool = Field(True, description="是否检查复权因子")
    sample_size: int = Field(5, description="样本数量")


class DataCheckResponse(BaseModel):
    """数据检查响应."""
    total_stocks: int
    date_range: List[str]
    trading_days: int
    data_coverage: float
    adj_factor_coverage: float
    sample_data: List[dict]
    issues: List[str]


@router.post("/api/v1/qlib/data/check", response_model=DataCheckResponse)
async def check_database_data(body: DataCheckRequest) -> DataCheckResponse:
    """检查数据库源数据质量.
    
    检查内容：
    1. 数据覆盖率
    2. 复权因子可用性
    3. 数据样本预览
    """
    try:
        from datetime import date as date_type
        import pandas as pd
        
        # Build the checked stock universe with the same SH/SZ-only export rule.
        normalized_exchanges = set(normalize_stock_export_exchanges(body.exchanges))
        if body.ts_codes:
            codes = body.ts_codes
            if any(str(code).strip().upper().endswith(".BJ") or str(code).strip().upper().startswith("BJ") for code in codes):
                raise HTTPException(status_code=400, detail="BJ/BSE stocks are excluded from AIstock stock data exports; use sh/sz only")
        else:
            codes = _db_reader.get_all_ts_codes()
            def match_exchange(code: str) -> bool:
                uc = code.upper()
                if uc.endswith(".SH"): return "sh" in normalized_exchanges
                if uc.endswith(".SZ"): return "sz" in normalized_exchanges
                if uc.endswith(".BJ"): return False
                return False
            codes = [c for c in codes if match_exchange(c)]
        issues = []
        
        # 加载少量数据进行检查
        sample_codes = codes[:min(body.sample_size, len(codes))]
        
        # 使用新的 Qlib 格式加载方法
        df = _db_reader.load_qlib_daily_data(
            sample_codes, 
            body.start, 
            body.end,
            use_tushare_adj=body.check_adj_factor
        )
        
        if df.empty:
            return DataCheckResponse(
                total_stocks=len(codes),
                date_range=[str(body.start), str(body.end)],
                trading_days=0,
                data_coverage=0.0,
                adj_factor_coverage=0.0,
                sample_data=[],
                issues=["指定区间内无数据"],
            )
        
        # 统计
        dt_level = df.index.get_level_values("datetime")
        inst_level = df.index.get_level_values("instrument")
        
        trading_days = dt_level.nunique()
        total_instruments = inst_level.nunique()
        expected_rows = trading_days * total_instruments
        data_coverage = len(df) / expected_rows if expected_rows > 0 else 0
        
        # 检查复权因子
        factor_col = "$factor"
        if factor_col in df.columns:
            factor_null_rate = df[factor_col].isna().sum() / len(df)
            adj_factor_coverage = 1 - factor_null_rate
            
            # 检查是否有非1的复权因子（说明复权因子生效）
            non_one_factors = (df[factor_col] != 1.0).sum()
            if non_one_factors == 0:
                issues.append("所有复权因子均为1.0，可能未正确获取复权数据")
        else:
            adj_factor_coverage = 0.0
            issues.append("数据中缺少 $factor 列")
        
        # 检查价格数据
        close_col = "$close"
        if close_col in df.columns:
            invalid_prices = (df[close_col] <= 0).sum()
            if invalid_prices > 0:
                issues.append(f"存在 {invalid_prices} 条无效价格（≤0）")
        
        # 生成样本数据
        sample_data = []
        for inst in df.index.get_level_values("instrument").unique()[:3]:
            inst_df = df.loc[df.index.get_level_values("instrument") == inst]
            for idx in inst_df.head(2).index:
                row = inst_df.loc[idx]
                sample_data.append({
                    "datetime": str(idx[0].date()) if hasattr(idx[0], 'date') else str(idx[0]),
                    "instrument": str(idx[1]),
                    "$close": float(row["$close"]) if "$close" in row else None,
                    "$factor": float(row["$factor"]) if "$factor" in row else None,
                    "$volume": float(row["$volume"]) if "$volume" in row else None,
                })
        
        return DataCheckResponse(
            total_stocks=len(codes),
            date_range=[str(body.start), str(body.end)],
            trading_days=trading_days,
            data_coverage=round(data_coverage, 4),
            adj_factor_coverage=round(adj_factor_coverage, 4),
            sample_data=sample_data,
            issues=issues,
        )
        
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/v1/qlib/data/preview")
async def preview_qlib_data(
    ts_code: str,
    start: date,
    end: date,
    limit: int = 20,
) -> dict:
    """预览单只股票的 Qlib 格式数据.
    
    Args:
        ts_code: 股票代码（如 601919.SH）
        start: 开始日期
        end: 结束日期
        limit: 返回行数限制
    """
    try:
        df = _db_reader.load_qlib_daily_data(
            [ts_code], start, end, use_tushare_adj=True
        )
        
        if df.empty:
            return {
                "ts_code": ts_code,
                "rows": 0,
                "columns": [],
                "data": [],
                "factor_range": None,
            }
        
        # 转换为可序列化格式
        df_reset = df.head(limit).reset_index()
        df_reset["datetime"] = df_reset["datetime"].astype(str)
        
        # 复权因子范围
        factor_range = None
        if "$factor" in df.columns:
            factor_range = {
                "min": float(df["$factor"].min()),
                "max": float(df["$factor"].max()),
                "unique_count": int(df["$factor"].nunique()),
            }
        
        return {
            "ts_code": ts_code,
            "rows": len(df),
            "columns": list(df.columns),
            "data": df_reset.to_dict(orient="records"),
            "factor_range": factor_range,
        }
        
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# H5 文件导出 CSV API（支持 daily_basic、bak_basic、margin_detail、cyq_perf、sector_data、moneyflow 等）
# =============================================================================


@router.get("/api/v1/qlib/snapshots/{snapshot_id}/csv")
async def export_h5_to_csv(
    snapshot_id: str,
    data_type: str = "daily_basic",
):
    """将指定的 H5 数据文件导出为 CSV 格式下载。

    支持的数据类型：
    - daily_basic: daily_basic.h5
    - bak_basic: bak_basic.h5
    - margin_detail: margin_detail.h5
    - cyq_perf: cyq_perf.h5
    - sector_data: sector_data.h5
    - moneyflow: moneyflow.h5
    - daily: daily_pv.h5
    - minute: minute_1min.h5

    Args:
        snapshot_id: Snapshot ID
        data_type: 数据类型标识

    Returns:
        CSV 文件下载响应
    """
    sid = (snapshot_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="snapshot_id 不能为空")

    # 数据类型到文件名的映射
    file_map = {
        "daily_basic": "daily_basic.h5",
        "bak_basic": "bak_basic.h5",
        "margin_detail": "margin_detail.h5",
        "cyq_perf": "cyq_perf.h5",
        "sector_data": "sector_data.h5",
        "moneyflow": "moneyflow.h5",
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
    }

    if data_type not in file_map:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的数据类型: {data_type}。支持类型: {', '.join(file_map.keys())}"
        )

    root = QLIB_SNAPSHOT_ROOT.resolve()
    snap_dir = (root / sid).resolve()

    # 安全检查：确保路径在允许的根目录内
    if root not in snap_dir.parents and snap_dir != root:
        raise HTTPException(status_code=400, detail="invalid snapshot_id")

    if not snap_dir.exists() or not snap_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Snapshot not found: {sid}")

    h5_path = snap_dir / file_map[data_type]
    if not h5_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"数据文件不存在: {file_map[data_type]}"
        )

    try:
        # 读取 H5 文件
        df = pd.read_hdf(h5_path, key="data")

        if df.empty:
            raise HTTPException(status_code=400, detail="H5 文件数据为空")

        # 重置索引，将 MultiIndex 转换为普通列
        if isinstance(df.index, pd.MultiIndex):
            df_reset = df.reset_index()
        else:
            df_reset = df.copy()

        # 处理 datetime 列，确保可序列化
        for col in df_reset.columns:
            if pd.api.types.is_datetime64_any_dtype(df_reset[col]):
                df_reset[col] = df_reset[col].astype(str)

        # 生成 CSV 到内存
        csv_buffer = io.StringIO()
        df_reset.to_csv(csv_buffer, index=False, encoding="utf-8")
        csv_content = csv_buffer.getvalue()

        # 返回 CSV 下载响应
        filename = f"{sid}_{data_type}.csv"
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "text/csv; charset=utf-8",
        }

        return StreamingResponse(
            io.BytesIO(csv_content.encode("utf-8")),
            media_type="text/csv",
            headers=headers,
        )

    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"CSV 导出失败: {exc}")


@router.get("/api/v1/qlib/snapshots/{snapshot_id}/csv/preview")
async def preview_h5_as_csv(
    snapshot_id: str,
    data_type: str = "daily_basic",
    limit: int = 100,
):
    """预览 H5 数据的前 N 行（JSON 格式，用于前端预览）。

    Args:
        snapshot_id: Snapshot ID
        data_type: 数据类型标识
        limit: 返回的最大行数（默认 100）

    Returns:
        JSON 格式的数据预览
    """
    sid = (snapshot_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="snapshot_id 不能为空")

    file_map = {
        "daily_basic": "daily_basic.h5",
        "bak_basic": "bak_basic.h5",
        "margin_detail": "margin_detail.h5",
        "cyq_perf": "cyq_perf.h5",
        "sector_data": "sector_data.h5",
        "moneyflow": "moneyflow.h5",
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
    }

    if data_type not in file_map:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的数据类型: {data_type}"
        )

    root = QLIB_SNAPSHOT_ROOT.resolve()
    snap_dir = (root / sid).resolve()

    if root not in snap_dir.parents and snap_dir != root:
        raise HTTPException(status_code=400, detail="invalid snapshot_id")

    h5_path = snap_dir / file_map[data_type]
    if not h5_path.exists():
        raise HTTPException(status_code=404, detail=f"数据文件不存在: {file_map[data_type]}")

    try:
        df = pd.read_hdf(h5_path, key="data")

        if df.empty:
            return {
                "snapshot_id": sid,
                "data_type": data_type,
                "rows": 0,
                "columns": [],
                "preview": [],
            }

        # 重置索引
        if isinstance(df.index, pd.MultiIndex):
            df_reset = df.reset_index()
        else:
            df_reset = df.copy()

        # 处理 datetime 列
        for col in df_reset.columns:
            if pd.api.types.is_datetime64_any_dtype(df_reset[col]):
                df_reset[col] = df_reset[col].astype(str)

        # 限制行数
        preview_df = df_reset.head(limit)

        return {
            "snapshot_id": sid,
            "data_type": data_type,
            "total_rows": len(df),
            "preview_rows": len(preview_df),
            "columns": list(preview_df.columns),
            "preview": preview_df.to_dict(orient="records"),
        }

    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"预览失败: {exc}")

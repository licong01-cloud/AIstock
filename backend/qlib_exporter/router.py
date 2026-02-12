from __future__ import annotations

"""Qlib 导出相关 FastAPI 路由.

支持的 API：
- GET  /api/v1/qlib/config              获取当前配置
- GET  /api/v1/qlib/snapshots           罗列现有 Snapshot
- DELETE /api/v1/qlib/snapshots/{id}    删除指定 Snapshot
- POST /api/v1/qlib/snapshots/daily     日频全量导出
- POST /api/v1/qlib/snapshots/minute    分钟线全量导出
- POST /api/v1/qlib/boards/daily        板块日线导出
"""

import json
import os
import shutil
import io
import zipfile
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from ..infra.wsl_qlib_runner import QlibWSLConfigError, run_qlib_script_in_wsl, win_to_wsl_path
from .config import (
    DAILY_QFQ_TABLE,
    FIELD_MAPPING_DB_DAILY,
    FIELD_MAPPING_DB_MINUTE,
    MINUTE_QFQ_TABLE,
    MONEYFLOW_TS_TABLE,
    QLIB_MARKET,
    QLIB_SNAPSHOT_ROOT,
    TDX_BOARD_DAILY_TABLE,
    TDX_BOARD_INDEX_TABLE,
    TDX_BOARD_MEMBER_TABLE,
)
from .exporter import (
    ExportResult,
    QlibBakBasicExporter,
    QlibBoardDailyExporter,
    QlibBoardExporter,
    QlibBoardIndexExporter,
    QlibBoardMemberExporter,
    QlibCyqPerfExporter,
    QlibDailyBasicExporter,
    QlibDailyExporter,
    QlibFactorExporter,
    QlibMinuteExporter,
    QlibMoneyflowExporter,
)
from .field_map_service import export_field_map_for_snapshot
from .data_quality import DataReporter, DataValidator
from .db_reader import DBReader


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
        description="可选，按交易所过滤：支持 'sh', 'sz', 'bj'；为空表示不过滤（全市场）",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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
        description="可选，按交易所过滤：支持 'sh', 'sz', 'bj'；为空表示不过滤（全市场）",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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
    has_cyq_perf: bool


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
_board_exporter = QlibBoardExporter()
_board_daily_exporter = QlibBoardDailyExporter()
_board_index_exporter = QlibBoardIndexExporter()
_board_member_exporter = QlibBoardMemberExporter()
_factor_exporter = QlibFactorExporter()
_moneyflow_exporter = QlibMoneyflowExporter()
_bak_basic_exporter = QlibBakBasicExporter()
_cyq_perf_exporter = QlibCyqPerfExporter()


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
        # 未预料的错误 → 500
        raise HTTPException(status_code=500, detail=str(exc))


class DailyBasicSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz', 'bj'；为空表示不过滤（全市场）",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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
        description="可选，按交易所过滤：支持 'sh', 'sz', 'bj'；为空表示不过滤（全市场）",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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
            import traceback
            print(f"[WARN] Auto field map failed: {e}")
            traceback.print_exc()
        return BakBasicSnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


class CyqPerfSnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，按交易所过滤：支持 'sh', 'sz', 'bj'；为空表示不过滤（全市场）",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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
    freq: Literal["day", "1m", "5m", "15m"] = Field(
        "day",
        description="导出频率：日线 day 或分钟线 1m/5m/15m（当前仅实现 day 和 1m）",
    )
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，交易所过滤：sh, sz, bj；为空表示全市场",
    )
    run_health_check: bool = Field(
        True,
        description="是否在 dump_bin 后运行 check_data_health.py",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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
    CyqPerfSnapshotRequest.model_rebuild()
    MoneyflowSnapshotRequest.model_rebuild()
    FieldMapExportRequest.model_rebuild()
    MinuteSnapshotRequest.model_rebuild()
    BoardDailySnapshotRequest.model_rebuild()
    BoardIndexRequest.model_rebuild()
    BoardMemberRequest.model_rebuild()
    IncrementalExportRequest.model_rebuild()
    FactorExportRequest.model_rebuild()
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
) -> Path:
    """从 DB 导出日线宽表为 CSV，供 dump_bin.py 使用。

    CSV 结构：date,symbol,open,high,low,close,volume,amount
    """

    csv_root = os.getenv("QLIB_CSV_ROOT_WIN")
    if not csv_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_CSV_ROOT_WIN")

    csv_root_path = Path(csv_root)
    csv_dir = csv_root_path / snapshot_id
    csv_dir.mkdir(parents=True, exist_ok=True)

    df = _db_reader.load_qlib_daily_data_all(
        start=start,
        end=end,
        exchanges=list(exchanges) if exchanges else None,
        use_tushare_adj=True,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
    )

    if df.empty:
        raise HTTPException(status_code=400, detail="指定区间内无可导出的日线数据（可能被过滤条件排除）")

    # 将 Qlib 宽表转换成 dump_bin.py 期望的 CSV 结构
    df_reset = df.reset_index()
    # datetime -> date (YYYY-MM-DD), instrument -> symbol
    df_reset["date"] = df_reset["datetime"].dt.date.astype(str)
    # 直接使用 instrument 作为 symbol，instrument 已统一为 ts_code（例如 000001.SZ / 600000.SH）
    df_reset["symbol"] = df_reset["instrument"].astype(str)

    # 映射列名
    rename_cols = {}
    if "$open" in df_reset.columns:
        rename_cols["$open"] = "open"
    elif "open" in df_reset.columns:
        rename_cols["open"] = "open"

    if "$high" in df_reset.columns:
        rename_cols["$high"] = "high"
    elif "high" in df_reset.columns:
        rename_cols["high"] = "high"

    if "$low" in df_reset.columns:
        rename_cols["$low"] = "low"
    elif "low" in df_reset.columns:
        rename_cols["low"] = "low"

    if "$close" in df_reset.columns:
        rename_cols["$close"] = "close"
    elif "close" in df_reset.columns:
        rename_cols["close"] = "close"

    if "$volume" in df_reset.columns:
        rename_cols["$volume"] = "volume"
    elif "volume" in df_reset.columns:
        rename_cols["volume"] = "volume"

    if "$amount" in df_reset.columns:
        rename_cols["$amount"] = "amount"
    elif "amount" in df_reset.columns:
        rename_cols["amount"] = "amount"

    if "$factor" in df_reset.columns:
        rename_cols["$factor"] = "factor"
    elif "factor" in df_reset.columns:
        rename_cols["factor"] = "factor"

    df_reset = df_reset.rename(columns=rename_cols)

    csv_cols = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
    ]

    # 缺列兜底：一律用 NaN（禁止用 0）
    for col in csv_cols:
        if col not in df_reset.columns:
            df_reset[col] = float("nan")

    if df_reset["factor"].isna().all():
        raise HTTPException(
            status_code=400,
            detail="bin 导出失败：factor 列为空或全为 NaN（请检查复权因子 adj_factor / 数据加载逻辑）",
        )
    df_csv = df_reset[csv_cols]

    # 为兼容 dump_bin.py dump_all 的行为，这里按 symbol 拆分为多个文件：每只股票一个 CSV。
    # DumpDataAll 会把每个文件名视为一个 instrument（忽略列内的 symbol 字段），
    # 因此必须避免只生成 daily_all.csv，否则会得到单一标的 DAILY_ALL。
    for symbol, g in df_csv.groupby("symbol"):
        # 使用 ts_code 作为文件名，例如 000001.SZ.csv / 600000.SH.csv
        csv_path = csv_dir / f"{symbol}.csv"
        g.to_csv(csv_path, index=False)

    return csv_dir


def _export_minute_to_csv_for_dump_bin(
    snapshot_id: str,
    start: date,
    end: date,
    exchanges: Optional[List[str]],
    *,
    exclude_st: bool,
    exclude_delisted_or_paused: bool,
    freq: str = "1m",
) -> Path:
    """从 DB 导出分钟线宽表为 CSV，供 dump_bin.py 使用。

    CSV 结构：date,symbol,open,high,low,close,volume,amount

    注意：
    - date 使用高频格式 "YYYY-MM-DD HH:MM:SS"（上海时间）
    - symbol 使用 Qlib instrument，例如 SH600000
    """

    csv_root = os.getenv("QLIB_CSV_ROOT_WIN")
    if not csv_root:
        raise HTTPException(status_code=500, detail="缺少环境变量 QLIB_CSV_ROOT_WIN")

    csv_root_path = Path(csv_root)
    # 为不同频率预留独立子目录，避免与日线 CSV 混在一起
    csv_dir = csv_root_path / snapshot_id / f"minute_{freq}"
    csv_dir.mkdir(parents=True, exist_ok=True)

    df = _db_reader.load_qlib_minute_data_all(
        start=start,
        end=end,
        exchanges=list(exchanges) if exchanges else None,
        use_tushare_adj=True,
        exclude_st=exclude_st,
        exclude_delisted_or_paused=exclude_delisted_or_paused,
        freq=freq,
    )

    if df.empty:
        raise HTTPException(status_code=400, detail="指定区间内无可导出的分钟线数据（可能被过滤条件排除）")

    # 将 Qlib 宽表转换成 dump_bin.py 期望的 CSV 结构
    df_reset = df.reset_index()
    # datetime -> date (YYYY-MM-DD HH:MM:SS), instrument -> symbol
    df_reset["date"] = df_reset["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    # 直接使用 instrument 作为 symbol，instrument 已统一为 ts_code（例如 000001.SZ / 600000.SH）
    df_reset["symbol"] = df_reset["instrument"].astype(str)

    rename_cols: Dict[str, str] = {}
    if "$open" in df_reset.columns:
        rename_cols["$open"] = "open"
    elif "open" in df_reset.columns:
        rename_cols["open"] = "open"

    if "$high" in df_reset.columns:
        rename_cols["$high"] = "high"
    elif "high" in df_reset.columns:
        rename_cols["high"] = "high"

    if "$low" in df_reset.columns:
        rename_cols["$low"] = "low"
    elif "low" in df_reset.columns:
        rename_cols["low"] = "low"

    if "$close" in df_reset.columns:
        rename_cols["$close"] = "close"
    elif "close" in df_reset.columns:
        rename_cols["close"] = "close"

    if "$volume" in df_reset.columns:
        rename_cols["$volume"] = "volume"
    elif "volume" in df_reset.columns:
        rename_cols["volume"] = "volume"

    if "$amount" in df_reset.columns:
        rename_cols["$amount"] = "amount"
    elif "amount" in df_reset.columns:
        rename_cols["amount"] = "amount"

    if "$factor" in df_reset.columns:
        rename_cols["$factor"] = "factor"
    elif "factor" in df_reset.columns:
        rename_cols["factor"] = "factor"

    df_reset = df_reset.rename(columns=rename_cols)

    csv_cols = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
    ]

    # 缺列兜底：一律用 NaN（禁止用 0）
    for col in csv_cols:
        if col not in df_reset.columns:
            df_reset[col] = float("nan")

    if df_reset["factor"].isna().all():
        raise HTTPException(
            status_code=400,
            detail="bin 导出失败：factor 列为空或全为 NaN（请检查复权因子 adj_factor / 数据加载逻辑）",
        )
    df_csv = df_reset[csv_cols]

    csv_path = csv_dir / f"minute_{freq}_all.csv"
    df_csv.to_csv(csv_path, index=False)

    return csv_dir


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

    # 缺列兜底：一律用 NaN（禁止用 0）
    for col in csv_cols:
        if col not in df_csv.columns:
            df_csv[col] = float("nan")

    df_csv = df_csv[csv_cols]

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

    # 1. 导出 CSV（根据 freq 分支）
    if body.freq == "day":
        csv_dir = _export_daily_to_csv_for_dump_bin(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
        )
        dump_freq = "day"
    elif body.freq == "1m":
        csv_dir = _export_minute_to_csv_for_dump_bin(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            exclude_st=body.exclude_st,
            exclude_delisted_or_paused=body.exclude_delisted_or_paused,
            freq="1m",
        )
        dump_freq = "1m"
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

    # 5. 写出一次导出的 meta 信息，便于后续在 /api/v1/qlib/bin/exports 中展示
    try:
      meta = {
          "snapshot_id": body.snapshot_id,
          "start": body.start.isoformat(),
          "end": body.end.isoformat(),
          "exchanges": list(body.exchanges) if body.exchanges else None,
          "exclude_st": body.exclude_st,
          "exclude_delisted_or_paused": body.exclude_delisted_or_paused,
          "run_health_check": body.run_health_check,
          # 根据导出频率标记数据类型，方便前端展示（日K / 分钟K）
          "freq_types": [
              "daily" if dump_freq == "day" else dump_freq,
          ],
      }
      meta_path = bin_dir / "meta_export.json"
      meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
      # meta 写入失败不影响主流程
      pass

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
        description="可选，按交易所过滤：支持 'sh', 'sz', 'bj'；为空表示不过滤（全市场）",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（曾经 / 当前 ST）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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


class BoardDailySnapshotRequest(BaseModel):
    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名（与日线/分钟共用目录）")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    board_codes: Optional[List[str]] = Field(
        None,
        description="可选，指定导出的板块代码列表；为空则导出全部板块",
    )
    idx_types: Optional[List[str]] = Field(
        None,
        description="可选，按板块类型过滤（来自 tdx_board_index.idx_type）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _end_not_before_start(self) -> "BoardDailySnapshotRequest":  # noqa: D401, N805
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class BoardDailySnapshotResponse(BaseModel):
    snapshot_id: str
    freq: str
    start: date
    end: date
    board_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "BoardDailySnapshotResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            board_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/boards/daily", response_model=BoardDailySnapshotResponse)
async def create_board_daily_snapshot(body: BoardDailySnapshotRequest) -> BoardDailySnapshotResponse:
    """导出 TDX 板块日线行情到 Snapshot 目录的 boards/board_daily.h5。"""

    try:
        result = _board_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            board_codes=body.board_codes,
            idx_types=body.idx_types,
        )
        return BoardDailySnapshotResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
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
    board_index_table: str = Field(..., description="板块索引表名")
    board_daily_table: str = Field(..., description="板块日线表名")
    field_mapping_daily: Dict[str, str] = Field(..., description="日频字段映射")
    field_mapping_minute: Dict[str, str] = Field(..., description="分钟线字段映射")


@router.get("/api/v1/qlib/config", response_model=QlibConfigResponse)
async def get_qlib_config() -> QlibConfigResponse:
    """获取当前 Qlib 导出配置."""

    return QlibConfigResponse(
        snapshot_root=str(QLIB_SNAPSHOT_ROOT.absolute()),
        market=QLIB_MARKET,
        daily_table=DAILY_QFQ_TABLE,
        minute_table=MINUTE_QFQ_TABLE,
        board_index_table=TDX_BOARD_INDEX_TABLE,
        board_daily_table=TDX_BOARD_DAILY_TABLE,
        field_mapping_daily=FIELD_MAPPING_DB_DAILY,
        field_mapping_minute=FIELD_MAPPING_DB_MINUTE,
    )


class SnapshotInfo(BaseModel):
    """单个 Snapshot 的信息."""

    snapshot_id: str = Field(..., description="Snapshot ID")
    path: str = Field(..., description="Snapshot 目录路径")
    has_daily: bool = Field(..., description="是否包含日频数据")
    has_minute: bool = Field(..., description="是否包含分钟线数据")
    has_board: bool = Field(..., description="是否包含板块日线数据")
    has_board_index: bool = Field(..., description="是否包含板块索引数据")
    has_board_member: bool = Field(..., description="是否包含板块成员数据")
    has_factor_data: bool = Field(False, description="是否包含 RD-Agent 因子数据")
    has_moneyflow: bool = Field(False, description="是否包含资金流向数据")
    has_daily_basic: bool = Field(False, description="是否包含 daily_basic 指标数据")
    has_bak_basic: bool = Field(False, description="是否包含 bak_basic 历史股票数据")
    has_cyq_perf: bool = Field(False, description="是否包含 cyq_perf 筹码胜率数据")
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
        has_board = (item / "boards" / "board_daily.h5").exists()
        has_board_index = (item / "boards" / "board_index.h5").exists()
        has_board_member = (item / "boards" / "board_member.h5").exists()
        has_moneyflow = (item / "moneyflow.h5").exists()
        has_daily_basic = (item / "daily_basic.h5").exists()
        has_bak_basic = (item / "bak_basic.h5").exists()
        has_cyq_perf = (item / "cyq_perf.h5").exists()

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
                has_board=has_board,
                has_board_index=has_board_index,
                has_board_member=has_board_member,
                has_factor_data=has_daily_pv,  # daily_pv.h5 同时用于日线和因子数据
                has_moneyflow=has_moneyflow,
                has_daily_basic=has_daily_basic,
                has_bak_basic=has_bak_basic,
                has_cyq_perf=has_cyq_perf,
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
# 板块索引和成员导出 API
# =============================================================================


class BoardIndexRequest(BaseModel):
    """板块索引导出请求."""

    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    idx_types: Optional[List[str]] = Field(
        None,
        description="可选，按板块类型过滤（如 'TDX_BOARD_HY', 'TDX_BOARD_GN' 等）",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _end_not_before_start(self) -> "BoardIndexRequest":  # noqa: D401, N805
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class BoardIndexResponse(BaseModel):
    """板块索引导出响应."""

    snapshot_id: str
    freq: str
    start: date
    end: date
    board_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "BoardIndexResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            board_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/boards/index", response_model=BoardIndexResponse)
async def create_board_index_snapshot(body: BoardIndexRequest) -> BoardIndexResponse:
    """导出板块索引数据（tdx_board_index）到 boards/board_index.h5。"""

    try:
        result = _board_index_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            idx_types=body.idx_types,
        )
        return BoardIndexResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


class BoardMemberRequest(BaseModel):
    """板块成员导出请求."""

    snapshot_id: str = Field(..., description="Snapshot ID，作为导出目录名")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    board_codes: Optional[List[str]] = Field(
        None,
        description="可选，指定导出的板块代码列表；为空则导出全部",
    )

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2

    @model_validator(mode="after")
    def _end_not_before_start(self) -> "BoardMemberRequest":  # noqa: D401, N805
        if self.end < self.start:
            raise ValueError("end 日期不能早于 start")
        return self


class BoardMemberResponse(BaseModel):
    """板块成员导出响应."""

    snapshot_id: str
    freq: str
    start: date
    end: date
    board_codes: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "BoardMemberResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            board_codes=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/boards/member", response_model=BoardMemberResponse)
async def create_board_member_snapshot(body: BoardMemberRequest) -> BoardMemberResponse:
    """导出板块成员数据（tdx_board_member）到 boards/board_member.h5。"""

    try:
        result = _board_member_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            board_codes=body.board_codes,
        )
        return BoardMemberResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# 增量导出 API
# =============================================================================


class IncrementalExportRequest(BaseModel):
    """增量导出请求（通用）."""

    snapshot_id: str = Field(..., description="Snapshot ID")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，交易所过滤（仅分钟线有效）：sh, sz, bj",
    )
    exclude_st: bool = Field(
        False,
        description="是否排除所有在 stock_st 中出现过的股票（仅分钟线有效）",
    )
    exclude_delisted_or_paused: bool = Field(
        False,
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


@router.post("/api/v1/qlib/boards/daily/incremental", response_model=IncrementalExportResponse)
async def create_board_daily_snapshot_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出板块日线数据。从上次导出位置继续。"""

    try:
        result = _board_daily_exporter.export_incremental(
            snapshot_id=body.snapshot_id,
            end=body.end,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/boards/index/incremental", response_model=IncrementalExportResponse)
async def create_board_index_snapshot_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出板块索引数据。从上次导出位置继续。"""

    try:
        result = _board_index_exporter.export_incremental(
            snapshot_id=body.snapshot_id,
            end=body.end,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/boards/member/incremental", response_model=IncrementalExportResponse)
async def create_board_member_snapshot_incremental(body: IncrementalExportRequest) -> IncrementalExportResponse:
    """增量导出板块成员数据。从上次导出位置继续。"""

    try:
        result = _board_member_exporter.export_incremental(
            snapshot_id=body.snapshot_id,
            end=body.end,
        )
        return IncrementalExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# RD-Agent 因子数据导出 API
# =============================================================================


class FactorExportRequest(BaseModel):
    """因子数据导出请求."""

    snapshot_id: str = Field(..., description="Snapshot ID")
    start: date = Field(..., description="开始日期，YYYY-MM-DD")
    end: date = Field(..., description="结束日期（含），YYYY-MM-DD")
    exchanges: Optional[List[str]] = Field(
        None,
        description="可选，交易所过滤：sh, sz, bj",
    )
    filename: str = Field("daily_pv.h5", description="输出文件名")

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_not_empty(cls, v: str) -> str:  # noqa: D401, N805
        v2 = v.strip()
        if not v2:
            raise ValueError("snapshot_id 不能为空")
        return v2


class FactorExportResponse(BaseModel):
    """因子数据导出响应."""

    snapshot_id: str
    freq: str
    start: date
    end: date
    instruments: List[str]
    rows: int

    @classmethod
    def from_result(cls, result: ExportResult) -> "FactorExportResponse":
        return cls(
            snapshot_id=result.snapshot_id,
            freq=result.freq,
            start=result.start,
            end=result.end,
            instruments=result.ts_codes,
            rows=result.rows,
        )


@router.post("/api/v1/qlib/factors", response_model=FactorExportResponse)
async def create_factor_snapshot(body: FactorExportRequest) -> FactorExportResponse:
    """导出 RD-Agent 因子数据（daily_pv.h5 格式）."""

    try:
        result = _factor_exporter.export_full(
            snapshot_id=body.snapshot_id,
            start=body.start,
            end=body.end,
            exchanges=body.exchanges,
            filename=body.filename,
        )
        return FactorExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/v1/qlib/factors/incremental", response_model=FactorExportResponse)
async def create_factor_snapshot_incremental(body: IncrementalExportRequest) -> FactorExportResponse:
    """增量导出 RD-Agent 因子数据."""

    try:
        result = _factor_exporter.export_incremental(
            snapshot_id=body.snapshot_id,
            end=body.end,
            exchanges=body.exchanges,
        )
        return FactorExportResponse.from_result(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# 数据质量报告 API
# ─────────────────────────────────────────────────────────────────────────────

_data_reporter = DataReporter()
_data_validator = DataValidator()


class QualityReportRequest(BaseModel):
    """数据质量报告请求."""
    data_type: str = Field(..., description="数据类型: daily, minute, board_daily, board_index, board_member")
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
        data_type: 数据类型 (daily, minute, board_daily, board_index, board_member)
        detect_anomalies: 是否检测异常数据
    """
    snapshot_path = Path(QLIB_SNAPSHOT_ROOT) / snapshot_id
    if not snapshot_path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} 不存在")
    
    # 根据数据类型确定文件路径
    file_map = {
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
        "board_daily": "boards/board_daily.h5",
        "board_index": "boards/board_index.h5",
        "board_member": "boards/board_member.h5",
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
        "board_daily": "boards/board_daily.h5",
        "board_index": "boards/board_index.h5",
        "board_member": "boards/board_member.h5",
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
        "board_daily": "boards/board_daily.h5",
        "board_index": "boards/board_index.h5",
        "board_member": "boards/board_member.h5",
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
        
        # 获取股票列表
        if body.ts_codes:
            codes = body.ts_codes
        else:
            codes = _db_reader.get_all_ts_codes()
            # 按交易所过滤
            if body.exchanges:
                normalized = {e.strip().lower() for e in body.exchanges}
                def match_exchange(code: str) -> bool:
                    uc = code.upper()
                    if uc.endswith(".SH"): return "sh" in normalized
                    if uc.endswith(".SZ"): return "sz" in normalized
                    if uc.endswith(".BJ"): return "bj" in normalized
                    return True
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
# H5 文件导出 CSV API（支持 daily_basic、bak_basic、cyq_perf、moneyflow 等）
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
    - cyq_perf: cyq_perf.h5
    - moneyflow: moneyflow.h5
    - daily: daily_pv.h5
    - minute: minute_1min.h5
    - board_daily: boards/board_daily.h5
    - board_index: boards/board_index.h5
    - board_member: boards/board_member.h5

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
        "cyq_perf": "cyq_perf.h5",
        "moneyflow": "moneyflow.h5",
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
        "board_daily": "boards/board_daily.h5",
        "board_index": "boards/board_index.h5",
        "board_member": "boards/board_member.h5",
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
        "cyq_perf": "cyq_perf.h5",
        "moneyflow": "moneyflow.h5",
        "daily": "daily_pv.h5",
        "minute": "minute_1min.h5",
        "board_daily": "boards/board_daily.h5",
        "board_index": "boards/board_index.h5",
        "board_member": "boards/board_member.h5",
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

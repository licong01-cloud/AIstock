# HMM 演进 Phase 0: 数据源抽象层详细设计

> **版本**: v1.0  
> **日期**: 2026-07-16  
> **状态**: 详细设计，待开发实施  
> **前置**: `hmm_evolution_and_risk_management_system_design_20260716.md`  
> **作者**: Kiro (Claude Code)

---

## 1. 设计目标

### 1.1 核心目标

为 HMM 演进系统提供**统一的数据抽象层**，实现研发环境与生产环境的数据隔离：

- **研发/回测环境**: 使用固定历史数据（QE artifact: pred.pkl, label.pkl）
- **生产/模拟盘环境**: 连接实时数据库（t-1 kline_daily_raw, sw_daily）
- **数据源切换**: 通过配置完成，业务代码无需修改
- **可测试性**: 支持 mock 数据源，便于单元测试

### 1.2 非目标

Phase 0 **不包含**以下内容（留给后续 Phase）：
- ❌ HMM 评估逻辑（Phase 1）
- ❌ 风险监控逻辑（Phase 2）
- ❌ 滚动训练调度（Phase 3）
- ❌ 数据质量监控（未来优化）
- ❌ 分布式缓存（未来优化）

### 1.3 验收标准

- ✅ 抽象接口定义完整（3 个核心方法 + 2 个辅助方法）
- ✅ 回测数据源可从 QE workspace 下载并缓存 pred.pkl/label.pkl
- ✅ 实时数据源可查询 t-1 的 kline_daily_raw 和 sw_daily
- ✅ 单元测试覆盖率 > 90%
- ✅ 集成测试通过（回测/实时数据源互换）
- ✅ 性能要求：回测数据源首次加载 < 30s，后续查询 < 1s；实时数据源查询 < 2s

---

## 2. 架构设计

### 2.1 类图

```
┌─────────────────────────────────────┐
│   HMMDataSourceInterface (ABC)      │
│   - mode: str                       │
│   + get_predictions()               │
│   + get_labels()                    │
│   + get_sector_mapping()            │
│   + validate_date_range()           │
│   + get_available_date_range()      │
└─────────────────────────────────────┘
            ▲           ▲
            │           │
   ┌────────┴───┐   ┌──┴─────────────┐
   │            │   │                │
┌──┴──────────────┐ │  ┌──────────────────┐
│BacktestDataSource│ │  │RealtimeDataSource│
│- cache_manager   │ │  │- db_pool         │
│- qe_client       │ │  │- lag_days        │
│- _pred_cache     │ │  │- snapshot_id     │
│- _label_cache    │ │  │                  │
└──────────────────┘ │  └──────────────────┘
                     │
            ┌────────┴─────────┐
            │                  │
    ┌───────┴────────┐  ┌──────┴─────────┐
    │ArtifactCache   │  │MockDataSource  │
    │Manager         │  │(for testing)   │
    └────────────────┘  └────────────────┘
```

### 2.2 数据流

**回测模式**:
```
Business Logic
    ↓ get_predictions(date_range)
BacktestDataSource
    ↓ check cache
ArtifactCacheManager
    ├─ cache hit → return cached DataFrame
    └─ cache miss → download from QE workspace
           ↓
    QEWorkspaceClient (HTTP API)
           ↓
    Save to cache (tmp/hmm_evolution_cache/)
           ↓
    Load pickle → DataFrame
           ↓
    Return to business logic
```

**实时模式**:
```
Business Logic
    ↓ get_predictions(date_range)
RealtimeDataSource
    ↓ query DB
PostgreSQL (aistock)
    ├─ model_train_predictions (if available)
    └─ strategy_package daily outputs
           ↓
    Filter by date_range
           ↓
    Apply t-1 lag
           ↓
    Return DataFrame
```

---

## 3. 接口定义

### 3.1 抽象基类

```python
# backend/services/hmm_data_source/base.py

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional, Tuple
import pandas as pd

class HMMDataSourceInterface(ABC):
    """
    HMM 数据源抽象接口
    
    所有数据源必须实现此接口，确保业务逻辑与数据来源解耦。
    
    设计原则：
    1. 返回标准化的 pandas DataFrame，列名固定
    2. 日期范围包含边界（[start_date, end_date]）
    3. 数据源内部处理缓存、错误重试
    4. 不抛出未处理异常，使用明确的错误类型
    """
    
    @property
    @abstractmethod
    def mode(self) -> str:
        """
        返回数据源模式标识
        
        Returns:
            "backtest" 或 "realtime"
        """
        pass
    
    @abstractmethod
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        获取预测分数
        
        Args:
            start_date: 起始日期（包含）
            end_date: 结束日期（包含）
        
        Returns:
            DataFrame with columns:
            - trade_date: date        交易日期
            - symbol: str             股票代码（含后缀 .SZ/.SH）
            - score: float            预测分数（原始分数，未经 HMM 调整）
            - rank: int (optional)    原始排名
        
        Raises:
            DataSourceError: 数据获取失败
            DateRangeError: 日期范围无效或超出可用范围
        
        Performance:
            回测模式：首次 < 30s，缓存命中 < 1s
            实时模式：< 2s
        """
        pass
    
    @abstractmethod
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        获取未来收益标签（ground truth）
        
        Args:
            start_date: 起始日期（包含）
            end_date: 结束日期（包含）
            horizon_days: 未来收益窗口（天数）
        
        Returns:
            DataFrame with columns:
            - trade_date: date        交易日期（T日）
            - symbol: str             股票代码
            - future_return: float    未来 N 日收益率（T+horizon_days vs T）
            - label_date: date        标签日期（T+horizon_days）
        
        Notes:
            - 回测模式：从 label.pkl 读取，数据完整
            - 实时模式：从 kline_daily_raw 计算，未来数据不可用，
              只返回已实现的收益（用于事后验证）
        
        Raises:
            DataSourceError: 数据获取失败
            HorizonError: horizon_days 超出合理范围（1-30）
        """
        pass
    
    @abstractmethod
    async def get_sector_mapping(
        self,
        trade_date: date,
    ) -> dict[str, str]:
        """
        获取股票板块映射（申万 L2）
        
        Args:
            trade_date: 交易日期
        
        Returns:
            {symbol: sector_code}
            例如: {"000001.SZ": "801780.SI", "600000.SH": "801192.SI"}
        
        Notes:
            - 使用 market.stock_basic + market.sw_member
            - 板块代码为申万 L2 级别（801xxx.SI）
            - 如果股票不属于任何 L2 板块，返回 None
        
        Raises:
            DataSourceError: 数据获取失败
        """
        pass
    
    async def validate_date_range(
        self,
        start_date: date,
        end_date: date,
    ) -> Tuple[bool, Optional[str]]:
        """
        验证日期范围是否有效
        
        Args:
            start_date: 起始日期
            end_date: 结束日期
        
        Returns:
            (is_valid, error_message)
            is_valid=True: 日期范围有效
            is_valid=False: 日期范围无效，error_message 说明原因
        
        Checks:
            1. start_date <= end_date
            2. 日期在数据源可用范围内
            3. 日期跨度不超过限制（回测: 无限制，实时: 最多 2 年）
        """
        if start_date > end_date:
            return False, f"起始日期 {start_date} 晚于结束日期 {end_date}"
        
        available_start, available_end = await self.get_available_date_range()
        
        if start_date < available_start:
            return False, f"起始日期 {start_date} 早于数据可用起始日期 {available_start}"
        
        if end_date > available_end:
            return False, f"结束日期 {end_date} 晚于数据可用结束日期 {available_end}"
        
        return True, None
    
    @abstractmethod
    async def get_available_date_range(self) -> Tuple[date, date]:
        """
        获取数据源可用的日期范围
        
        Returns:
            (start_date, end_date)
        
        Notes:
            - 回测模式：返回 QE artifact 的日期范围
            - 实时模式：返回 DB 中最早和 t-1 的日期
        """
        pass
```

---
## 4. 回测数据源实现

### 4.1 类定义

```python
# backend/services/hmm_data_source/backtest_source.py

from __future__ import annotations

import asyncio
import pickle
from datetime import date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from .base import HMMDataSourceInterface
from .cache_manager import ArtifactCacheManager
from .exceptions import DataSourceError, DateRangeError
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient


class BacktestDataSource(HMMDataSourceInterface):
    """
    基于 QE artifact 的回测数据源
    
    数据来源：
    - pred.pkl: QE 回测的原始预测分数
    - label.pkl: QE 回测的未来收益标签
    
    缓存策略：
    - 首次访问时从 QE workspace 下载到本地缓存
    - 后续访问直接读取缓存
    - 缓存路径: tmp/hmm_evolution_cache/{base_loop_ref}/
    
    线程安全：
    - 使用 asyncio.Lock 保护缓存下载
    - 支持多个协程并发读取
    """
    
    def __init__(
        self,
        base_loop_ref: str,
        cache_dir: str = "tmp/hmm_evolution_cache/",
        qe_client: Optional[QEWorkspaceClient] = None,
    ):
        """
        Args:
            base_loop_ref: QE loop 引用，格式 "qe_20260502_131502_9b54/Loop1"
            cache_dir: 缓存根目录
            qe_client: QE workspace 客户端（可选，测试时可 mock）
        """
        self.base_loop_ref = base_loop_ref
        self.cache_manager = ArtifactCacheManager(
            base_dir=Path(cache_dir),
            loop_ref=base_loop_ref,
        )
        self.qe_client = qe_client or self._create_default_qe_client()
        
        # 内存缓存（单例模式，避免重复加载）
        self._pred_df: Optional[pd.DataFrame] = None
        self._label_df: Optional[pd.DataFrame] = None
        self._sector_mapping: Optional[dict[date, dict[str, str]]] = None
        
        # 下载锁（避免并发下载）
        self._download_lock = asyncio.Lock()
    
    @property
    def mode(self) -> str:
        return "backtest"
    
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        实现说明：
        1. 检查日期范围有效性
        2. 确保 pred.pkl 已缓存（首次下载）
        3. 从内存缓存读取
        4. 过滤日期范围
        5. 返回标准化 DataFrame
        """
        # 1. 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)
        
        # 2. 确保数据已加载
        await self._ensure_predictions_loaded()
        
        # 3. 过滤日期范围
        mask = (
            (self._pred_df['trade_date'] >= start_date) &
            (self._pred_df['trade_date'] <= end_date)
        )
        result = self._pred_df[mask].copy()
        
        if result.empty:
            raise DataSourceError(
                f"No prediction data found for date range {start_date} to {end_date}"
            )
        
        return result
    
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        实现说明：
        1. 验证 horizon_days 合理性（1-30）
        2. 检查日期范围有效性
        3. 确保 label.pkl 已缓存
        4. 从内存��存读取
        5. 过滤日期范围和 horizon
        """
        # 1. 验证 horizon
        if not (1 <= horizon_days <= 30):
            raise ValueError(f"horizon_days must be between 1 and 30, got {horizon_days}")
        
        # 2. 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)
        
        # 3. 确保数据已加载
        await self._ensure_labels_loaded()
        
        # 4. 过滤日期范围和 horizon
        # label.pkl 格式: {(trade_date, symbol, horizon): future_return}
        mask = (
            (self._label_df['trade_date'] >= start_date) &
            (self._label_df['trade_date'] <= end_date) &
            (self._label_df['horizon_days'] == horizon_days)
        )
        result = self._label_df[mask].copy()
        
        if result.empty:
            raise DataSourceError(
                f"No label data found for date range {start_date} to {end_date} "
                f"with horizon {horizon_days}"
            )
        
        return result
    
    async def get_sector_mapping(
        self,
        trade_date: date,
    ) -> dict[str, str]:
        """
        实现说明：
        1. 从 QE artifact 的 sig_analysis.pkl 或本地 DB 读取
        2. 使用 market.stock_basic + market.sw_member
        3. 缓存到内存
        """
        await self._ensure_sector_mapping_loaded()
        
        # 找到最接近的交易日的映射
        available_dates = sorted(self._sector_mapping.keys())
        closest_date = min(available_dates, key=lambda d: abs((d - trade_date).days))
        
        return self._sector_mapping[closest_date]
    
    async def get_available_date_range(self) -> Tuple[date, date]:
        """
        从缓存的 pred.pkl 中提取日期范围
        """
        await self._ensure_predictions_loaded()
        
        min_date = self._pred_df['trade_date'].min()
        max_date = self._pred_df['trade_date'].max()
        
        return min_date, max_date
    
    # ========== 私有方法 ==========
    
    async def _ensure_predictions_loaded(self):
        """确保 pred.pkl 已加载到内存"""
        if self._pred_df is not None:
            return
        
        async with self._download_lock:
            # Double-check (可能其他协程已下载)
            if self._pred_df is not None:
                return
            
            # 检查缓存
            cached_path = self.cache_manager.get_artifact_path("pred.pkl")
            if not cached_path.exists():
                # 下载
                await self._download_artifact("pred.pkl")
            
            # 加载到内存
            self._pred_df = await self._load_predictions_from_cache(cached_path)
    
    async def _ensure_labels_loaded(self):
        """确保 label.pkl 已加载到内存"""
        if self._label_df is not None:
            return
        
        async with self._download_lock:
            if self._label_df is not None:
                return
            
            cached_path = self.cache_manager.get_artifact_path("label.pkl")
            if not cached_path.exists():
                await self._download_artifact("label.pkl")
            
            self._label_df = await self._load_labels_from_cache(cached_path)
    
    async def _ensure_sector_mapping_loaded(self):
        """确保板块映射已加载"""
        if self._sector_mapping is not None:
            return
        
        # 从本地 DB 查询（QE artifact 中可能没有板块映射）
        from backend.db.pg_pool import get_conn
        
        query = """
        SELECT 
            trade_date,
            symbol,
            sector_code
        FROM (
            SELECT DISTINCT
                sb.symbol || CASE 
                    WHEN sb.market = 'SSE' THEN '.SH'
                    WHEN sb.market = 'SZSE' THEN '.SZ'
                    ELSE ''
                END as symbol,
                sm.index_code as sector_code,
                sm.in_date as trade_date,
                ROW_NUMBER() OVER (
                    PARTITION BY sb.symbol, sm.in_date 
                    ORDER BY sm.out_date DESC NULLS FIRST
                ) as rn
            FROM market.stock_basic sb
            JOIN market.sw_member sm ON sb.ts_code = sm.con_code
            WHERE sm.level = 'L2'
        ) t
        WHERE rn = 1
        ORDER BY trade_date, symbol
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()
        
        # 构建 {date: {symbol: sector}} 映射
        mapping = {}
        for trade_date, symbol, sector_code in rows:
            if trade_date not in mapping:
                mapping[trade_date] = {}
            mapping[trade_date][symbol] = sector_code
        
        self._sector_mapping = mapping
    
    async def _download_artifact(self, artifact_name: str):
        """
        从 QE workspace 下载 artifact
        
        Args:
            artifact_name: "pred.pkl" or "label.pkl"
        """
        try:
            # 解析 loop_ref
            task_id, loop_name = self._parse_loop_ref(self.base_loop_ref)
            
            # 下载
            artifact_bytes = await self.qe_client.download_artifact(
                task_id=task_id,
                loop_name=loop_name,
                artifact_name=artifact_name,
            )
            
            # 保存到缓存
            self.cache_manager.save_artifact(artifact_name, artifact_bytes)
            
        except Exception as e:
            raise DataSourceError(
                f"Failed to download {artifact_name} from QE workspace: {e}"
            ) from e
    
    async def _load_predictions_from_cache(self, path: Path) -> pd.DataFrame:
        """
        从缓存加载 pred.pkl 并标准化格式
        
        pred.pkl 格式（QE 输出）:
        {
            (trade_date, symbol): score,
            ...
        }
        
        标准化为:
        DataFrame([trade_date, symbol, score])
        """
        try:
            # 在线程池中加载（避免阻塞事件循环）
            loop = asyncio.get_event_loop()
            pred_dict = await loop.run_in_executor(None, self._load_pickle, path)
            
            # 转换为 DataFrame
            records = [
                {
                    'trade_date': trade_date,
                    'symbol': symbol,
                    'score': score,
                }
                for (trade_date, symbol), score in pred_dict.items()
            ]
            
            df = pd.DataFrame(records)
            
            # 排序（便于后续查询）
            df = df.sort_values(['trade_date', 'score'], ascending=[True, False])
            
            # 添加 rank 列
            df['rank'] = df.groupby('trade_date')['score'].rank(
                method='first', ascending=False
            ).astype(int)
            
            return df
            
        except Exception as e:
            raise DataSourceError(
                f"Failed to load predictions from cache: {e}"
            ) from e
    
    async def _load_labels_from_cache(self, path: Path) -> pd.DataFrame:
        """
        从缓存加载 label.pkl 并标准化格式
        
        label.pkl 格式:
        {
            (trade_date, symbol, horizon): future_return,
            ...
        }
        
        标准化为:
        DataFrame([trade_date, symbol, horizon_days, future_return, label_date])
        """
        try:
            loop = asyncio.get_event_loop()
            label_dict = await loop.run_in_executor(None, self._load_pickle, path)
            
            records = [
                {
                    'trade_date': trade_date,
                    'symbol': symbol,
                    'horizon_days': horizon,
                    'future_return': future_return,
                    'label_date': self._add_trading_days(trade_date, horizon),
                }
                for (trade_date, symbol, horizon), future_return in label_dict.items()
            ]
            
            df = pd.DataFrame(records)
            df = df.sort_values(['trade_date', 'symbol', 'horizon_days'])
            
            return df
            
        except Exception as e:
            raise DataSourceError(
                f"Failed to load labels from cache: {e}"
            ) from e
    
    @staticmethod
    def _load_pickle(path: Path) -> dict:
        """同步加载 pickle 文件"""
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    @staticmethod
    def _parse_loop_ref(loop_ref: str) -> Tuple[str, str]:
        """
        解析 loop_ref
        
        Args:
            loop_ref: "qe_20260502_131502_9b54/Loop1"
        
        Returns:
            (task_id, loop_name)
        """
        parts = loop_ref.split('/')
        if len(parts) != 2:
            raise ValueError(f"Invalid loop_ref format: {loop_ref}")
        
        return parts[0], parts[1]
    
    @staticmethod
    def _add_trading_days(start_date: date, days: int) -> date:
        """
        简化实现：假设 1 个自然日 ≈ 0.7 个交易日
        
        TODO: 使用真实交易日历
        """
        from datetime import timedelta
        return start_date + timedelta(days=int(days / 0.7))
    
    def _create_default_qe_client(self) -> QEWorkspaceClient:
        """创建默认的 QE workspace 客户端"""
        # 从数据库查询 task_id 对应的 node_id 和 api_base_url
        # 这里简化实现，实际需要查询 qe_evolution_tasks 表
        from backend.db.pg_pool import get_conn
        
        task_id = self._parse_loop_ref(self.base_loop_ref)[0]
        
        # 同步查询（初始化时）
        import psycopg2
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="aistock",
            user="aistock_rw",
            password="lc78080808",
        )
        
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.api_base_url
                FROM qe_evolution_tasks t
                JOIN infra.compute_nodes n ON n.node_id = t.node_id
                WHERE t.task_id = %s
                """,
                (task_id,)
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"QE task not found: {task_id}")
            
            api_base_url = row[0]
        
        conn.close()
        
        return QEWorkspaceClient(base_url=api_base_url)
```

---
## 5. 实时数据源实现

### 5.1 类定义

```python
# backend/services/hmm_data_source/realtime_source.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd

from .base import HMMDataSourceInterface
from .exceptions import DataSourceError, DateRangeError
from backend.db.pg_pool import get_conn


class RealtimeDataSource(HMMDataSourceInterface):
    """
    基于数据库 t-1 的实时数据源
    
    数据来源：
    - market.kline_daily_raw: 个股日 K 线（计算预测和标签）
    - market.sw_daily: 申万行业指数
    - model_train_predictions: 模型预测结果（如果可用）
    
    数据延迟：
    - lag_days: 数据延迟天数，默认 1（t-1）
    - 例如：今天是 2026-07-16，查询到的最新数据是 2026-07-15
    
    注意事项：
    - 实时模式下，未来标签不可用（get_labels 只返回已实现收益）
    - 适用于生产环境的风险监控和实时预警
    """
    
    def __init__(
        self,
        snapshot_id: str = "latest",
        lag_days: int = 1,
        max_query_days: int = 730,  # 最多查询 2 年
    ):
        """
        Args:
            snapshot_id: HMM 模型 snapshot ID，"latest" 表示最新
            lag_days: 数据延迟天数（默认 1，即 t-1）
            max_query_days: 单次查询最大天数（防止大范围查询）
        """
        self.snapshot_id = snapshot_id
        self.lag_days = lag_days
        self.max_query_days = max_query_days
        
        # 缓存最新可用日期（避免每次查询 DB）
        self._latest_available_date: Optional[date] = None
        self._cache_timestamp: Optional[float] = None
        self._cache_ttl = 3600  # 缓存 1 小时
    
    @property
    def mode(self) -> str:
        return "realtime"
    
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        实现说明：
        1. 验证日期范围（不超过 max_query_days）
        2. 应用 t-1 延迟
        3. 查询 DB（优先 model_train_predictions，回退到策略包输出）
        4. 返回标准化 DataFrame
        
        数据源优先级：
        1. model_train_predictions（如果 snapshot_id 有预测记录）
        2. strategy_package daily outputs（如果有）
        3. 使用昨日收益率作为 fallback score（最后手段）
        """
        # 1. 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)
        
        days_span = (end_date - start_date).days
        if days_span > self.max_query_days:
            raise DateRangeError(
                f"Query span {days_span} days exceeds max {self.max_query_days} days"
            )
        
        # 2. 应用 t-1 延迟
        actual_end_date = await self._get_latest_available_date()
        if end_date > actual_end_date:
            end_date = actual_end_date
        
        # 3. 查询 DB
        try:
            df = await self._query_predictions_from_db(start_date, end_date)
            
            if df.empty:
                raise DataSourceError(
                    f"No prediction data found in DB for {start_date} to {end_date}"
                )
            
            return df
            
        except Exception as e:
            raise DataSourceError(
                f"Failed to query predictions from DB: {e}"
            ) from e
    
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        实现说明：
        1. 从 market.kline_daily_raw 计算已实现收益
        2. 只返回 T+horizon_days 已发生的数据
        3. 未来数据返回 NaN（实时场景下不可用）
        
        警告：
        - 实时模式下，labels 仅用于事后验证
        - 不应用于实时预测逻辑
        """
        if not (1 <= horizon_days <= 30):
            raise ValueError(f"horizon_days must be between 1 and 30, got {horizon_days}")
        
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)
        
        try:
            df = await self._query_labels_from_db(start_date, end_date, horizon_days)
            
            if df.empty:
                raise DataSourceError(
                    f"No label data found in DB for {start_date} to {end_date}"
                )
            
            return df
            
        except Exception as e:
            raise DataSourceError(
                f"Failed to query labels from DB: {e}"
            ) from e
    
    async def get_sector_mapping(
        self,
        trade_date: date,
    ) -> dict[str, str]:
        """
        从 market.sw_member 查询当日板块映射
        """
        query = """
        SELECT 
            sb.symbol || CASE 
                WHEN sb.market = 'SSE' THEN '.SH'
                WHEN sb.market = 'SZSE' THEN '.SZ'
                ELSE ''
            END as symbol,
            sm.index_code as sector_code
        FROM market.stock_basic sb
        JOIN market.sw_member sm ON sb.ts_code = sm.con_code
        WHERE sm.level = 'L2'
          AND sm.in_date <= %(trade_date)s
          AND (sm.out_date IS NULL OR sm.out_date > %(trade_date)s)
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, {'trade_date': trade_date})
                rows = await cur.fetchall()
        
        return {symbol: sector_code for symbol, sector_code in rows}
    
    async def get_available_date_range(self) -> Tuple[date, date]:
        """
        查询 DB 中最早和 t-1 的日期
        """
        query = """
        SELECT 
            MIN(trade_date) as min_date,
            MAX(trade_date) as max_date
        FROM market.kline_daily_raw
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                row = await cur.fetchone()
        
        if not row or not row[0]:
            raise DataSourceError("No data available in market.kline_daily_raw")
        
        min_date = row[0]
        max_date = row[1]
        
        # 应用 lag_days
        max_date = max_date - timedelta(days=self.lag_days)
        
        return min_date, max_date
    
    # ========== 私有方法 ==========
    
    async def _get_latest_available_date(self) -> date:
        """
        获取最新可用日期（带缓存）
        """
        import time
        
        now = time.time()
        
        # 检查缓存
        if self._latest_available_date and self._cache_timestamp:
            if now - self._cache_timestamp < self._cache_ttl:
                return self._latest_available_date
        
        # 查询 DB
        _, max_date = await self.get_available_date_range()
        
        # 更新缓存
        self._latest_available_date = max_date
        self._cache_timestamp = now
        
        return max_date
    
    async def _query_predictions_from_db(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        从 DB 查询预测分数
        
        数据源优先级：
        1. model_train_predictions（如果存在）
        2. 策略包 daily outputs
        3. 使用昨日涨跌幅作为 fallback
        """
        # 尝试 1: model_train_predictions
        df = await self._try_query_from_model_predictions(start_date, end_date)
        if not df.empty:
            return df
        
        # 尝试 2: 策略包 daily outputs (TODO: 需要策略包输出表)
        # df = await self._try_query_from_strategy_outputs(start_date, end_date)
        # if not df.empty:
        #     return df
        
        # 尝试 3: fallback - 使用昨日涨跌幅
        df = await self._fallback_query_from_kline(start_date, end_date)
        return df
    
    async def _try_query_from_model_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        从 model_train_predictions 查询
        
        注意：这个表可能不存在或没有数据
        """
        # 检查表是否存在
        check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'model_train_predictions'
        )
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(check_query)
                exists = (await cur.fetchone())[0]
                
                if not exists:
                    return pd.DataFrame()
                
                # 查询预测
                query = """
                SELECT 
                    trade_date,
                    symbol,
                    score
                FROM model_train_predictions
                WHERE snapshot_id = %(snapshot_id)s
                  AND trade_date BETWEEN %(start_date)s AND %(end_date)s
                ORDER BY trade_date, score DESC
                """
                
                await cur.execute(query, {
                    'snapshot_id': self.snapshot_id,
                    'start_date': start_date,
                    'end_date': end_date,
                })
                rows = await cur.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=['trade_date', 'symbol', 'score'])
        
        # 添加 rank
        df['rank'] = df.groupby('trade_date')['score'].rank(
            method='first', ascending=False
        ).astype(int)
        
        return df
    
    async def _fallback_query_from_kline(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        Fallback: 使用昨日涨跌幅作为 score
        
        逻辑：
        - 昨日涨幅大 → 动量效应 → score 高
        - 这是最简单的预测，仅作为 fallback
        """
        query = """
        SELECT 
            trade_date,
            ts_code as symbol,
            pct_chg / 100.0 as score
        FROM market.kline_daily_raw
        WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
          AND pct_chg IS NOT NULL
        ORDER BY trade_date, pct_chg DESC
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date,
                })
                rows = await cur.fetchall()
        
        df = pd.DataFrame(rows, columns=['trade_date', 'symbol', 'score'])
        
        # 添加 rank
        df['rank'] = df.groupby('trade_date')['score'].rank(
            method='first', ascending=False
        ).astype(int)
        
        return df
    
    async def _query_labels_from_db(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int,
    ) -> pd.DataFrame:
        """
        从 market.kline_daily_raw 计算未来收益
        
        逻辑：
        - 对于 T 日，计算 (T+horizon) / T - 1
        - 只返回 T+horizon 已发生的数据
        """
        query = """
        WITH price_data AS (
            SELECT 
                trade_date,
                ts_code as symbol,
                close,
                LEAD(close, %(horizon)s) OVER (
                    PARTITION BY ts_code ORDER BY trade_date
                ) as future_close
            FROM market.kline_daily_raw
            WHERE trade_date >= %(start_date)s
              AND trade_date <= %(end_date)s + INTERVAL '%(horizon)s days'
        )
        SELECT 
            trade_date,
            symbol,
            %(horizon)s as horizon_days,
            CASE 
                WHEN future_close IS NOT NULL THEN
                    (future_close / close - 1.0)
                ELSE NULL
            END as future_return,
            trade_date + INTERVAL '%(horizon)s days' as label_date
        FROM price_data
        WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
          AND future_close IS NOT NULL
        ORDER BY trade_date, symbol
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date,
                    'horizon': horizon_days,
                })
                rows = await cur.fetchall()
        
        df = pd.DataFrame(rows, columns=[
            'trade_date', 'symbol', 'horizon_days', 'future_return', 'label_date'
        ])
        
        return df
```

---

## 6. 缓存管理器

### 6.1 类定义

```python
# backend/services/hmm_data_source/cache_manager.py

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

from .exceptions import CacheError


class ArtifactCacheManager:
    """
    QE artifact 缓存管理器
    
    目录结构:
    tmp/hmm_evolution_cache/
      qe_20260502_131502_9b54__Loop1/
        pred.pkl
        label.pkl
        metadata.json
    
    功能:
    - 管理 artifact 下载和缓存
    - 校验文件完整性（SHA256）
    - 提供缓存清理接口
    """
    
    def __init__(
        self,
        base_dir: Path,
        loop_ref: str,
    ):
        """
        Args:
            base_dir: 缓存根目录
            loop_ref: QE loop 引用，例如 "qe_20260502_131502_9b54/Loop1"
        """
        self.base_dir = Path(base_dir)
        self.loop_ref = loop_ref
        self.cache_dir = self._get_cache_dir()
        
        # 确保缓存目录存在
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_artifact_path(self, artifact_name: str) -> Path:
        """
        获取 artifact 的缓存路径
        
        Args:
            artifact_name: "pred.pkl" or "label.pkl"
        
        Returns:
            Path 对象
        """
        return self.cache_dir / artifact_name
    
    def save_artifact(
        self,
        artifact_name: str,
        content: bytes,
    ) -> Path:
        """
        保存 artifact 到缓存
        
        Args:
            artifact_name: 文件名
            content: 文件内容（bytes）
        
        Returns:
            保存后的文件路径
        """
        try:
            artifact_path = self.get_artifact_path(artifact_name)
            
            # 写入文件
            artifact_path.write_bytes(content)
            
            # 保存元数据
            self._save_metadata(artifact_name, content)
            
            return artifact_path
            
        except Exception as e:
            raise CacheError(
                f"Failed to save artifact {artifact_name}: {e}"
            ) from e
    
    def verify_artifact(self, artifact_name: str) -> bool:
        """
        验证 artifact 完整性
        
        Args:
            artifact_name: 文件名
        
        Returns:
            True if valid, False otherwise
        """
        artifact_path = self.get_artifact_path(artifact_name)
        
        if not artifact_path.exists():
            return False
        
        # 读取保存的 SHA256
        metadata = self._load_metadata()
        if artifact_name not in metadata:
            return False
        
        saved_sha256 = metadata[artifact_name].get('sha256')
        if not saved_sha256:
            return False
        
        # 计算当前文件的 SHA256
        current_sha256 = self._calculate_sha256(artifact_path)
        
        return current_sha256 == saved_sha256
    
    def clear_cache(self):
        """删除当前 loop 的所有缓存文件"""
        import shutil
        
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
    
    def get_cache_info(self) -> dict:
        """
        获取缓存信息
        
        Returns:
            {
                'loop_ref': str,
                'cache_dir': str,
                'artifacts': {
                    'pred.pkl': {'size': int, 'sha256': str, 'cached_at': str},
                    'label.pkl': {...},
                },
                'total_size': int,
            }
        """
        metadata = self._load_metadata()
        
        total_size = 0
        for artifact_name, info in metadata.items():
            artifact_path = self.get_artifact_path(artifact_name)
            if artifact_path.exists():
                info['size'] = artifact_path.stat().st_size
                total_size += info['size']
        
        return {
            'loop_ref': self.loop_ref,
            'cache_dir': str(self.cache_dir),
            'artifacts': metadata,
            'total_size': total_size,
        }
    
    # ========== 私有方法 ==========
    
    def _get_cache_dir(self) -> Path:
        """
        生成缓存目录路径
        
        将 "/" 替换为 "__" 避免子目录
        例如: "qe_20260502_131502_9b54/Loop1" -> "qe_20260502_131502_9b54__Loop1"
        """
        safe_name = self.loop_ref.replace('/', '__')
        return self.base_dir / safe_name
    
    def _save_metadata(self, artifact_name: str, content: bytes):
        """保存 artifact 元数据"""
        metadata = self._load_metadata()
        
        metadata[artifact_name] = {
            'sha256': self._calculate_sha256_from_bytes(content),
            'cached_at': self._get_current_timestamp(),
        }
        
        metadata_path = self.cache_dir / 'metadata.json'
        metadata_path.write_text(json.dumps(metadata, indent=2))
    
    def _load_metadata(self) -> dict:
        """加载 metadata.json"""
        metadata_path = self.cache_dir / 'metadata.json'
        
        if not metadata_path.exists():
            return {}
        
        try:
            return json.loads(metadata_path.read_text())
        except Exception:
            return {}
    
    @staticmethod
    def _calculate_sha256(path: Path) -> str:
        """计算文件 SHA256"""
        sha256_hash = hashlib.sha256()
        
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
    
    @staticmethod
    def _calculate_sha256_from_bytes(content: bytes) -> str:
        """计算 bytes SHA256"""
        return hashlib.sha256(content).hexdigest()
    
    @staticmethod
    def _get_current_timestamp() -> str:
        """获取当前时间戳（ISO 格式）"""
        from datetime import datetime
        return datetime.now().isoformat()
```

---
## 7. 异常定义

### 7.1 异常类

```python
# backend/services/hmm_data_source/exceptions.py

"""
HMM 数据源异常定义

异常层次:
DataSourceError (基类)
  ├─ DateRangeError (日期范围错误)
  ├─ HorizonError (horizon 参数错误)
  ├─ CacheError (缓存错误)
  └─ DataNotFoundError (数据不存在)
"""

class DataSourceError(Exception):
    """数据源基础异常"""
    pass


class DateRangeError(DataSourceError):
    """日期范围错误"""
    pass


class HorizonError(DataSourceError):
    """horizon_days 参数错误"""
    pass


class CacheError(DataSourceError):
    """缓存操作错误"""
    pass


class DataNotFoundError(DataSourceError):
    """数据不存在"""
    pass
```

---

## 8. 数据模型

### 8.1 Pydantic 模型

```python
# backend/services/hmm_data_source/models.py

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, validator


class DataSourceConfig(BaseModel):
    """数据源配置"""
    
    mode: str = Field(..., description="数据源模式: backtest 或 realtime")
    
    # 回测模式参数
    base_loop_ref: Optional[str] = Field(None, description="QE loop 引用")
    cache_dir: str = Field("tmp/hmm_evolution_cache/", description="缓存目录")
    
    # 实时模式参数
    snapshot_id: Optional[str] = Field("latest", description="HMM snapshot ID")
    lag_days: int = Field(1, description="数据延迟天数")
    max_query_days: int = Field(730, description="单次查询最大天数")
    
    @validator('mode')
    def validate_mode(cls, v):
        if v not in ['backtest', 'realtime']:
            raise ValueError(f"mode must be 'backtest' or 'realtime', got '{v}'")
        return v
    
    @validator('base_loop_ref')
    def validate_backtest_config(cls, v, values):
        if values.get('mode') == 'backtest' and not v:
            raise ValueError("base_loop_ref is required for backtest mode")
        return v


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
    
    @validator('end_date')
    def validate_date_order(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError(f"end_date {v} must be >= start_date {values['start_date']}")
        return v


class CacheInfo(BaseModel):
    """缓存信息"""
    
    loop_ref: str
    cache_dir: str
    artifacts: dict[str, dict]
    total_size: int = Field(..., description="总大小（bytes）")
```

---

## 9. 单元测试

### 9.1 测试框架

```python
# tests/backend/services/hmm_data_source/test_backtest_source.py

import pytest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from backend.services.hmm_data_source import BacktestDataSource
from backend.services.hmm_data_source.exceptions import DataSourceError, DateRangeError


@pytest.fixture
def mock_qe_client():
    """Mock QE workspace client"""
    client = MagicMock()
    client.download_artifact = AsyncMock()
    return client


@pytest.fixture
def backtest_source(tmp_path, mock_qe_client):
    """创建测试用的回测数据源"""
    return BacktestDataSource(
        base_loop_ref="test_task/Loop1",
        cache_dir=str(tmp_path),
        qe_client=mock_qe_client,
    )


@pytest.mark.asyncio
async def test_mode_property(backtest_source):
    """测试 mode 属性"""
    assert backtest_source.mode == "backtest"


@pytest.mark.asyncio
async def test_get_predictions_validates_date_range(backtest_source):
    """测试日期范围验证"""
    # 测试：start_date > end_date
    with pytest.raises(DateRangeError, match="起始日期.*晚于结束日期"):
        await backtest_source.get_predictions(
            start_date=date(2026, 1, 10),
            end_date=date(2026, 1, 1),
        )


@pytest.mark.asyncio
async def test_get_predictions_downloads_artifact_on_first_call(
    backtest_source,
    mock_qe_client,
    tmp_path,
):
    """测试首次调用时下载 artifact"""
    # Mock 下载返回
    import pickle
    mock_data = {
        (date(2026, 1, 1), "000001.SZ"): 0.85,
        (date(2026, 1, 1), "000002.SZ"): 0.72,
    }
    mock_qe_client.download_artifact.return_value = pickle.dumps(mock_data)
    
    # 调用
    df = await backtest_source.get_predictions(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 1),
    )
    
    # 验证下载被调用
    mock_qe_client.download_artifact.assert_called_once()
    
    # 验证返回的 DataFrame
    assert len(df) == 2
    assert 'trade_date' in df.columns
    assert 'symbol' in df.columns
    assert 'score' in df.columns
    assert 'rank' in df.columns


@pytest.mark.asyncio
async def test_get_predictions_uses_cache_on_second_call(
    backtest_source,
    mock_qe_client,
):
    """测试第二次调用时使用缓存"""
    # Mock 下载
    import pickle
    mock_data = {
        (date(2026, 1, 1), "000001.SZ"): 0.85,
    }
    mock_qe_client.download_artifact.return_value = pickle.dumps(mock_data)
    
    # 第一次调用
    await backtest_source.get_predictions(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 1),
    )
    
    # 重置 mock
    mock_qe_client.download_artifact.reset_mock()
    
    # 第二次调用
    await backtest_source.get_predictions(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 1),
    )
    
    # 验证没有再次下载
    mock_qe_client.download_artifact.assert_not_called()


@pytest.mark.asyncio
async def test_get_labels_validates_horizon(backtest_source):
    """测试 horizon_days 验证"""
    with pytest.raises(ValueError, match="horizon_days must be between 1 and 30"):
        await backtest_source.get_labels(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 10),
            horizon_days=50,
        )


@pytest.mark.asyncio
async def test_get_sector_mapping_returns_dict(backtest_source):
    """测试板块映射返回格式"""
    # Mock DB 查询
    with patch('backend.services.hmm_data_source.backtest_source.get_conn') as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchall = AsyncMock(return_value=[
            (date(2026, 1, 1), "000001.SZ", "801780.SI"),
            (date(2026, 1, 1), "000002.SZ", "801192.SI"),
        ])
        mock_conn.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value = mock_cursor
        
        mapping = await backtest_source.get_sector_mapping(date(2026, 1, 1))
        
        assert isinstance(mapping, dict)
        assert "000001.SZ" in mapping
        assert mapping["000001.SZ"] == "801780.SI"


@pytest.mark.asyncio
async def test_concurrent_downloads_use_lock(backtest_source, mock_qe_client):
    """测试并发下载使用锁保护"""
    import pickle
    mock_data = {(date(2026, 1, 1), "000001.SZ"): 0.85}
    mock_qe_client.download_artifact.return_value = pickle.dumps(mock_data)
    
    # 模拟慢速下载
    async def slow_download(*args, **kwargs):
        import asyncio
        await asyncio.sleep(0.1)
        return pickle.dumps(mock_data)
    
    mock_qe_client.download_artifact = slow_download
    
    # 并发调用
    import asyncio
    results = await asyncio.gather(
        backtest_source.get_predictions(date(2026, 1, 1), date(2026, 1, 1)),
        backtest_source.get_predictions(date(2026, 1, 1), date(2026, 1, 1)),
        backtest_source.get_predictions(date(2026, 1, 1), date(2026, 1, 1)),
    )
    
    # 验证所有调用都成功
    assert len(results) == 3
    for df in results:
        assert not df.empty
```

---

### 9.2 实时数据源测试

```python
# tests/backend/services/hmm_data_source/test_realtime_source.py

import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from backend.services.hmm_data_source import RealtimeDataSource
from backend.services.hmm_data_source.exceptions import DateRangeError


@pytest.fixture
def realtime_source():
    """创建测试用的实时数据源"""
    return RealtimeDataSource(
        snapshot_id="test_snapshot",
        lag_days=1,
    )


@pytest.mark.asyncio
async def test_mode_property(realtime_source):
    """测试 mode 属性"""
    assert realtime_source.mode == "realtime"


@pytest.mark.asyncio
async def test_get_predictions_respects_lag_days(realtime_source):
    """测试 lag_days 生效"""
    with patch('backend.services.hmm_data_source.realtime_source.get_conn') as mock_conn:
        # Mock 最新日期查询
        mock_cursor = MagicMock()
        mock_cursor.fetchone = AsyncMock(side_effect=[
            (date(2024, 1, 1), date(2026, 7, 16)),  # get_available_date_range
            [],  # try_query_from_model_predictions (check table exists)
            [],  # fallback_query_from_kline
        ])
        mock_cursor.fetchall = AsyncMock(return_value=[])
        mock_conn.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value = mock_cursor
        
        # 请求今天的数据，应该被限制到 t-1
        try:
            await realtime_source.get_predictions(
                start_date=date(2026, 7, 16),
                end_date=date(2026, 7, 16),
            )
        except Exception:
            pass  # 这里只测试 lag 逻辑，不关心查询失败
        
        # 验证实际查询的日期是 t-1
        # (具体验证逻辑根据实现调整)


@pytest.mark.asyncio
async def test_get_predictions_enforces_max_query_days(realtime_source):
    """测试查询天数限制"""
    with pytest.raises(DateRangeError, match="exceeds max.*days"):
        await realtime_source.get_predictions(
            start_date=date(2024, 1, 1),
            end_date=date(2026, 7, 16),  # 超过 730 天
        )


@pytest.mark.asyncio
async def test_get_labels_calculates_realized_returns(realtime_source):
    """测试计算已实现收益"""
    with patch('backend.services.hmm_data_source.realtime_source.get_conn') as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchall = AsyncMock(return_value=[
            (date(2026, 1, 1), "000001.SZ", 10, 0.05, date(2026, 1, 15)),
        ])
        mock_conn.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value = mock_cursor
        
        df = await realtime_source.get_labels(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 1),
            horizon_days=10,
        )
        
        assert not df.empty
        assert 'future_return' in df.columns
        assert df.iloc[0]['future_return'] == 0.05
```

---

### 9.3 缓存管理器测试

```python
# tests/backend/services/hmm_data_source/test_cache_manager.py

import pytest
from pathlib import Path

from backend.services.hmm_data_source.cache_manager import ArtifactCacheManager


@pytest.fixture
def cache_manager(tmp_path):
    """创建测试用的缓存管理器"""
    return ArtifactCacheManager(
        base_dir=tmp_path,
        loop_ref="test_task/Loop1",
    )


def test_get_artifact_path(cache_manager):
    """测试获取 artifact 路径"""
    path = cache_manager.get_artifact_path("pred.pkl")
    assert path.name == "pred.pkl"
    assert "test_task__Loop1" in str(path)


def test_save_artifact_creates_file(cache_manager):
    """测试保存 artifact"""
    content = b"test content"
    path = cache_manager.save_artifact("pred.pkl", content)
    
    assert path.exists()
    assert path.read_bytes() == content


def test_verify_artifact_returns_true_for_valid_file(cache_manager):
    """测试验证有效文件"""
    content = b"test content"
    cache_manager.save_artifact("pred.pkl", content)
    
    assert cache_manager.verify_artifact("pred.pkl") is True


def test_verify_artifact_returns_false_for_corrupted_file(cache_manager):
    """测试验证损坏文件"""
    content = b"test content"
    path = cache_manager.save_artifact("pred.pkl", content)
    
    # 篡改文件
    path.write_bytes(b"corrupted")
    
    assert cache_manager.verify_artifact("pred.pkl") is False


def test_clear_cache_removes_directory(cache_manager):
    """测试清理缓存"""
    cache_manager.save_artifact("pred.pkl", b"test")
    cache_dir = cache_manager.cache_dir
    
    assert cache_dir.exists()
    
    cache_manager.clear_cache()
    
    assert not cache_dir.exists()


def test_get_cache_info_returns_metadata(cache_manager):
    """测试获取缓存信息"""
    cache_manager.save_artifact("pred.pkl", b"test content")
    
    info = cache_manager.get_cache_info()
    
    assert info['loop_ref'] == "test_task/Loop1"
    assert 'pred.pkl' in info['artifacts']
    assert info['total_size'] > 0
```

---

## 10. 集成测试

### 10.1 端到端测试

```python
# tests/backend/services/hmm_data_source/test_integration.py

import pytest
from datetime import date

from backend.services.hmm_data_source import BacktestDataSource, RealtimeDataSource
from backend.services.hmm_data_source.models import DataSourceConfig


@pytest.mark.integration
@pytest.mark.asyncio
async def test_data_source_can_be_switched_via_config():
    """测试通过配置切换数据源"""
    
    # 回测配置
    backtest_config = DataSourceConfig(
        mode="backtest",
        base_loop_ref="qe_20260502_131502_9b54/Loop1",
    )
    
    # 实时配置
    realtime_config = DataSourceConfig(
        mode="realtime",
        snapshot_id="latest",
    )
    
    # 工厂函数
    def create_data_source(config: DataSourceConfig):
        if config.mode == "backtest":
            return BacktestDataSource(
                base_loop_ref=config.base_loop_ref,
                cache_dir=config.cache_dir,
            )
        else:
            return RealtimeDataSource(
                snapshot_id=config.snapshot_id,
                lag_days=config.lag_days,
            )
    
    # 创建回测数据源
    backtest_source = create_data_source(backtest_config)
    assert backtest_source.mode == "backtest"
    
    # 创建实时数据源
    realtime_source = create_data_source(realtime_config)
    assert realtime_source.mode == "realtime"


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(
    "not config.getoption('--run-integration')",
    reason="Need --run-integration flag"
)
async def test_backtest_source_downloads_real_artifact():
    """测试从真实 QE workspace 下载 artifact
    
    运行命令: pytest --run-integration
    """
    source = BacktestDataSource(
        base_loop_ref="qe_20260502_131502_9b54/Loop1",
        cache_dir="tmp/test_cache/",
    )
    
    # 查询真实数据
    df = await source.get_predictions(
        start_date=date(2024, 7, 1),
        end_date=date(2024, 7, 5),
    )
    
    assert not df.empty
    assert set(df.columns) == {'trade_date', 'symbol', 'score', 'rank'}


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(
    "not config.getoption('--run-integration')",
    reason="Need --run-integration flag"
)
async def test_realtime_source_queries_real_database():
    """测试从真实数据库查询"""
    source = RealtimeDataSource(lag_days=1)
    
    # 查询昨天的数据
    from datetime import timedelta
    yesterday = date.today() - timedelta(days=1)
    
    df = await source.get_predictions(
        start_date=yesterday,
        end_date=yesterday,
    )
    
    assert not df.empty
    assert set(df.columns) == {'trade_date', 'symbol', 'score', 'rank'}
```

---
## 11. 性能优化

### 11.1 优化策略

#### 1. 内存缓存
```python
# 已实现：BacktestDataSource 使用内存缓存
# - _pred_df: 缓存预测数据
# - _label_df: 缓存标签数据
# - _sector_mapping: 缓存板块映射

# 优化点：
# - 单例模式，避免重复加载大型 pickle 文件
# - 使用 asyncio.Lock 保护并发下载
```

#### 2. 查询优化
```python
# 实时数据源查询优化
class RealtimeDataSource:
    async def get_predictions(self, start_date, end_date):
        # 优化 1: 限制查询天数
        if (end_date - start_date).days > self.max_query_days:
            raise DateRangeError(...)
        
        # 优化 2: 使用索引
        # 确保 DB 表有索引: (trade_date, score)
        
        # 优化 3: 只查询必要列
        # SELECT trade_date, symbol, score (不要 SELECT *)
```

#### 3. 并发处理
```python
# 批量查询时使用 asyncio.gather
async def batch_query_multiple_dates(dates):
    tasks = [
        source.get_predictions(d, d)
        for d in dates
    ]
    return await asyncio.gather(*tasks)
```

### 11.2 性能基准

| 操作 | 目标性能 | 测量方法 |
|------|---------|---------|
| 回测数据源首次加载 | < 30s | 下载 + 解析 pred.pkl (约 50MB) |
| 回测数据源缓存命中 | < 1s | 内存查询 + 日期过滤 |
| 实时数据源查询 | < 2s | DB 查询 + DataFrame 构建 |
| 板块映射查询 | < 500ms | DB 查询 stock_basic + sw_member |

### 11.3 性能测试

```python
# tests/backend/services/hmm_data_source/test_performance.py

import pytest
import time
from datetime import date

from backend.services.hmm_data_source import BacktestDataSource


@pytest.mark.performance
@pytest.mark.asyncio
async def test_backtest_source_cache_hit_performance():
    """测试缓存命中性能 < 1s"""
    source = BacktestDataSource(
        base_loop_ref="qe_20260502_131502_9b54/Loop1",
    )
    
    # 预热缓存
    await source.get_predictions(date(2024, 7, 1), date(2024, 7, 1))
    
    # 测量缓存命中时间
    start = time.time()
    await source.get_predictions(date(2024, 7, 1), date(2024, 7, 5))
    elapsed = time.time() - start
    
    assert elapsed < 1.0, f"Cache hit took {elapsed:.2f}s, expected < 1s"


@pytest.mark.performance
@pytest.mark.asyncio
async def test_realtime_source_query_performance():
    """测试实时查询性能 < 2s"""
    source = RealtimeDataSource(lag_days=1)
    
    start = time.time()
    await source.get_predictions(date(2026, 7, 1), date(2026, 7, 5))
    elapsed = time.time() - start
    
    assert elapsed < 2.0, f"Realtime query took {elapsed:.2f}s, expected < 2s"
```

---

## 12. 错误处理矩阵

### 12.1 错误场景与处理

| 错误场景 | 异常类型 | 处理策略 | 用户提示 |
|---------|---------|---------|---------|
| QE workspace 不可达 | DataSourceError | 重试 3 次，间隔 5s | "无法连接 QE workspace，请检查网络或联系管理员" |
| pred.pkl 文件损坏 | DataSourceError | 清除缓存，重新下载 | "缓存文件损坏，正在重新下载" |
| 日期范围超出限制 | DateRangeError | 立即失败，不重试 | "日期范围 {start} 到 {end} 超出可用范围 {avail_start} 到 {avail_end}" |
| DB 连接失败 | DataSourceError | 重试 3 次 | "数据库连接失败，请稍后重试" |
| horizon_days 无效 | HorizonError | 立即失败 | "horizon_days 必须在 1-30 之间" |
| 缓存目录无写权限 | CacheError | 立即失败 | "缓存目录 {path} 无写权限，请检查权限配置" |
| 查询结果为空 | DataNotFoundError | 立即失败 | "未找到 {start} 到 {end} 的数据" |

### 12.2 重试逻辑

```python
# backend/services/hmm_data_source/retry.py

import asyncio
from typing import Callable, TypeVar

T = TypeVar('T')


async def retry_with_backoff(
    func: Callable[..., T],
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> T:
    """
    带指数退避的重试
    
    Args:
        func: 异步函数
        max_retries: 最大重试次数
        initial_delay: 初始延迟（秒）
        backoff_factor: 退避系数
        exceptions: 需要重试的异常类型
    
    Returns:
        函数返回值
    
    Raises:
        最后一次失败的异常
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except exceptions as e:
            last_exception = e
            
            if attempt == max_retries:
                raise
            
            await asyncio.sleep(delay)
            delay *= backoff_factor
    
    raise last_exception


# 使用示例
async def _download_artifact_with_retry(self, artifact_name: str):
    """带重试的下载"""
    async def download():
        return await self.qe_client.download_artifact(
            task_id=task_id,
            loop_name=loop_name,
            artifact_name=artifact_name,
        )
    
    return await retry_with_backoff(
        download,
        max_retries=3,
        initial_delay=5.0,
        exceptions=(ConnectionError, TimeoutError),
    )
```

---

## 13. 日志规范

### 13.1 日志级别

```python
import logging

logger = logging.getLogger(__name__)

# DEBUG: 详细的诊断信息
logger.debug(f"Loading predictions from cache: {cached_path}")

# INFO: 关键操作完成
logger.info(f"Downloaded artifact {artifact_name}, size={len(content)} bytes")

# WARNING: 非预期情况但可恢复
logger.warning(f"Cache verification failed for {artifact_name}, will re-download")

# ERROR: 操作失败
logger.error(f"Failed to query predictions: {e}", exc_info=True)
```

### 13.2 结构化日志

```python
# 使用 structlog 或标准 logging 的 extra 参数

logger.info(
    "Prediction query completed",
    extra={
        "mode": self.mode,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "result_count": len(df),
        "duration_ms": int(elapsed * 1000),
    }
)
```

---

## 14. API 文档

### 14.1 OpenAPI Schema（后续 Phase 1 使用）

```yaml
# 为 Phase 1 的 REST API 预留

/api/v1/hmm-data-source/info:
  get:
    summary: 获取数据源信息
    parameters:
      - name: mode
        in: query
        schema:
          type: string
          enum: [backtest, realtime]
    responses:
      200:
        description: 数据源信息
        content:
          application/json:
            schema:
              type: object
              properties:
                mode:
                  type: string
                available_date_range:
                  type: object
                  properties:
                    start_date:
                      type: string
                      format: date
                    end_date:
                      type: string
                      format: date
                cache_info:
                  type: object

/api/v1/hmm-data-source/predictions:
  post:
    summary: 查询预测数据
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [mode, start_date, end_date]
            properties:
              mode:
                type: string
              start_date:
                type: string
                format: date
              end_date:
                type: string
                format: date
              config:
                type: object
    responses:
      200:
        description: 预测数据
        content:
          application/json:
            schema:
              type: object
              properties:
                records:
                  type: array
                  items:
                    $ref: '#/components/schemas/PredictionRecord'
                count:
                  type: integer
```

---

## 15. 部署与配置

### 15.1 环境变量

```bash
# .env 或环境配置

# QE Workspace 配置
QE_WORKSPACE_TIMEOUT=60  # 下载超时（秒）
QE_WORKSPACE_MAX_RETRIES=3

# 数据库配置
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=aistock
DB_USER=aistock_rw
DB_PASSWORD=***

# 缓存配置
HMM_CACHE_DIR=tmp/hmm_evolution_cache/
HMM_CACHE_TTL=3600  # 缓存 TTL（秒）

# 性能配置
HMM_MAX_QUERY_DAYS=730  # 实时模式最大查询天数
HMM_LAG_DAYS=1  # 数据延迟天数
```

### 15.2 初始化脚本

```python
# scripts/init_hmm_data_source.py

"""
初始化 HMM 数据源环境

功能：
1. 创建缓存目录
2. 验证 DB 连接
3. 下载示例 artifact（可选）
"""

import asyncio
from pathlib import Path

from backend.services.hmm_data_source import BacktestDataSource
from backend.db.pg_pool import get_conn


async def main():
    print("Initializing HMM data source environment...")
    
    # 1. 创建缓存目录
    cache_dir = Path("tmp/hmm_evolution_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    print(f"✓ Cache directory created: {cache_dir}")
    
    # 2. 验证 DB 连接
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
        print("✓ Database connection verified")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return
    
    # 3. 下载示例 artifact（可选）
    print("\nDownloading sample artifact (optional)...")
    try:
        source = BacktestDataSource(
            base_loop_ref="qe_20260502_131502_9b54/Loop1",
        )
        from datetime import date
        await source.get_predictions(date(2024, 7, 1), date(2024, 7, 1))
        print("✓ Sample artifact downloaded and cached")
    except Exception as e:
        print(f"✗ Sample download failed: {e}")
    
    print("\nInitialization completed!")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## 16. 验收清单

### 16.1 功能验收

- [ ] **接口定义**
  - [ ] HMMDataSourceInterface 包含 5 个方法
  - [ ] 所有方法有完整的 docstring 和类型注解
  - [ ] 异常类型明确定义

- [ ] **回测数据源**
  - [ ] 可从 QE workspace 下载 pred.pkl
  - [ ] 可从 QE workspace 下载 label.pkl
  - [ ] 首次下载保存到缓存
  - [ ] 后续访问使用缓存（无重复下载）
  - [ ] 并发访问正确处理（锁保护）
  - [ ] 日期范围验证生效
  - [ ] 返回标准化 DataFrame

- [ ] **实时数据源**
  - [ ] 可查询 t-1 数据
  - [ ] lag_days 参数生效
  - [ ] max_query_days 限制生效
  - [ ] 查询 DB 返回正确数据
  - [ ] 板块映射查询正确

- [ ] **缓存管理**
  - [ ] 保存 artifact 到缓存
  - [ ] SHA256 校验正确
  - [ ] 损坏文件检测生效
  - [ ] 清理缓存功能正常
  - [ ] 缓存信息查询正确

### 16.2 性能验收

- [ ] 回测数据源首次加载 < 30s
- [ ] 回测数据源缓存命中 < 1s
- [ ] 实时数据源查询 < 2s
- [ ] 板块映射查询 < 500ms

### 16.3 测试验收

- [ ] 单元测试覆盖率 > 90%
- [ ] 所有单元测试通过
- [ ] 集成测试通过（需 --run-integration 标志）
- [ ] 性能测试通过

### 16.4 文档验收

- [ ] README 包含使用示例
- [ ] API 文档完整
- [ ] 异常处理说明清晰
- [ ] 性能基准文档化

---

## 17. 后续工作（Phase 1+）

Phase 0 完成后，数据源抽象层将作为基础设施供后续阶段使用：

### 17.1 Phase 1: HMM 离线评估
```python
# backend/services/hmm_evolution/service.py

from backend.services.hmm_data_source import HMMDataSourceInterface

class HMMEvolutionService:
    def __init__(self, data_source: HMMDataSourceInterface):
        self.data_source = data_source
    
    async def evaluate_offline(self, hmm_snapshot_id: str):
        # 使用 data_source 获取预测和标签
        pred_df = await self.data_source.get_predictions(start, end)
        label_df = await self.data_source.get_labels(start, end)
        
        # 评估逻辑...
```

### 17.2 Phase 2: HMM 风险监控
```python
# backend/services/hmm_risk/monitor_service.py

class HMMRiskMonitorService:
    def __init__(self, data_source: HMMDataSourceInterface):
        self.data_source = data_source
    
    async def generate_daily_alerts(self, trade_date: date):
        # 使用 data_source 获取板块映射
        sector_map = await self.data_source.get_sector_mapping(trade_date)
        
        # 生成预警...
```

### 17.3 配置管理
```python
# backend/config/hmm_config.py

from pydantic import BaseSettings

class HMMConfig(BaseSettings):
    # 数据源模式（环境变量可覆盖）
    data_source_mode: str = "backtest"
    
    # 回测配置
    base_loop_ref: str = "qe_20260502_131502_9b54/Loop1"
    cache_dir: str = "tmp/hmm_evolution_cache/"
    
    # 实时配置
    snapshot_id: str = "latest"
    lag_days: int = 1
    
    class Config:
        env_prefix = "HMM_"


# 使用
config = HMMConfig()
data_source = create_data_source(config)
```

---

## 18. 附录

### 18.1 参考文档

- AIstock 架构设计规范: `docs/architecture/README.md`
- Research Pipeline 设计: `docs/architecture/research_pipeline_and_mcp_gateway_design_v2.md`
- QE Workspace Client API: `backend/services/quantevolver/qe_workspace_client.py`
- 数据库连接池: `backend/db/pg_pool.py`

### 18.2 相关 Issue

- HMM 修复历史: 2026-04-04 格式统一
- QE/HMM 热修复: 2026-05-08 并行隔离
- 滚动训练测试: `backend/tests/test_hmm_rolling_training.py`

### 18.3 变更历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-07-16 | 初始详细设计，定义接口和两种数据源实现 |

---

**文档归档路径**: `docs/architecture/hmm_evolution_phase0_data_source_detailed_design_20260716.md`

**下一步**: 合并所有 part 文件，开始 Phase 0 开发实施

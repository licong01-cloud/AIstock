# miniQMT 数据集统计信息增强设计方案

## 1. 设计目标

在现有的 miniQMT 数据管理页面中补充数据集统计信息功能，提供：
1. **数据集概览**：展示所有可用数据集及其状态
2. **数据范围统计**：每个数据集覆盖的日期和股票范围
3. **进度可视化**：完整的进度条和状态指示
4. **一键补齐**：针对每个数据集提供一键补齐到当前日期功能
5. **任务监控**：可观察同步任务的执行情况、更新日志

## 2. 现状分析

### 2.1 xtquant 文档分析

根据 `F:/Dev/AIstock/xtquant/doc/xtdata.md`，xtquant 提供以下数据下载能力：

**行情数据接口：**
- `download_history_data()` - 下载历史行情数据（单只股票）
- `download_history_data2()` - 批量下载历史行情数据，支持进度回调
- `download_financial_data()` - 下载财务数据
- `download_financial_data2()` - 批量下载财务数据，支持进度回调
- `download_sector_data()` - 下载板块分类信息
- `download_holiday_data()` - 下载节假日数据
- `download_etf_info()` - 下载 ETF 申赎清单信息
- `download_cb_data()` - 下载可转债基础信息

**数据获取接口：**
- `get_market_data()` - 从缓存获取行情数据
- `get_local_data()` - 从本地数据文件获取行情数据
- `get_financial_data()` - 获取财务数据
- `get_instrument_detail()` - 获取合约基础信息

**进度回调示例：**
```python
def on_progress(data):
    print(data)
    # {'finished': 1, 'total': 50, 'stockcode': '000001.SZ', 'message': ''}
```

### 2.2 现有 miniQMT 数据管理功能分析

根据 `F:/Dev/AIstock/frontend/src/app/local-data/page.tsx`，现有功能包括：

**MiniQMTTab 组件功能：**
1. **数据范围查询**：
   - 支持查询 1d、1m、5m、1h 四个周期的数据范围
   - 显示每个周期的起始日期、结束日期、条数
   - 显示最新交易日（miniQMT）

2. **一键更新功能**：
   - 支持多周期选择（1d、1m、5m、1h）
   - 支持范围选择（全市场/自选股）
   - 支持设置回补起始日期
   - 调用 `/api/v1/qmt/data/one-click-update` 接口

3. **财务数据下载**：
   - 调用 `/api/v1/qmt/data/download-financial` 接口
   - 支持下载 Capital、Balance、Income、CashFlow 四张表

4. **任务进度监控**：
   - 显示任务进度百分比
   - 显示任务状态（success/failed/running）
   - 显示任务消息和错误信息
   - 显示当前进度（finished/total）和最近处理的股票

**现有接口：**
- `GET /api/v1/qmt/data/range` - 查询数据范围
- `GET /api/v1/qmt/data/latest-day` - 查询最新交易日
- `POST /api/v1/qmt/data/one-click-update` - 一键更新
- `POST /api/v1/qmt/data/download-financial` - 下载财务数据
- `GET /api/v1/qmt/data/task/{tid}/progress` - 查询任务进度

### 2.3 存在的问题

1. **缺乏数据集概览**：无法快速了解所有数据集的完整状态
2. **数据范围信息不完整**：只显示参考股票代码的数据范围，无法了解整体覆盖情况
3. **进度信息不够详细**：无法看到详细的更新日志和每个股票的处理状态
4. **缺乏一键补齐功能**：无法针对特定数据集快速补齐到最新日期
5. **任务监控不够全面**：无法查看所有同步任务的详细执行情况

## 3. 设计方案

### 3.1 数据集定义

基于 xtquant 文档和现有功能，定义以下数据集：

| 数据集 ID | 数据集名称 | 周期 | 说明 |
|---------|---------|------|------|
| kline_1d | 日线 K 线 | 1d | 日线行情数据 |
| kline_1m | 1分钟 K 线 | 1m | 1分钟行情数据 |
| kline_5m | 5分钟 K 线 | 5m | 5分钟行情数据 |
| kline_1h | 1小时 K 线 | 1h | 1小时行情数据 |
| tick | 分笔数据 | tick | 分笔成交数据 |
| financial | 财务数据 | - | 财务报表数据 |
| instrument | 合约基础信息 | - | 股票基础信息 |
| sector | 板块分类信息 | - | 板块信息 |
| holiday | 节假日数据 | - | 节假日历 |
| dividend | 除权数据 | - | 除权因子 |

### 3.2 后端 API 设计

#### 3.2.1 数据集统计信息接口

**GET /api/v1/qmt/data/datasets**

返回所有数据集的统计信息：

```json
{
  "datasets": [
    {
      "id": "kline_1d",
      "name": "日线 K 线",
      "period": "1d",
      "status": "complete", // complete, partial, empty, unknown
      "date_range": {
        "start": "2010-01-04",
        "end": "2025-01-24",
        "latest_available": "2025-01-24",
        "latest_trading_day": "2025-01-24",
        "gap_days": 0
      },
      "stock_range": {
        "total_count": 5000,
        "covered_count": 5000,
        "coverage_rate": 1.0
      },
      "data_size": {
        "record_count": 12500000,
        "size_mb": 150.5
      },
      "last_updated": "2025-01-24T15:30:00+08:00",
      "update_status": "up_to_date" // up_to_date, outdated, unknown
    }
  ]
}
```

#### 3.2.2 数据集详细统计接口

**GET /api/v1/qmt/data/datasets/{dataset_id}**

返回指定数据集的详细统计信息：

```json
{
  "dataset": {
    "id": "kline_1d",
    "name": "日线 K 线",
    "period": "1d",
    "status": "complete",
    "date_range": {
      "start": "2010-01-04",
      "end": "2025-01-24",
      "latest_available": "2025-01-24",
      "latest_trading_day": "2025-01-24",
      "gap_days": [
        "2023-01-27",
        "2023-01-30"
      ],
      "total_trading_days": 3750,
      "covered_trading_days": 3748
    },
    "stock_range": {
      "total_count": 5000,
      "covered_count": 5000,
      "coverage_rate": 1.0,
      "sample_stocks": [
        {
          "code": "000001.SZ",
          "name": "平安银行",
          "start": "2010-01-04",
          "end": "2025-01-24",
          "count": 3750
        }
      ]
    },
    "data_size": {
      "record_count": 12500000,
      "size_mb": 150.5
    },
    "last_updated": "2025-01-24T15:30:00+08:00",
    "update_status": "up_to_date",
    "quality_metrics": {
      "completeness": 0.998,
      "consistency": 0.995,
      "timeliness": 1.0
    }
  }
}
```

#### 3.2.3 一键补齐接口

**POST /api/v1/qmt/data/datasets/{dataset_id}/catch-up**

一键补齐指定数据集到当前日期：

```json
{
  "task_id": "task_123456",
  "status": "queued",
  "message": "任务已提交，正在排队执行"
}
```

#### 3.2.4 任务进度查询接口

**GET /api/v1/qmt/data/tasks/{task_id}/progress**

返回任务详细进度：

```json
{
  "task_id": "task_123456",
  "status": "running", // queued, running, success, failed, cancelled
  "progress": 65,
  "message": "正在处理 000002.SZ",
  "started_at": "2025-01-24T15:30:00+08:00",
  "finished_at": null,
  "counters": {
    "total": 5000,
    "finished": 3250,
    "running": 1,
    "pending": 1749,
    "success": 3248,
    "failed": 2,
    "skipped": 0
  },
  "logs": [
    {
      "timestamp": "2025-01-24T15:35:00+08:00",
      "level": "info",
      "message": "开始下载 000001.SZ 的日线数据",
      "stock_code": "000001.SZ"
    },
    {
      "timestamp": "2025-01-24T15:35:05+08:00",
      "level": "success",
      "message": "成功下载 000001.SZ 的 250 条数据",
      "stock_code": "000001.SZ",
      "record_count": 250
    }
  ],
  "current_stock": {
    "code": "000002.SZ",
    "name": "万科A",
    "progress": 45
  }
}
```

#### 3.2.5 任务列表接口

**GET /api/v1/qmt/data/tasks**

返回所有同步任务列表：

```json
{
  "tasks": [
    {
      "task_id": "task_123456",
      "dataset_id": "kline_1d",
      "dataset_name": "日线 K 线",
      "type": "catch_up", // catch_up, download, update
      "status": "running",
      "progress": 65,
      "created_at": "2025-01-24T15:30:00+08:00",
      "started_at": "2025-01-24T15:30:05+08:00",
      "finished_at": null,
      "counters": {
        "total": 5000,
        "finished": 3250,
        "running": 1,
        "pending": 1749,
        "success": 3248,
        "failed": 2
      },
      "meta": {
        "start_date": "2025-01-20",
        "end_date": "2025-01-24",
        "scope": "all"
      }
    }
  ]
}
```

### 3.3 前端 UI 设计

#### 3.3.1 数据集概览卡片

**位置：** MiniQMTTab 组件中新增"数据集概览"区域

**布局：**
```
┌─────────────────────────────────────────────────────────┐
│ 📊 数据集概览                                              │
├─────────────────────────────────────────────────────────┤
│ 数据集    状态    数据范围        覆盖率    更新状态    操作  │
│ 日线      ✅完整   2010-01-04    100%     最新      [补齐]  │
│           │       ~2025-01-24                              │
│ 1分钟     ⚠️部分  2023-01-01    95%      过期2天   [补齐]  │
│           │       ~2025-01-23                              │
│ 5分钟     ❌空    -             0%       未知      [补齐]  │
│ 财务数据   ✅完整   2020-01-01    100%     最新      [补齐]  │
│           │       ~2025-01-24                              │
└─────────────────────────────────────────────────────────┘
```

**状态标识：**
- ✅ 完整：数据完整，最新
- ⚠️ 部分：数据不完整或过期
- ❌ 空：无数据
- ❓ 未知：无法确定状态

**更新状态：**
- 最新：数据已更新到最新交易日
- 过期 N 天：数据过期 N 天
- 未知：无法判断

**操作按钮：**
- [补齐]：一键补齐到当前日期
- [详情]：查看详细统计信息

#### 3.3.2 数据集详情弹窗

**触发：** 点击数据集的[详情]按钮

**内容：**
```
┌─────────────────────────────────────────────────────────┐
│ 📊 日线 K 线 - 详细统计                                    │
├─────────────────────────────────────────────────────────┤
│ 基本信息                                                  │
│ • 数据集 ID: kline_1d                                    │
│ • 周期: 日线                                              │
│ • 状态: ✅ 完整                                           │
│ • 最后更新: 2025-01-24 15:30:00                           │
│                                                           │
│ 📅 日期范围                                               │
│ • 起始日期: 2010-01-04                                    │
│ • 结束日期: 2025-01-24                                    │
│ • 最新可用: 2025-01-24                                     │
│ • 最新交易日: 2025-01-24                                  │
│ • 缺失交易日: 2 天                                        │
│   - 2023-01-27                                            │
│   - 2023-01-30                                            │
│ • 覆盖交易日: 3748/3750 (99.9%)                           │
│                                                           │
│ 📈 股票范围                                               │
│ • 总股票数: 5000                                           │
│ • 已覆盖: 5000 (100%)                                     │
│                                                           │
│ 💾 数据规模                                               │
│ • 记录数: 12,500,000                                      │
│ • 存储大小: 150.5 MB                                      │
│                                                           │
│ 📊 质量指标                                               │
│ • 完整性: 99.8%                                           │
│ • 一致性: 99.5%                                           │
│ • 及时性: 100%                                            │
│                                                           │
│ [关闭] [一键补齐] [查看任务日志]                            │
└─────────────────────────────────────────────────────────┘
```

#### 3.3.3 进度展示组件

**位置：** 数据集详情弹窗或任务监控区域

**布局：**
```
┌─────────────────────────────────────────────────────────┐
│ ⏳ 正在更新数据...                                        │
├─────────────────────────────────────────────────────────┤
│ ████████████████████████████░░░░░░░░░ 65%                │
│                                                           │
│ 进度: 3,250 / 5,000 (65%)                                │
│ • 完成: 3,248                                             │
│ • 运行中: 1                                               │
│ • 排队中: 1,749                                           │
│ • 成功: 3,248                                             │
│ • 失败: 2                                                 │
│                                                           │
│ 当前处理: 000002.SZ 万科A (45%)                             │
│                                                           │
│ 📝 最新日志                                               │
│ [15:35:00] 开始下载 000001.SZ 的日线数据                   │
│ [15:35:05] ✅ 成功下载 000001.SZ 的 250 条数据               │
│ [15:35:10] 开始下载 000002.SZ 的日线数据                   │
│ [15:35:15] ⚠️ 下载 000002.SZ 失败: 网络超时                  │
│                                                           │
│ [取消任务] [查看完整日志]                                 │
└─────────────────────────────────────────────────────────┘
```

**进度条样式：**
- 正常运行：蓝色进度条
- 失败状态：红色进度条
- 成功完成：绿色进度条

**日志级别：**
- INFO：普通信息（蓝色）
- SUCCESS：成功信息（绿色）
- WARNING：警告信息（黄色）
- ERROR：错误信息（红色）

#### 3.3.4 任务监控列表

**位置：** 新增"任务监控"标签页或集成到现有"任务监视器"中

**布局：**
```
┌─────────────────────────────────────────────────────────┐
│ 📊 任务监控                                              │
├─────────────────────────────────────────────────────────┤
│ [自动刷新 ☑] [仅显示运行中 ☑] [清除已完成] [手动刷新]     │
├─────────────────────────────────────────────────────────┤
│ 任务ID    数据集    类型    状态    进度    时间        操作  │
│ task_1   日线      补齐    运行中  65%    15:30:00   [详情] │
│ task_2   1分钟     更新    成功    100%   15:20:00   [日志] │
│ task_3   财务数据  下载    失败    45%    15:10:00   [重试] │
└─────────────────────────────────────────────────────────┘
```

**操作按钮：**
- [详情]：查看任务详情和进度
- [日志]：查看完整任务日志
- [重试]：重新执行失败的任务
- [取消]：取消正在运行的任务

### 3.4 后端实现要点

#### 3.4.1 数据集统计信息收集

**实现方式：**
1. 使用 xtquant 的 `get_local_data()` 和 `get_market_data()` 接口查询本地数据
2. 使用 `get_trading_calendar()` 获取交易日历
3. 使用 `get_instrument_detail()` 获取股票基础信息
4. 扫描本地数据文件，统计每个数据集的记录数和存储大小

**数据范围计算：**
- 起始日期：查询最早的数据日期
- 结束日期：查询最新的数据日期
- 缺失交易日：对比交易日历和数据日期
- 覆盖率：已覆盖交易日 / 总交易日

**股票范围计算：**
- 总股票数：从 `get_instrument_detail()` 获取
- 已覆盖股票数：扫描本地数据文件统计
- 样本股票：随机抽取若干股票展示详情

#### 3.4.2 一键补齐功能实现

**实现方式：**
1. 使用 xtquant 的 `download_history_data2()` 批量下载
2. 支持进度回调，实时更新进度
3. 支持任务队列管理，避免重复下载
4. 支持断点续传，记录已下载的股票

**进度回调处理：**
```python
def on_progress(data):
    # data: {'finished': 1, 'total': 50, 'stockcode': '000001.SZ', 'message': ''}
    task_id = data.get('task_id')
    finished = data.get('finished', 0)
    total = data.get('total', 0)
    stockcode = data.get('stockcode')
    message = data.get('message', '')
    
    # 更新任务进度
    update_task_progress(task_id, {
        'total': total,
        'finished': finished,
        'current_stock': stockcode,
        'message': message
    })
```

#### 3.4.3 任务日志记录

**实现方式：**
1. 使用日志文件记录任务执行过程
2. 支持日志级别（INFO、SUCCESS、WARNING、ERROR）
3. 支持日志查询和分页
4. 支持日志实时推送（WebSocket 或轮询）

**日志格式：**
```json
{
  "timestamp": "2025-01-24T15:35:00+08:00",
  "level": "info",
  "message": "开始下载 000001.SZ 的日线数据",
  "stock_code": "000001.SZ",
  "record_count": 250
}
```

### 3.5 前端实现要点

#### 3.5.1 数据集概览组件

**状态管理：**
```typescript
interface DatasetSummary {
  id: string;
  name: string;
  period: string;
  status: 'complete' | 'partial' | 'empty' | 'unknown';
  dateRange: {
    start: string | null;
    end: string | null;
    latestAvailable: string | null;
    latestTradingDay: string | null;
    gapDays: number;
  };
  stockRange: {
    totalCount: number;
    coveredCount: number;
    coverageRate: number;
  };
  lastUpdated: string | null;
  updateStatus: 'up_to_date' | 'outdated' | 'unknown';
}

const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
const [loading, setLoading] = useState(false);
```

**数据加载：**
```typescript
const loadDatasets = async () => {
  setLoading(true);
  try {
    const data = await backendRequest('GET', '/api/v1/qmt/data/datasets');
    setDatasets(data.datasets || []);
  } catch (e) {
    console.error('加载数据集失败:', e);
  } finally {
    setLoading(false);
  }
};
```

**一键补齐：**
```typescript
const handleCatchUp = async (datasetId: string) => {
  try {
    const resp = await backendRequest('POST', `/api/v1/qmt/data/datasets/${datasetId}/catch-up`);
    if (resp.task_id) {
      setActiveTaskId(resp.task_id);
      // 打开进度弹窗
      setShowProgressModal(true);
    }
  } catch (e) {
    console.error('一键补齐失败:', e);
  }
};
```

#### 3.5.2 进度监控组件

**进度轮询：**
```typescript
useEffect(() => {
  if (!activeTaskId) return;
  
  const timer = setInterval(async () => {
    const progress = await backendRequest('GET', `/api/v1/qmt/data/tasks/${activeTaskId}/progress`);
    setTaskProgress(progress);
    
    if (progress.status === 'success' || progress.status === 'failed') {
      clearInterval(timer);
      setActiveTaskId(null);
    }
  }, 2000);
  
  return () => clearInterval(timer);
}, [activeTaskId]);
```

**进度条渲染：**
```typescript
const ProgressBar = ({ progress, status }: { progress: number; status: string }) => {
  const color = status === 'failed' ? '#ef4444' : status === 'success' ? '#22c55e' : '#0ea5e9';
  
  return (
    <div style={{ width: '100%', background: '#e5e7eb', height: 8, borderRadius: 4, overflow: 'hidden' }}>
      <div 
        style={{ 
          width: `${progress}%`, 
          height: '100%', 
          background: color,
          transition: 'width 0.3s ease' 
        }} 
      />
    </div>
  );
};
```

**日志渲染：**
```typescript
const LogItem = ({ log }: { log: any }) => {
  const levelColors = {
    info: '#3b82f6',
    success: '#22c55e',
    warning: '#f59e0b',
    error: '#ef4444'
  };
  
  return (
    <div style={{ fontSize: 12, color: levelColors[log.level] || '#6b7280', marginBottom: 2 }}>
      [{log.timestamp}] [{log.level.toUpperCase()}] {log.message}
      {log.stock_code && ` (${log.stock_code})`}
      {log.record_count && ` - ${log.record_count} 条`}
    </div>
  );
};
```

## 4. 实施计划

### 4.1 后端开发任务

#### 阶段一：数据集统计接口
1. 实现 `GET /api/v1/qmt/data/datasets` 接口
2. 实现 `GET /api/v1/qmt/data/datasets/{dataset_id}` 接口
3. 实现数据集统计信息收集逻辑
4. 实现数据范围和股票范围计算

#### 阶段二：一键补齐功能
1. 实现 `POST /api/v1/qmt/data/datasets/{dataset_id}/catch-up` 接口
2. 实现任务队列管理
3. 实现进度回调处理
4. 实现断点续传支持

#### 阶段三：任务监控功能
1. 实现 `GET /api/v1/qmt/data/tasks` 接口
2. 实现 `GET /api/v1/qmt/data/tasks/{task_id}/progress` 接口
3. 实现任务日志记录和查询
4. 实现任务状态管理

### 4.2 前端开发任务

#### 阶段一：数据集概览
1. 创建数据集概览组件
2. 实现数据集列表展示
3. 实现状态标识和更新状态显示
4. 实现操作按钮（补齐、详情）

#### 阶段二：数据集详情
1. 创建数据集详情弹窗
2. 实现详细统计信息展示
3. 实现质量指标展示
4. 实现操作按钮（一键补齐、查看日志）

#### 阶段三：进度监控
1. 创建进度展示组件
2. 实现进度条和状态指示
3. 实现日志列表展示
4. 实现实时进度轮询

#### 阶段四：任务监控
1. 创建任务监控列表
2. 实现任务列表展示
3. 实现任务操作（详情、日志、重试、取消）
4. 实现自动刷新功能

### 4.3 测试与优化

#### 功能测试
1. 测试数据集统计信息准确性
2. 测试一键补齐功能
3. 测试进度监控功能
4. 测试任务监控功能

#### 性能优化
1. 优化数据集统计信息查询性能
2. 优化进度轮询频率
3. 优化日志查询性能
4. 优化大数据集的加载速度

#### 用户体验优化
1. 添加加载动画
2. 添加错误提示
3. 添加操作确认
4. 添加快捷键支持

## 5. 技术约束

### 5.1 不修改的内容
- **不修改 F:/Dev/AIstock/xtquant/** 文件夹中的任何内容**
- 只通过 xtquant 提供的 API 接口进行数据操作
- 不直接操作 miniQMT 的数据文件

### 5.2 技术栈
- **后端**：FastAPI（Python）
- **前端**：Next.js + React + TailwindCSS
- **数据源**：xtquant（miniQMT）
- **通信**：REST API + WebSocket（可选）

### 5.3 性能要求
- 数据集统计信息查询响应时间 < 2 秒
- 进度轮询间隔 2 秒
- 日志查询支持分页，每页 50 条
- 支持大数据集（> 10000 只股票）的统计

## 6. 风险与注意事项

### 6.1 数据一致性
- 统计信息应与实际数据保持一致
- 避免统计信息过期，需要定期刷新
- 处理数据更新过程中的并发问题

### 6.2 性能问题
- 大数据集的统计信息查询可能较慢
- 避免频繁查询导致性能下降
- 考虑使用缓存机制

### 6.3 错误处理
- 处理 miniQMT 连接失败的情况
- 处理数据下载失败的情况
- 处理任务执行中断的情况

### 6.4 用户体验
- 避免长时间阻塞 UI
- 提供清晰的操作反馈
- 支持任务取消和重试

## 7. 总结

本设计方案通过以下方式增强 miniQMT 数据管理功能：

1. **数据集概览**：提供所有数据集的快速概览，包括状态、数据范围、覆盖率等
2. **一键补齐**：针对每个数据集提供一键补齐到当前日期功能
3. **进度可视化**：提供完整的进度条和状态指示，实时显示更新进度
4. **任务监控**：可观察所有同步任务的执行情况，包括进度、日志、错误信息
5. **详细统计**：提供数据集的详细统计信息，包括数据范围、股票范围、质量指标等

该方案完全基于 xtquant 提供的 API 接口实现，不修改 F:/Dev/AIstock/xtquant/** 文件夹中的任何内容，符合用户要求。

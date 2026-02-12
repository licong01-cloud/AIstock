# 自选股票池功能增强设计方案

## 一、需求概述

### 1.1 功能需求

#### 1.1.1 全选按钮
- 在自选股票池列表中添加全选按钮
- 点击按钮一次性选择当前页面显示的全部股票

#### 1.1.2 来源TASK筛选
- 在搜索区域添加下拉菜单
- 可选择来源TASK进行筛选
- 实现针对特定来源TASK的股票筛选功能

#### 1.1.3 列排序功能（基础字段）
- 所有列表列支持点击列名排序
- 第一次点击：降序排列，列名后显示下箭头（↓）
- 第二次点击：升序排列，列名后显示上箭头（↑）
- 第三次点击：恢复默认排序，移除箭头
- 排序范围：当前列表所有数据（跨页）

#### 1.1.4 价格刷新机制
- **页面初始加载**：不刷新股票实时价格，只显示基础字段（股票代码、股票名称、TASK来源等）
- **价格刷新按钮**：用户点击后，获取所有符合条件股票的实时价格
- **未刷新状态**：实时价格、加入以来涨幅等字段显示为空（显示为"-"）
- **刷新后状态**：显示所有字段数据，启用所有字段的排序功能

#### 1.1.5 字段排序规则
- **支持排序的字段**：
  - 股票代码
  - 股票名称
  - 加入时间
  - 加入以来涨幅（刷新后）
  
- **不支持排序的字段**：
  - 最新价
  - 开盘价
  - 昨收价
  - 最高价
  - 最低价
  - 投资评级

#### 1.1.6 排序交互规则
- **未刷新价格时**：
  - 只对支持排序的基础字段提供排序功能
  - 点击实时价格等字段，不触发排序（显示提示："请先刷新价格"）
  
- **刷新价格后**：
  - 所有支持排序的字段提供排序功能
  - 实时价格等字段仍不提供排序功能

### 1.2 技术约束

- 前端：Next.js + TailwindCSS + shadcn/ui
- 后端：FastAPI
- **排序实现**：所有排序功能在前端内存中实现，不使用数据库排序
- **数据获取**：前端页面加载时获取全量数据（不分页），缓存到内存
- **分页显示**：前端分页只用于显示，不影响数据获取

---

## 二、前端设计方案

### 2.1 全选按钮

#### 位置
- 放置在表格操作栏（checkbox列的表头）
- 与"批量操作"按钮组相邻

#### 交互设计
- **默认状态**：显示"全选"文字 + 未选中图标
- **选中状态**：显示"取消全选"文字 + 选中图标
- **点击行为**：
  - 切换当前页面所有行的选中状态
  - 更新批量操作按钮的可用状态

#### UI组件
```typescript
// 使用 shadcn/ui 的 Checkbox 组件
<Checkbox 
  checked={allSelected}
  onCheckedChange={handleSelectAll}
  aria-label="全选当前页"
/>
```

#### 状态管理
```typescript
const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set());
const [allSelected, setAllSelected] = useState(false);

const handleSelectAll = (checked: boolean) => {
  if (checked) {
    // 选中当前页所有行
    const currentPageIds = paginatedData.map(row => row.id);
    setSelectedRows(new Set(currentPageIds));
    setAllSelected(true);
  } else {
    // 取消选中
    setSelectedRows(new Set());
    setAllSelected(false);
  }
};
```

### 2.2 来源TASK筛选

#### 位置
- 放置在搜索栏的右侧
- 与搜索框并列显示

#### 交互设计
- **下拉菜单内容**：
  - "全部来源"（默认选项）
  - 动态加载所有可用的TASK列表
  - 每个TASK显示：TASK名称 + 创建时间

- **选择行为**：
  - 选择TASK后，自动筛选出该TASK来源的股票
  - 更新列表显示
  - 重置分页到第一页
  - 保持其他筛选条件（搜索关键词等）

#### UI组件
```typescript
// 使用 shadcn/ui 的 Select 组件
<Select 
  value={selectedTaskId} 
  onValueChange={handleTaskChange}
>
  <SelectTrigger className="w-[200px]">
    <SelectValue placeholder="选择来源TASK" />
  </SelectTrigger>
  <SelectContent>
    <SelectItem value="all">全部来源</SelectItem>
    {tasks.map(task => (
      <SelectItem key={task.id} value={task.id}>
        {task.name} ({formatDate(task.createdAt)})
      </SelectItem>
    ))}
  </SelectContent>
</Select>
```

#### 数据获取
```typescript
// 从后端API获取TASK列表
const { data: tasks } = useQuery({
  queryKey: ['tasks'],
  queryFn: () => fetch('/api/tasks').then(res => res.json())
});
```

### 2.3 价格刷新机制

#### 2.3.1 刷新按钮设计

#### 位置
- 放置在表格右上角
- 与搜索框、TASK筛选器并列

#### 交互设计
- **默认状态**：显示"刷新价格"按钮，蓝色背景
- **加载状态**：显示加载动画，按钮禁用
- **刷新后状态**：显示"已刷新"文字，灰色背景，5秒后恢复默认状态

- **点击行为**：
  - 调用后端API获取所有符合条件股票的实时价格
  - 更新列表显示
  - 启用所有字段的排序功能
  - 显示刷新成功提示

#### UI组件
```typescript
<Button 
  onClick={handleRefreshPrices}
  disabled={isRefreshing}
  variant={isRefreshed ? "outline" : "default"}
>
  {isRefreshing ? (
    <>
      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
      刷新中...
    </>
  ) : isRefreshed ? (
    "已刷新"
  ) : (
    <>
      <RefreshCw className="mr-2 h-4 w-4" />
      刷新价格
    </>
  )}
</Button>
```

#### 状态管理
```typescript
const [isRefreshing, setIsRefreshing] = useState(false);
const [isRefreshed, setIsRefreshed] = useState(false);
const [priceData, setPriceData] = useState<Record<string, PriceInfo>>({});

const handleRefreshPrices = async () => {
  setIsRefreshing(true);
  try {
    // 获取所有符合条件的股票代码
    const stockCodes = filteredData.map(row => row.code);
    
    // 调用后端API获取实时价格
    const response = await fetch('/api/stocks/prices', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ codes: stockCodes })
    });
    
    const data = await response.json();
    setPriceData(data.prices);
    setIsRefreshed(true);
    
    // 5秒后恢复按钮状态
    setTimeout(() => setIsRefreshed(false), 5000);
    
    toast.success('价格刷新成功');
  } catch (error) {
    toast.error('价格刷新失败');
  } finally {
    setIsRefreshing(false);
  }
};
```

#### 2.3.2 字段显示规则

#### 未刷新状态
```typescript
const renderCellValue = (column: string, value: any) => {
  // 价格相关字段
  if (['latestPrice', 'openPrice', 'closePrice', 'highPrice', 'lowPrice'].includes(column)) {
    return isRefreshed ? value : '-';
  }
  
  // 涨跌幅字段
  if (column === 'changeSinceAdded') {
    return isRefreshed ? value : '-';
  }
  
  // 投资评级字段
  if (column === 'rating') {
    return isRefreshed ? value : '-';
  }
  
  // 其他字段正常显示
  return value;
};
```

#### 刷新后状态
```typescript
// 所有字段正常显示
const renderCellValue = (column: string, value: any) => {
  return value;
};
```

### 2.4 列排序功能

#### 2.4.1 排序字段定义

```typescript
// 支持排序的字段
const SORTABLE_FIELDS = [
  'code',           // 股票代码
  'name',           // 股票名称
  'addedAt',        // 加入时间
  'changeSinceAdded' // 加入以来涨幅（刷新后）
] as const;

// 不支持排序的字段
const NON_SORTABLE_FIELDS = [
  'latestPrice',    // 最新价
  'openPrice',      // 开盘价
  'closePrice',     // 昨收价
  'highPrice',      // 最高价
  'lowPrice',       // 最低价
  'rating'          // 投资评级
] as const;
```

#### 2.4.2 排序交互设计

#### 点击列名行为
```typescript
const handleSort = (column: string) => {
  // 检查字段是否支持排序
  if (NON_SORTABLE_FIELDS.includes(column as any)) {
    // 如果未刷新价格，显示提示
    if (!isRefreshed) {
      toast.warning('请先刷新价格');
      return;
    }
    // 如果已刷新价格，但仍不支持排序
    toast.warning('该字段不支持排序');
    return;
  }
  
  // 检查是否已刷新价格（针对涨幅字段）
  if (column === 'changeSinceAdded' && !isRefreshed) {
    toast.warning('请先刷新价格');
    return;
  }
  
  // 执行排序
  setSortState(prev => {
    // 如果点击同一列，切换排序方向
    if (prev.column === column) {
      const directions = ['desc', 'asc', null] as const;
      const currentIndex = directions.indexOf(prev.direction as any);
      const nextDirection = directions[(currentIndex + 1) % 3];
      return {
        column: nextDirection ? column : null,
        direction: nextDirection
      };
    }
    // 如果点击不同列，设置为降序
    return {
      column,
      direction: 'desc'
    };
  });
};
```

#### 排序状态管理
```typescript
interface SortState {
  column: string | null;
  direction: 'asc' | 'desc' | null;
}

const [sortState, setSortState] = useState<SortState>({
  column: null,
  direction: null
});
```

#### 排序箭头显示
```typescript
const SortIcon = ({ column, sortState }: { column: string; sortState: SortState }) => {
  if (sortState.column !== column) return null;
  
  return (
    <span className="ml-1">
      {sortState.direction === 'asc' ? (
        <ArrowUp className="h-4 w-4" />
      ) : (
        <ArrowDown className="h-4 w-4" />
      )}
    </span>
  );
};

// 使用
<TableHead onClick={() => handleSort('code')}>
  <div className="flex items-center">
    股票代码
    <SortIcon column="code" sortState={sortState} />
  </div>
</TableHead>
```

#### 2.4.3 数据排序实现

#### 方案选择：前端内存排序 vs 后端数据库排序

**方案A：前端内存排序**
```typescript
const sortedData = useMemo(() => {
  if (!sortState.column || !sortState.direction) {
    return filteredData;
  }

  return [...filteredData].sort((a, b) => {
    const aValue = a[sortState.column];
    const bValue = b[sortState.column];
    
    if (sortState.direction === 'asc') {
      return aValue > bValue ? 1 : -1;
    } else {
      return aValue < bValue ? 1 : -1;
    }
  });
}, [filteredData, sortState]);
```

**方案B：后端数据库排序**
```typescript
// 前端调用
const { data } = useQuery({
  queryKey: ['watchlist', page, pageSize, sortState],
  queryFn: () => fetch(`/api/watchlist?page=${page}&pageSize=${pageSize}&sortColumn=${sortState.column}&sortDirection=${sortState.direction}`)
});

// 后端实现
@router.get("/watchlist")
async def get_watchlist(
    page: int = 1,
    pageSize: int = 20,
    sortColumn: str = None,
    sortDirection: str = None
):
    """获取自选股票池数据"""
    offset = (page - 1) * pageSize
    
    # 构建排序子句
    order_clause = "ORDER BY created_at DESC"
    if sortColumn and sortDirection:
        order_clause = f"ORDER BY {sortColumn} {sortDirection}"
    
    query = f"""
        SELECT * FROM watchlist 
        WHERE user_id = :user_id 
        {order_clause}
        LIMIT :limit OFFSET :offset
    """
    
    data = await db.query(query, {
        "user_id": current_user.id,
        "limit": pageSize,
        "offset": offset
    })
    
    return {
        "data": data,
        "total": total,
        "page": page,
        "pageSize": pageSize
    }
```

#### 2.4.4 方案对比分析

| 维度 | 前端内存排序 | 后端数据库排序 |
|------|-------------|---------------|
| **性能** | 1000条数据排序 < 100ms | 数据库排序更快，但需要网络请求 |
| **数据一致性** | 需要定期同步数据 | 始终保持最新数据 |
| **内存占用** | 中等（缓存所有数据） | 低（只缓存当前页） |
| **开发复杂度** | 低（纯前端实现） | 中（需要后端支持） |
| **用户体验** | 响应快，无网络延迟 | 需要等待网络请求 |
| **数据量限制** | 适合 < 5000条数据 | 无限制 |

**推荐方案：前端内存排序**

**理由**：
1. 自选股票池数据量通常不大（< 1000条）
2. 前端排序响应更快，用户体验更好
3. 减少后端API调用，降低服务器压力
4. 实现简单，维护成本低

**实现要点**：
1. 页面加载时，获取所有符合条件的股票数据（不分页）
2. 将数据缓存到前端内存
3. 排序、筛选等操作在内存中进行
4. 分页只用于显示，不影响数据获取

#### 2.4.5 内存缓存方案详细设计

#### 数据缓存策略
```typescript
interface WatchlistData {
  id: string;
  code: string;
  name: string;
  taskId: string;
  addedAt: string;
  // 价格字段（刷新后才有）
  latestPrice?: number;
  openPrice?: number;
  closePrice?: number;
  highPrice?: number;
  lowPrice?: number;
  rating?: string;
  changeSinceAdded?: number;
}

// 缓存所有数据
const [cachedData, setCachedData] = useState<WatchlistData[]>([]);

// 页面加载时获取所有数据
useEffect(() => {
  const fetchAllData = async () => {
    const response = await fetch('/api/watchlist/all');
    const data = await response.json();
    setCachedData(data);
  };
  
  fetchAllData();
}, []);
```

#### 筛选逻辑
```typescript
const filteredData = useMemo(() => {
  let result = cachedData;
  
  // TASK筛选
  if (selectedTaskId !== 'all') {
    result = result.filter(item => item.taskId === selectedTaskId);
  }
  
  // 搜索筛选
  if (searchKeyword) {
    const keyword = searchKeyword.toLowerCase();
    result = result.filter(item => 
      item.code.toLowerCase().includes(keyword) ||
      item.name.toLowerCase().includes(keyword)
    );
  }
  
  return result;
}, [cachedData, selectedTaskId, searchKeyword]);
```

#### 排序逻辑
```typescript
const sortedData = useMemo(() => {
  if (!sortState.column || !sortState.direction) {
    return filteredData;
  }

  return [...filteredData].sort((a, b) => {
    const aValue = a[sortState.column];
    const bValue = b[sortState.column];
    
    // 处理空值
    if (aValue == null && bValue == null) return 0;
    if (aValue == null) return 1;
    if (bValue == null) return -1;
    
    if (sortState.direction === 'asc') {
      return aValue > bValue ? 1 : -1;
    } else {
      return aValue < bValue ? 1 : -1;
    }
  });
}, [filteredData, sortState]);
```

#### 分页逻辑
```typescript
const paginatedData = useMemo(() => {
  const start = (currentPage - 1) * pageSize;
  const end = start + pageSize;
  return sortedData.slice(start, end);
}, [sortedData, currentPage, pageSize]);
```

#### 性能优化
```typescript
// 使用 useMemo 缓存计算结果
const filteredData = useMemo(() => { /* ... */ }, [dependencies]);
const sortedData = useMemo(() => { /* ... */ }, [dependencies]);
const paginatedData = useMemo(() => { /* ... */ }, [dependencies]);

// 使用虚拟滚动（数据量 > 1000时）
import { FixedSizeList } from 'react-window';

<FixedSizeList
  height={600}
  itemCount={paginatedData.length}
  itemSize={50}
>
  {({ index, style }) => (
    <div style={style}>
      {renderRow(paginatedData[index])}
    </div>
  )}
</FixedSizeList>
```

---

## 三、后端设计方案

### 3.1 API接口设计

#### 3.1.1 获取TASK列表
```
GET /api/tasks
Response: {
  "tasks": [
    {
      "id": "task_001",
      "name": "因子生成任务-20250124",
      "createdAt": "2025-01-24T10:00:00Z"
    },
    ...
  ]
}
```

#### 3.1.2 获取自选股票池数据（全量，不分页）
```
GET /api/watchlist/all
Response: {
  "data": [
    {
      "id": "watchlist_001",
      "userId": "user_001",
      "code": "000001",
      "name": "平安银行",
      "taskId": "task_001",
      "addedAt": "2025-01-24T10:00:00Z"
    },
    ...
  ],
  "total": 100
}
```

#### 3.1.3 获取股票实时价格
```
POST /api/stocks/prices
Request: {
  "codes": ["000001", "000002", ...]
}
Response: {
  "prices": {
    "000001": {
      "latestPrice": 12.34,
      "openPrice": 12.20,
      "closePrice": 12.18,
      "highPrice": 12.50,
      "lowPrice": 12.10,
      "rating": "买入"
    },
    ...
  }
}
```

### 3.2 数据库设计

#### 3.2.1 watchlist表
```sql
CREATE TABLE watchlist (
  id VARCHAR(50) PRIMARY KEY,
  user_id VARCHAR(50) NOT NULL,
  stock_code VARCHAR(10) NOT NULL,
  stock_name VARCHAR(50) NOT NULL,
  task_id VARCHAR(50),
  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_task_id (task_id),
  INDEX idx_stock_code (stock_code)
);
```

#### 3.2.2 tasks表
```sql
CREATE TABLE tasks (
  id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_created_at (created_at)
);
```

### 3.3 后端实现要点

#### 3.3.1 TASK列表API
```python
@router.get("/tasks")
async def get_tasks():
    """获取所有TASK列表"""
    tasks = await db.query(
        "SELECT id, name, created_at FROM tasks ORDER BY created_at DESC"
    )
    return {"tasks": tasks}
```

#### 3.3.2 自选股票池API（全量数据）
```python
@router.get("/watchlist/all")
async def get_watchlist_all():
    """获取所有自选股票池数据"""
    data = await db.query("""
        SELECT 
            id,
            user_id,
            stock_code as code,
            stock_name as name,
            task_id as taskId,
            added_at as addedAt
        FROM watchlist 
        WHERE user_id = :user_id 
        ORDER BY added_at DESC
    """, {"user_id": current_user.id})
    
    return {"data": data, "total": len(data)}
```

#### 3.3.3 股票实时价格API
```python
@router.post("/stocks/prices")
async def get_stock_prices(request: StockPriceRequest):
    """获取股票实时价格"""
    codes = request.codes
    
    # 调用行情API获取实时价格
    prices = {}
    for code in codes:
        try:
            # 调用行情API
            price_data = await fetch_stock_price(code)
            prices[code] = {
                "latestPrice": price_data["price"],
                "openPrice": price_data["open"],
                "closePrice": price_data["pre_close"],
                "highPrice": price_data["high"],
                "lowPrice": price_data["low"],
                "rating": price_data.get("rating", "未评级")
            }
        except Exception as e:
            logger.error(f"获取股票{code}价格失败: {e}")
            prices[code] = None
    
    return {"prices": prices}
```

---

## 四、执行计划

### 阶段一：后端开发（1-2天）

#### 任务1.1：数据库表结构调整
- [ ] 在 `watchlist` 表中添加 `task_id` 字段
- [ ] 创建 `tasks` 表
- [ ] 添加索引优化查询性能
- [ ] 编写数据库迁移脚本

#### 任务1.2：后端API开发
- [ ] 实现 `GET /api/tasks` 接口
- [ ] 实现 `GET /api/watchlist/all` 接口（全量数据）
- [ ] 实现 `POST /api/stocks/prices` 接口
- [ ] 添加参数验证和错误处理
- [ ] 编写单元测试

#### 任务1.3：数据迁移
- [ ] 为现有数据添加 `task_id` 字段（可选）
- [ ] 验证数据完整性

### 阶段二：前端开发（2-3天）

#### 任务2.1：全选按钮功能
- [ ] 在表格表头添加全选Checkbox
- [ ] 实现全选/取消全选逻辑
- [ ] 更新选中状态管理
- [ ] 添加批量操作按钮状态联动

#### 任务2.2：来源TASK筛选功能
- [ ] 添加TASK下拉菜单组件
- [ ] 实现TASK列表获取
- [ ] 实现TASK选择后的筛选逻辑
- [ ] 添加加载状态和错误处理

#### 任务2.3：价格刷新功能
- [ ] 添加刷新价格按钮
- [ ] 实现价格刷新逻辑
- [ ] 实现字段显示规则（未刷新显示空）
- [ ] 添加加载状态和成功提示

#### 任务2.4：列排序功能
- [ ] 实现排序状态管理
- [ ] 添加列名点击事件处理
- [ ] 实现排序箭头显示逻辑
- [ ] 实现字段排序规则（支持/不支持）
- [ ] 添加排序图标组件

#### 任务2.5：内存缓存方案
- [ ] 实现数据缓存逻辑
- [ ] 实现前端排序逻辑
- [ ] 实现筛选逻辑
- [ ] 实现分页逻辑
- [ ] 性能优化（useMemo、虚拟滚动）

#### 任务2.6：UI优化
- [ ] 优化交互体验（hover效果、过渡动画）
- [ ] 添加空状态提示
- [ ] 优化移动端适配
- [ ] 添加加载骨架屏

### 阶段三：测试与优化（1-2天）

#### 任务3.1：功能测试
- [ ] 测试全选按钮功能
- [ ] 测试TASK筛选功能
- [ ] 测试价格刷新功能
- [ ] 测试列排序功能
- [ ] 测试组合功能（筛选+排序）

#### 任务3.2：性能测试
- [ ] 测试大数据量排序性能（1000条、5000条）
- [ ] 测试TASK列表加载性能
- [ ] 测试价格刷新性能
- [ ] 优化查询和渲染性能

#### 任务3.3：用户体验优化
- [ ] 添加操作反馈提示
- [ ] 优化错误提示信息
- [ ] 添加键盘快捷键支持（可选）

### 阶段四：文档与部署（0.5天）

#### 任务4.1：文档更新
- [ ] 更新用户使用文档
- [ ] 更新API文档
- [ ] 添加功能说明截图

#### 任务4.2：部署准备
- [ ] 准备部署脚本
- [ ] 编写数据库迁移说明
- [ ] 准备回滚方案

---

## 五、技术要点

### 5.1 前端技术要点

#### 5.1.1 状态管理
- 使用 React Hooks 管理选中状态、排序状态、刷新状态
- 使用 useMemo 优化排序计算性能
- 使用 useCallback 优化事件处理函数

#### 5.1.2 性能优化
- 使用虚拟滚动处理大数据量
- 使用 debounce 优化搜索输入
- 使用缓存减少重复计算

#### 5.1.3 用户体验
- 添加加载状态和错误提示
- 优化交互反馈
- 添加空状态和骨架屏

### 5.2 后端技术要点

#### 5.2.1 数据库优化
- 添加索引提升查询性能
- 使用连接池优化数据库连接
- 使用缓存减少数据库查询

#### 5.2.2 API设计
- RESTful API设计
- 参数验证和错误处理
- 异步处理提升性能

#### 5.2.3 数据一致性
- 事务处理确保数据一致性
- 添加数据验证
- 使用乐观锁处理并发

### 5.3 内存缓存方案技术要点

#### 5.3.1 数据缓存策略
- 页面加载时获取全量数据
- 使用 useState + useMemo 缓存数据
- 定期刷新数据（可选）

#### 5.3.2 内存占用控制
- 限制缓存数据量（< 5000条）
- 使用 WeakMap 自动释放内存
- 数据量过大时使用虚拟滚动

#### 5.3.3 性能优化
- 使用 useMemo 缓存计算结果
- 使用 useCallback 优化函数引用
- 使用虚拟滚动减少DOM节点

---

## 六、风险评估

### 6.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 大数据量排序性能问题 | 高 | 使用前端排序、虚拟滚动、限制数据量 |
| 内存占用过高 | 中 | 限制缓存数据量、使用虚拟滚动 |
| 价格刷新失败 | 中 | 添加重试机制、错误提示 |
| 浏览器兼容性问题 | 低 | 使用现代浏览器、polyfill |

### 6.2 业务风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 用户操作复杂度增加 | 中 | 提供清晰的操作提示、优化UI设计 |
| 数据迁移失败 | 高 | 备份数据、编写回滚脚本 |
| 价格数据不准确 | 高 | 使用可靠的数据源、添加数据验证 |

### 6.3 内存缓存方案风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 内存占用过高 | 高 | 限制缓存数据量、使用虚拟滚动 |
| 数据不一致 | 中 | 定期刷新数据、添加手动刷新按钮 |
| 性能下降 | 中 | 使用 useMemo、虚拟滚动优化 |

---

## 七、验收标准

### 7.1 功能验收

- [ ] 全选按钮能够正确选择/取消选择当前页所有股票
- [ ] TASK下拉菜单能够正确显示所有TASK并筛选股票
- [ ] 列排序功能能够正确排序所有数据（跨页）
- [ ] 排序箭头能够正确显示当前排序状态
- [ ] 价格刷新按钮能够正确获取实时价格
- [ ] 未刷新时价格字段显示为空
- [ ] 刷新后所有字段正常显示
- [ ] 不支持排序的字段点击不触发排序

### 7.2 性能验收

- [ ] 1000条数据排序响应时间 < 100ms
- [ ] 5000条数据排序响应时间 < 500ms
- [ ] TASK列表加载时间 < 500ms
- [ ] 价格刷新时间 < 3秒（100只股票）
- [ ] 页面渲染流畅，无明显卡顿

### 7.3 兼容性验收

- [ ] 支持主流浏览器（Chrome、Firefox、Edge）
- [ ] 支持移动端基本功能
- [ ] 内存占用 < 200MB（1000条数据）

---

## 八、后续优化建议

1. **多列排序**：支持按住Shift键点击多列进行组合排序
2. **排序记忆**：记住用户上次选择的排序方式
3. **批量操作**：支持批量删除、批量导出等操作
4. **高级筛选**：支持多条件组合筛选
5. **数据导出**：支持导出筛选后的股票列表
6. **自动刷新**：支持定时自动刷新价格
7. **价格预警**：支持设置价格预警提醒
8. **数据同步**：支持与第三方数据源同步

---

## 九、附录

### 9.1 数据流图

```
用户操作 → 前端状态更新 → 内存排序/筛选 → 分页显示
            ↓
        调用API → 后端查询 → 返回数据 → 更新缓存
            ↓
        价格刷新 → 调用行情API → 获取价格 → 更新显示
```

### 9.2 状态管理图

```
┌─────────────────────────────────────────┐
│           前端状态管理                    │
├─────────────────────────────────────────┤
│ - cachedData: 所有股票数据（缓存）       │
│ - filteredData: 筛选后的数据              │
│ - sortedData: 排序后的数据               │
│ - paginatedData: 分页后的数据             │
│ - selectedRows: 选中的行                 │
│ - sortState: 排序状态                    │
│ - isRefreshing: 是否正在刷新             │
│ - isRefreshed: 是否已刷新               │
│ - priceData: 价格数据                    │
└─────────────────────────────────────────┘
```

### 9.3 字段分类表

| 字段名称 | 字段类型 | 支持排序 | 初始显示 | 刷新后显示 |
|---------|---------|---------|---------|-----------|
| 股票代码 | 基础字段 | ✅ | ✅ | ✅ |
| 股票名称 | 基础字段 | ✅ | ✅ | ✅ |
| TASK来源 | 基础字段 | ✅ | ✅ | ✅ |
| 加入时间 | 基础字段 | ✅ | ✅ | ✅ |
| 最新价 | 价格字段 | ❌ | - | ✅ |
| 开盘价 | 价格字段 | ❌ | - | ✅ |
| 昨收价 | 价格字段 | ❌ | - | ✅ |
| 最高价 | 价格字段 | ❌ | - | ✅ |
| 最低价 | 价格字段 | ❌ | - | ✅ |
| 投资评级 | 价格字段 | ❌ | - | ✅ |
| 加入以来涨幅 | 涨跌幅字段 | ✅ | - | ✅ |

---

**设计方案完成时间**：2025年1月24日  
**预计开发周期**：4-6天  
**预计上线时间**：2025年1月30日

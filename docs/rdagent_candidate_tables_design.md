# RD-Agent 备选TASK和LOOP数据库表设计

## 设计目标

1. 缓存RD-Agent的TASK和LOOP信息到数据库，避免每次都从文件系统读取
2. 支持检测新的TASK目录并自动入库
3. 支持LOOP详情的懒加载和缓存
4. 跟踪文件删除状态和同步状态

## 表结构设计

### 1. rdagent_candidate_tasks 表（备选TASK表）

存储从RD-Agent log目录扫描到的所有TASK信息。

```sql
CREATE TABLE IF NOT EXISTS rdagent.rdagent_candidate_tasks (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) UNIQUE NOT NULL,  -- 任务ID，如 2026-01-24_16-04-09-307734
    log_dir TEXT NOT NULL,  -- log目录路径
    
    -- SOTA因子信息
    has_sota BOOLEAN DEFAULT NULL,  -- 是否有SOTA因子
    sota_factors_count INTEGER DEFAULT 0,  -- SOTA因子数量
    sota_checked_at TIMESTAMP WITH TIME ZONE,  -- SOTA检查时间
    
    -- 任务基本信息
    hist_len INTEGER DEFAULT 0,  -- trace历史长度
    task_status VARCHAR(50),  -- 任务状态：success, failed, running等
    
    -- 同步状态
    is_synced BOOLEAN DEFAULT FALSE,  -- 是否已同步到AIstock
    sync_status VARCHAR(50),  -- 同步状态
    synced_at TIMESTAMP WITH TIME ZONE,  -- 同步时间
    
    -- 文件状态
    dir_exists BOOLEAN DEFAULT TRUE,  -- 目录是否存在
    dir_checked_at TIMESTAMP WITH TIME ZONE,  -- 目录检查时间
    
    -- 时间戳
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,  -- 首次发现时间
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,  -- 最后更新时间
    
    -- 索引
    CONSTRAINT rdagent_candidate_tasks_task_id_key UNIQUE (task_id)
);

CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_has_sota ON rdagent.rdagent_candidate_tasks(has_sota);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_is_synced ON rdagent.rdagent_candidate_tasks(is_synced);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_dir_exists ON rdagent.rdagent_candidate_tasks(dir_exists);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_discovered_at ON rdagent.rdagent_candidate_tasks(discovered_at DESC);
```

### 2. rdagent_candidate_loops 表（备选LOOP表）

存储TASK中的LOOP详情信息。

```sql
CREATE TABLE IF NOT EXISTS rdagent.rdagent_candidate_loops (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL,  -- 关联的任务ID
    loop_id INTEGER NOT NULL,  -- LOOP编号
    
    -- 实验信息
    exp_type VARCHAR(100),  -- 实验类型：FactorTask等
    hypothesis TEXT,  -- 假设/因子名称
    reason TEXT,  -- 原因/描述
    
    -- 性能指标
    valid_score DOUBLE PRECISION,  -- IC值（验证集）
    test_score DOUBLE PRECISION,  -- 测试集得分
    annualized_return DOUBLE PRECISION,  -- 年化收益
    max_drawdown DOUBLE PRECISION,  -- 最大回撤
    information_ratio DOUBLE PRECISION,  -- 信息比率
    
    -- SOTA标记
    is_sota BOOLEAN DEFAULT FALSE,  -- 是否为SOTA因子
    feedback TEXT,  -- 反馈信息
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束
    CONSTRAINT rdagent_candidate_loops_task_loop_key UNIQUE (task_id, loop_id),
    
    -- 外键约束
    CONSTRAINT fk_rdagent_candidate_loops_task 
        FOREIGN KEY (task_id) 
        REFERENCES rdagent.rdagent_candidate_tasks(task_id) 
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_loops_task_id ON rdagent.rdagent_candidate_loops(task_id);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_loops_is_sota ON rdagent.rdagent_candidate_loops(is_sota);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_loops_valid_score ON rdagent.rdagent_candidate_loops(valid_score DESC);
```

## 数据流程

### 1. 页面加载时

```
1. 前端调用 GET /api/v1/rdagent/candidate-tasks
2. 后端执行：
   a. 扫描RD-Agent log目录，获取所有TASK目录
   b. 对比数据库中的task_id，找出新的TASK
   c. 对新TASK调用RD-Agent API获取基本信息（has_sota等）
   d. 插入到rdagent_candidate_tasks表
   e. 检查已有TASK的目录是否存在，更新dir_exists字段
3. 返回所有TASK列表（包含数据库状态）
```

### 2. 点击LOOP详情时

```
1. 前端调用 GET /api/v1/rdagent/tasks/{task_id}/loops
2. 后端执行：
   a. 检查rdagent_candidate_loops表是否已有该TASK的LOOP数据
   b. 如果没有，调用RD-Agent API获取LOOP详情
   c. 将LOOP数据批量插入到rdagent_candidate_loops表
   d. 返回LOOP列表
3. 下次请求直接从数据库读取，无需再调用API
```

### 3. 定期维护

```
1. 定时任务（每小时）检查目录存在性
2. 更新dir_exists和dir_checked_at字段
3. 清理超过30天且目录已删除的记录
```

## API接口设计

### 1. GET /api/v1/rdagent/candidate-tasks
- 功能：获取所有备选TASK列表
- 参数：limit（可选）
- 返回：TASK列表，包含SOTA状态、同步状态、文件状态

### 2. GET /api/v1/rdagent/tasks/{task_id}/candidate-loops
- 功能：获取指定TASK的LOOP详情
- 参数：task_id
- 返回：LOOP列表，包含所有性能指标

### 3. POST /api/v1/rdagent/candidate-tasks/refresh
- 功能：手动刷新TASK列表
- 返回：新发现的TASK数量

## 实现优先级

1. ✅ 增强LOOP详情数据（IC、年化收益、最大回撤）
2. 创建数据库表
3. 实现TASK扫描和入库逻辑
4. 实现LOOP详情缓存逻辑
5. 修改前端显示更多指标
6. 实现目录存在性检查

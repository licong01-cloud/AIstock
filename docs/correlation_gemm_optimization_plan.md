# 因子相关性计算 — GEMM 加速 + 进度追踪 + 超时取消

## Context

### 问题
当前 cache_only 模式对 467 因子的相关性矩阵计算耗时 **166+ 分钟**（仍在运行），原因：

```
pandas .corr(method='spearman', min_periods=30) 内部:
  Cython nancorr 双循环: 108,811 对/天 × 3500 行 × 252 天
  行间跳步 3736 bytes >> cache line 64 bytes
  → L3 缓存延迟主导，单日 30.5 秒
```

同时存在两个运维问题：
- **无进度可见性**：matrix_compute 阶段 `total=1, done=0`，252 天循环完全黑盒
- **无法取消/超时**：Future 未保存，无 stop_event，计算一旦开始只能等结束或杀进程

### 解决方案

将 pandas nancorr 逐对循环替换为 **5 次 numpy BLAS 矩阵乘法 (GEMM)**，数学完全等价、零精度损失：

| 方案 | 每天耗时 | 252天 | 加速比 |
|------|---------|-------|--------|
| pandas nancorr (当前) | 30.5 秒 | **128 分钟** | 1x |
| numpy BLAS GEMM | 76 ms | **19 秒** | 400x |
| + 排序 + winsorize + I/O | — | **~45 秒** | ~170x |

### 数学等价性证明

`pandas .corr(method='spearman')` 内部实现 = `.rank(na_option='keep')` + `.corr(method='pearson', min_periods=30)`

Pearson with pairwise NaN 可以用矩阵运算表达：

```python
X = np.where(np.isnan(R), 0, R)    # 排名数据，NaN→0
M = (~np.isnan(R)).astype(float)    # 有效掩码

N   = M.T  @ M      # (K×K) 每对有效样本数
S1  = X.T  @ M      # (K×K) S1[i,j] = Σ rank_i (where j valid)
S2  = (X²).T @ M    # (K×K) S2[i,j] = Σ rank_i² (where j valid)
SXY = X.T  @ X      # (K×K) 交叉乘积

num = N * SXY - S1 * S1.T
den = sqrt((N * S2 - S1²) * (N * S2.T - S1.T²))
corr = where((den > 0) & (N >= 30), num / den, NaN)
```

每次 GEMM: `(467×3500).T @ (3500×467)` = OpenBLAS AVX2 优化，~15ms。

---

## 修改文件清单

| # | 文件 | 改动 |
|---|------|------|
| 1 | `backend/services/quantevolver/correlation_engine.py` | GEMM 加速 + 向量化 winsorize + 进度回调 + stop_event |
| 2 | `backend/routers/quantevolver_evolution.py` | 超时机制 + 取消 API + 进度接入 |
| 3 | `frontend/.../ComputePanel.tsx` | 取消按钮 |

---

## Step 1: correlation_engine.py — GEMM 加速 + 进度回调

### 1.1 新增 import

```python
import threading  # 新增
```

### 1.2 向量化 `_winsorize_cross_section` (替换 lines 483-497)

当前代码 Python 循环 467 列，替换为全数组 numpy 操作：

```python
def _winsorize_cross_section(self, section: pd.DataFrame) -> pd.DataFrame:
    """截面 Winsorize: 将每列超出 [q, 1-q] 分位数的值截断。(向量化)"""
    data = section.values.copy()
    valid_counts = np.sum(~np.isnan(data), axis=0)
    q_lo = self._winsorize_q * 100
    q_hi = (1.0 - self._winsorize_q) * 100
    lo = np.nanpercentile(data, q_lo, axis=0)
    hi = np.nanpercentile(data, q_hi, axis=0)
    skip = valid_counts < 10
    lo[skip] = -np.inf
    hi[skip] = np.inf
    data = np.clip(data, lo[np.newaxis, :], hi[np.newaxis, :])
    return pd.DataFrame(data, index=section.index, columns=section.columns)
```

### 1.3 新增 `_cross_sectional_spearman_gemm` 方法 (在 `_cross_sectional_spearman` 之后)

```python
def _cross_sectional_spearman_gemm(
    self,
    section: pd.DataFrame,
) -> Optional[np.ndarray]:
    """单日截面 Spearman via 5-GEMM。

    数学等价于 pandas .corr(method='spearman', min_periods=30)，
    但使用 BLAS 矩阵乘法替代 Cython 逐对循环，快 400 倍。
    """
    K = section.shape[1]

    valid_counts = section.count()
    usable_cols = valid_counts[valid_counts >= 30].index
    if len(usable_cols) < 2:
        return None

    sub = section[usable_cols]

    # Step 1: 每列独立排名，NaN 保留 (pandas Cython, ~10ms)
    ranked = sub.rank(method="average", na_option="keep")
    R = ranked.values  # (N, K_sub), float64, NaN preserved

    # Step 2: 5-GEMM Pearson on ranks = Spearman
    nan_mask = np.isnan(R)
    M = (~nan_mask).astype(np.float64)
    X = np.where(nan_mask, 0.0, R)

    N_pairs = M.T @ M
    SX      = X.T @ M
    SX2     = (X ** 2).T @ M
    SXY     = X.T @ X

    numerator = N_pairs * SXY - SX * SX.T
    var_x = N_pairs * SX2 - SX ** 2
    var_y = N_pairs * SX2.T - SX.T ** 2
    denominator = np.sqrt(var_x * var_y)

    valid_pair = (denominator > 0) & (N_pairs >= 30)
    sub_mat = np.where(valid_pair, numerator / denominator, np.nan)
    np.fill_diagonal(sub_mat, 1.0)

    if len(usable_cols) == K:
        return sub_mat

    # 嵌入完整 K×K 矩阵 (向量化索引)
    corr_mat = np.full((K, K), np.nan)
    np.fill_diagonal(corr_mat, 1.0)
    col_names = list(section.columns)
    idx = np.array([col_names.index(c) for c in usable_cols])
    corr_mat[np.ix_(idx, idx)] = sub_mat
    return corr_mat
```

### 1.4 修改 `compute_full_matrix` 签名 (lines 189-194)

```python
def compute_full_matrix(
    self,
    factor_names: List[str],
    as_of_date: Optional[str] = None,
    save_hdf5: bool = True,
    on_progress: Optional[callable] = None,      # 新增
    stop_event: Optional[threading.Event] = None, # 新增
) -> CorrelationResult:
```

### 1.5 修改日循环 (lines 236-254)

```python
        for day_idx, date_str in enumerate(window_dates):
            # 协作式取消检查
            if stop_event is not None and stop_event.is_set():
                raise RuntimeError(
                    f"相关性计算被取消/超时 (已完成 {len(daily_corrs)}/{len(window_dates)} 天)"
                )

            ts = pd.Timestamp(date_str)
            try:
                section = panel.loc[ts]
            except KeyError:
                # 进度回调 (跳过的天也计数)
                if on_progress is not None:
                    on_progress(day_idx + 1, len(window_dates))
                continue

            if len(section) < self._min_stocks:
                if on_progress is not None:
                    on_progress(day_idx + 1, len(window_dates))
                continue

            section_w = self._winsorize_cross_section(section)
            corr_mat = self._cross_sectional_spearman_gemm(section_w)  # ← 使用 GEMM
            if corr_mat is not None:
                daily_corrs.append(corr_mat)
                valid_dates.append(date_str)
                stocks_per_day.append(len(section))

            # 进度回调
            if on_progress is not None:
                on_progress(day_idx + 1, len(window_dates))
```

### 1.6 保留旧方法

保留 `_cross_sectional_spearman` 不删除（用于验证对比），但日循环改为调用 `_cross_sectional_spearman_gemm`。验证通过后的后续版本可删除旧方法。

---

## Step 2: quantevolver_evolution.py — 超时 + 取消 + 进度接入

### 2.1 新增模块变量 (line 578 之后)

```python
_stop_event = threading.Event()
_compute_future: Optional[Future] = None
_MATRIX_TIMEOUT_SEC = 1800  # 30 分钟 (GEMM 方案应在 2 分钟内完成)
```

import 补充: `from concurrent.futures import ThreadPoolExecutor, Future`

### 2.2 修改 `_run_correlation_compute` (line 715+)

在 `with _computing_lock:` 内部最开始：

```python
_stop_event.clear()
timeout_timer = threading.Timer(_MATRIX_TIMEOUT_SEC, _stop_event.set)
timeout_timer.daemon = True
timeout_timer.start()
```

Phase 2 中接入进度回调和 stop_event (line 820-822 区域)：

```python
# 当前: total=1, done=0/1 (黑盒)
# 改为: total=交易日数, done=逐日更新
_correlation_progress.advance(
    phase="matrix_compute",
    phase_label="计算相关性矩阵",
    done=0,
    total=252,  # 初始值，engine 回调会精确更新
)

def _matrix_progress(done: int, total: int):
    _correlation_progress.advance(done=done, total=total)

result = engine.compute_full_matrix(
    compute_factors,
    as_of_date=as_of_date,
    save_hdf5=True,
    on_progress=_matrix_progress,
    stop_event=_stop_event,
)
```

异常处理区分取消 vs 失败 (line 874 区域)：

```python
except Exception as e:
    was_cancelled = _stop_event.is_set()
    status = "cancelled" if was_cancelled else "failed"
    error_msg = "计算被用户取消" if was_cancelled else str(e)
    logger.error(f"相关性计算{status}: {e}", exc_info=not was_cancelled)
    _correlation_logs.append(f"计算{status}: {error_msg}", "WARN" if was_cancelled else "ERROR")
    _correlation_progress.finish(status, error_msg)
    _update_job_status(job_id, status, error_msg)
finally:
    timeout_timer.cancel()
    _stop_event.clear()
```

### 2.3 保存 Future (lines 929, 954, 994)

```python
# 当前:
_compute_executor.submit(_run_correlation_compute, ...)

# 改为:
global _compute_future
_compute_future = _compute_executor.submit(_run_correlation_compute, ...)
```

### 2.4 新增取消端点 (在 line ~1001 之后)

```python
@router.post("/correlations/cancel", summary="取消正在进行的相关性计算")
def cancel_correlation_compute():
    if not _computing_lock.locked():
        return {"status": "idle", "message": "当前无计算任务在执行"}

    _stop_event.set()
    _correlation_logs.append("收到取消请求，正在中断计算...", "WARN")

    if _compute_future is not None:
        _compute_future.cancel()

    return {"status": "cancelling", "message": "已发送取消信号，计算将在当前天完成后中断"}
```

---

## Step 3: ComputePanel.tsx — 取消按钮

在 line 329 ("计算中..." 按钮) 之后，增加取消按钮：

```tsx
{isComputing && (
  <button
    onClick={async () => {
      const BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
      await fetch(`${BASE}/quantevolver/evolution/correlations/cancel`, { method: "POST" });
    }}
    style={{
      padding: "10px 20px",
      fontSize: 13,
      fontWeight: 600,
      background: "rgba(239,68,68,0.7)",
      color: "#fff",
      border: "none",
      borderRadius: 8,
      cursor: "pointer",
      whiteSpace: "nowrap",
    }}
  >
    取消计算
  </button>
)}
```

进度条区域 (line 382) 已有 `${progress.done}/${progress.total}` 显示，接入逐日进度后自动生效，无需改动。

---

## 实施顺序

```
Step 1: correlation_engine.py
  1.1 新增 import threading
  1.2 向量化 _winsorize_cross_section
  1.3 新增 _cross_sectional_spearman_gemm
  1.4 修改 compute_full_matrix 签名
  1.5 修改日循环 (GEMM + 回调 + stop_event)
  ↓
Step 2: quantevolver_evolution.py
  2.1 新增 _stop_event, _compute_future, _MATRIX_TIMEOUT_SEC
  2.2 修改 _run_correlation_compute (超时 + 进度 + 取消处理)
  2.3 保存 Future
  2.4 新增 POST /correlations/cancel
  ↓
Step 3: ComputePanel.tsx
  3.1 取消按钮
  ↓
验证
```

---

## 验证步骤

### V1: GEMM 精度验证
在计算开始时对第一天数据执行新旧方法对比：
```python
old = self._cross_sectional_spearman(section_w)
new = self._cross_sectional_spearman_gemm(section_w)
max_diff = np.nanmax(np.abs(old - new))
logger.info(f"GEMM 验证: max_diff={max_diff}")  # 期望 < 1e-10
```
确认通过后移除验证代码。

### V2: 性能验证
触发 cache_only 计算，观察：
- 进度条显示 "计算相关性矩阵 X/252" 实时更新
- 总耗时从 ~128 分钟降至 **< 2 分钟**（含数据加载）
- 内存峰值 < 1GB

### V3: 取消功能验证
- 触发计算后点击 "取消计算"
- 确认状态变为 "cancelled"
- 日志显示取消信息
- 可以重新启动新计算

### V4: 超时验证
- 临时设置 `_MATRIX_TIMEOUT_SEC = 5`
- 确认计算在 5 秒后自动中断

### V5: 前端验证
- `npx next build --no-lint` 通过
- 进度条显示逐日更新
- 取消按钮可见可用

---

## 不修改的部分

- `_ewma_aggregate()` — 已向量化，无需改动
- `compute_pairwise()` — 增量模式用，K=2 不是瓶颈
- `FactorValueLoader` — 数据加载层，不涉及计算优化
- `_persist_correlations_batch()` — 已用 execute_values 优化
- `CorrelationResult` 数据类 — 纯数据容器
- `correlation_scheduler.py` — 调用 `_run_correlation_compute()`，自动继承超时/取消
- 前端进度条 UI — 已有 `done/total` 显示，自动生效

## GPU 扩展 (未来)

当前 PyTorch 2.7.1+cu118 不兼容 RTX 5080 (需 sm_120)。升级 PyTorch 后可将 GEMM 操作替换为 `torch.matmul` on CUDA：

```python
# 未来: 将 M.T @ M 等替换为
M_gpu = torch.from_numpy(M).cuda()
X_gpu = torch.from_numpy(X).cuda()
N_pairs = M_gpu.T @ M_gpu
# ... 同样公式 ...
result = sub_mat.cpu().numpy()
```

预计从 ~45 秒降至 ~24 秒（排序仍在 CPU，是主要开销）。当前 CPU GEMM 已足够快（128分钟→45秒），GPU 优先级不高。

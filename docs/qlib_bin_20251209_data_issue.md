# qlib_bin_20251209 数据状态与修正方案

## 1. 当前实际状态说明

针对快照 `qlib_bin_20251209`，目前排查结果如下：

- **日线 bin 数据**  
  `features/daily_all/*.day.bin` 中已经包含了 **完整的股票日线数据**（包括正常交易的股票，以及我们关心的基准指数 000300.SH 对应的日线数据）。

- **`instruments/index.txt`（指数）**  
  已按 Qlib 内部实现要求修正为：

  - 使用制表符 `\t` 作为分隔符；
  - 精确三列：`instrument`、`start_date`、`end_date`；
  - 无表头；
  - 通过 `rewrite_qlib_index_instruments.py` 生成，Qlib 能正确读取并列出指数（例如 `SH000300` / `000300.SH` 的映射关系已经验证通过）。

  这一块目前 **已经 OK**，不需要再调整。

- **`instruments/all.txt`（股票池）**  
  当前文件内容中 **只有一条记录**：

  ```text
  000300.SH 2010-01-07 2025-12-01
  ```

  也就是说，在 Qlib 看来：

  - `market="all"` 或默认股票池下，实际上只暴露了这一个代码；
  - 虽然底层 bin 里股票日线数据是完整的，但因为 `all.txt` 没有列出这些股票，Qlib 在上层 API（如 `D.list_instruments`）访问不到它们。

## 2. 期望的 instruments 规范

结合最初在备忘录（`RD-Agent_AIstock_Qlib_备忘录.md`）中的约定，这里统一记录我们期望的规范：

### 2.1 指数：`instruments/index.txt`

- 仅维护指数（例如 `000300.SH` 等）；
- 使用 **制表符 `\t`** 分隔；
- 三列：`instrument`、`start_date`、`end_date`；
- 无表头；
- 已由 `rewrite_qlib_index_instruments.py` 负责生成，当前行为符合 Qlib 的读取逻辑。

### 2.2 股票：`instruments/all.txt`

- 列出 **所有合规的股票**，每行一只股票；
- 三列：
  - `ts_code`（如 `000001.SZ`, `600000.SH`）；
  - `start_date`；
  - `end_date`；
- 使用 **单个空格** 分隔；
- 无表头；
- 示例：

  ```text
  000001.SZ 2010-01-07 2025-12-01
  600000.SH 2010-01-07 2025-12-01
  ...
  ```

> 说明：
>
> - 指数仍然只在 `index.txt` 中维护，不混入 `all.txt`；
> - `all.txt` 主要用于定义“股票池”（`market="all"` / 默认 universe），指数相关逻辑通过 `index.txt` 处理即可。

## 3. 不重新导数据的修正方案：仅重写 instruments/all.txt

由于底层日线 bin 已经完整生成，不希望重新跑一遍导出流程，这里给出一个 **只重写 `all.txt` 的修正路径**，供 AIstock 侧实现。

### 3.1 数据来源建议

有两种可选的数据来源，可以任选其一，或按实际情况折中：

- **来源 A：数据库中的股票日线表**（推荐）  
  例如 `market.daily` / `market.stock_daily` 等：

  - 优点：
    - 以数据库为“真源”，可精确控制哪些股票纳入合规股票池；
    - 可以结合停牌规则、上市日期等做过滤（例如剔除 ST、退市股票等）。

- **来源 B：数据库中的股票基础信息表**  
  例如 `market.stock_basic`：

  - 以 `ts_code` + `list_date` / `delist_date` 估算 start/end；
  - 适合作为 A 的补充或兜底方案。

这里以 **来源 A：日线表** 为例说明。

### 3.2 从 DB 计算每只股票的起止日期

在数据库中执行类似如下 SQL（以 `market.daily` 为例，实际表名按 AIstock 项目为准调整）：

```sql
SELECT
    ts_code,
    MIN(trade_date) AS start_date,
    MAX(trade_date) AS end_date
FROM market.daily
GROUP BY ts_code
ORDER BY ts_code;
```

得到的结果示意：

```text
 ts_code    |  start_date  |  end_date
------------+--------------+------------
 000001.SZ  | 2010-01-07   | 2025-12-01
 600000.SH  | 2010-01-07   | 2025-12-01
 ...
```

> 注意：
>
> - 可以根据实际业务需要，对 `ts_code` 再做一层过滤（例如只保留 A 股主板 / 创业板等）；
> - 也可以限制一个统一的 `start_date`，例如强制所有股票从 `2010-01-07` 开始，以保持与当前指数和日线 bin 的时间对齐。

### 3.3 用脚本重写 `instruments/all.txt`

在不重导 bin 的前提下，只需写一个独立脚本（Python / Go / 任意），流程大致如下：

1. 从 DB 读取上一步 SQL 结果（`ts_code`, `start_date`, `end_date`）。
2. 在目标 qlib 快照目录下定位：

   ```text
   {QLIB_BIN_ROOT}/qlib_bin_20251209/instruments/all.txt
   ```

3. **备份旧文件**：

   ```text
   all.txt -> all.txt.bak_YYYYMMDD_HHMMSS
   ```

4. 按以下格式逐行写入新文件：

   ```text
   {ts_code} {start_date} {end_date}\n
   ```

   示例输出：

   ```text
   000001.SZ 2010-01-07 2025-12-01
   600000.SH 2010-01-07 2025-12-01
   ...
   ```

5. 保持 `index.txt` 不变（仍由 `rewrite_qlib_index_instruments.py` 负责），确保指数逻辑不受影响。

完成后，可以在 WSL/rdagent-gpu 环境下，用 Qlib 做一次简单校验：

```python
import qlib
from qlib.config import C
from qlib.data import D

qlib.init(provider_uri="/path/to/qlib_bin_20251209")

# 检查股票池是否补齐
stocks = D.list_instruments("all")
print(len(stocks), stocks[:10])

# 检查指数仍正常
indexes = D.list_instruments("index")
print(indexes)
```

如果：

- `stocks` 数量明显大于 1 且包含典型股票（例如 `000001.SZ` 等）；
- `indexes` 中仍然能看到 `000300.SH` / 对应内部代码；

则说明：

- **无需重新导出 bin 数据**；
- 仅通过重写 `instruments/all.txt` 即完成了 Qlib 视角下的股票与指数 universe 对齐。

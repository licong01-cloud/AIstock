# QE Recorder Binding Contract (2026-05-06)

## 背景

QE 旧版 `read_exp_res.py` 在没有指定 recorder id 时，会扫描当前 `mlruns` 下所有 experiment/recorder，并按 recorder `end_time` 选择最新 recorder。并行 backtest-only、pred-backtest 或复用 `mlruns` 时，这可能把当前 loop 的结果抽成另一个并行 loop 的 recorder。

本次修复不修改 Qlib 源码。Qlib recorder 机制本身没有问题，问题在 QE runner 与结果抽取脚本没有传递“本轮创建的 recorder id”。

## 新契约

### 1. runner 必须写出当前 recorder 引用

`qrun_limit_minute.py` 和 `qrun_limit.py` 在创建 recorder 后写出当前工作目录下的：

```json
{
  "schema_version": 1,
  "recorder_id": "<mlflow recorder id>",
  "experiment_name": "<qlib experiment name>",
  "experiment_id": "<mlflow experiment id if available>",
  "mode": "full | train_only | backtest_only | pred_backtest",
  "runner": "qrun_limit_minute.py | qrun_limit.py",
  "cwd": "<loop workspace>",
  "mlflow_tracking_uri": "<tracking uri>",
  "written_at": "<UTC ISO timestamp>"
}
```

文件名固定为：`qe_current_recorder.json`。

### 2. read_exp_res 必须优先按绑定 recorder 抽取

`read_exp_res.py` 的选择优先级：

1. `QE_RECORDER_ID` 环境变量。
2. 当前目录 `qe_current_recorder.json`。
3. 当前目录 `qe_recorder_id.txt`（兼容轻量手工场景）。
4. legacy fallback：仅在没有启用严格模式时，按旧逻辑选择 latest recorder。

新 QE 任务执行结果抽取时必须带：

```bash
QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py
```

严格模式下，如果找不到绑定 recorder 或绑定 recorder 不存在，必须失败，不允许回退到 latest recorder。

### 3. 旧实验兼容性

历史 loop 通常没有 `qe_current_recorder.json`。为了不影响旧数据抽取：

- 不设置 `QE_REQUIRE_RECORDER_ID` 时，仍保留旧的 latest-recorder fallback。
- fallback 会输出 warning：`using legacy latest-recorder fallback for old experiments`。
- 设置了 `QE_RECORDER_ID` 或存在 `qe_current_recorder.json` 时，绝不静默抽取其他 recorder。

因此旧实验可以继续读取；但如果旧实验本身是并行 backtest-only 且 `mlruns` 混合多个 recorder，需要用 mismatch 扫描脚本单独标记可信度。


### 4. 绑定 recorder 查询范围

绑定了 `recorder_id` 时，`read_exp_res.py` 会扫描当前 tracking URI 可见的所有 experiments 来定位该 recorder，而不是只按 `experiment_name` 缩小范围。原因是 MLflow file store 在并行首次创建同名 experiment 时可能出现同名 experiment 竞争；按 recorder id 全局精确匹配更安全。如果 recorder id 前缀匹配多个 recorder，必须失败并要求使用完整 recorder id。
## 数仓模块需要更新的规则

数仓侧如果归档 QE 结果，建议把 recorder 绑定信息作为结果可信度字段纳入归档：

- 归档 `qe_current_recorder.json`，记录 runner 实际创建的 `recorder_id`。
- 如存在 `qe_extracted_recorder.json`，记录实际抽取的 `selected_recorder_id`。
- 如果 `recorder_id != selected_recorder_id`，归档状态应标记为 `recorder_mismatch`，不得作为可信回测结果进入主指标表。
- 如果没有绑定文件且是旧实验，可标记为 `legacy_latest_fallback`，指标可保留但需要降低可信等级或要求人工复核。
- 不要从共享 `mlruns` 中自行选择 latest recorder 作为当前 loop 结果。
- 不要修改 Qlib recorder 存储结构，也不要依赖 Qlib 内部私有字段；使用上述 QE contract 文件即可。

## 模拟盘 / StrategyPackage / Selection Center 需要更新的规则

模拟盘与选股运行时如果使用 QE loop 生成 StrategyPackage 或 selection artifact：

- 不要直接扫描 `mlruns` 的 latest recorder。
- 优先使用 QE 后端已同步的 loop result 与 recorder binding metadata。
- 如果来源 loop 标记为 `recorder_mismatch`，不得生成可用于模拟盘/选股的 authoritative artifact。
- 对 `legacy_latest_fallback` 的旧实验，允许展示诊断指标，但用于模拟盘前应要求重新抽取或人工确认。
- Paper/Selection 不需要修改 Qlib，也不需要读取远端 worker 文件系统；通过 QE API/归档字段消费 recorder 可信度即可。

## 验证要求

每次修改 QE runner 或 result reader 后，至少验证：

1. 新任务命令包含 `QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py`。
2. `qrun_limit_minute.py` 的 full、train-only、backtest-only、pred-backtest 都会写 `qe_current_recorder.json`。
3. `qrun_limit.py` 日线 full 模式会写 `qe_current_recorder.json`。
4. 远端已知 mismatch workspace 中，严格绑定目标 recorder 后能抽取目标 recorder，不再抽 latest recorder。
5. 旧实验无绑定文件时，不设置严格模式仍能按旧逻辑读取。
6. 严格模式无绑定文件时必须失败，不能静默 fallback。


# StrategyPackage freeze runtime oracle findings (2026-07-01)

Evidence source: 2026-07-01 Tier2 independent read-only verification. Environment: real WSL `rdagent-gpu` qlib (not stub), trade_date=2026-06-30; production DB DML/DDL was not executed.

## 结论(推翻上一会话"stub 限制"定性)

上一会话把 feature-count 包 oracle 失败定性为 "stub 环境缺 Alpha158 基础特征, 非固化缺陷"。**真环境核验证明这是错的**: 失败在完整 qlib 管线里稳定复现, 根因是**固化本身不完整**, 不是环境。

15 个包(2 已退役), 13 个 active 中经真环境核验:
- **真正 self-contained (2)**: `pkg_5a5ccb56`(57 因子, 出 1032 只评分), `pkg_b668f8a`(50 因子). 模型为 qlib 原生(LGBM 类), 且期望特征数==冻结动态因子数。
- **固化缺陷-Alpha158 (2 已证)**: `pkg_006a`(冻结 32, 模型期望 63, 缺 31), `pkg_99142c`(冻结 50, 模型期望 70, 缺 20)。报错 `strict inference model feature count mismatch: expected=N, actual=M`。
- **固化缺陷-自定义模型类 (2 已证 + 疑似 7)**: `pkg_2a9fccb`/`pkg_cfa3c5`(57 因子)报 `Can't get attribute 'LSTM_10D_hs64_d02' on module 'model'` — 自定义 NN 模型类(LSTM/TCN)定义在 QE 期 `model.py`, **该文件未冻进包**, pkl 无法反序列化。其余 7 个 BACKTEST_APPROVED 包在纯 pkl 载入时同样报 `No module named 'model'`, 高度疑似同缺陷(未逐一跑全推理确认)。

## 两个独立固化缺陷(均为真缺陷, 非环境)

### 缺陷 A: Alpha158 基础特征 schema 未冻结
- `package_asset_freeze.py:freeze_manifest_assets` 只冻 **FACTOR_CODE**(动态因子)+ **MODEL_WEIGHT**, 不冻 Alpha158 schema/conf。
- 运行时 `live_inference.py:_manifest_runtime_custom_params`(461-484) **强制 `disable_alpha158=True`**, 且 `_source_from_package_assets`(757 行)写空 `conf.yaml`(`task: {}`)。
- 后果: 若模型训练时用了 Alpha158 基础特征(NN 模型常见), 冻结包运行时只能重算动态因子, 永远补不回 Alpha158 那部分 → `expected>actual` 特征数不匹配, strict 模式拒绝 pad/truncate(正确行为, 不 silent)。
- 注意: Alpha158 是 qlib 库算的确定性特征, 本可运行时重算不需冻 raw 数据 — 但当前固化把 `disable_alpha158` 写死为 True, 把这条路堵死了。修复方向 = 固化时记录该模型是否用 Alpha158 + 运行时按记录重算, 而非无条件 disable。

### 缺陷 B: 自定义模型类 model.py 未冻结
- 冻结工作区只有 `params.pkl`(无 `model.py`)。`inference_engine.load_model_from_pkl`(198-208)把 model 目录加 sys.path 期望 `model.py` 与 pkl 同级。
- 自定义 NN(如 `LSTM_10D_hs64_d02`)的类定义在 QE 期 `model.py`, 未冻 → pkl `Can't get attribute`。
- qlib 原生模型(LGBModel 等)无此问题, 所以 2 个 passing 包恰好都是原生模型。

## 关键影响: 固化里程碑对 11/13 包不成立

"建包即冻结副本、删实验不影响包" 目前**只对 2 个包成立**。其余 11 个 active 包的冻结"副本"不完整(缺 Alpha158 schema 和/或自定义 model.py), **删 QE 源会让它们永久不可运行** — 正是固化本要防止的"删实验静默半死"。

## 对下一步的直接建议(阻断性)

1. **批6(删源无 guard)现在是危险动作**, 必须暂缓。在缺陷 A/B 修复前, 对 11 个包删源=不可逆损坏。
2. **批5(候选退役, 含删 candidate/prediction_ref)** 可继续(与本缺陷正交), 但涉及删源的部分同样受阻。
3. **优先级最高 = 修固化缺陷 A/B**:
   - A: 固化记录 Alpha158 使用标记(从 QE conf 读), 运行时按标记重算而非写死 disable。
   - B: 固化把自定义 `model.py`(模型类定义)一并冻进包并在 prepare_workspace 释放到 pkl 同级。
   - 修复后**回填 11 个包**并逐一跑真实 oracle 复验 == self-contained。
4. 修复前**不得对外宣称固化闭环** — 当前只 2/15 真闭环。

## Reproducible read-only verification entrypoints
- `debug_tools/strategy_package/freeze_completeness_20260701/frozen_runtime_oracle_readonly.py <pkg> <date>` runs frozen resolve with a bogus QE source id, prepares the workspace from package blobs, then executes real WSL qlib inference. It asserts `origin==package_asset` and finite scores; it does not call `artifact_repository.save()`.
- `debug_tools/strategy_package/freeze_completeness_20260701/frozen_feature_count_runtime_audit.py` uses the same runtime frozen resolve + prepare path, then reads model expected features through `inference_engine.load_model_from_pkl()` and compares them with prepared `factor_order` length.
- `debug_tools/strategy_package/freeze_completeness_20260701/frozen_feature_count_blob_audit.py` is a fast auxiliary pre-check that reads the frozen `MODEL_WEIGHT` blob feature count. The runtime audit/oracle remains the authoritative evidence path.
- These tools are read-only debug tools for human/agent evidence gathering. They are not imported by production services, schedulers, formal APIs, or release gates.

## Environment note
- The WSL `rdagent-gpu` environment was missing `aiofiles` during the original verification and was repaired with a dev-only `pip install`. This was not a production dependency change, did not start/restart services, and does not affect the freeze-completeness conclusion.

## PR cleanup note (2026-07-02)
- The original root untracked `scripts/_scratch_frozen_*.py` files are preserved here as cleaned read-only tools under `debug_tools/strategy_package/freeze_completeness_20260701/` instead of formal `scripts/`.
- `production_ddl_gate=noop`; `production_dml=noop`; this PR only preserves evidence and reproduction entrypoints, and does not run backfill, retirement, source deletion, DDL, or DML.

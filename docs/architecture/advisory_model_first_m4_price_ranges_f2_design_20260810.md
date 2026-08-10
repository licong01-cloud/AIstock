# Advisory Model-First M4 价格范围详细设计

> 日期：2026-08-10
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v2.6
> 当前阶段：`DESIGN_READY`
> 适用范围：学术研究与历史回测参考，不构成实时投资建议或交易执行

## 1. Background / 背景

M1/M2 已实现真实 LightGBM 排名和 Advisory 只读在线影子推理；M3 已实现五个期限的收益分位数、正收益概率、signal survival、MFE/MAE 和持股周期预测。M3 bundle `17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0` 包含 46 个真实 LightGBM 模型；PR #3234 已合入。运行时 commit `0ab6dec36c6bc05f7d9655de63b07bbd5353dfd2` 已在 `decision=2026-07-15 / target=2026-07-16` 的真实多 Alpha Program 上完成只读 readback：20 个候选均返回五期限非空结果，耗时 33.236 秒，无 reason code。

M4 不再建设新的历史证据、归档或通用平台。它直接补齐用户要求的最后一项短反弹真实功能：基于真实模型、M3 预测和数据库 decision-cutoff 行情，为每只候选给出下一交易日买入参考范围、止盈参考范围、移动保护参考范围和止损参考范围。

## 2. Scope / 范围

M4 分为同一完整交付中的两个顺序阶段：

1. **M4A 训练**：在 WSL `rdagent-gpu` 环境复用 M1 候选/103 特征、M3 时间切分和 QE 日线文件，训练下一交易日可执行概率以及可执行开盘缺口 q10/q50/q90 四个真实 LightGBM 模型头，发布原子 `PriceRangeBundle`。
2. **M4B 在线投影**：在现有 model-shadow 链路加载 exact M4 bundle，复用 M3 已有 MFE/MAE 与持股周期输出，并用数据库 decision-cutoff 的未复权价格和 PIT 静态属性转换成 CNY 价格范围；接入现有 API/UI。

M4A 完成时只能报告模型训练完成，不能冒充 M4 页面功能完成。只有 M4B 源码合入、用户重启和真实只读 API/UI readback 分别完成后，M4 才能标记 `COMPLETED_RUNTIME_VERIFIED`。

## 3. Non-goals / 非目标

- 不读取分钟 Bin，不建模盘中触达顺序，不输出委托价、订单或交易执行指令。
- 不重复训练 M3 已有的五期限 MFE/MAE、收益或持股周期模型。
- 不处理 Historical Range、Phase 1R、SEALED/CAS、历史证据、旧 batch、旧 root、归档或遗留状态。
- 不新增数据库表、DDL/DML、自动训练调度、模型注册平台、通用缓存、ModelOps 或治理后台。
- 不修改 Selection、StrategyPackage、Paper、模拟盘或 QE 的业务逻辑。
- 不增加角色、审批、人工 ACK、策略包二次准入、收益门槛、发布门禁或任何未经用户确认的门禁。

## 4. 不变业务边界

- 基础历史行情训练输入只读取现有 QE H5/Parquet/Qlib 日线 Bin、suspend sidecar 和 M1 candidates/features Parquet；不读取生产 PostgreSQL 历史行情作为训练源。
- 模型训练只在 WSL Conda `rdagent-gpu` 执行；Windows 只负责生成请求、调用 WSL、读取结果以及运行在线推理。
- 正式预测只读取当前 Program/ReviewRun/Selection、数据库 decision-cutoff 行情和当时可知的 PIT 静态属性；不得读取 target trade date 的未来行情。
- 所有预测保持 `EXPERIMENTAL_SHADOW / UNCALIBRATED`，价格范围是模型研究结果，不是保证价格或自动交易参数。
- M4 子信封失败只关闭价格范围，M2 排名、M3 outcome 和现有规则荐股继续运行；不得用常数、默认百分比或规则结果冒充模型结果。
- M4 的风险规则只允许收紧模型范围，不能放宽现有 Program `stop_loss_bps` 或改变既有列表淘汰语义。
- 单进程峰值 RSS 必须低于 8GB，训练目标为小时级；按日期/候选分批并使用临时 Parquet，不新建 SQLite 或历史证据平台。

## 5. Architecture / 架构

```text
M1 candidates/features + QE daily OHLC/limit/suspend
  -> M4 next-session executable/open-gap labels
  -> exact M3 purged train/validation/test split
  -> 1 binary + 3 quantile LightGBM heads
  -> atomic PriceRangeBundle bound to parent + outcome bundle
  -> database decision-cutoff unadjusted prices/PIT attributes
  -> M4 price projection using M3 MFE/MAE/holding predictions
  -> existing model-shadow API/UI price_range child envelope
```

M4 不复制 M2 FeatureBuilder，也不改变 M3 outcome 预测。`price_range` 是现有 `outcome` 的平级子信封；它显式引用 M3 outcome bundle 和逐候选的 M3 输出。M4 不得反向写入候选、排序、荐股列表或 Review 状态。

## 6. Contracts / 精确输入身份

M4 training request 必须绑定：

```text
parent_request_id = advmreq_ac5959aa8dc14a25e3b8c139
parent_bundle_id = 9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629
outcome_request_id = advoutreq_d16081c54d47b3602c89e3b2
outcome_bundle_id = 17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0
package_id = pkg_ma_8ec5e389fa2c5e484a1ac7e9
manifest_sha256 = f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016
style_profile_id = short_rebound_pkg_ma_8ec5e389_v1
feature_schema_version = advisory_feature_schema_v1
feature_count = 103
candidate_semantics = OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2
decision_range = 2024-07-04..2026-03-10
data_cutoff = 2026-06-30
```

request 同时保存 candidates/features、M3 split、QE daily/limit/suspend 输入的路径身份、SHA256、行数和代码 commit。该精确绑定只保证本次模型训练输入和 parent/outcome bundle 不被误换，不扩展成历史证据平台或人工审批。

## 7. M4 标签合同

### 7.1 下一交易日可执行性

对每个 decision date 的候选，以交易日历的下一交易日为 target date：

```text
entry_executable = 1
  iff target daily row exists
  and target is not suspended
  and target is not one-price limit-up
  and target open/pre_close are finite and positive
else 0
```

涨跌停、停牌和前收直接使用现有 QE 日线 Bin/sidecar 的同一回测语义，不重复建设 `stk_limit` 训练文件。缺失日历、缺失前收或非法 OHLC 属于 typed label failure，不能默认为不可执行。

### 7.2 可执行开盘缺口

仅对 `entry_executable=1` 的样本构建回归标签：

```text
entry_gap_return = target_open / decision_close - 1
```

训练标签只表达真实市场开盘价相对 decision close 的比例，交易成本不是价格，不得混入买入价格标签。`decision_close` 与 `target_open` 必须来自同一 QE 价格口径；在线转换时再使用未复权 CNY 价格。M3 outcome 的收益、MFE 和 MAE 继续使用其已冻结的 `OPEN_COST=0.000095 / CLOSE_COST=0.000595` 净收益口径。

## 8. 时间切分与防泄漏

M4 逐行复用 M3 已冻结的 train/validation/test decision-date membership 和 purge 结果，不允许重新随机切分、移动边界或在 test 上选择超参数。M4 标签只需要下一交易日，但仍沿用 M3 覆盖最长 20 日 outcome 的 25 日 purge，以保证 M4 与被引用的 M3 bundle 来自同一可比较样本体系。

target date 的开盘、停牌和涨跌停只用于离线标签。在线特征、在线价格边界和 UI 输出不得读取 target date 行情。

## 9. 真实模型合同

M4A 训练四个 LightGBM heads：

| model_name | objective | training rows | output |
|---|---|---|---|
| `entry_executable_probability` | binary | 所有具有确定标签的候选 | `P(entry_executable=1)` |
| `entry_gap_q10` | quantile alpha=0.10 | `entry_executable=1` | 开盘缺口 q10 |
| `entry_gap_q50` | quantile alpha=0.50 | `entry_executable=1` | 开盘缺口 q50 |
| `entry_gap_q90` | quantile alpha=0.90 | `entry_executable=1` | 开盘缺口 q90 |

禁止使用常数模型、规则函数、训练集分位数或随机数代替任一 head。binary 标签单类或可执行样本不足时，以 `ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING` 或 `ADVISORY_PRICE_RANGE_SAMPLE_INSUFFICIENT` 失败，不发布不完整 bundle。

quantile raw prediction 按排序后的 q10/q50/q90 输出单调化结果，并在 metrics 中记录原始 crossing 行数和比例；不能静默覆盖原始质量问题。

## 10. PriceRangeBundle

原子 bundle 至少包含：

- `manifest.json`
- `training_request.json`
- `feature_schema.json`
- `label_policy.json`
- `split.json`
- `metrics.json`
- `test_predictions.parquet`
- 四个非空 LightGBM model files

bundle canonical identity 覆盖全部成员 hash、parent bundle、outcome bundle、package/style、feature schema、label policy、split 和 code commit。先在同 root 临时目录完整写入并读回校验，再原子 rename；已有相同 identity 内容不一致时 fail-closed。

M4 binding 使用 package/manifest/style/parent bundle/outcome bundle 的 exact key。运行时禁止扫描 latest、跨 Program、跨策略包或跨 outcome bundle 套用。

## 11. 在线价格数据合同

在线转换从现有数据库 FeatureSource 的 decision-cutoff 数据读取：

- decision date 未复权 `close`，作为 `decision_reference_price`；
- decision date 当时可知的股票板块、ST 状态和价格 tick；
- target date 对应的交易日历日期，但不读取该日行情；
- Program 已冻结的 `review_policy.stop_loss_bps` 和 `trailing_stop_bps`。

下一交易日法规价格边界必须由 decision time 已知的板块/ST 属性和确定性交易所规则计算。不得通过读取 target date 的真实涨跌停价、开盘价或行情行来生成范围。属性不完整、价格口径不明或无法确定 tick 时，返回 typed M4 unavailable。

若 target date 存在 decision time 已知的除权除息等会改变法规参考价的公司行动，必须使用当时已知且可确定性计算的调整后参考价；当前数据源不能提供时返回 `ADVISORY_PRICE_RANGE_REGULATORY_BOUNDARY_UNAVAILABLE`，不得继续以 decision close 猜测。

全部 API 绝对价格使用未复权 CNY，并显式返回 `price_basis=UNADJUSTED_CNY_DECISION_CLOSE`、`tick_size` 和边界来源。训练时的复权比例不得直接显示成价格。

## 12. Contracts / 价格范围计算

### 12.1 买入参考范围

```text
raw_entry_price_qx = decision_reference_price * (1 + entry_gap_qx)
```

先单调化 q10/q50/q90，再将 q10/q90 裁剪到 decision-time 可计算的下一交易日法规价格区间。下界向下按 tick 取整，上界向上按 tick 取整，中位数按最近 tick 取整，并重新验证：

```text
regulatory_low <= entry_low <= entry_mid <= entry_high <= regulatory_high
```

若裁剪后区间为空、输入非有限数或排序仍不成立，只关闭该候选的 M4 结果并返回明确 reason code，不退化为 decision close 或固定百分比范围。

### 12.2 目标持有期限

价格范围使用 M3 `holding_period.mode_days` 作为目标 horizon，并要求它处于 M3 `holding_period.range_low_days..holding_period.range_high_days` 内。目标值必须属于 `{1,3,5,10,20}`；否则该候选 M4 unavailable。不得为价格范围重新发明持有期限模型。

### 12.3 止盈参考范围

在目标 horizon 读取 M3 的 `mfe_q50` 与 `mfe_q90`。由于 M3 MFE 是扣除开仓和退出成本后的净路径收益，必须先反解为市场价格收益：

```text
market_mfe_qx = max(0, (1 + OPEN_COST) * (1 + mfe_qx) / (1 - CLOSE_COST) - 1)
take_profit_low  = entry_mid * (1 + market_mfe_q50)
take_profit_high = entry_mid * (1 + market_mfe_q90)
```

按 tick 向外取整并验证 `entry_mid <= take_profit_low <= take_profit_high`。该范围表达模型在目标持有期限内的潜在最大有利幅度，不表示盘中必然可成交，也不替代人工决策。

### 12.4 止损参考范围与硬边界

在同一 horizon 读取 M3 的 `mae_q50` 与 `mae_q90`，两者按扣除开仓/退出成本后的非负最大不利幅度解释。先反解对应市场价格回撤，再与现有 Program 硬止损比较：

```text
hard_stop_drawdown = stop_loss_bps / 10000 when stop_loss_bps > 0 else null
market_drawdown_qx = max(0, 1 - (1 + OPEN_COST) * (1 - mae_qx) / (1 - CLOSE_COST))
model_stop_near = min(market_drawdown_q50, hard_stop_drawdown) when hard stop exists else market_drawdown_q50
model_stop_far  = min(market_drawdown_q90, hard_stop_drawdown) when hard stop exists else market_drawdown_q90
stop_loss_high = entry_mid * (1 - model_stop_near)
stop_loss_low  = entry_mid * (1 - model_stop_far)
```

当 `stop_loss_bps>0` 时，`stop_loss_low` 不得低于 `entry_mid * (1 - hard_stop_drawdown)`，即模型结果只能比既有硬止损更紧，不能更松。`hard_stop_price`、`stop_loss_low` 和 `stop_loss_high` 均向上按 tick 取整，使显示价格不会因舍入放宽风险边界。当 `stop_loss_bps=0` 时，`hard_stop_price=null`，保留模型 MAE 范围，不能错误压成零回撤。有效硬止损存在时验证：

```text
hard_stop_price <= stop_loss_low <= stop_loss_high < entry_mid
```

若反解后的市场回撤为零导致无法形成正宽度区间，保留显式 zero-adverse prediction 和单点值，不伪造区间宽度。

### 12.5 移动保护参考范围

仅当 `review_policy.take_profit_mode=trailing` 且 `trailing_stop_bps>0` 时，移动保护由模型幅度与现有规则叠加，且必须分开标识：

```text
activation_return_low/high = market_mfe_q50/q90
activation_low/high = entry_mid * (1 + activation_return_low/high)
trailing_drawdown = trailing_stop_bps / 10000
protective_floor_low/high = entry_mid * (1 + activation_return_low/high - trailing_drawdown)
```

`activation_*` 来源是 M3 MFE 模型；`trailing_drawdown` 来源是既有 review policy，不宣称由模型训练。保护地板使用与当前 `return_bps <= max_runup_bps - trailing_stop_bps` 一致的加法收益语义，不采用乘法近似。若计算结果低于已启用的硬止损价格，则展示值收紧到硬止损价格。`take_profit_mode=fixed` 或 `trailing_stop_bps=0` 时返回 `protective_price.status=NOT_APPLICABLE` 和空价格，不伪造移动保护范围。M4 只展示研究参考范围，不改变 `advisory_list_transition.py` 的真实淘汰/移动止盈判定。

## 13. Contracts / API 合同

现有 `GET /api/v1/advisory/programs/{program_id}/model-shadow` 响应增加平级 `price_range` 子信封：

```json
{
  "price_range": {
    "status": "EXPERIMENTAL_SHADOW",
    "calibration_state": "UNCALIBRATED",
    "price_range_bundle_id": "...",
    "parent_bundle_id": "...",
    "outcome_bundle_id": "...",
    "price_basis": "UNADJUSTED_CNY_DECISION_CLOSE",
    "candidates": [
      {
        "symbol": "001229.SZ",
        "status": "EXPERIMENTAL_SHADOW",
        "entry_executable_probability": 0.0,
        "decision_reference_price": 0.0,
        "entry_price": {"low": 0.0, "mid": 0.0, "high": 0.0},
        "take_profit_price": {"low": 0.0, "high": 0.0, "horizon_trade_days": 5},
        "protective_price": {"status": "AVAILABLE", "activation_low": 0.0, "activation_high": 0.0, "floor_low": 0.0, "floor_high": 0.0},
        "stop_loss_price": {"low": 0.0, "high": 0.0, "hard_stop_price": 0.0},
        "tick_size": 0.01,
        "regulatory_price_range": {"low": 0.0, "high": 0.0, "source": "DECISION_TIME_BOARD_ST_RULE"},
        "review_policy": {"stop_loss_bps": 800, "trailing_stop_bps": 700},
        "reason_code": null,
        "message": null
      }
    ],
    "reason_code": null,
    "message": null
  }
}
```

候选级错误不得导致整个 model-shadow HTTP 失败；但错误必须在候选 `status/reason_code/message` 和后端结构化日志中可见。bundle 或共同输入失败时返回顶层 M4 unavailable。M4 不得删改 M2/M3 字段，不改变既有响应状态码。

## 14. Contracts / UI 合同

现有 Advisory 页面在 M3 outcome 面板后新增“价格范围（实验影子）”区域：

- 逐候选展示可执行概率、买入、止盈、移动保护、止损和目标持有期限；
- 显示未复权 CNY、decision reference、tick、硬止损与规则/模型来源标签；
- 明确显示 `EXPERIMENTAL_SHADOW / UNCALIBRATED` 和学术研究属性；
- M4 unavailable 时展示 reason code/message，M2/M3 表格仍正常显示；
- 不提供买入、下单、同步模拟盘或应用止损等操作按钮；
- 延续现有响应式表格/面板风格，并验证 375x812、768x1024、1440x900 无溢出或遮挡。

## 15. 指标与真实训练验收

M4A 必须保存 test-only 指标：

- executable：logloss、Brier、ROC-AUC（双类时）、正类率、样本数；
- entry gap：q10/q50/q90 pinball loss、q10-q90 empirical coverage、raw quantile crossing 数量/比例；
- 价格转换：有限值率、单调率、tick 合规率、法规边界合规率和候选完整率；
- 研究诊断：entry band 与后续 target open 的覆盖；TP/SL 分别与日线 high/low path 的触达率，但不得推断同日盘中触达先后。

指标差不能静默丢弃，也不能冒充已校准；首个真实 bundle 只标记 `UNCALIBRATED`。除非模型无法训练、产物损坏或合同不完整，指标本身不作为新增发布审批门槛。

`entry_executable_probability` 只作为研究信息展示，不设隐藏阈值、不筛除候选、不改变 Top5，也不作为 M4 bundle 的激活门槛。

## 16. 错误与日志

至少定义并测试：

- `ADVISORY_PRICE_RANGE_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`
- `ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH`
- `ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING`
- `ADVISORY_PRICE_RANGE_SAMPLE_INSUFFICIENT`
- `ADVISORY_PRICE_RANGE_DECISION_PRICE_UNAVAILABLE`
- `ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE`
- `ADVISORY_PRICE_RANGE_REGULATORY_BOUNDARY_UNAVAILABLE`
- `ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH`
- `ADVISORY_PRICE_RANGE_PROJECTION_INVALID`
- `ADVISORY_PRICE_RANGE_INFERENCE_FAILED`

日志只记录 program、decision/target date、bundle identities、候选计数、reason code、耗时以及 crossing/clipping/invalid 计数；不得输出模型文件内容、完整特征矩阵、秘密或逐行无价值日志。

## 17. 计划变更范围

预期新增：

- `backend/services/advisory_model_first/price_range_contracts.py`
- `backend/services/advisory_model_first/price_range_labels.py`
- `backend/services/advisory_model_first/price_range_training.py`
- `backend/services/advisory_model_first/price_range_bundle.py`
- `backend/services/advisory_model_first/price_range_pipeline.py`
- `backend/services/advisory_model_first/price_range_runtime_bundle.py`
- `backend/services/advisory_model_first/price_range_inference.py`
- 对应 `backend/tests/advisory_model_first/test_price_range_*.py`

预期修改：

- `backend/services/advisory_model_first/model_inference.py`
- `backend/services/advisory_model_first/errors.py`
- Advisory model-shadow API schema/typing（若当前 router 无显式 schema，则不为形式统一扩大重构）
- `frontend/src/lib/api/advisory.ts`
- `frontend/src/app/paper-v2/advisory/page.tsx`
- `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`
- Advisory model-first nox/session、ownership 和 runtime target catalog 的必要声明

禁止修改 Selection、StrategyPackage、Paper、模拟盘、QE、Historical Range 或共享推理基础设施。若实现发现必须修改这些受保护模块，应停止并报告设计缺口，不得静默扩大范围。

## 18. Implementation Plan / 实施顺序

1. M4A：contracts/labels/split reuse/trainer/bundle/pipeline 与定向测试。
2. 在源码提交对应的 WSL worktree 执行真实四头训练，读回 bundle、test predictions、指标、RSS 和 wall time。
3. M4B：exact loader、在线价格源、projection、model-shadow isolation 与 API 测试。
4. UI 类型、面板、错误态和三视口 Playwright。
5. 完整 Advisory model-first 直接依赖测试、F2 validator、ownership、lint 和 DESIGN-COMPLIANCE-001 审核。
6. 源码合入后由用户重启，再执行真实单/原生多 Alpha 只读 readback；训练、合入、重启和 readback 分开报告。

不得在上述步骤之间插入历史证据、旧数据清理、通用性平台或额外门禁工作。

## 19. Verification Plan / 验证方案

- label：交易日、正常开盘、停牌、一字涨停、缺失行情、成本、复权比值和 typed failure。
- split：逐行等于 M3 membership，purge/test 不漂移。
- trainer：真实 LightGBM 小矩阵、四个非空 model、单类/少样本 failure、quantile crossing 记录。
- bundle：原子发布、canonical identity、四模型完整性、parent/outcome exact binding、tamper/readback/path containment。
- projection：未复权 CNY、法规上下限、tick rounding、quantile monotonic、M3 horizon、MFE/MAE、硬止损不放宽、规则来源标识。
- isolation：M4 failure 不改变 M2/M3、候选顺序和列表；不跨 Program/包/bundle 套用。
- boundary：Selection/Paper/模拟盘/QE/Historical Range 零写入和零反向依赖。
- WSL：环境身份、commit、四头、test 非空、RSS<8GB、小时级 receipt。
- API/UI：单 Alpha无 bundle typed unavailable，目标原生多 Alpha全量候选价格范围，三视口、console/network、无交易操作。

只运行变更模块和真实依赖模块测试；不以无关全库失败掩盖本阶段结果。

## 20. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-341 | M3 源码、bundle 和重启后真实 20 候选 readback 状态与蓝图一致 |
| F-342 | M4 只读取现有 QE 日线文件、M1 candidates/features、M3 split 和 exact parent/outcome bundle 训练 |
| F-343 | 下一交易日可执行标签完整覆盖停牌、一字涨停、缺失/非法行情且错误可见 |
| F-344 | 可执行开盘缺口按无交易成本的真实市场价格比例构建，不把手续费或复权价格冒充 CNY 价格 |
| F-345 | 1 个 binary 与 3 个 quantile 真实 LightGBM heads 均非空训练和预测，无常数/mock fallback |
| F-346 | M4 逐行复用 M3 split/purge，test 不参与选择且无 target 行情泄漏 |
| F-347 | PriceRangeBundle 原子发布、成员 hash/readback 和 parent/outcome exact identity 完整 |
| F-348 | WSL `rdagent-gpu` 真实训练、峰值 RSS<8GB、目标小时级且不新建缓存/证据平台 |
| F-349 | 在线只读数据库 decision-cutoff 未复权价格和 PIT 属性；法规范围不读取 target 行情 |
| F-350 | 买入 q10/q50/q90 完成单调化、法规裁剪和 tick rounding，异常 typed unavailable |
| F-351 | M3 holding mode/range 决定目标 horizon，不新建或伪造周期模型 |
| F-352 | 止盈/止损范围从 M3 净 MFE/MAE 反解双边成本后生成市场价格，且不放宽 Program 硬止损 |
| F-353 | 移动保护明确区分 MFE 模型激活范围和既有 trailing policy，不修改列表 transition |
| F-354 | M4 平级子信封逐候选错误可见，失败不阻断 M2/M3/规则荐股且无静默 fallback |
| F-355 | UI 展示实验影子价格范围、来源、单位和错误，不提供订单或模拟盘操作 |
| F-356 | 单/原生多 Alpha Program 独立 exact binding，不跨包、Program 或 bundle 套用 |
| F-357 | Selection、StrategyPackage、Paper、模拟盘、QE、Historical Range 零写入和零业务逻辑修改 |
| F-358 | 无简化版、placeholder、静默错误、业务语义漂移、角色审批、二次准入或未经确认门禁 |
| F-359 | 无 DDL/DML；训练、源码合入、binding、用户重启和 deployed readback 分开报告 |

## 21. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-341 | M3 blueprint/design ledger | `artifact:advisory-m3-runtime-readback-20260810`; `backend/tests/advisory_model_first/test_outcome_inference.py` | verified | none |
| F-342 | planned M4 request/pipeline | `backend/tests/advisory_model_first/test_price_range_contracts.py`; `backend/tests/advisory_model_first/test_price_range_pipeline.py` | design_ready | none |
| F-343 | planned `price_range_labels.py` executable label | `backend/tests/advisory_model_first/test_price_range_labels.py` | design_ready | none |
| F-344 | planned open-gap label and price-basis contract | `backend/tests/advisory_model_first/test_price_range_labels.py`; `backend/tests/advisory_model_first/test_price_range_inference.py` | design_ready | none |
| F-345 | planned `price_range_training.py` four real heads | `backend/tests/advisory_model_first/test_price_range_training.py`; `artifact:m4-wsl-training-receipt` | design_ready | none |
| F-346 | planned exact M3 split membership reuse | `backend/tests/advisory_model_first/test_price_range_pipeline.py` | design_ready | none |
| F-347 | planned `price_range_bundle.py` | `backend/tests/advisory_model_first/test_price_range_bundle.py` | design_ready | none |
| F-348 | planned Windows launcher + WSL entrypoint | `backend/tests/advisory_model_first/test_price_range_pipeline.py`; `artifact:m4-wsl-training-receipt` | design_ready | none |
| F-349 | planned realtime decision price/PIT attribute projection | `backend/tests/advisory_model_first/test_price_range_inference.py` | design_ready | none |
| F-350 | planned entry range projection | `backend/tests/advisory_model_first/test_price_range_inference.py` | design_ready | none |
| F-351 | planned M3 holding projection | `backend/tests/advisory_model_first/test_price_range_inference.py` | design_ready | none |
| F-352 | planned MFE/MAE price conversion | `backend/tests/advisory_model_first/test_price_range_inference.py` | design_ready | none |
| F-353 | planned protective overlay | `backend/tests/advisory_model_first/test_price_range_inference.py`; `backend/tests/advisory_model_first/test_price_range_boundaries.py` | design_ready | none |
| F-354 | planned `model_inference.py` isolated child envelope | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_model_shadow_api.py` | design_ready | none |
| F-355 | planned Advisory UI price-range panel | `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | design_ready | none |
| F-356 | planned exact M4 binding | `backend/tests/advisory_model_first/test_price_range_runtime_bundle.py` | design_ready | none |
| F-357 | planned changed-file and import-boundary review | `backend/tests/advisory_model_first/test_price_range_boundaries.py` | design_ready | none |
| F-358 | DESIGN-COMPLIANCE-001 review | `backend/tests/advisory_model_first/test_price_range_boundaries.py`; `artifact:m4-design-compliance-review` | design_ready | none |
| F-359 | rollout ledger | `artifact:m4-training-merge-restart-readback-ledger` | design_ready | none |

## 22. DESIGN-COMPLIANCE-001 设计审核要求

实现完成后必须逐项提供直接证据：

1. **无简化交付**：四个真实模型头、完整 bundle、在线投影、API/UI 和真实 readback 全部实现；不能用 POC、mock-only 或固定比例冒充。
2. **无静默错误**：标签、训练、bundle、价格源、法规边界、投影和 UI 的失败均有 typed reason、有效日志和隔离状态。
3. **无业务语义偏移**：M4 只作为 Advisory 影子子信封；不改变候选、排名、M3、列表淘汰、Paper、模拟盘、QE 或策略包。
4. **无未经确认门禁/审批**：不新增角色、审批、二次准入、收益阈值、人工 ACK 或发布门禁；模型指标如实展示，不作为隐藏阻断。

任一项缺少直接证据时，不得报告 `PASS`、可合入或 M4 完成。

## 23. Rollout / Rollback

- M4A 只新增独立 PriceRangeBundle，不修改 parent/outcome bundle；失败时不发布不完整目录。
- M4B 源码合入、M4 binding、用户重启和 deployed readback 是独立状态，不合并成一个“完成”。
- M4 unavailable 时移除或不配置 exact M4 binding，只关闭 `price_range`；M2/M3 和规则荐股保持原样。
- rollback 不删除模型产物、不修改数据库、不回滚 Selection/Program，只停止加载指定 M4 bundle。

## 24. 风险与处置

| 风险 | 处置 |
|---|---|
| entry executable 类别单一 | typed failure，不训练常数分类器 |
| quantile crossing | 显式记录 raw crossing，再确定性单调化 |
| 复权训练价格与未复权展示混淆 | 模型只预测比例；在线仅由 decision-cutoff 未复权价格转换 |
| 利用 target 行情计算法规上下限 | 只用 decision-time 板块/ST 属性和确定性规则；缺失即 unavailable |
| 模型止损比现有硬止损更松 | drawdown 取 `min(model_mae, hard_stop)`，测试硬边界 |
| 日线无法判断 TP/SL 同日先后 | 只报告各自日线触达诊断，不声称盘中顺序或可成交性 |
| M4 失败影响现有荐股 | 平级子信封隔离；M2/M3/规则结果保持不变 |
| 设计膨胀到平台建设 | 变更限于 Advisory model-first；历史证据、归档、ModelOps 延后且不属于 M4 |

## 25. Production Gates / 生产影响（无新增业务门禁）

以下字段只分别陈述交付影响，不是应用运行时审批：

```text
production_ddl_gate = noop
production_dml_gate = noop
production_backend_dependency_gate = noop
production_frontend_dependency_gate = noop
backend_restart = user-owned; only relevant after M4B source merge
runtime_activation = exact M4 binding; reported separately from training, source merge and restart
```

若实际实现确实需要依赖清单或数据库变更，必须停止并更新设计、说明真实必要性并取得用户确认；不得静默修改上述 `noop`，也不得据此发明应用内审批。

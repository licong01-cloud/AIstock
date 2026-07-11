# 板块轮动因子 Batch E 研究资产研发与入库记录

- 批次：`sector_rotation_batch_e_20260711`
- 关联：`sector_rotation_factors_develop_spec_20260710.md`、Batch A/B/C receipts
- 状态：5 个候选均已完成真实数据执行、输出契约验证和 research-only 入库。
- 目标：形成可执行、可追踪、可供 QE 选择的 research-only 因子资产；独立指标与相关性由用户后续单独触发。
- 生产边界：本批不做 production promotion，不启动 QE、模拟盘或实盘，不应用生产 DDL。

## 1. 研究资产准入边界

本批准入只要求：公式和名称冻结、PIT/无泄漏、真实代码执行、输出契约通过、exact/语义查重完成、风险元数据齐全。单因子 IC、HAC、相关性、成本容量和 QE 回测属于后续门禁，不作为 catalog 保存的前置否决条件。

## 2. 候选

| ID | factor | 冻结公式 | 当前方向解释 | catalog 处置 |
|---|---|---|---|---|
| E1 | `m_sector_breadth_persistence_10d_20d` | 个股满 20 日 MA 后，在同一连续 PIT L2 spell 内连续 10 日全部站上 MA20；按当日板块成员取比例 | train/validation 冻结为 `-1`；最近 6 个月发生方向翻转 | 已入库，ID `1528` |
| E2 | `m_sector_leadership_concentration_change_5d_20d` | 正相对收益权重的标准化 concentration=`1-entropy`，取同板块 5 日变化 | train/validation 方向不一致，保持 `UNKNOWN` | 已入库，ID `1529` |
| E3 | `m_sector_crowding_build_unwind_5d_20d` | 板块内 ret20 前 20% 与个股 turnover TsRank60 前 20% 的即时重合率，取同板块 5 日变化 | train 的 IC 与 RankIC 异号，保持 `UNKNOWN` | 已入库，ID `1530` |
| E4 | `m_sector_downside_resilience_breadth_20d` | 全 A 等权日收益为负时，板块成员跑赢全 A 的比例；20 日内至少 5 个下跌日的均值 | 预注册 `+1` 失败；两段均指向 `-1`，待 QE 冻结 | 已入库，ID `1531` |
| E5 | `m_sector_flow_price_divergence_10d_20d` | 板块 MA10(net_amt/amount) 截面 rank 减板块 ret20 截面 rank | 预注册并维持 `+1` | 已入库，ID `1532`；标注近邻 `m_industry_mf_large_divergence` |

## 3. 数据与失败策略

- 输入使用冻结 candidate snapshot：`daily_pv.h5`、`daily_basic.h5`、`sector_data.h5`、`static_factors.parquet`。
- `l2_code_id` 只作离散类别键；`-1` 不参与聚合或映射。
- 个股收益必须按 instrument 使用 `pct_change(fill_method=None)`。
- E1/E2/E3 的 sector rolling/shift 均在唯一 `(datetime,l2_code_id)` 面板上完成。
- sector 原生字段若同一 `(datetime,l2_code_id)` 出现冲突，E5 loud fail。
- 所有输出为单列 `(datetime,instrument)` MultiIndex，无重复、无 NaN。

## 4. 评估与选择偏差

公式由 Batch A/B/C 和 2026-03-30～2026-04-28 retention receipt 启发，因此既有 2024-02-01～2026-04-28 区间不再是全新 production holdout。本批可报告 train/validation、历史诊断窗口和近期科技 regime，用于研究资产说明；不得据此宣称生产晋级。

## 5. 真实执行与输出契约

所有因子在冻结 candidate snapshot 上串行执行成功，`result.h5` 均为单列 `(datetime,instrument)` MultiIndex，重复数、空值数和非有限值数均为 0。

| ID | 日期范围 | 交易日 | 行数 | 股票数 | 值域 |
|---|---:|---:|---:|---:|---:|
| E1 | 2018-09-10～2026-04-28 | 1848 | 7,186,056 | 4680 | `[0, 1]` |
| E2 | 2018-09-05～2026-04-28 | 1851 | 7,174,821 | 4680 | `[-0.9922, 0.9692]` |
| E3 | 2018-10-10～2026-04-28 | 1832 | 7,144,292 | 4680 | `[-0.4, 0.4]` |
| E4 | 2018-08-14～2026-04-28 | 1843 | 7,145,446 | 4680 | `[0.0833, 1]` |
| E5 | 2018-08-29～2026-04-28 | 1846 | 7,189,834 | 4689 | `[-0.9924, 0.9924]` |

## 6. h20 训练/验证与确认性诊断

标签统一为 `T21T1`，horizon 20 日，HAC lag 19。方向未知的因子只有在训练与验证的 IC、RankIC 四项符号一致时才冻结。训练期为 2018-08-01～2022-11-30，验证期为 2023-01-02～2023-12-29；确认性诊断期只对 Stage 1 晋级的 E1、E5 打开，为 2024-02-01～2026-04-28。

| ID | train IC / RankIC | validation IC / RankIC | 冻结方向 | 诊断期方向 IC / RankIC | 解释 |
|---|---:|---:|---:|---:|---|
| E1 | `-0.0104 / -0.0128` | `-0.0333 / -0.0279` | `-1` | `+0.0260 / +0.0252` | 长周期反转证据一致，但近期发生 regime 翻转 |
| E2 | `-0.0012 / +0.0000` | `-0.0031 / -0.0017` | 未冻结 | 未打开 | 训练期近零且符号不一致，仅作为结构状态资产 |
| E3 | `-0.0009 / +0.0001` | `-0.0088 / -0.0074` | 未冻结 | 未打开 | 训练期 IC/RankIC 背离，仅作为拥挤状态资产 |
| E4 | `-0.0130 / -0.0080` | `-0.0204 / -0.0159` | 预注册 `+1` 失败 | 未打开 | 可在 QE 中验证反向风险/拥挤解释，不直接改方向宣称通过 |
| E5 | `+0.0150 / +0.0154` | `+0.0173 / +0.0147` | `+1` | `+0.0184 / +0.0191` | 三段同向，是本批最稳定的单因子候选 |

这里的“未打开”只表示没有把确认性测试用于未晋级候选，并不表示不能入因子库或不能进入 QE。因子库准入是研究资产准入，不是生产准入。

## 7. 最近一年与科技抱团诊断

以下均为冻结方向调整后的 stock-mapped IC。核心科技申万一级口径为电子、计算机、通信、传媒；最近 1 个月只有 19 个交易日，数值只能作为状态警报，不能单独用于筛选。

| 因子 | 全市场近1年 | 近6月 | 近3月 | 近1月 | 核心科技近1年 | 核心科技近3月 | 核心科技近1月 |
|---|---:|---:|---:|---:|---:|---:|---:|
| E1 `-1` | `+0.0040` | `-0.0129` | `-0.0249` | `-0.1084` | `-0.0281` | `-0.1835` | `-0.2883` |
| E5 `+1` | `+0.0047` | `+0.0093` | `+0.0105` | `+0.0392` | `-0.0150` | `-0.0413` | `-0.1181` |

结论：

- E1 的长期板块广度反转关系在最近半年、尤其科技抱团样本内明显失效；它仍有研究价值，但 QE 应显式加入 regime gate，而不是使用静态 `-1`。
- E5 在全市场最近 6/3/1 个月保持正向，但科技子样本转负，说明资金价格背离在抱团行业内可能表现为趋势确认或挤兑风险，不能把全市场方向机械外推到科技板块。
- 两个因子都能“体现”近期科技抱团：体现方式是 cohort 条件表现显著偏离全市场，而不是简单得到更高 IC。

## 8. 因子库写入与 MCP 边界

- MCP `factor_library_plan_register` 对 5 个名称逐一完成 exact preflight，均为 `duplicate_check=clear`。
- 当前 MCP `register-confirmed` 受生产库缺失 `aistock_factor_catalog.updated_at` 影响，且该端点只接收摘要元数据，不适合作为本批完整源码的唯一写入路径。
- 本批沿用 `develop-factor` 既有 `ManualFactorService.save_factor` 产品路径，保存 `code_text`、`expression`、中文描述与资产文件，并执行 LLM 分类；未调用 `batch_compute_metrics`。
- MCP exact search 已复核 ID `1528`～`1532` 全部 `is_available=true`、`transformation_status=PENDING`，独立指标与相关性字段均为空。
- E5 与 `m_industry_mf_large_divergence` 是同 family 近邻但并非同公式：旧因子使用大单+超大单净额与当日涨跌幅排名的 5 日均值，E5 使用净流入占成交额 MA10 与 ret20。两者先并存，后续由增量相关性和 QE 决定保留、替换或组合。

## 9. 后续独立门禁

1. 用户触发官方独立指标计算。
2. 用户触发增量相关性与同 family 去重。
3. 通过研究筛选的因子进入 QE 对照回测。
4. 只有 QE、成本容量和稳定性通过后，另行讨论生产门禁。

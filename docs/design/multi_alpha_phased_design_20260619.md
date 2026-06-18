# 多 Alpha 闭环 详细分期实施设计

> 文档类型:详细设计 + 分期实施。日期 2026-06-19。作者:strategy session。
> 依据:`storage_signal_layer_unified_blueprint_20260618.md`(Rev 4)+ §13 基线扫描(已读源码,结论坚实)。
> 关键决策:**多 Alpha 组合回测按新架构从头实现,不复用/不修旧 `MultiAlphaEngine`(手工组合,历史未跑通);仅复用 QE 标准回测引擎 + `strategy_packages_*` + prediction-store。**
> 顺序原则(用户 2026-06-19):**先基础架构,再首先实现「多 Alpha QE 实验回测」功能**,之后闭环到 paper/选股/荐股,最后补存储严谨化与滚动训练。
> 边界:本文是设计;实现按阶段拆 PR(worktree),禁碰 research-assistant,MCP-first,UI 沿用因子库/QE 演进风格,不静默错误。

---

## 0. 基线收口(已完成)
- 已读源码确认:params.pkl=可复用模型;qe_archive 表全 + `run_data_context` 含 `data_version_hash`/`dataset_snapshot_id`/`feature_snapshot_id`/`factor_cache_snapshot_id`/全窗口/pit_cutoff(vintage 齐);selection 已按 `manifest.alpha_components` 列表消费;候选→包晋升已通。
- 唯一真实缺失链:**包↔pred 链接(`prediction_ref`)**。
- 旧 MultiAlphaEngine:**弃用**(不验证不复用)。

## 1. 分期总览
| 阶段 | 目标 | 关键交付 | 闭环作用 |
|---|---|---|---|
| **P1 基础架构** | 让"多 Alpha 组合"有数据底座与结构 | 预测入库 + `prediction_ref` + 多 Alpha 表结构 + 最小 sha 完整性 | 前置 |
| **P2 多 Alpha QE 实验回测(首功能)** | 选包→权重→组合回测→组合指标→多 Alpha 候选包 | 新组合引擎 + 组合实验 + 多包产出 + compose UI | 核心 |
| **P3 组合晋级 + 入 paper/选股/荐股** | 闭合多 Alpha 全链 | 多包晋级门 + 下游验证 | 闭环 |
| **P4 存储严谨化** | 删 workspace / 严格 DB-文件 | label 去重 + GC + registry 门控 + 对账 + 冷热分层 | 生产化 |
| **P5 滚动训练 B + 荐股优化** | 上线防衰减 + 客户稳定列表 | champion/challenger + 稳定列表 + 止盈止损标定 | 运营 |

---

## 2. P1 — 基础架构(多 Alpha 数据底座)

**目标**:组合引擎能"按包取到各腿 pred",并有结构存放多 Alpha 包。

**P1.1 预测入库**
- 合并 #1237(自动入库 executor 透传)+ 用户重启 + **回填 R24 的 14 run**(pred/label/params 已确认存活)→ prediction-store 有 6 腿 pred。
- 验收:`prediction_store_get_pointer` 对 R24 run 返回 present;sha 可校验。

**P1.2 包↔pred 链接(`prediction_ref`)——补唯一缺失链**
- `strategy_package`(JSONB manifest + 提升列)新增 **`prediction_ref`**(prediction-store URI + sha256),晋升单包时写入,绑定其 store pred。
- 验收:任一单 Alpha 包可经 `prediction_ref` 拉到其 pred 且 sha 一致。

**P1.3 多 Alpha 数据结构(蓝图 §5.9)**
- `strategy_package`:加列 `alpha_mode`(single/multi 提升为列)、`signal_domain`、`display_name`/`legacy_name`、`data_vintage`、artifact `uri+sha256` 列。
- **新建唯一关系表 `strategy_pkg.strategy_package_components`**:`parent_package_id`(FK,multi)、`child_package_id`(FK,single)、`child_manifest_sha256`、`component_weight`、`score_normalization`、`position`;约束:子=single、深度=1、退役守卫。
- 验收:能创建一个 alpha_mode=multi 的包 + N 条组件边,FK/约束生效。

**P1.4 最小文件↔DB 完整性**
- pred/model artifact 的 `uri+sha256` 落 DB;**回测/组合加载前 verify-on-use,不符 fail-loud**。
- (label 去重、workspace GC、registry 全量化、对账 job → 延到 P4。)

**P1 命名**:按蓝图 §5.8(`单A·域·自定义·日期`)。

---

## 3. P2 — 多 Alpha QE 实验回测(首个功能,新架构从头实现)

**目标**:在 QE 实验框架内,选 N 个单 Alpha 包 → 选权重方案 → 组合回测 → 归档组合指标 → 产出多 Alpha 候选包。**只有经此回测、有 backtest evidence 的组合才可进 P3。**

**P2.1 新组合引擎 `multi_alpha_combiner`(后端服务,全新)**
输入:`[single_package_id…]` + `weighting_scheme` + 参数。流程:
1. 经各包 `prediction_ref` 从 store 拉 pred(sha 校验);
2. 对齐公共 (trade_date, instrument);按 scheme 对各腿 score 归一;
3. **加权 blend → combined_score**;
4. 写 `combined_prediction.pkl` → **喂 QE 标准回测引擎**(复用单 Alpha QE 回测路径:V25 执行 + topk25/nd2 + eval 口径)产出回测;
5. 组合指标(CAGR/MDD/Calmar/Sharpe/换手 + Top-K)→ 归档 qe_archive(`alpha_mode=multi` 的 run);
6. 失败 fail-loud,不静默。
> **不调用旧 MultiAlphaEngine**;blend 与编排是新代码;回测原语沿用 QE 标准引擎。

**P2.2 权重方案**(可选,内置多种)
- 等权 / IC 加权 / 风险平价(腿收益逆波动)/ **正交感知**(用 P3-A 矩阵下调高相关腿)。
- **防过拟合**:权重 walk-forward 在滚动窗拟合、样本外应用(非全样本拟合)。

**P2.3 选腿依据**:P3-A 正交服务(#1227)提供预测相关 + 持仓 Jaccard 矩阵。

**P2.4 多 Alpha 候选包产出**
- 组合回测达标 → 建 `alpha_mode=multi` 候选包:`alpha_components[]` 每项 `ref_package_id+权重+归一`(写 manifest + `strategy_package_components` 边表)、组合指标、`source_backtest_evidence`(组合回测)→ 冻结(manifest_sha256)。

**P2.5 MCP + UI**
- MCP-first:只读查询(组合候选/正交)+ 创建组合(确认门)。
- **UI 扩展 `quantevolver/compose`**(非独立模块,风格用 QE 演进基线):选单包(从策略包库 single 过滤)→ 选权重方案 → 跑组合回测 → 看组合指标/正交矩阵 → 晋升多 Alpha 候选包。

**P2 验收**:从 6 个单 Alpha 包选若干 → 组合回测出 CAGR/MDD/Top-K + 归档 → 生成一个多 Alpha 候选包(含组件边、组合指标、冻结 sha)。

---

## 4. P3 — 组合晋级 + 进入 paper/选股/荐股(闭合)

**P3.1 多包晋级门**:扩 `asset_eligibility`/asset_checks——校验 ① 组合回测证据存在;② 组件 `ref_package_id` 均有效且子=single;③ `child_manifest_sha256` 钉死一致;④ 文件↔DB sha 一致。不过门禁入下游。
**P3.2 下游验证(零改契约)**:多 Alpha 包经 governance 晋级 → selection_center 选股(已按 alpha_components 列表消费)/ advisory 荐股 / paper 模拟盘,跑通验证。
**P3.3 单 Alpha 模拟盘**:单包直接进 paper(已具备,补验证)。
**验收**:一个多 Alpha 包跑通 选股 + 荐股 + 模拟盘;一个单 Alpha 包跑通模拟盘。

---

## 5. P4 — 存储严谨化(MVP 闭环后并行)
- label:按 (定义,horizon,universe) canonical 追加序列 / 按需重算(零 per-run);
- workspace GC:引用守卫 + sha 校验 + 宽限期回填 + 冷盘(E:)归档;非候选直接删;
- registry:候选及以上自动注册完整模型(params.pkl);
- 文件↔DB 对账 job + 资格门一致性检查;
- 冷热分层:X(M.2)放 store/registry/MLflow(M4)/共享 label + WSL 文件挂载。

## 6. P5 — 滚动训练 B + 荐股下游优化
- 场景 B:调度重训 challenger → 样本外门 → 晋升 champion(信号层热切换)→ 回滚(registry 版本化);
- 荐股稳定列表(缓冲带+最短持有+日换上限+分数平滑+核心/观察两层,阈值回测标定);
- 止盈止损/追价 数据驱动标定(V25+PriceGuard 回测,波动率/regime/信号域自适应)。

---

## 7. Cross-cutting(全程)
- **MCP-first**;UI 沿用因子库/QE 演进风格(蓝图 §12.1);eval 口径复用 #1184;DB 结构化权威 + 文件 sha 校验;禁 RA;worktree+PR;不静默错误;节点/并行度沿用。
- **owner**:strategy session 设计/评审/合并序;Codex 实现;用户合并审批 + 重启 + 节点操作。

## 8. 依赖与顺序
P1.1(#1237 合并+回填,在途)→ P1.2/1.3/1.4 → **P2(首功能)** → P3(闭环)→ P4/P5(并行补)。
P3-A(#1227)与 P1 并行;eval(#1184 已合)为口径基线。

## 9. 阶段验收 = 闭环里程碑
- **M1(P1 完):** store 有 6 腿 pred + 包可经 prediction_ref 取 pred + 多包表结构就绪。
- **M2(P2 完,首功能):** 选包→组合回测→组合指标→多 Alpha 候选包,全在 QE + compose UI。
- **M3(P3 完,闭环):** 多 Alpha 包跑通 选股/荐股/模拟盘;单包跑通模拟盘。
- **M4/M5:** 存储严谨化 + 滚动训练 + 荐股优化。

# 统一存储 + 信号层 + 策略包统一模型 架构蓝图(独立框架)

> 文档类型:设计蓝图。日期 2026-06-18。作者:strategy session。
> **Rev 4(2026-06-19):schema 精简定稿——单包阶段 0 新表(仅加列),多 Alpha 阶段补唯一 1 张 `strategy_package_components`;新增 §5.9.1 表数量/实施补表 + §5.10 文件↔DB 完整性对账;DB 结构化为唯一权威,manifest 降派生。**
> Rev 3(2026-06-19):新增 §5.8 命名规范 + §5.9 组合模型与数据库设计(实体表+关系表,消除自引用嵌套隐患)。
> Rev 2(2026-06-19):统一为单一一级概念「策略包」(`alpha_mode` 区分 单/组合);取消独立的"腿/Alpha 组件库"实体。
> Rev 1(2026-06-18):§11 六项决策确认。
> 状态:**独立蓝图,框架与决策已确认**;待与既有方案(P1-P6 / P2 / P3 / eval)整合为分期实施步骤(§10「待整合」,本文不做整合、不含代码改动)。
> 范围:QE 实验数据统一存储、可删除 workspace、策略包统一模型(每包独立指标)、信号层解耦、两类"补到最新"流水线。

---

## 1. 背景与目标

存储按历史演进堆叠(workspace / qe_archive / MLflow / prediction-store / 策略包),重复多、workspace 失控(≈903G,F: 余 221G);多 Alpha 组合与"补最新/滚动训练"缺统一数据契约与包级独立指标。

**目标**:① 零重复;② workspace 可删除(降级缓存);③ **策略包统一模型**(单/组合一个概念,每包独立指标,分类、查询、从 QE 添加、组合选择、生命周期迭代);④ 信号层解耦(多 Alpha 对模拟盘/选股/荐股零大改);⑤ 两类补最新(研究重训 A + 上线滚动 B,**B 本期纳入**)。

---

## 2. 各存储层数据清单(现状)

| 层 | 主要内容 | 权威源? | 量级 |
|---|---|---|---|
| 实验 workspace(qe_workspace/mlruns) | 训练模型、pred.pkl、label.pkl、params.pkl、positions/trades/return_curves、日志、分钟级中间件 | 否(中间产物) | **≈903G(主体)** |
| QE 数仓 qe_archive(PG) | run 指标(CAGR/MDD/IC/TopK)、配置、factor_set_hash、指针、分析视图、因子重要性/血缘/seed 稳定性 | **是**(指标/元数据/**实验史**) | 小(<2G) |
| MLflow | params/metrics/artifact 追踪 | **当前 deferred(M4)** | ≈0 |
| prediction-store(P2) | pred/params/label artifact + manifest(sha256)+ 指针 | **是**(预测/权重 artifact) | 现空,见 §4 |
| 策略包 strategy_packages | manifest(alpha_mode / alpha_components[] / alpha_combination_policy / factor_set / model_asset / 各策略·执行·风控政策 / metrics)、selection artifacts、validation runs、model_state、assets | **是**(部署契约 + **统一模型**) | MB 级 |

> 现状核实(2026-06-19):策略包库共 5 个,**全部 `alpha_mode=single_alpha`、component=1,尚无组合包**;manifest 已 `alpha_core_v1`(为多 Alpha 预留)。

---

## 3. 重复数据分析

1. **label(最大冗余)**:label = 未来 horizon 前瞻收益,**与模型无关、不同周期(horizon)不同**;同口径所有 run 相同却每 run 一份。→ §4.1 共享 canonical 序列 / 按需重算。
2. **pred / params**:workspace ↔ store ↔ registry,2–3 副本。
3. **指标/配置**:qe_archive(权威)↔ workspace 内嵌 ↔ 策略包内嵌。
4. **因子快照**:已共享 `single/`,冗余小(现状正确)。
5. **workspace 明细**(positions/trades/curves/日志):指标入 qe_archive 后基本可弃。

---

## 4. 目标统一存储架构

### 4.1 三个 canonical 库 + label/registry 策略
- **qe_archive** = 指标 / 元数据 / 血缘 / 指针 + **实验史(所有 loop 全进,见 §4.2)**。
- **prediction-store** = pred artifact(晋升为候选策略包的 run)。
- **model_registry** = 完整可再推理模型 artifact,按 (策略包, train_cutoff) 版本化。
- **label 策略**:**每 (label定义, horizon, universe) 一条 canonical 时间序列**,区间延长 **append**,run 引用切片;label 不在实时/信号路径(§7),纯评估侧,**可按需由价格重算** → **每 run 零 label 存储**。
- **registry 门控**:**只对 candidate 及以上的策略包注册完整模型**(非候选 run 不注册,见 §6)。

### 4.2 workspace 降级为缓存 + 全量入数仓 + 候选才进其他库
- **所有 loop → qe_archive 完整分析记录**(指标含 Top-K、配置、factor_set、因子重要性、seed 稳定性、交易统计摘要、血缘、**data_snapshot_hash/data_vintage**)→ 实验史可分析/查询,不依赖 workspace。
- **只有晋升为候选策略包的 run → prediction-store(pred)+ registry(模型)**;其余 loop 仅留 qe_archive。
- **候选判定时机**:候选常跑完后才定 → **workspace 留宽限期(默认 7–14 天)**,期内分析→晋升候选策略包→**回填**(#1237)pred/模型到 store/registry→再 GC。
  - 子策略:模型 artifact **严格候选门控**(大/贵);pred **倾向候选回填**;#1237 当前全量自动入库,可调为"候选回填"或"全量后过期清非候选 pred"。
- **GC 守卫**:被 strategy_package/paper/registry 引用 → 保护位;入库 sha256 校验通过才删;留宽限期;深度明细则压缩归档冷盘。
- **范围界定**:qe_archive 存可查询分析字段,非全量字节;逐笔/净值明细→冷盘归档或接受丢失;GC 前核对字段覆盖度。
- **可复现锚点**:非候选 run 重跑靠"配置+数据",故 qe_archive 必须记 **data_snapshot_hash**,否则数据更新后无法精确复现。

### 4.3 冷热分层(盘符已校正)
- **hot — F: SSD**:workspace 缓存(宽限期内)+ 近期数据。
- **hot — X: M.2 SSD**:**MLflow artifact(M4)+ prediction-store + model_registry + 共享 label** 等"除 workspace 外热数据";规划 **WSL 文件挂载 X**,网关按挂载点寻址(缓解 F: 95% 满)。
- **cold — E: 机械盘**:历史 run 明细压缩归档;**禁作热数据**。

### 4.4 统一寻址与网关
`prediction-store URI + sha256` = 跨模块唯一引用;qe_archive 存指针。后端 `model_store`/prediction-store 扩为**统一存储网关**,各模块只经网关读写,**禁直接访问 workspace**。

---

## 5. 策略包统一模型(唯一一级概念)

### 5.1 单一概念,两个正交维度
系统内**只有「策略包(Strategy Package)」一个一级实体**(消除"腿/组件库"等近似孪生概念的混淆):
- **维度① `alpha_mode`**:`single_alpha`(单)/ `multi_alpha`(组合)。
- **维度② `status`**:candidate → backtest_approved → selection_enabled → paper → retired(沿用现有)。

| 形态 | 定义 | 角色 |
|---|---|---|
| **单 Alpha 策略包**(single,component=1) | 自含 factor_set+model+pred,属一个信号域 | **基础构件**:可独立部署,**也可被组合引用** |
| **组合(多 Alpha)策略包**(multi,component=N) | `alpha_components[]` 每项 = **引用某单 Alpha 包(package_id+vintage)+ 权重 + 归一** + `alpha_combination_policy`;pred=合成、回测=组合 | **组合**:由若干基础包拼成 |

- **"Alpha 组件"不是一级实体**,仅是组合包 manifest 内部 `alpha_components[]` 的**引用条目**(package_id+权重)。
- **固定组合 = 冻结的多 Alpha 策略包**(有 manifest_sha256):同表、同生命周期、同 UI、同下游契约,**无需新命名**。

### 5.2 现有 manifest 已支持本模型(无需重建)
已具备 `alpha_mode`、`alpha_components[]`(alpha_id/component_weight/factor_ids/model_ref/score_direction/score_normalization/**metrics_snapshot**/lineage)、`alpha_combination_policy`、factor_set、model_asset、各政策、backtest_summary。当前 5 包均 single(component=1)。

### 5.3 需新增/增强字段(增量,向后兼容)
| # | 字段 | 加在哪 | 原因 |
|---|---|---|---|
| a | `signal_domain` | 单包(组合包=跨域) | 正交分类与选包组合 |
| b | metrics **补全** | 单&组合 | 现仅 ic/rank_ic/icir/sharpe/annual_return/mdd;**缺 Top-K 全套 + 多seed均值/CV**(§5.4) |
| c | `prediction_ref` | 单=自身pred / 组合=合成pred | 组合/回测取数 |
| d | `data_vintage`/`train_cutoff` | 包 + model_asset | 两类补最新 + 版本化 |
| e | champion/challenger 指针 | model_asset | 场景 B 热切换/回滚 |
| f | `alpha_components[].ref_package_id` | 组合包 | 组合按引用拼装 + 血缘 |
| g | `display_name` / `legacy_name` | 单&组合 | 人类可读名(§5.8)+ 兼容旧名 alias |

### 5.4 **每个策略包独立指标(必须记录 —— 组合与选择依据)**
每包、每 data_vintage、**多 seed 聚合(均值+CV+min/max)**(组合包记组合后指标):
- 收益/风险(主):CAGR、年化、MDD、Calmar、Sharpe、年化换手。
- Top-K(荐股口径):topk_return@20/@50、hit_rate@20/@50、decay、within_portfolio_rankic、dispersion@20/@50、observation_count。
- 信号诊断(辅):IC、ICIR、RankIC、RankICIR。
- 稳健性:seed 数、CAGR 均值/CV、种子彩票标记。
- 成对正交(包×包,仅单包间):预测 Spearman、持仓 Jaccard(P3-A),供组合选择。
- 落地:run 完成写 qe_archive(已有大部分),策略包**按包聚合多 run/多 seed** 落"包级指标快照"。

### 5.5 库 = 全部策略包(过滤即视图)
- `alpha_mode=single` → 可组合的**基础信号集**(取代原"组件库"概念,实为策略包过滤视图)。
- `alpha_mode=multi` → 已成型**组合**。
- 叠加 `status` / `signal_domain` 过滤。

### 5.6 生命周期(不固定;持续迭代)与候选门
- candidate → backtest_approved → selection_enabled → paper → degraded → retired;周期重验刷新指标;衰减降级/退役;组合随名册重优化权重。
- **晋升候选门**:多 seed CAGR 均值≥阈 + CV<阈 + 独立信号域 + 与既有单包正交(P3-A 低相关)。**此门决定哪些 loop 越过 qe_archive 成为(候选)策略包、进 store/registry。**
- 防膨胀:每个单包须独立信号域,拒高相关冗余。

### 5.7 录入与查询
- **从 QE 实验添加**:沿用 `strategy_packages_create_candidate_from_qe_loop/_experiment`,创建**单 Alpha 候选策略包**(快照配置 + pred 指针 + 包级指标 + signal_domain)。
- **组合创建**:选若干**单 Alpha 策略包** + 权重方案 → 生成**多 Alpha 候选策略包** → `--pred-backtest` 组合回测 → 晋升。
- **UI**:统一"策略包库"页(alpha_mode/status/signal_domain/指标 过滤、看正交矩阵、多选基础包组合、下钻源 run、从 QE 一键登记)。
- **MCP**:只读查询策略包库 + 登记/组合(写走确认门)。

### 5.8 命名规范(`package_id` 机器键不变;`package_name` 人类可读)
- **`package_id`**:沿用 `pkg_<hex>` 作唯一标识(机器用,不面向人)。
- **`package_name`**:**禁止裸 ID/hash/run_id/无意义串**;结构化、可读、可机读(`split('·')` 得字段)。
- **模板**:`[模式] · [信号域/主题] · [自定义名] · [日期] · [版本(可选)]`
  | 字段 | 取值 | 来源 |
  |---|---|---|
  | 模式 | `单A` / `组合×N`(N=alpha 数) | alpha_mode + component 数 |
  | 信号域/主题 | 单:6 域之一;组合:自定义主题 | `signal_domain` |
  | 自定义名 | 人给;缺省=`因子集×模型`简写 | 用户可只覆盖此段 |
  | 日期 | `YYYYMMDD`(data_vintage/created_at) | |
  | 版本 | 同名冲突 `v2/v3`,首版省 | |
- **示例**:`单A·基本面动量·FM12×TCN·20260617` / `单A·融资情绪·MARG10×LGBM·20260617` / `组合×6·核心多Alpha·20260619` / `组合×3·防御低回撤·20260620·v2`。
- **约束**:字段内禁 `·`;name ≤64 字,自定义名 ≤16 字;唯一性由 package_id 保证,name 不必全局唯一。
- **自动生成 + 覆盖**:从 QE 添加/组合创建时按模板自动产名;用户可覆盖"自定义名"段。
- **兼容现有(5 个 legacy)**:原名("qe_… 策略回测N")存为 `legacy_name`/alias,不强改;可选按元数据 backfill `display_name`(原名留 alias);新规范**只对新建包强制**,UI 同时显示 legacy。新增字段 `display_name`、`legacy_name`(见 §5.3)。

### 5.9 组合模型与数据库设计(实体表 + 关系表,消除自引用嵌套隐患)
**问题**:若仅靠 manifest JSON 在同一张表里埋"组合→子包"自引用,有隐患:① 悬挂引用(子包删/退役→引用断,无 FK);② 递归/成环(组合套组合→任意深度/环);③ 版本漂移(子包重训/换 champion→组合被静默改变);④ 单/组合不变量混杂;⑤ JSON 埋引用无法 FK 校验、反查贵。

**解法(既保"一个概念"又消隐患)= 一张实体表 + 一张组合关系表**:
- **`strategy_packages`(实体表,唯一概念)**:单/组合共用,`alpha_mode` 区分。
- **`strategy_package_components`(组合边表,新增)**:`parent_package_id`(FK→组合)、`child_package_id`(FK→单包)、**钉死的 `child_manifest_sha256`**、`component_weight`、`score_normalization`、`position`;唯一约束 `(parent_package_id, position)`。
- **硬约束**:① 子包必须 `alpha_mode=single`(CHECK/触发器)→ **禁止组合套组合,深度=1、天然无环**;② FK 完整性;③ **退役守卫**(单包被未退役组合引用时禁止/告警退役)。
- **冻结(固化)**:多 Alpha 包创建即冻结——pin 每子包 `(package_id+manifest_sha256+权重+归一)` + `alpha_combination_policy` + 组合回测证据 → 算父 `manifest_sha256` → frozen 状态;**子包后续变化不影响已冻结组合**(钉 sha);采用新版本=新建组合版本。
- **真相源分工(DB 权威)**:**DB 结构化(strategy_packages 列 + 组合边表)= 唯一权威**;manifest 文件 = 从 DB 渲染的**冻结派生快照**(仅复现/传输/审计,`manifest_sha256` 入库),**不可独立编辑、不作权威**。
- **区分**:单包 = 实体表中无组合边的行;组合 = 实体表一行 + 关系表 N 条边(+ `alpha_mode`/命名/asset_checks 分支)。

### 5.9.1 表数量(精简)与实施补表节奏
- **大多数是"列",不是"表"**:文件资产 = `uri+sha256` 成对列(model/pred/manifest…);包级指标 = JSONB 列 + 少数热字段(cagr/mdd/sharpe/topk_return_20/cagr_cv)提升为索引列;正交 = P3-A **按需计算,不持久化建表**;历史 = **版本行**(冻结不可变,re-vintage=新版本行),**不建 metrics 历史表**。
- **表数量**:现在(单包阶段)**0 新表**——仅在 `strategy_packages` 加列(alpha_mode/signal_domain/display_name/legacy_name/data_vintage/各 uri+sha/champion 指针)。
- **实施补表**:**多 Alpha 实现阶段补建唯一 1 张关系表 `strategy_package_components`**(M:N 组合,FK+深度1+退役守卫)。**该表与多 Alpha 组合引擎(P3-B)同期交付,开发完成即可立即开始:多 Alpha 组合回测 → 组合包 → 模拟盘/荐股 验证。**

### 5.10 文件↔DB 完整性与对账(DB 权威,fail-loud)
- **内容寻址 + sha 列**:每个文件 artifact 在包行存 `uri+sha256`。
- **用前校验(verify-on-use)**:回测/paper/荐股加载任何 artifact 前比对文件 sha == DB sha,**不符硬失败、阻断使用**(禁静默)。
- **原子/outbox 写**:先写内容寻址 artifact 再单事务写 DB(记 sha);崩溃留可检测 marker。
- **对账 job**:周期校验 artifact 存在+sha 匹配、组合边==预期、无悬挂 → 不符**隔离+告警**。
- **资格门升级**:现有 `asset_eligibility`/asset_checks 扩入"文件↔DB sha 一致 + 组合一致";**不过门的包禁入 paper/荐股/被组合**。出现不对应 → 置 FAIL → 自动隔离 → 从 DB 权威重渲 manifest / 恢复 artifact / 标废,全程不静默。

---

## 6. 两类"补到最新"流水线(B 本期纳入)

### 6.1 场景 A — 研究期:数据集补到最新(离线重验)
用策略包 `canonical_config` 把结束日期延到最新,QE 重跑 → 新 vintage 的 pred/指标;因子按新日期重算、label 追加、快照延长;**不需旧模型(重训)**。刷新 §5.4 包级指标。

### 6.2 场景 B — 上线后:滚动训练(champion/challenger)
**自动调度**对部署策略包用最新数据重训 challenger → 近端样本外验证 → 过准入门晋升 champion(实盘信号热切换)→ 旧 champion 留存可回滚(MLflow Model Registry)。多出:调度器 + 自动准入门 + champion 指针 + 回滚 + 信号层热切换。每次重训新模型 artifact 进 registry(按 train_cutoff 版本化)。

### 6.3 registry 门控(决策 6)
库按 (策略包, vintage/train_cutoff) 版本化存:配置(重训)+ 完整模型 artifact(免重训扩展/B)+ pred(同区间组合回测)。**"扩到最新不重训"必须有完整模型 artifact**(仅 pred 不够)。**只对 candidate 及以上的策略包注册完整模型**;探索 run 只留 qe_archive 指标(+config),pred 随 workspace GC。

---

## 7. 信号层解耦(对下游零大改)

- 信号层 = prediction-store(各单包 pred)+ 组合引擎(P3-B 多权重融合)+ **合成包装器(输出多 Alpha 策略包)**。
- 单包与组合包**同结构**:combined score → 包装成 strategy_package(multi)。
- 下游(selection_center 选股 → advisory 荐股 → paper 模拟盘)**消费"策略包",不区分单/组合** → **零大改**。
- **label 不在此路径**:实时/模拟盘决策只用 score;label 仅评估/归因。
- **单 Alpha 模拟盘 = 跑单包(已验证,如 pkg_a2f5… 已 150 paper 组合);多 Alpha 选股 = 跑组合包**。

### 7.1 组合在 QE 实验中产生(只有回测过的组合才进 paper/荐股)
- 组合 = 选 N 个单 Alpha 策略包 → 选权重方案 → 加权 blend → **新建的 QE 多 Alpha 实验回测**(从头实现,**不复用旧 `MultiAlphaEngine` 手工组合**;复用的是 QE 标准回测引擎喂入组合 score)→ 产 QE 归档的**组合后指标** = 多 Alpha 包的 backtest evidence。
- **硬原则**:**只有经 QE 回测、有 backtest evidence 的组合**才允许晋升多 Alpha 包并进 paper/荐股(由资格门 `source_backtest_evidence` 强制)。
- **UI:扩展现有 QE `quantevolver/compose` 页**(非独立新模块):选单包 → 权重方案 → 跑组合回测 → 看组合指标 → 晋升;配合 §5.5 策略包库过滤视图。风格用 QE 演进页基线。

### 7.2 荐股下游优化(信号层之后,与多 Alpha 解耦)
- **稳定客户列表(降 churn)**:Top-20 加 **缓冲/迟滞带**(跌出 top-20 不剔、跌出 top-30 才换)+ **最短持有期** + **日换仓上限** + **分数平滑(EMA/多日)** + **显著性门**;客户面出**"核心列表(稳)+ 观察列表(候选)"** 两层 + 加入/剔除理由(可解释)。阈值**由回测标定**,非手拍。(机构 buffering 经验:换手 ~−50%、alpha 损失极小。)
- **止盈止损/追价 数据驱动标定**:现状 `price_guidance.py` 为**固定规则**(止损 600/400bps+波动率倍数 2.5、止盈 1200bps 且当前禁用、买入上限=信号收盘×(1+alpha预算)∧涨停),**非预测、未回测标定**。优化:用 V25(含真实涨跌停)+ PriceGuard 回测**标定**最优止盈止损/追价,并做 **波动率(ATR)+ HMM regime + 按信号域** 自适应;take_profit 据回测证据重启评估。最终委托价仍由 PriceGuard/执行层按当时行情确认。

---

## 8. 与现有代码的差距 + 修复方向(高层;分期待整合)

| 差距 | 现状 | 修复方向 |
|---|---|---|
| label 去重 | 每 run 一份 | canonical 追加序列 / 按需重算(改 model_store + 上传) |
| workspace GC | 无 | GC 服务 + 引用索引 + sha256 + 宽限期回填 + 冷盘归档 |
| 全量入数仓 | 指标/明细部分仅在 workspace | 确认 qe_archive 覆盖分析字段 + 记 data_snapshot_hash |
| registry 化 | 未自动;params 是否=完整模型存疑 | **候选门控**自动注册完整模型 |
| store 落库 | 现空(#1237 未合) | 合 #1237 + 重启 + 验证 |
| 策略包字段 | 缺 signal_domain/Top-K指标/pred_ref/vintage/champion/ref_package_id | §5.3 增量加字段 |
| 组合引擎/库 UI | 无组合包;无统一库 UI | 复用 MultiAlphaEngine(`--pred-backtest`+combiner)出组合包 + 统一策略包库页 |
| 滚动训练 B | 无 | 调度 + champion/challenger + 回滚 |
| X 盘热层 | 未纳入网关 | X(M.2)纳入热层 + WSL 文件挂载 |

---

## 9. 兼容性矩阵

| 模块 | 依赖契约 | 兼容 |
|---|---|---|
| 策略包选股(selection_center) | 策略包(单/组合同构)+ selection artifacts | ✅ 零大改 |
| 荐股(advisory) | 策略包 + 行情只读 | ✅ |
| 模拟盘(paper v2) | 策略包 + 执行策略 + 行情 | ✅(单包已跑;组合包同构;B 需信号层热切换钩子;label 不参与) |
| 存储 GC | 引用保护位 | ✅(GC 前晋升被引用 artifact) |

---

## 10. 待整合(确认后再做,不在本文)
- 并入 `multi_alpha_phased_implementation_plan_20260616`(P1-P6):补 P2.5(存储统一)+ P3.5(策略包统一模型/库 UI)+ P5.5(滚动训练 B)。
- 与 P2 doc:label 去重 + registry 候选门控为增量。
- 与 P3 正交(#1227)+ P3-B 组合:组合引擎产出多 Alpha 策略包。
- 与 eval(#1184):包级指标复用其 Top-K/CAGR/MDD 口径。
- 多 Alpha 组合回测**按新架构从头实现**(旧 `MultiAlphaEngine` 手工组合历史未跑通,不复用);复用 `strategy_packages_*`(候选→包)+ QE 标准回测引擎 + prediction-store。

---

## 11. 决策(Rev 2 已确认)
1. label:**共享 canonical 追加序列 / 按需重算,零 per-run 存储;不在实时路径(模拟盘不用);不同周期(horizon)各一份**。✅
2. workspace:**所有 loop 全进 qe_archive(实验史)**;**仅晋升候选策略包的 run 进 store/registry**;宽限期 + 回填 + GC;其余直接删;qe_archive 记 data_snapshot_hash。✅
3. **统一模型**:系统只有一个一级概念「策略包」,`alpha_mode` 区分 单/组合;**取消独立"腿/Alpha 组件库"实体**;"Alpha 组件"仅为组合包 manifest 内部引用;固定组合=多 Alpha 策略包,无需新名;复用扩展 `strategy_packages_candidates` + §5.3 增量字段。✅
4. 场景 B 滚动训练:**本期纳入**。✅
5. **X(M.2 SSD)= 热层**(MLflow/store/registry/共享 label;规划 WSL 文件挂载);冷归档用 E: 机械盘。✅
6. registry 模型 artifact:**只对 candidate 及以上策略包**,非全量。✅
7. **命名规范**:`package_id` 沿用 `pkg_<hex>`(机器键);`package_name` 必须人类可读结构化 `[模式]·[信号域/主题]·[自定义名]·[日期]·[版本]`,禁裸 ID;legacy 名保留 alias,新规范仅对新建包强制(§5.8)。✅
8. **组合数据库设计**:一张实体表 `strategy_packages` + 一张组合边表 `strategy_package_components`(pin child_manifest_sha256 + 权重 + position);子包限 single(深度=1 无环)+ FK + 退役守卫;消除自引用嵌套隐患(§5.9)。✅

---

## 12. UI 设计(现阶段暂缓;先功能后换肤)

> **现阶段决策(2026-06-19):暂不做前端模板/换肤;优先功能开发与设计。** 本节为未来项备忘。

### 12.1 现阶段对 Codex 的硬要求(立即生效)
- **今后所有新 UI 必须沿用现有界面风格**——以 **因子库(factor library)、QE 自动演进(quantevolver/evolution)** 页面为风格基线(布局/配色/组件/表格图表约定/三态),保持站内一致。
- 新页(多 Alpha 正交、策略包库、compose 组合页等)**照此基线开发**,不引入异构风格。

### 12.2 未来项(暂缓,后续再评估整体换肤/模板)
待功能稳定后再考虑是否切换到统一开源模板/设计系统;候选(MIT/Apache、Next.js 友好、数据看板适配),供未来对照:
- 看板/图表:**Tremor**(tremor.so)、**shadcn/ui**(ui.shadcn.com)、**Apache ECharts**(echarts.apache.org)。
- 综合后台:**Tabler**(tabler.io)、**Ant Design Pro**(pro.ant.design)、**Horizon UI**(horizon-ui.com)。
- 组件库:**Mantine**(mantine.dev)、**Ant Design**(ant.design)、**Refine**(refine.dev)。
- 重表格:**AG Grid 社区版**(ag-grid.com)。
- 迁移原则(未来):strangler 增量换皮、共享组件库、路由/数据契约不变(后端零影响)。

---

## 13. 闭环审计 + MVP 路径 + 基线核验状态(防空中楼阁)

### 13.1 闭环现状
- **单 Alpha 闭环:✅ 今天已通**(QE→晋升单包→paper/选股/荐股;`pkg_a2f5…` 已 150 paper 组合为证)。
- **多 Alpha 闭环:未闭合**,缺中段:`预测入库 → 包↔pred 链接 → P3-A 正交 → P3-B 组合引擎 → 多包表示 → 多包晋级门`,跑通后自动接现有 paper/选股/荐股。

### 13.2 多 Alpha MVP 最小闭环(有序)
1) **预测入库**(合 #1237 + 重启 + 回填 R24)→ 2) **单包补 `prediction_ref`**(包↔pred 绑定,**易漏的隐藏环**)→ 3) **P3-A 正交实测**(#1227)→ 4) **P3-B 组合引擎 + 多包表示**(唯一新表 `strategy_package_components`)→ 5) **多包晋级门**(组合回测证据 + 组件完整性)→ 6) 接现有 paper/选股/荐股(零改)。地基(GC/label去重/registry门控/整合/滚动训练 B/荐股优化)MVP 闭环后并行补。

### 13.3 基线核验状态(已扫码 ✅ / 待扫 ⬜)
**已扫码确认(本轮已读源码,非推断)**:
- 上传机制 `qe_prediction_store_client.py`、callback base `callback_urls.py`、远端 **SSH 执行** `node_execution/config_composer`;
- eval Top-K 计算 `read_exp_res.py`、P3-A `orthogonality.py`、荐股定价 `price_guidance.py`(固定规则非预测);
- `MultiAlphaEngine`(reuse_prediction + `--pred-backtest` 已有件;reuse_model **未实现**);
- **策略包真实 schema = PG JSONB**:`strategy_pkg.strategy_package` + `candidate_strategy_package`(`manifest_json`/`factor/model/strategy_manifest_json`;source_type∈{qe_experiment, qe_evolution_loop, candidate_strategy_package});**组合现埋在 manifest_json JSONB**;
- 包多 Alpha 骨架(`alpha_mode`/`alpha_components`/`alpha_combination_policy`,现 5 包全 single)。
> **修正**:manifest 是 **DB 内 JSONB**,不是文件;§5.9/§5.10 的"DB 权威"=JSONB+结构化列权威,需 sha 校验的是被引用的 **model/pred 文件 artifact**。组合边表是把 JSONB 里的引用**外提为带 FK 的关系行**。

**基线扫描结论(2026-06-19 已读源码,现状↔目标)**:
| 项 | 扫描结论(证据) | 对计划的影响 |
|---|---|---|
| ① params.pkl=模型? | ✅ **是可复用模型**(`backtest-only` 靠 `source_model/params.pkl` 跳训练直接回测/推理) | "免重训扩展"成立;registry 化=存 params.pkl(待确认 NN/树两类均覆盖) |
| ② selection/paper 读包 | ✅ 读 `manifest.alpha_components`(列表)+ manifest_sha256;`load_source_for_strategy_package` | **多包(N 组件)天然可被迭代消费 → 下游零改可信** |
| ③ candidate→package 晋升 | ✅ **已存在**(C_FundVal 已建;MCP create_candidate_from_qe_loop/create_from_candidate) | 单包来源已通;多组件 manifest 构建=P3-B 新增 |
| ④ 多 Alpha 组合回测引擎 | 旧 `MultiAlphaEngine`(手工组合)虽 wired 但**历史多次尝试失败、从未跑通** | **决策:不复用、不验证旧代码;P3-B 按新架构从头实现**(新 QE 多 Alpha 实验回测,见详细设计) |
| ⑤ qe_archive 全量分析 + vintage | ✅ 表很全 + **run_data_context 含 `data_version_hash`/`dataset_snapshot_id`/`feature_snapshot_id`/`factor_cache_snapshot_id`/qlib_dataset_version/全窗口/pit_cutoff** | **删 workspace 后分析 + 精确复现锚定齐全**(vintage 已有,无需新增) |
| ⑥ 包↔pred 链接 | ❌ **无 `prediction_ref`/pred 指针**(包只引模型/因子) | **真实缺口(隐藏环)**:需给包加 `prediction_ref` 绑定其 store pred |

**净结论**:6 项中 **5 项已存在/可行**(params=模型、qe_archive 全 + vintage 齐、selection 迭代 alpha_components、候选→包已通、qe backtest 引擎可喂组合 pred);**仅 ⑥ 是真实缺失链**;**④ 旧 MultiAlphaEngine 不复用,P3-B 新建**。**计划有坚实代码基线,非空中楼阁;基线核验已收口,进入详细分期设计**(见 `multi_alpha_phased_design_20260619.md`)。

---

## 附录 A — 后续演进方向(参考备忘,2026-06-20)

> 性质:**非本期承诺范围**,是 MVP 多 Alpha 闭环(§13.2)跑通后并行/后续可排的演进 backlog。来源:R24 回填后基于 QE 数仓 `promotion_candidates` 的筛选讨论。排期时各自走 QE 方法论 + eval 口径(#1184)+ 节点/并行度约定。

### A.1 两道门的区分(保留门 ≠ 部署门)
回填/保留与晋升部署是**两件事,用两道门**,不可混用:
- **保留门(防数据丢失,宽)**:`cagr_mean >= 0.6`(沿用 CAGR 下限)即保留其全部存活 seed 的 pred/params/label,**不卡 CV/Sharpe**。理由:每个 seed 的 pred 都是有效集成成员;高方差常可降(见 A.2)。
- **部署门(晋升为生产腿,严)**:`passes_gate=true`(CAGR≥0.6 ∧ MDD≥-0.2 ∧ CAGR_CV≤0.15 ∧ 无过拟合)且 `Sharpe_mean>=2.0`。
- **推论**:高均值 / 高 CV 的"彩票"配置**不丢**(保留门收),但**不直接部署**(部署门拦);它标记一个值得补 seed 验证的方向。

### A.2 种子增强轮(SEED augmentation)
**动机**:5-6 个 seed 时 CV 本身是噪声估计,且"少 seed 集成"天然高方差。数仓实证:同因子集换模型 CV 天差地别(`ba2668`:TCN CV=0.074 vs LSTM CV=0.169),seed 多的配置(11 seed)CV 稳定落 0.05~0.12。
**方法**:对"保留门内、Tier-B(高均值但高 CV / seed<6 / Sharpe 略低)"配置补 seed 到 ~10-12,重测均值与 CV。
**判定**:CV 随 n 收敛 → 方差可降,留作候选腿(种子集成部署);CV 不降 → 真不稳,淘汰。
**评估口径**:锚定**跨 seed 均值 + 种子集成**,严禁用"最佳单 seed"(彩票不可部署)。
**候选来源**:回填普查报告里的「Tier-B 建议补 seed」清单(例:`ba2668`×LSTM 0.863/CV0.169、`d6c9ed`×LGBM_golden 0.675/CV0.176 等)。

### A.3 H10 周期分散腿(暂记 R25)
**背景**:h10 历史上曾是 QE 年化收益最高方向;近轮(R17~R24)为换手/成本控制 + n_drop/topk 线统一钉死 **h20**,**h10 被搁置(非因其差)**。
**对多 Alpha 的价值**:h10 腿与 h20 腿**周期正交**(持有期不同=结构不同的信号)→ 潜在分散化腿,而非冗余。
**方法**:① 用当前 eval 口径**净成本**复跑最佳 h10 factor×model(h10 换手更高,必须净成本);② 测 h10 腿与 h20 腿的预测相关 / 持仓 Jaccard(预期更低=分散);③ 若提升组合**边际 Sharpe**则纳入为周期分散腿。
**组合注意**:h10 与 h20 **不在同一 blend 里裸混**(预测目标不同),在组合层作为**各自独立的腿**处理(各自再平衡周期),由组合引擎按 (trade_date, instrument) 对齐其分数。
**门槛**:必须**净成本下**仍达标 + 对组合有正边际贡献,否则不纳入。

### A.4 其他后续方向(占位)
- **腿去留 = 边际分散贡献**:用留一法(LOO)逐腿测"去掉它组合 Sharpe/Calmar 升还是降",≤0 即替换/砍;腿不是越多越好(2-3 条真正交常胜过多腿稀释)。
- **正交驱动选腿**:以 P3-A 预测相关 + 持仓 Jaccard 矩阵为选腿/换腿依据,而非单腿绝对收益。
- **新信号源接力**:MA1/MA2 sourcing 产出的新候选(Flow 微观资金流、FundVal 种子鲁棒冠军等)按保留门入库、按部署门晋升。
- **更多权重方案 / 兜底**:等权 / IC 加权 / 风险平价 / 正交感知 之外,若最优正交子集仍打不过最强单腿的风险调整后表现,允许结论"部署种子集成单腿 + 小卫星",不为"多 Alpha"硬凑。

> 排期原则:以上均在 **MVP 多 Alpha 闭环(§13.2)跑通后**评估优先级;每项立项时单独走设计 → worktree → PR → Tier2 审,**不在 MVP 关键路径上**。

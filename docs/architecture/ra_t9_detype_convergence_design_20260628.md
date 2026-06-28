# T9:去类型化收敛设计稿 — RA 处理逻辑向 Claude Code 模式靠拢

> 状态:设计稿,待用户确认后逐条派实现。作者:战略 session(Tier2)。日期:2026-06-28。
> 隔离 worktree:`F:\Dev\AIstock_worktrees\ra-t9-detype-design-20260628`(分支 docs/ra-t9-detype-design-20260628,从 origin/main=6344f80d)。不污染 main 根目录。
> 上游:B9 总纲(决策权交还模型,框架只保留审批门 + 反幻觉两道护栏)。T1-T5 + T7 + T8 已合并;本稿收 B9 总纲早标记的"去类型化收敛"终态。

## 0. 目标与判据
**目标**:让 RA 对任意提问都走**统一的、不分问题类型**的处理逻辑(= Claude Code 模式:模型看全部工具、自决调几个/几轮/要不要联网、自当质检;guard 只统一查"有据/不编造/标风险",根本不分问题类型)。

**判据(B9 总纲铁律)**:消除"按问题类型/措辞分类"的分类器。一个改动若引入或保留"识别某类问题/某类措辞"的规则=违背;若改成"对所有答案/所有结果一视同仁的机制属性判定"=对。

**两道护栏绝不动**:① 审批门(写/提交/训练/晋升/生产动作必须用户确认);② 反幻觉(有据/不编造/数字有源/禁占位符/标 not_verified)。

## 1. 现状盘点:7 个按问题类型分类的分类器 + 1 片关键词路由
(全部已代码核实,worktree=origin/main)

### 组A — guard 判定层(react_grounding.py)
| # | 分类器 | 行 | 关键词弹药 | 现在 gate 什么 | 问题 |
|---|---|---|---|---|---|
| A1 | `_is_future_question` | 1285 | future_answer_terms(未来/趋势/预测/forecast…) | 1335:命中才查方向预测护栏 | **漏词=护栏漏网**(真预测却没拦) |
| A2 | `_is_stock_depth_query` | 576 | STOCK_DEPTH_DIMENSION_TERMS + 深度/跌停三元组 | 682/1905:命中才强制多工具取证闸 | 换措辞=深度要求落空 |
| A3 | `_is_factual_list_query` | 1269 | FACTUAL_LIST/LOOKUP/JUDGEMENT_TERMS + top N/前N | 1341/1397:命中才允许清单形态、跳过综合闸、行级引用 | 换措辞误判 |
| A4 | `_is_information_query` | 892 | INFORMATION_QUERY_TERMS | 915/1759/1938:命中才走 web 兜底/信息类分支 | 换措辞漏触发联网兜底 |

### 组B — service 路由/分支层(service.py)
| # | 分类器 | 行 | 现在 gate 什么 | 问题 |
|---|---|---|---|---|
| B1 | `_is_mcp_tool_catalog_inquiry` | 5463 | 4 处:走 catalog 目录回答(T8 已加 grounded 前置) | 关键词路由;T8 已部分收 |
| B2 | `_is_stock_depth_analysis_request` | 5811 | 5 处:seed 全 7 工具 + max_tool_iterations 6→10 | 换措辞=不 seed/不加轮数 |
| B3 | `_is_qe_experiment_status_read_request` | 5900 | QE 状态读路由分支 | 关键词路由 |
| B4 | `_is_qe_draft_creation_request_text` | 7670 | 2 处:QE draft 安全回答 | 关键词路由 |

### 组C — tool_router.py 整片关键词预路由
~25 张关键词表(QE_RANK/LOCAL_DATA_*/STRATEGY_*/WRITE/PLAN/SEARCH…)+ `route_request/select_tool/score_domains`。B2/T4 后已降级为 **route_seeds(种子建议)**,模型可无视/追加(service.py:6933 模型自选了工具就清空 seeds)。是"建议"非"钉死",但仍是按措辞分类的一大片。

### 不是问题分类(保留,机制性,按结果结构/状态判)
`_is_success_result`/`_is_empty_success_result`/`_is_business_source_result`/`_is_read_only_result`/`_is_terminal_summary_result`/`_is_business_synthesis_summary`/`_is_insufficient_evidence_text`/`_classify_exception_reason` — 判工具返回结构/状态,非"用户问哪类问题",符合 Claude Code 机制化做法。**不动。**

## 2. 逐个收敛方案(每条:改成什么通用机制 / 是删是改 / 风险)

### A1 `_is_future_question` → **删除问题门,方向预测护栏对所有答案生效**
- 现状:只有"命中未来词"才查"有没有做方向预测"。漏词则真预测也不拦 = **护栏漏网**。
- 改:把"方向预测检测"升级为**对所有 evidence-required 答案一视同仁**的反幻觉子项 —— 任何答案出现未否定的方向预测 marker(`_has_unnegated_future_directional_marker`,T7 已建)即拦,**不再先问"是不是未来类问题"**。
- 收益:既去类型化,又**补强护栏漏网**(这条是去类型化里少有的"反而更严")。属反幻觉护栏增强,符合"护栏不削"。
- 风险:个别非未来类答案里出现"将上涨"等词(如复述历史"昨日涨停")可能误拦 → 用 T7 已有的 negation/上下文判定 + 仅对"预测语气"敏感降低误伤;回归断言覆盖"历史复述不误拦"。

### A2 `_is_stock_depth_query` → **删强制多工具闸,深度交模型+prompt**
- 现状:命中深度关键词才强制"必须调够 N 类工具",否则 guard 拦 `stock_depth_required_evidence_missing`。
- 改:删除 guard 层的 `_passes_stock_depth_required_evidence` 硬闸(同 T7 删 future 模板闸的套路);"深度问题该多取证"交还**模型 + prompt**(mode.analysis 已引导多维取证)。guard 只保留通用的"有据/不编造"。
- 收益:去类型化;模型强时自然多调,不靠关键词。
- 风险:删了确定性下限保障 → 模型偷懒只调 1-2 工具时不再被拦。**缓解**:不在 guard 拦,而在 prompt 强化"深度/多维问题逐维取证";真模型验证看取证充分度。**这是 T9 里风险最高的一条**(下限保障转嫁给模型),建议**最后做、单独 PR、真模型回归**。

### A3 `_is_factual_list_query` → **删类型分流,综合与否交模型**
- 现状:命中清单词才"允许清单形态、跳过综合闸、走行级引用"。
- 改:删 `_requires_synthesis_answer`/`_passes_multi_source_synthesis` 的"按问题类型决定要不要综合";综合/清单是**风格**,交模型+prompt。guard 只保留"行级数字有据"(对所有含数字事实的行一视同仁要 source/as_of,不分问题类型)。
- 收益:去类型化(T1 的过渡产物收敛)。
- 风险:模型对该综合的问题给了流水账 → 不再被 guard 拦。缓解同 A2,靠 prompt。

### A4 `_is_information_query` → **web 兜底改机制信号触发**
- 现状:命中信息词才走 web 兜底(B3 确定性兜底的触发条件之一)。
- 改:web 兜底触发条件从"问题是信息类"改为**纯机制信号**:本轮调了业务 MCP 工具 + 返回空/无证据 + 本轮没调过 external → 触发一次 external_research。不看问题措辞,只看"有没有取到证据"。
- 收益:去类型化;任何"该有数据却没取到"的问题都能触发兜底,不靠关键词。
- 依赖:与线B(真 provider)协同 —— 兜底真有意义的前提是 external_research 真能联网(否则仍 stub 降级)。**A4 排在线B 之后或同期。**

### 组B(service 路由)→ **降为 route_seeds 建议,不硬分支**
- B1/B2/B3/B4 的共性:用关键词决定"走哪个罐头分支/seed 哪些工具/加多少轮"。
- 改方向:与 T4(工具可见集=全只读域)一致,把这些"按问题类型预置"降为**对模型的种子建议**(seed 可加但模型可无视),**移除"命中关键词就强制走某分支/改参数"的硬控**。
  - B2 的"seed 全 7 工具 + max_iterations→10":改为对**所有**分析类对话放宽 max_iterations 上限(safety cap,T5 已是 24),工具 seed 作建议;不再"识别是深度问题才给"。
  - B1 catalog:T8 已加 grounded 前置;终态是 catalog 仅在"零业务证据 + 用户明确问能力"时作兜底(可进一步用机制信号替代关键词,低优先)。
  - B3/B4:QE 状态/draft 分支降为 seeds,模型自选对应工具。
- 风险:中。这些分支承载了一些确定性行为(如 QE draft 安全回答),贸然降级可能丢安全语义 → 逐个核"删了关键词后,审批门/安全回答是否仍由机制信号保证",**保住的走机制、丢不起的留**。

### 组C(tool_router)→ **保留为 seeds,长期可选淡化**
- 已是种子建议非钉死,危害最小。终态可逐步缩词表/降权,但**非本轮重点**;本轮先收 A、B 组。

## 3. 实施切片(逐片 Tier2,串行避 react_grounding/service stacked)
按"风险低→高、护栏增强优先"排:
- **T9-1(护栏增强,先做)**:A1 方向预测护栏去类型化 + 全局生效。这条是"删类型门 + 补护栏漏网",收益明确风险低。改 react_grounding,串行。
- **T9-2**:A4 web 兜底改机制信号触发(**依赖线B 真 provider 落地后更有意义**,排线B 之后)。
- **T9-3**:A3 清单/综合去类型化(综合交模型+prompt)。
- **T9-4(风险最高,最后)**:A2 删 stock_depth 强制闸(下限保障转模型)。单独 PR + 真模型回归。
- **T9-5**:组B service 路由分支降 seeds(逐个核安全语义)。
- 组C tool_router 淡化:本轮不做,记 follow-up。

每片:先注册 BUG+issue+allowed_write_scope;隔离 worktree;同改 react_grounding/service 的片**串行不 stacked**;真模型回归(国城矿业深度/QE排名/未来类/清单类/无数据,验"换措辞不失效"+"护栏未削")。

## 4. 绝不动(护栏)
- 审批门:APPROVE_* + 逐能力令牌 + 四重校验 + side_effect 分级 + 只读才自动执行。
- 反幻觉:silent_fallback=False;inline source/as_of 强制;数字必有据;禁占位符;禁内部行话;not_verified 标注。
- A1 改动是**增强**反幻觉(方向预测全局拦),不是削弱。

## 5. 验收(每片通用)
- **去类型化断言**:同一问题换 3+ 种措辞(不含触发关键词的说法)→ 行为一致(都正确多取证/都正确拦方向预测/都正确综合),证明不再靠关键词。
- **护栏未削断言**:方向预测仍拦、无源仍拦、占位符仍拦、审批门仍触发、写动作不自动执行。
- **真模型**:国城矿业深度(换措辞仍多工具+完整)、QE排名(放行+not_verified)、未来类(不预测)、无数据(诚实降级)。
- **无新分类器**:diff 里不得新增 `_is_xxx_query`,不得给现有分类器加词。

## 6. 风险总览
- 最大风险 = A2(删深度强制闸):确定性下限保障转模型,弱模型/波动时可能取证不足。**故排最后、单独 PR、真模型多轮回归;若误拒/取证不足率高,prompt 加强而非恢复关键词闸。**
- 与线B 协同:A4 依赖真 provider;其余与线B 文件零重叠(线B 不碰 react_grounding/service)。
- 整体哲学风险:去类型化把更多判断压给模型(DeepSeek)。B9 结论是"DeepSeek 接 Claude 框架基本够 + 保留两道护栏兜底",故 T9 是"尽量去类型化 + 关键处留护栏",非极端裸交模型。

---

## 7. T9-6:Skill 平权 — 让 LLM 像选工具一样自主选 skill(保留审批门)

### 7.0 动因(用户诉求)
用户要求:LLM 应清楚自己有哪些工具**和 skill**、能基于问题**自主选择** MCP 或 skill、未来新增 MCP/skill **由 LLM 自己判断**(免硬编码)。代码核实现状(2026-06-28):
- **MCP 工具侧**:T4(BUG-527)已达标 —— 整个只读域以 function spec(name+描述+schema)暴露,LLM `tool_choice="auto"` 自选;新增只读工具进 `TOOL_MANIFEST` 即自动可见,免改 service.py。
- **Skill 侧:落后一代**。skill **从不进 LLM 的 function-calling 列表**;触发靠 `runtime_context.yaml` 的 `natural_language_triggers` **关键词分类器** + `propose_skill_reuse` 显式 API,且必经 Action Proposal → 审批。新增 skill 要改 YAML 注册 capability(skill_refs + 关键词),**永不**进 LLM 可选集。
- 结论:skill 既不平权、新增又靠关键词硬注册 = 正是 B9 要消灭的"框架按关键词代选"。这是 T9 漏盘的一片,补为 T9-6。

### 7.1 目标态(对齐 Claude Code 的 skill 模型)
- **skill 平权进 LLM 可选集**:每个 approved skill 生成一个 function spec(用其 `description_for_llm` + "何时用"),与 MCP 工具同列暴露给 LLM,**LLM 基于问题自主决定要不要用某 skill**(推理选择,非关键词匹配)。
- **去关键词触发**:废除/降级 `natural_language_triggers` 关键词分类器作为 skill 触发的主路径;触发改由 LLM function-calling 决定(= 去类型化,同 T9 主线哲学)。
- **新增即可见**:skill 进 catalog(seed)即自动对 LLM 可见可选,免在 YAML 手写关键词触发。
- **审批门绝不动(护栏)**:LLM "选了某 skill" ≠ 直接执行。skill 的 `direct_execution_allowed=False` / `action_proposal_required=True` 保持;LLM 选中 → 生成 Action Proposal → preflight → **用户审批** → 才执行。即:**把"选择权"交还 LLM,把"执行权"仍锁在审批门后**。这正是 RA 相对 Claude Code/OpenClaw 的护栏优势(金融生产必需),不削弱。

### 7.2 与渐进式披露(借鉴 Claude Code)
- skill 描述常驻成本低(只放 name + 一句"何时用",~百 token 级),类似 Claude Code 的 L1 frontmatter;完整 skill 指令/步骤在选中后才注入(L2),避免全量灌上下文。
- 当前 RA 仅 6 个 skill,常驻全部描述零压力;此设计为未来 skill 增多预留。

### 7.3 实施(排在 A 组之后,单独 PR,串行)
- T9-6a:skill → function spec 生成器(复用现有 `description_for_llm`),并入 `_agentic_function_tools` 的可选集(与 MCP 工具同列,带 skill 标记)。
- T9-6b:LLM 选中 skill 的 native tool_call → 路由到 `propose_skill_reuse` 生成 Action Proposal(不直接执行),接现有审批门。
- T9-6c:废除 `natural_language_triggers` 作为触发主路径(降为可选 seed 提示,或删);新增 skill 进 catalog 即自动进可选集。
- T9-6d:回归断言 —— ① skill 换措辞(不含原关键词)LLM 仍能基于语义选中;② 选中 skill 必出 Action Proposal 且未经审批不执行(审批门未削);③ 新增一个 mock skill 进 catalog,无需写关键词即被 LLM 可见可选。

## 8. 明确边界:T9 全程不引入 RAG / 向量检索(对齐既有"长期记忆不是 RAG"原则)

**这是本设计稿的硬约束,防止误引入向量层:**

1. **长期记忆维持现状,坚决不用 RAG**。依据 `aistock_research_agent_console_design_20260520.md` §4.4「长期记忆不是 RAG」:RAG/向量检索只能辅助召回,不能作事实源/规则源/审批源/任务状态源。现实现 `memory_tree.py` + `graph_context.expand_neighbors` 是**纯确定性 SQL 树遍历 + 图边遍历**(按 `tree_path` 前缀 / 关系边,**无 embedding、无向量、无余弦相似度**)。项目级记忆(`project.*`)与个人习惯记忆(`personal.preference.*`/`personal.habit.*`)均树状结构 + 确定性检索。**T9 不碰记忆层,不引入向量。**

2. **工具 / skill 选择也不用 RAG / 向量**。外部框架(LangGraph bigtool 等)对工具做向量检索 top-k,是为解决**上千工具撞 128 上限**的过载问题。**RA 仅 283 工具 + 6 skill,量级远未到过载阈值**,DeepSeek 上下文可全量承载(T4 已全只读域暴露并跑通)。故:
   - 工具:维持 T4 的"全只读域 function spec 全量暴露 + LLM function-calling 自选",**不做向量检索子集**(向量检索 = 隐性分类,违背 T9 去类型化)。
   - skill:T9-6 同样"全量描述暴露 + LLM 自选",**不做向量检索**。
   - Claude Code 本身也不对工具/skill 做向量检索 —— 它是 frontmatter 全量常驻 + LLM 推理选。RA 与之同构。

3. **若未来工具/skill 增至数百上千**(目前没有),再评估"懒加载 schema"(选中才拉完整 schema,如 Tool Attention)这类**确定性**优化 —— 仍非向量召回事实。届时单独设计,不在 T9 范围。

**一句话:T9 把"选择权交还 LLM(function-calling,去关键词分类)",但既不碰确定性长期记忆树,也不引入任何向量/RAG 层。**

## 9. 产品对比与取长补短(附录,佐证选型)
| 维度 | Claude Code | OpenClaw | LangGraph 等框架 | RA 现状 | T9 目标 |
|---|---|---|---|---|---|
| MCP 工具选择 | LLM 推理(frontmatter 常驻) | LLM + Gateway | function-calling | ✅ T4 已达标 | 维持 |
| 工具过载处理 | 全量常驻(量级可控) | 文件化 | 向量 RAG(上千工具时) | 283 全量(可控) | 维持全量,不引向量 |
| Skill 选择 | LLM 像选工具一样选(meta-tool) | skill 原生调 MCP | — | ❌ 关键词分类器+审批 | **T9-6 平权** |
| 新增 skill 可见 | 丢 SKILL.md 即可见 | 文件化自动发现 | — | ❌ YAML 硬注册+关键词 | **T9-6 自动可见** |
| 自进化 | — | Skill Workshop 自动起草 | — | ❌ 无 | 远期(走审批) |
| 审批门/护栏 | 沙箱 | 签名 manifest+沙箱 | — | ✅ 审批门+反幻觉(更严) | **保留强化(RA 优势)** |

取长补短结论:**借 Claude Code 的 skill 平权 + 描述常驻(T9-6);维持 RA 已对齐的工具 function-calling(T4);保留并强化 RA 独有的审批门+反幻觉(金融生产必需);均不引入向量/RAG;OpenClaw 式自进化列为远期、必经审批。**

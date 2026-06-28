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

## 7bis. T9-7:能力/记忆调用去机械化 — 区分"能力询问"与"执行请求",自然语言触发记忆

### 7bis.0 动因(2026-06-29 真实失败实证)
用户问"**你是否可以记住我的待办**"(纯能力询问,尚未给内容),RA 回复:中英文混杂 + 机械追问"请提供 exact todo item / subject_key / memory_type / 需要你审批确认"。这是一次典型失败,根因经代码核实是**两个去类型化缺陷叠加**:

1. **能力询问被当成执行请求**:用户问"能不能记住"(询问功能是否存在),框架按 `memory.write_candidate` capability 的 `input_slots.required:[memory_type, subject_key, title]`(`runtime_context.yaml:815`)机械反问缺失参数,把"问功能"误判成"立即写入"。
2. **记忆写入靠英文关键词触发**:`MemoryCurator._extract_candidates`(`memory_curator.py:118-182`)只对**用户消息**做固定英文前缀扫描(`"project directive:"` / `"prefer"` / `"habit:"` / `"always/must"`)才提炼记忆候选。用户用**中文自然语言**"帮我记住待办"**不命中任何关键词 → 根本不会被记**;且只看用户消息、丢弃助手回复。
3. 附带:反问用了内部字段名(`subject_key`/`memory_type`)甩给用户,违反"禁内部行话"护栏精神;中英混杂违反全中文要求。

这三条 T9-1..T9-6 均未覆盖,补为 T9-7。

### 7bis.1 目标态
- **能力询问 ≠ 执行请求**:用户问"能不能/支不支持 X"时,LLM 自主回答"能,你把内容给我我就记",**不机械追问内部参数、不触发写操作**。是否执行由 LLM 基于对话判断,不由框架按 `input_slots.required` 代决。
- **`input_slots.required` 不作为框架强制追问依据**:LLM 拿到工具 schema 后**自己判断缺什么、用人话问**。缺内容就用中文人话问"要记什么",**绝不**把 `subject_key`/`memory_type` 这类内部字段名暴露给用户(由反幻觉"禁内部行话"护栏兜底)。
- **记忆触发去关键词化**:记忆提炼不再靠 `"prefer"/"habit:"/"always"` 等固定英文前缀。改为**机制/语义驱动** —— 由 LLM 在对话中识别"用户表达了需长期记住的偏好/习惯/项目指令/待办"并发起记忆候选(经审批门),中文自然语言"帮我记住…/我习惯…/这个项目要…"都能触发,不靠魔法词。curator 的关键词规则降为可选 seed 提示,不作为唯一提炼路径。
- **审批门/scope 不动(护栏)**:① project directive 仍走草稿+审批;② personal preference/habit 仍可直接 approved + resident 常驻;③ MCP 写 approved / high risk 仍必经 `_consume_approval_gate`。改的是"怎么触发和追问",不是"写入授权策略"。

### 7bis.2 实施(排在 A 组之后,可与 T9-6 并列,单独 PR)
- T9-7a:capability 调用层区分 inquiry vs execute —— 能力询问不触发 `input_slots.required` 追问、不发起写操作;由 LLM 决定是否执行。
- T9-7b:缺参数时改为 LLM 用人话询问(基于工具 schema 的 description),禁暴露内部字段名;接反幻觉"禁内部行话"+ 全中文。
- T9-7c:记忆提炼去关键词化 —— LLM 语义识别"需长期记住"的表达(中文自然语言)发起记忆候选,curator 关键词规则降为可选 seed;助手回复也可参与提炼(不止用户消息),但写入仍经审批门。
- T9-7d:回归断言 —— ① 问"能不能记住 X"→ 答"能,给我内容"不追问内部字段不写库;② 中文"帮我记住明天要复盘"→ 能正确发起记忆候选(走审批);③ 缺内容追问用中文人话无 subject_key/memory_type 字样;④ 审批门/scope 写入策略未变(project directive 仍 draft+审批)。

## 7ter. T9-8:记忆召回去关键词化 — 借鉴 Claude Code"索引常驻 + LLM 自选展开"(存储不变)

### 7ter.0 动因与定位
用户问:Claude Code 的"MEMORY.md 索引 + 每条 detail MD"方式 vs RA 的"DB 树状结构"哪个更适合长期记忆?结论(已代码核实双方机制):
- **存储层保留 RA 的 DB 树状,不换文件**。理由:RA 是金融生产助手,需要审批门(draft/approved)、审计(memory_access_log/use_count)、scope 隔离(project/personal)、版本失效(valid_to/supersedes)、知识图遍历(relations 表)、多会话并发持久 —— 这些 DB 行天然支持,memory.md 文件模型靠 git 兜不住。这是 RA 相对 Claude Code 的护栏优势,不丢。
- **召回层借鉴 Claude Code 的精髓**:Claude 的关键不是"用文件",而是 **L1 轻量索引常驻 + L2 命中才展开详情 + LLM 自主决定展开哪条**(渐进式披露)。RA 现在召回靠 `_TREE_KEYWORDS`(memory_tree.py:23-31,7 组关键词→分支)seed + 前缀匹配 —— **关键词 seed 不命中就漏召回**,正是 T9 要去的"按措辞分类"。
- 定位:**存储不动(DB 树状),只改召回的"关键词驱动"为"LLM 自选驱动"**。是 DB 树状(存储)+ Claude 式 LLM 自选召回(检索逻辑)的组合,非二选一。T9-7 修记忆写入触发,T9-8 修记忆召回触发,合起来记忆链读写都去关键词化。

### 7ter.1 现状(memory_tree.py 召回链,已核实)
`select_memory_branches`(memory_tree.py:34-93):① `list_records` 拉 approved 记忆(确定性 SQL);② `_seed_branches`(:64)用 `_TREE_KEYWORDS` 关键词命中分支;③ `_matches_branch_or_query`(:65)按 seed 分支前缀 + 词项子串匹配;④ resident 强制注入(:55);⑤ importance/recency 打分 + token 预算截断(:70-71)。**漏召回风险**:用户措辞不含 `_TREE_KEYWORDS` 关键词 → seed 落空 → 相关记忆拉不进(除非 resident)。

### 7ter.2 目标态(索引常驻 + LLM 自选展开)
- **L1 记忆索引常驻**:每轮给 LLM 一份**轻量记忆树索引** —— branch 路径 + 每条 fact 的一句话摘要(title/前 N 字),覆盖该 namespace/scope 下全部 approved 记忆(或按 importance 取上限,token 可控)。类似 Claude Code 的 MEMORY.md 索引"每条一行"。
- **L2 LLM 自选展开**:LLM 基于对话**自主判断**要展开哪几条 fact 的完整内容(通过一个确定性"展开记忆"调用按 memory_id/tree_path 拉全文),而非框架用关键词 seed 代选。展开是确定性 SQL 取数(无向量)。
- **去关键词 seed**:`_TREE_KEYWORDS` 关键词→分支映射降为**可选提示**(或移除),召回相关性不再由关键词命中决定,改由"索引常驻 + LLM 自选 + resident 强制"三者。
- **resident 不变**:personal directive/preference/habit 的 `resident=True` 仍每轮强制注入(等价 Claude"每会话先读"段),这是确定性护栏不动。
- **存储/审批/审计/图/scope 全部不动**:DB 树状 schema、approval_status、memory_access_log、relations 图遍历、valid_to 失效、scope 隔离 —— 零改动。只改"哪些 fact 进上下文"的选择逻辑。

### 7ter.3 不引入的东西(防误解,非新增开发项,仅澄清边界)
- 不改存储为文件;不引入向量/embedding(索引是确定性 SQL 取 title/摘要,展开是确定性按 id 取全文)。

### 7ter.4 实施(排在 A 组之后,可与 T9-6/T9-7 并列,单独 PR;只改 memory_tree.py + build_context_pack 召回侧 + 测试)
- T9-8a:构造 L1 记忆索引(branch + fact 一句话摘要,按 scope/importance 控规模),常驻注入上下文。
- T9-8b:提供确定性"展开记忆"调用(按 memory_id/tree_path 取全文),供 LLM 自选展开;接 use_count/last_used_at 审计(不变)。
- T9-8c:`_TREE_KEYWORDS` seed 降为可选提示或移除;召回相关性改由索引常驻 + LLM 自选 + resident。
- T9-8d:回归断言 —— ① 用户措辞不含任何 `_TREE_KEYWORDS` 关键词但语义相关 → 相关记忆仍能经"索引常驻 + LLM 自选展开"被用到(改前因 seed 落空而漏,改后不漏);② resident 记忆仍每轮强制注入;③ 存储/审批/审计/scope/图召回零行为变更(parity 测试);④ 不引入向量,展开为确定性取数;⑤ token 预算仍受控(索引轻量 + 按需展开)。

## 8. 产品对比与取长补短(附录,佐证选型)
| 维度 | Claude Code | OpenClaw | RA 现状 | T9 目标 |
|---|---|---|---|---|
| MCP 工具选择 | LLM 推理(frontmatter 常驻) | LLM + Gateway | ✅ T4 已达标(全只读域 function-calling 自选) | 维持 |
| Skill 选择 | LLM 像选工具一样选(meta-tool) | skill 原生调 MCP | ❌ 关键词分类器 + 审批 | **T9-6 平权** |
| 新增 skill 可见 | 丢 SKILL.md 即可见 | 文件化自动发现 | ❌ YAML 硬注册 + 关键词 | **T9-6 自动可见** |
| 自进化 | — | Skill Workshop 自动起草 | ❌ 无 | 远期(必经审批) |
| 审批门 / 护栏 | 沙箱 | 签名 manifest + 沙箱 | ✅ 审批门 + 反幻觉(更严) | **保留强化(RA 优势)** |

取长补短结论:**借 Claude Code 的 skill 平权 + 描述常驻(T9-6);维持 RA 已对齐的工具 function-calling 自选(T4);保留并强化 RA 独有的审批门 + 反幻觉(金融生产必需);OpenClaw 式自进化列为远期、必经审批。**

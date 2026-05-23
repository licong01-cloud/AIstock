# Research Assistant 上下文压缩设计方案

- **版本**: v1.1
- **日期**: 2026-05-24
- **状态**: Phase 1 基线已实现（版本 B），Phase 2/3 为后续开发目标
- **当前基线**: 版本 B — Token 感知滑动窗口（commit `325fd75`，待合入 main）
- **参考**: Claude Code AutoCompact 算法、LCM (Lossless Context Management) DAG 方案
- **关联**: BUG-105 (Research Assistant Chat Context Loss)

## 1. 问题定义

Research Assistant 是多轮对话系统，用户通过对话完成 QE 实验创建、因子分析、策略审查等任务。随着对话轮次增加，上下文管理面临三个核心问题：

| 问题 | 影响 |
|------|------|
| 上下文超出模型窗口 | API 调用失败或静默截断，丢失关键信息 |
| 无差别丢弃旧消息 | 用户早期确认的配置参数、风险边界被遗忘 |
| 压缩信息丢失 | 简单截断破坏决策链完整性 |

## 2. 设计目标

1. **不丢失有价值信息**：压缩过程保留决策链、用户确认、实验参数等关键语义
2. **对用户透明**：压缩自动触发，用户无需感知或手动操作
3. **可审计**：原始消息始终在 DB 中，压缩摘要可通过 API 回溯
4. **模型上下文能力匹配**：利用 1M token 上下文窗口，在接近上限前触发压缩
5. **不截断单条消息**：每条消息完整保留或完整压缩，不做 `content[:500]` 式的静默截断

## 3. 三阶段策略

### Phase 1（基线 — 版本 B）：Token 感知滑动窗口

**当前状态**: 已实现，待合入 main（工作目录: `claude/bug105-verify-20260523`，commit `325fd75`）

#### 3.1.1 设计原则

1. **不截断单条消息**：每条消息完整进入 LLM 上下文，不做 `content[:500]` 式静默截断
2. **不过滤角色**：user/assistant/system/tool 全部保留，system 消息可能包含重要的阶段切换提示，tool 消息包含 MCP 调用结果
3. **Token 预算感知**：基于模型 1M 上下文窗口，为历史消息分配 800K token 预算
4. **滑动窗口**：从最新消息开始累计，超出预算时丢弃最旧消息，保留完整性
5. **可观测性**：丢弃消息时输出 info 日志，记录保留/丢弃数量和 token 使用量

#### 3.1.2 核心常量

```python
# backend/services/research_assistant/service.py

# 历史消息 token 预算（1M 窗口 - 200K 系统开销）
_PRIOR_MESSAGES_TOKEN_BUDGET = 800_000

# 中文混合文本 token 估算系数（1 中文字符 ≈ 1-2 tokens，取保守值 2.0）
_TOKEN_ESTIMATE_CHARS_PER_TOKEN = 2.0
```

#### 3.1.3 Token 估算

```python
@classmethod
def _estimate_tokens(cls, text: str) -> int:
    """保守估算：字符数 / 2.0，至少 1 token"""
    return max(1, int(len(text) / cls._TOKEN_ESTIMATE_CHARS_PER_TOKEN))
```

上线后应对比 API 实际返回的 token 计数校准此系数。

#### 3.1.4 历史消息加载算法

```python
def _load_prior_chat_messages(self, conversation_id: str, current_message: str
                              ) -> list[dict[str, str]]:
    # 1. 从 DB 加载最近 500 条消息
    result = self.repository.list_records(
        "conversation_messages",
        filters={"conversation_id": conversation_id},
        limit=500,
    )
    items = sorted(result["items"], key=lambda item: str(item.get("created_at") or ""))

    # 2. 构建候选列表（完整内容、全部角色、排除空消息和当前消息）
    candidates: list[dict[str, str]] = []
    for item in items:
        content = str(item.get("content_text") or "").strip()
        if not content:
            continue
        if content == current_message:
            continue
        role = str(item.get("role") or "")
        candidates.append({"role": role, "content": content})

    # 3. 滑动窗口：从最新开始累加 token，不超过预算
    selected: list[dict[str, str]] = []
    tokens_used = 0
    for msg in reversed(candidates):                  # 从最新开始
        msg_tokens = self._estimate_tokens(msg["content"])
        if tokens_used + msg_tokens > self._PRIOR_MESSAGES_TOKEN_BUDGET and selected:
            break                                     # 超出预算，停止添加
        selected.append(msg)
        tokens_used += msg_tokens
    selected.reverse()                                # 恢复时间顺序

    # 4. 丢弃消息时记录日志
    if len(selected) < len(candidates):
        logger.info(
            "chat history window: kept %d/%d messages (~%d tokens), "
            "dropped %d oldest due to budget %d",
            len(selected), len(candidates), tokens_used,
            len(candidates) - len(selected), self._PRIOR_MESSAGES_TOKEN_BUDGET,
        )
    return selected
```

**算法复杂度**: O(n)，n ≤ 500。单次遍历候选列表，无重复分配。

#### 3.1.5 LLM 消息组装

```python
@staticmethod
def _chat_messages_for_llm(user_message, bundle, context_pack,
                           prior_messages=None) -> list[dict[str, str]]:
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": context_pack_summary},
    ]
    if prior_messages:
        messages.extend(prior_messages)    # 对话历史注入在 system 之后
    messages.append({"role": "user", "content": user_message})
    return messages
```

**消息顺序**:

```
[0] system    — Prompt Bundle 文本 + 行为约束
[1] user      — Context Pack 摘要（已审计记忆）
[2..n-1]      — 对话历史（prior_messages，按时间正序）
[n]   user    — 当前用户消息
```

#### 3.1.6 chat_turn 集成点

```python
def chat_turn(self, request):
    # ... 创建/获取 conversation，创建 task，记录 user_message ...

    # ① 加载历史消息（在模型路由之前，用于 token 估算）
    prior_messages = self._load_prior_chat_messages(conversation_id, data.message)
    history_tokens = sum(self._estimate_tokens(m["content"]) for m in prior_messages)
    estimated_total_tokens = len(data.message) * 2 + history_tokens + 32000

    # ② 模型路由（token 估算包含历史，可能路由到长上下文模型）
    route = self.route_model(ModelRouteRequest(
        role="primary_reasoner",
        risk_level=data.risk_level,
        token_estimate=estimated_total_tokens,  # 包含历史 token
    ))

    # ③ Context Pack（预算从 16K 提升到 64K）
    context_pack = self.build_context_pack(ContextPackBuildRequest(
        token_budget=64000,  # 原为 16000
    ))

    # ④ 组装 LLM 消息（含历史）
    messages = self._chat_messages_for_llm(data.message, bundle, context_pack, prior_messages)
    # ... LLM 调用 ...
```

#### 3.1.7 预算分配模型（1M 上下文窗口）

```
┌──────────────────────────────────────────────────────┐
│ System Prompt (~32K)                                  │
│   - Prompt Bundle 文本（由 Prompt Tree 组装）          │
│   - 行为约束（中文回复、禁止 raw JSON、禁止执行高风险） │
├──────────────────────────────────────────────────────┤
│ Context Pack 摘要 (~32K 预留, 实际可变)                │
│   - 已审计的 Memory Items 摘要                        │
│   - 临时记忆引用                                     │
├──────────────────────────────────────────────────────┤
│ 对话历史 — prior_messages (~800K)                     │
│   [msg_1] [msg_2] ... [msg_k]                        │
│   超出 800K 时从 msg_1（最旧）开始丢弃                 │
│   所有角色保留，所有消息完整                           │
├──────────────────────────────────────────────────────┤
│ 当前用户消息 + 预留 LLM 回复空间 (~136K)               │
└──────────────────────────────────────────────────────┘
```

#### 3.1.8 前端变更

**文件**: `frontend/src/app/research-assistant/chat/page.tsx`

```typescript
// 新增状态
const [conversationId, setConversationId] = useState<string | null>(null);

// createAdapter 新增参数
function createAdapter(
  onTurn, onStage, onCatalogIssue,
  conversationId: string | null,         // 新增
  setConversationId: (id: string) => void, // 新增
): ChatModelAdapter

// run() 方法内
const payload: Record<string, unknown> = { message, phase, risk_level, allow_execute };
if (conversationId) payload.conversation_id = conversationId;  // 传递已有 ID
result = await researchAssistantApi.chatTurn(payload);

// 首轮响应后保存 conversation_id
const newId = result.conversation?.conversation_id;
if (newId && !conversationId) setConversationId(newId);

// 新建对话按钮（重置所有状态）
const newConversation = useCallback(() => {
  setConversationId(null);
  setLatest(null);
  setSteps(initialSteps);
  setCatalogIssue(null);
  setCatalogInitMessage(null);
}, []);
```

#### 3.1.9 测试覆盖

**文件**: `backend/tests/research_assistant/test_service.py`（5 个 BUG-105 专项测试）

| 测试 | 验证点 |
|------|--------|
| `test_chat_turn_prior_messages_injected_into_llm_context` | 第二轮对话的 LLM 消息包含第一轮内容 |
| `test_new_conversation_has_no_prior_messages` | 无 conversation_id 时不发送历史 |
| `test_chat_history_includes_all_roles` | system/tool 消息被保留并注入 LLM |
| `test_chat_history_preserves_full_message_content` | 2000+ 字符消息完整保留，不截断 |
| `test_chat_history_token_budget_drops_oldest_first` | 超出预算时丢弃最旧而非最新消息 |

#### 3.1.10 与版本 A 的对比

| 维度 | 版本 A（已废弃） | 版本 B（当前基线） |
|------|-----------------|-------------------|
| 角色过滤 | ❌ `if role not in ("user", "assistant"): continue` | ✅ 保留所有角色 |
| 内容完整性 | ❌ `content[:500]` 截断 | ✅ 完整保留 |
| 消息数量 | ❌ `limit=20`, `prior[-20:]` 硬编码 | ✅ 按 800K token 预算动态滑动 |
| Token 感知 | ❌ 无 | ✅ `_estimate_tokens()` |
| 超出预算 | ❌ 静默丢弃 | ✅ `logger.info()` 记录 |
| 丢弃策略 | ❌ 先取后截，20 条限制 | ✅ 从最旧开始丢弃，保留最新 |
| 模型路由 | ❌ `len(msg)*2` | ✅ 包含实际历史 token 数 |
| Context Pack | ❌ 16K budget | ✅ 64K budget |
| DB 查询 | ❌ `limit=20` | ✅ `limit=500` |

### Phase 2（建议实现）：结构化压缩（LLM 驱动）

**触发**：历史消息估算 > 500K tokens 时自动触发

**设计原理**：参考 Claude Code AutoCompact 的 9 段结构化摘要，定制为 RA 领域。不是传统摘要（损失细节），而是结构化信息提取（保留关键信息）。

#### 3.2.1 压缩提示词结构

参考 Claude Code 的设计：

```
<analysis>
  (模型在生成摘要前的思考过程，压缩后自动剥离，不进入后续上下文)
</analysis>
<summary>
  1. 用户主要请求与意图
     - 记录用户所有明确的实验目标、分析需求、配置要求
  2. 关键技术概念与参数
     - 涉及的 QE 参数（loop 数、股票池、时间窗）
     - 涉及的因子名称、模型配置、风险等级
  3. 决策与确认记录
     - 用户明确确认的选项和参数值
     - 用户的否定态度和边界限定
     - 审批状态变化（approved/rejected/pending）
  4. 实验与任务追踪
     - 已创建的任务 ID 和状态摘要
     - 已生成的 Context Pack 和 Prompt Bundle 摘要
  5. 错误与修正
     - LLM 调用失败及重试
     - MCP 预检查未通过的项
     - 用户纠偏和方向调整
  6. 用户全部消息
     - 逐条列出用户的原始输入（非工具结果）
     - 关键：保留用户原话，防止需求漂移
  7. 未完成任务
     - 等待用户确认的操作
     - 待执行的 MCP 调用
  8. 当前工作状态
     - 压缩前正在进行的精确工作
     - 当前的 phase 和 status_rail 状态
  9. 下一步建议
     - 引用用户原话，给出下一步行动
     - 标注需要用户确认的决策点
</summary>
```

**关键设计点**（继承 Claude Code 的实践）：
- **XML 格式**：Claude 模型对 XML 输出更稳定，不用 JSON
- **Analysis-First**：强制 `<analysis>` 思考再输出，提升摘要质量
- **NO_TOOLS_PREAMBLE**：`CRITICAL: 只输出文本。禁止调用工具。` —— 压缩子调用不允许工具
- **低温度**：`temperature=0.2` 保证输出一致性

#### 3.2.2 Fresh Tail 保护

最近 8 轮对话（约 16-20 条消息）保持原始形式不压缩。这是 AI 的"工作记忆"，包含当前讨论的上下文和最新的确认状态。

```
┌──────────────────────────────────────────┐
│ [原始] 最近 8 轮（Fresh Tail）            │  ← 永远不压缩
├──────────────────────────────────────────┤
│ [压缩摘要] 8 轮之前的对话                  │  ← LLM 压缩为结构化摘要
├──────────────────────────────────────────┤
│ System Prompt + Context Pack             │
└──────────────────────────────────────────┘
```

#### 3.2.3 压缩摘要生命周期

1. **存储**：压缩摘要作为特殊消息存入 `assistant_conversation_messages`（role=system, 标记为压缩摘要）
2. **链接**：摘要的 `content_json` 包含被压缩的原始消息 ID 范围
3. **替换**：后续请求中，压缩摘要替换被压缩的原始消息进入上下文
4. **再压缩**：当压缩摘要也累积过多时，对多个摘要再次执行压缩（二级压缩）
5. **可检索**：通过 API 可查看被压缩的原始消息完整内容

#### 3.2.4 压缩后恢复提示词

```
继续之前的对话，不要向用户提问。直接从中断处恢复——
不要提及摘要，不要复述之前发生的事情。
像对话从未中断一样继续。
```

#### 3.2.5 触发条件伪代码

```python
def _maybe_compact(self, conversation_id: str, current_turn: int) -> str | None:
    """Return compact summary text if compaction was performed, else None."""
    if current_turn < 16:  # 至少 16 轮才考虑压缩
        return None

    history_tokens = self._estimate_history_tokens(conversation_id)
    if history_tokens < 500_000:  # 预算充足，不压缩
        return None

    # 保护最近 8 轮原始消息
    fresh_tail_ids = self._get_last_n_message_ids(conversation_id, n=16)
    old_messages = self._get_messages_before(conversation_id, fresh_tail_ids[0])

    # 用低温度 LLM 执行结构化压缩
    compact_result = self._llm_compact(old_messages)
    self._store_compact_summary(conversation_id, compact_result, old_messages)
    return compact_result
```

### Phase 3（远期可选）：分层摘要 DAG

**触发**：Phase 2 的压缩摘要累积超过 10 条时

**设计原理**：参考 LCM 的 DAG 分层模型，对多个压缩摘要进行二次压缩。

```
原始消息:  [m1]...[m30]          [m31]...[m60]         [m61]...[m90]
               ↓                      ↓                      ↓
一级摘要:  [compact_summary_1]   [compact_summary_2]    [compact_summary_3]
               ↓                      ↓
二级摘要:     [root_compact_summary_from_c1_c2_c3]
```

**评估标准**：当 RA 对话平均轮次显著增长（用户单会话 > 100 轮成为常态）时再实施。当前场景下 Phase 2 的线性压缩已足够。

## 4. 与 Claude Code / LCM 的对比

| 维度 | Claude Code AutoCompact | LCM DAG | RA Phase 1 | RA Phase 2（设计） |
|------|------------------------|---------|-----------|-------------------|
| 触发方式 | ~167K tokens (82%) | 75% 窗口 | 每次加载 | 500K tokens |
| 完整新消息 | 始终保留 | 最近 32-64 条 | 全部保留 | 最近 8 轮 |
| 压缩输出 | 9 段结构化摘要 | 分层摘要 DAG | 无 | 9 段 RA 定制摘要 |
| 原始消息 | 丢弃（被摘要替换） | 永久存 SQLite | 永久存 PostgreSQL | 永久存 PostgreSQL |
| LLM 调用 | 1 次子调用 | 多次分层 | 无 | 1 次子调用 |
| 可展开 | 否 | 是（MCP expand） | N/A | 可通过 API 查询 |
| 恢复提示 | 显式继续指令 | FTS5 搜索 | 无 | 显式继续指令 |

## 5. 实现文件清单

| 文件 | Phase | 变更类型 |
|------|-------|---------|
| `backend/services/research_assistant/service.py` | 1/2 | `_load_prior_chat_messages()`, `_estimate_tokens()`, `_maybe_compact()` |
| `backend/services/research_assistant/models.py` | 2 | `CompactSummaryCreate` 模型 |
| `backend/services/research_assistant/repository.py` | 2 | `compact_summaries` 表注册 |
| `backend/routers/research_assistant.py` | 2 | GET `/conversations/{id}/messages` 原始消息查询 |
| `backend/db/init_research_assistant_schema_20260521.sql` | 2 | `assistant_compact_summaries` DDL |
| `backend/tests/research_assistant/test_service.py` | 1/2 | 压缩触发、Fresh Tail、恢复测试 |

## 6. 验证标准

| # | 验证项 | 方法 |
|---|-------|------|
| 1 | 压缩不丢失用户确认的配置参数 | 多轮对话后查询 LLM 上下文，验证关键参数存在 |
| 2 | Fresh Tail 的 8 轮原始消息完整 | 检查压缩后 context 中最近 8 轮为原始消息 |
| 3 | 压缩后对话连续性 | 压缩前后发送同一跟进问题，验证回复引用正确上下文 |
| 4 | Token 估算准确性 | 对比估算值和实际 API reported tokens，偏差 < 20% |
| 5 | 超出预算时日志记录 | 检查 info 日志包含丢弃条数和 token 使用量 |

## 7. 风险评估

| 风险 | 概率 | 缓解措施 |
|------|------|---------|
| LLM 压缩遗漏关键信息 | 中 | 9 段强制结构 + 低温度 + 原始消息 DB 保留 |
| 压缩 latency 影响用户体验 | 低 | 压缩异步执行或流式响应中隐藏 latency |
| Token 估算偏差大 | 低 | 初始用 chars/2.0，上线后根据实际 token counting 校准 |
| 压缩提示词注入 | 低 | 历史消息来自可信 DB 源，当前用户消息不进入压缩 prompt |

## 8. Handoff

- `production_ddl_gate`: Phase 2 需新增 `assistant_compact_summaries` 表（DDL 评审后执行）
- `production_frontend_dependency_gate`: noop（压缩逻辑纯后端）
- `production_backend_dependency_gate`: noop（不引入新依赖）

## 附录 A：Claude Code 上下文压缩算法详解（参考）

本附录记录 Claude Code 上下文压缩算法的完整架构，作为 RA Phase 2 结构化压缩方案的设计参考。信息来源为 Claude Code 源代码逆向分析及社区文档。

### A.1 五层压缩流水线

Claude Code 不是"满了就总结"，而是一个按成本从低到高的 **5 层递进式压缩流水线**，实现在 `query.ts` 中：

```
┌──────────────────────────────────────────────────────────┐
│  ① Tool Result Budget  零 LLM 成本                       │
│     大工具结果(>50K chars) → 写磁盘，context仅保留路径+预览 │
│     使用3区决策树(mustReapply/frozen/fresh)保护缓存前缀    │
├──────────────────────────────────────────────────────────┤
│  ② Snip Compact        零 LLM 成本                       │
│     特征开关 HISTORY_SNIP 控制                            │
│     从历史头部截断最旧消息，释放的 token 数传给 AutoCompact │
├──────────────────────────────────────────────────────────┤
│  ③ Microcompact        低 LLM 成本                       │
│     路径A(时间): 空闲>60分钟 → 清除旧工具结果为占位文本    │
│     路径B(缓存): Anthropic cache_edits API 服务端删除      │
│     仅在主线程运行（不在 fork agent 中执行）               │
├──────────────────────────────────────────────────────────┤
│  ④ Context Collapse    中 LLM 成本                       │
│     ~90% 上下文填充时触发，将REPL历史投影为"commit log"    │
│     与⑤互斥：Collapse 开启时 AutoCompact 被禁用           │
│     阻塞阈值 ~95%                                         │
├──────────────────────────────────────────────────────────┤
│  ⑤ AutoCompact         最高 LLM 成本                     │
│     ~75-95% 上下文填充时触发                               │
│     两级: Session Memory 压缩 → 失败则 LLM 摘要（fork子调用）│
│     熔断机制: 连续 3 次失败 → 停止                         │
└──────────────────────────────────────────────────────────┘
```

**后调用恢复链**：当 API 返回 `prompt_too_long` 时：
1. Context Collapse 尝试 `recoverFromOverflow()`（如已启用）
2. Reactive Compact 尝试 `tryReactiveCompact()` 作为最后手段

### A.2 AutoCompact 阈值配置

```
effectiveContextWindow = modelContextWindow - maxOutputTokens
autoCompactThreshold    = effectiveContextWindow - AUTOCOMPACT_BUFFER_TOKENS (13,000)
```

对于 200K 窗口 + ≥20K max output：effective ≈ 180K，触发于 ≈ **167K tokens (82%)**。

社区建议：在 **50-60%** 上下文时主动压缩，而非等到 95% —— 更早触发产生更高质量的摘要。

### A.3 结构化压缩 Prompt 设计

Claude Code 使用 **XML 标签**（`<analysis>` + `<summary>`）而非 JSON 输出。选择 XML 的原因是 **Claude 模型在训练时大量接触 XML 标签**，输出稳定性优于 JSON。

#### A.3.1 NO_TOOLS_PREAMBLE

放在提示词最前面，大写强调：

```
CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.
Tool calls will be REJECTED and will waste your only turn.
```

**设计原因**：压缩在 `maxTurns: 1` 的 fork 子调用中执行。如果模型调工具被拒绝，就没有输出，浪费唯一的一次 API 调用。

#### A.3.2 三种压缩模式

| 模式 | 用途 | Prompt 模板 |
|------|------|------------|
| `BASE_COMPACT` | 全量压缩（`/compact` 命令） | 首次完整压缩 |
| `PARTIAL_COMPACT(from)` | 增量压缩 | 只压缩旧消息，保留近期 |
| `PARTIAL_COMPACT_UP_TO` | 部分压缩到某点 | 标记后面还有未压缩的新消息 |

#### A.3.3 9 段强制输出结构（原文对照）

```
<analysis>
  (模型在生成摘要前的思考过程 — 最终被 formatCompactSummary() 剥离，
   不进入后续 context。等同于 Chain-of-Thought，提升摘要质量)
</analysis>
<summary>
1. Primary Request and Intent
   — 捕获用户所有明确请求和意图

2. Key Technical Concepts
   — 列出所有重要技术概念、技术和框架

3. Files and Code Sections
   — 列举检查/修改/创建的文件，含完整代码片段

4. Errors and fixes
   — 所有遇到的错误及修复方式，尤其关注用户反馈

5. Problem Solving
   — 已解决问题和正在进行的故障排除

6. All user messages
   — 所有非工具结果的用户消息（关键设计！保留用户原话）

7. Pending Tasks
   — 明确要求处理的待办任务

8. Current Work / Work Completed
   — 摘要请求前正在进行的精确工作描述

9. Optional Next Step / Context for Continuing Work
   — 下一步行动（必须引用用户原话，防止任务漂移）
</summary>
```

#### A.3.4 压缩工作流程

```
原始消息列表
  → stripImages（去除图片/文档，只保留文本标记）
  → stripReinjectedAttachments（去除重复注入的附件）
  → createUserMessage(summarize_request)
  → 调用 AI 模型生成摘要（tools禁用, thinkingConfig禁用, temperature=0.2）
  → 生成 <analysis> + <summary> 结构化输出
  → formatCompactSummary() 剥离 <analysis>，只保留 <summary>
  → 重建消息链:
     [boundary_marker] + [summary] + [recent_messages] + [post_compact_attachments]
```

#### A.3.5 压缩后恢复提示词（原文）

```
Continue the conversation from where it left off without asking
the user any further questions. Resume directly — do not acknowledge
the summary, do not recap what was happening. Pick up the last task
as if the break never happened.
```

**设计意图**：禁止 AI 说"好的，我继续之前的工作"这类废话，直接无缝接上中断点。Proactive 模式下还有额外指令："你之前就在自主工作，不是首次唤醒，不要打招呼。"

#### A.3.6 压缩后恢复注入

压缩后系统重新注入以下内容以保持连续性：
- 最近 5 个已读文件（上限 50K tokens）
- Plan mode 状态
- 当前激活的 Skills
- 延迟的工具附件（deferred tool attachments）
- MCP 指令
- Session start hooks（包括 CLAUDE.md）

### A.4 Session Memory 背景提取

AutoCompact 的第一优先级路径（非 LLM）：

| 参数 | 值 | 说明 |
|------|---|------|
| 初始化阈值 | 10K context tokens | 上下文累积到 10K token 时首次提取 |
| 增量触发 | ≥5K token 增长 | 自上次提取后上下文增长 5K 时再次提取 |
| 工具调用触发 | 3 次累积工具调用 | 防止高频提取 |
| 自然暂停触发 | 上一轮无工具调用 | 对话自然断点 |

**两层门控**：`tengu_session_memory` + `tengu_sm_compact` 特征开关同时开启才启用此路径。

### A.5 Post-Compact 上下文组装

压缩后的上下文由以下部分拼接而成：

```
[System Prompt + CLAUDE.md 指令]
[压缩摘要 (9 段结构化)]
[最近 N 条原始消息 (未被压缩的部分)]
[重新注入的附件: 最近读取的文件、plan状态、skills、MCP指令]
[当前用户消息]
```

关键：**压缩摘要替换了被压缩的原始消息**，原始消息不再进入后续上下文，但摘要中保留了关键信息的结构化记录。

### A.6 已知约束

| 约束 | 详情 |
|------|------|
| Sonnet 4.6 工具调用违规率 | 2.79%（vs Sonnet 4.5 的 0.01%），压缩 prompt 中模型仍试图调工具 |
| Prompt 缓存兼容 | Microcompact 使用 cache_edits API 删除服务端缓存内容，不破坏本地缓存前缀 |
| Fork 子调用隔离 | 压缩在 fork agent 中执行，共享父 agent 的 prompt cache prefix |
| 与 Context Collapse 互斥 | 两个不能同时开启，Collapse 启用时 AutoCompact 自动禁用 |

## 附录 B：LCM (Lossless Context Management) DAG 方案详解（参考）

LCM 是社区方案 `@lossless-claude/lcm`，在 Claude Code AutoCompact 基础上进一步实现了 **零消息丢失 + 可展开 + 跨 session 记忆**。

### B.1 核心架构：DAG 分层摘要

```
原始消息:  [m1]...[m20]     [m21]...[m40]     [m41]...[m60]
               ↓                  ↓                  ↓
Leaf(d0):  [summary_1]      [summary_2]       [summary_3]    (~1,200 tokens/leaf)
               ↓                  ↓
Condensed:  [cond_1: s1+s2]  [cond_2: s3]                    (~2,000 tokens/condensed)
                    ↓
Root:           [root: c1+c2]
```

每个摘要节点存储：
- MD5 指纹（防重复压缩）
- 语义向量 embedding
- 时间范围 `ts_start` / `ts_end`
- 父/子节点指针（支持展开遍历）

### B.2 SQLite 持久化 Schema

```sql
-- 不可变消息存储（追加写入，永不修改或删除）
CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL UNIQUE,
    role TEXT NOT NULL,           -- user / assistant / system / tool
    content TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

-- DAG 摘要节点（分层）
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    level TEXT NOT NULL DEFAULT 'verbatim',
    content TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    seq_start INTEGER NOT NULL,
    seq_end INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

-- DAG 边（摘要 → 原始消息或子摘要）
CREATE TABLE node_children (
    node_id TEXT NOT NULL REFERENCES nodes(id),
    child_id TEXT NOT NULL,
    child_type TEXT NOT NULL DEFAULT 'message',  -- 'message' or 'node'
    PRIMARY KEY (node_id, child_id)
);

-- 跨 session FTS5 全文搜索记忆
CREATE VIRTUAL TABLE promoted_memory USING fts5(
    content, tags, source_session, created_at
);
```

### B.3 三级压缩升级（保证收敛）

| 级别 | 触发 | 方法 | 压缩率 |
|------|------|------|--------|
| Level 1: Normal Summary | 上下文 > 75% | LLM 摘要（temperature=0.2），保留决策/命令/错误/原因 | ~50% semantic reduction |
| Level 2: Aggressive Bullet | 重新压缩 DAG 上层节点时 | 更严格的 prompt（temperature=0.1），关键词提取 + 模板生成 | ~80% reduction |
| Level 3: Deterministic Truncation | LLM 摘要失败时 | 头部截断到 ~512 tokens + `[Truncated for context management]` 标记 | 保证收敛 |

### B.4 MCP 检索工具

| 工具 | 延迟 | 用途 |
|------|------|------|
| `lcm_search` | <100ms | 混合搜索 episoidic + semantic promoted memory |
| `lcm_grep` | <100ms | 正则或 FTS5 全文搜索存储的消息和摘要 |
| `lcm_describe` | <100ms | 检查节点元数据：深度、token 数、父子链接 |
| `lcm_expand` | 30-120s | DAG 遍历：将摘要节点递归展开为原始消息 |
| `lcm_store` | <100ms | 手动存储持久记忆 |
| `lcm_stats` | <50ms | Token 节省、压缩率、DAG 深度统计 |

### B.5 每轮上下文组装算法

```
context = [相关 DAG 摘要节点] + [fresh_tail 原始消息]

1. 零成本路径: token 不超窗口 → 不压缩，零开销
2. 预算感知: 从不同 DAG 深度选取摘要节点，最大化信息密度
3. Fresh tail: 最近 32-64 条消息始终保留原始形式（工作记忆）
4. DAG 遍历: 从 root 向下，选取 token 预算内信息密度最高的摘要
```

### B.6 生命周期 Hook 集成

| Hook | 命令 | 用途 |
|------|------|------|
| PreCompact | `lcm compact --hook` | 拦截 Claude 原生压缩，写入 DAG 摘要（不丢弃消息） |
| SessionStart | `lcm restore` | 恢复项目上下文、近期摘要、promoted memory |
| UserPromptSubmit | `lcm user-prompt` | 搜索记忆，注入 prompt 前提示 |
| SessionEnd | `lcm session-end` | 将完成的 Claude 对话记录写入 SQLite |

所有 Hook 具有自愈能力：每次执行前验证注册状态，修复缺失条目后再继续。

### B.7 性能基准（OOLONG benchmark）

| 上下文长度 | LCM Score | 传统方法 | 优势 |
|-----------|-----------|---------|------|
| 2,048 tokens | 74.8 | 70.3 | +4.5 |
| 8,192 tokens | 79.1 | 66.4 | +12.7 |
| 32,768 tokens | 保持准确 | 显著退化 | 差距大 |

- Token 节省: 40-70%（重复上下文传输成本降低）
- 摘要生成延迟: 85-120ms/次
- 关键信息遗漏率: 从 18% 降至 0.3%

### B.8 关键配置参数

| 参数 | 默认值 | 用途 |
|------|--------|------|
| `LCM_CONTEXT_THRESHOLD` | 0.75 | 触发压缩的上下文填充比例 |
| `LCM_FRESH_TAIL_COUNT` | 32-64 | 最近消息保护数量（不压缩） |
| `LCM_LEAF_MIN_FANOUT` | 8 | 生成一个叶摘要的最小原始消息数 |
| `LCM_CONDENSED_MIN_FANOUT` | 4 | 生成一个压缩节点的最小摘要数 |
| `LCM_INCREMENTAL_MAX_DEPTH` | 0-1 | 压缩级联深度（0=仅叶, 1=一级压缩, -1=无限制） |
| `LCM_LEAF_CHUNK_TOKENS` | 20,000 | 每次压缩通过的最大源 token 数 |
| `LCM_LEAF_TARGET_TOKENS` | 1,200 | 叶摘要目标 token 数 |
| `LCM_CONDENSED_TARGET_TOKENS` | 2,000 | 压缩摘要目标 token 数 |

## 附录 C：RA 与参考方案的适用性映射

| Claude Code / LCM 特性 | RA 是否采用 | 理由 |
|------------------------|-----------|------|
| 5 层压缩流水线 | 不需要 | RA 对话量远小于 Codex REPL，1-2 层足够 |
| 9 段结构化摘要 | ✅ Phase 2 | 核心参考，RA 定制字段（实验参数/确认决策/风险边界） |
| XML 输出格式 | ✅ Phase 2 | Claude 模型对 XML 更稳定 |
| Analysis-First | ✅ Phase 2 | `<analysis>` 剥离，提升摘要质量 |
| NO_TOOLS_PREAMBLE | ✅ Phase 2 | 压缩子调用禁止工具 |
| Fresh Tail 保护 | ✅ Phase 2 | 最近 8 轮不压缩 |
| 压缩后 Continuation Prompt | ✅ Phase 2 | "像对话从未中断一样继续" |
| Tool Result Budget | 不需要 | RA 当前无 > 50K 字符的工具结果 |
| Snip Compact | 被 Phase 1 替代 | Phase 1 的 token 滑动窗口实现等效功能 |
| Microcompact(Time) | 远期可选 | RA 会话通常短于 60 分钟 |
| Microcompact(Cache) | 不适用 | 需要 Anthropic cache_edits API |
| Context Collapse | 不适用 | RA 用结构化压缩替代 commit log 方式 |
| Session Memory 背景提取 | 远期可选 | RA 已有 Memory Ledger + Context Pack 体系 |
| SQLite DAG + Embedding | ❌ | RA 对话量不需要分层 DAG |
| FTS5 跨 session 记忆 | ❌ | RA 已有 Memory Ledger 体系 |
| SimHash 去重 | ❌ | RA 对话重复消息极少 |
| lcm_expand 可展开 | 部分采用 | RA 通过 API 查询原始消息（非 MCP 工具） |
| 三级压缩升级 | 部分采用 | Level 1 (LLM摘要) + Level 3 (头截断保底)，不需要 Level 2 |

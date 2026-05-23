# Research Assistant 上下文压缩设计方案

- **版本**: v1.0
- **日期**: 2026-05-24
- **状态**: Draft（Phase 1 已实现，Phase 2/3 待评审）
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

### Phase 1（已实现）：Token 感知滑动窗口

**触发**：每次 `chat_turn` 调用

**策略**：
- 从 DB 加载最近 500 条消息（所有角色：user/assistant/system/tool）
- 从最新到最旧累计 token 估算值
- 保留尽可能多的完整消息，不超过 800K token 预算
- 超出预算时，丢弃最旧消息，记录 info 日志
- 保留所有角色的完整内容，不截断单条消息

**Token 估算**：`len(content) / 2.0`（中文混合文本保守估计，1 中文字符 ≈ 1-2 tokens）

**预算分配**（基于 1M 上下文窗口）：

```
┌─────────────────────────────────────────────────────┐
│ System Prompt + Context Pack (~64K)                  │
├─────────────────────────────────────────────────────┤
│ 对话历史轮次 (~800K)                                 │
│  [最旧] ... [msg n-2] [msg n-1] [当前消息] [最旧]   │
│  超出预算时从左侧（最旧）开始丢弃                     │
├─────────────────────────────────────────────────────┤
│ 当前用户消息 + 预留回复空间 (~136K)                   │
└─────────────────────────────────────────────────────┘
```

**代码位置**：`backend/services/research_assistant/service.py`
- `_PRIOR_MESSAGES_TOKEN_BUDGET = 800_000`
- `_TOKEN_ESTIMATE_CHARS_PER_TOKEN = 2.0`
- `_estimate_tokens()` / `_load_prior_chat_messages()`

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

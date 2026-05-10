# Shadow Run Consistency Infrastructure — QE Backtest vs Paper v2 重放对账设计

> **作者**：engine-design teammate
> **日期**：2026-05-09
> **任务**：Phase 2 T3 双纸面设计之 (2)
> **范围**：纸面设计；不写代码、不动 schema、不动 main 业务路径
> **依赖**：
> - `backend/services/quantevolver/`（QE 引擎与 backtest executor）
> - `backend/services/paper_trading_v2/replay.py` + `live_session.py`（CATCHUP_THEN_LIVE 历史重放）
> - `backend/services/paper_trading_v2/day_runner.py`（撮合与 ledger）
> - `backend/services/strategy_package/runtime.py` + `backtest_contract.py`
> - `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` §A.3.4 Mode G 等价性
> - `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §24 类别 C 漂移
> - `docs/analysis/paper_v2_blockers_20260508.md` P0-A / P0-B
>
> **核心问题**：QE Qlib backtest 与 Paper v2 重放本应在同一 (manifest, score, portfolio) 输入下产出**等价**的 OrderIntent 序列与 NAV 轨迹，但因独立执行栈（参 P0-B），**无机制持续监测两者偏移**。本文档设计一套**纸面对账基础设施**，让 backtest run 与 paper 重放 run 在结束后能自动比对、产出 finding，喂给 `ValidationFindingStore` 走现有 review 流程。
>
> **核心约束**：
> - **纯只读 + 旁路**：本设施**不修改** QE 或 Paper v2 的执行路径；**只**消费两者落盘的 run artifact
> - **不改 finding_store schema**：复用现有 `BUG_SCHEMA = "aistock_validation_bug_v1"` 写入新种类 finding
> - **fail-fast**：对账失败 → 写 finding；决不静默 swallow

---

## 0. 修订说明（2026-05-09 本轮派单口径）

本文档 v1（前轮交付，commit `3d856f4`）覆盖三维 diff（OrderIntent / NAV / 持仓）。本轮按 Lead 派单口径扩展：

| # | 本轮新增覆盖 | 落地章节 |
| --- | --- | --- |
| 1 | 加 D4 换手率 diff（turnover）维度 | §3.4 / §4.2 / §6.1 |
| 2 | 加 D5 费用 diff（fees / commission / slippage cost）维度 | §3.5 / §4.2 / §6.1 |
| 3 | 与 Codex Validation Mode A-F + 附录 A.3.4 Mode G 整合（v1 仅 §7 涉及 Mode G，未含 Mode A-F） | §15 新增章节 |
| 4 | 容忍度阈值"按维度分级"显式表达 | §3.6 新增小节 |

v1 既有 §3.1-§3.3 / §4-§14 内容保留不动；本节后续 §3.4 起为本轮增量。

---

## 1. 设计目标与边界

### 1.1 目标

| # | 目标 | 验收 |
| --- | --- | --- |
| G1 | 给定一对 `(qe_backtest_run_id, paper_replay_run_id)`，自动跑对账并产出 typed report | report 含 OrderIntent diff / NAV diff / 持仓 diff 三维 |
| G2 | 对账偏移超阈值时写入 `tests/aistock_validation/bugs/<id>.json`（沿用 `BUG_SCHEMA`），让 finding_store + MCP server 可消费 | finding_store.list_bugs 能检索到本设施产出的 bug |
| G3 | 对账过程不破坏 QE / Paper v2 现有 artifact；不修改既有 NAV 序列 | 对账 reader 仅 open(...).read()；run dir 不写新文件（除日志输出） |
| G4 | 与 §A.3.4 Mode G Cross-Adapter Equivalence 互补：Mode G 是单元层（同 score 不同 adapter），本设施是端到端层（含撮合差异） | 文档明确两者分工 |
| G5 | 支持手动触发 + 定时触发；CI 上跑 smoke 子集 | 触发入口 ≥ 2 种 |

### 1.2 不目标

- ❌ **不替代 Mode G**：Mode G 是 OrderIntent byte-equal（决策侧）；本设施允许撮合层 NAV 容差（执行侧）
- ❌ **不修复偏移**：只检测 + 报告，不主动重跑 / 重写
- ❌ **不改 QE backtest_executor 路径**
- ❌ **不改 Paper v2 day_runner / live_session / replay 路径**
- ❌ **不引入新 DB schema**：复用 finding_store 文件式 bug 记录
- ❌ **不做 IC / Sharpe 等业绩指标对账**（那是 strategy_package validation 范畴，不在本设施 scope）

---

## 2. 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│ QE Backtest Run                                              │
│  - quantevolver/executors/backtest.py 执行                   │
│  - 落盘：rdagent_assets/backtest_runs/<run_id>/              │
│      ├── manifest.json     (frozen StrategyPackage v2)       │
│      ├── orders.csv        (OrderIntent 序列)                │
│      ├── nav.csv           (日级 NAV 轨迹)                   │
│      ├── positions.csv     (每日持仓)                        │
│      └── meta.json         (run metadata)                    │
└──────────────────────────────────────────────────────────────┘
                          │
                          ├─── 同 manifest_sha256 + 同 score 来源 ───┐
                          │                                          │
                          ▼                                          ▼
┌──────────────────────────────────────────────────────────────┐
│ Paper v2 Replay Run (CATCHUP_THEN_LIVE 模式 historical 段)   │
│  - paper_trading_v2/live_session.py 历史 catchup 阶段        │
│  - 落盘：tmp/paper_v2/replay_runs/<run_id>/                  │
│      ├── manifest_pointer.json  (引用 strategy_pkg.package)  │
│      ├── orders.jsonl                                        │
│      ├── ledger_snapshots.jsonl                              │
│      ├── positions.jsonl                                     │
│      └── meta.json                                           │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ NEW: Shadow Run Consistency Reconciler (本设计)              │
│  位置：backend/services/validation/shadow_run/               │
│   ├── reconciler.py    — 对账主入口                           │
│   ├── readers.py       — qe / paper artifact 读取器           │
│   ├── diff_engine.py   — 三维 diff（orders / nav / positions）│
│   └── finding_writer.py — 写 BUG_SCHEMA 至 bug_root           │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ ValidationFindingStore（现有，read-only 消费）                │
│  + Validation Center MCP Server（task #37 (1)）              │
│  → agent 可查 shadow_run findings                             │
└──────────────────────────────────────────────────────────────┘
```

**关键边界**：
- Reconciler 是**第三个 finding 写入源**（前两个：guardrail 扫描器 + bug 录入流程）
- Reconciler **只读** QE / Paper run artifacts；artifact 由 QE / Paper v2 各自写
- Reconciler 自身不持有任何业务状态；每次 invocation 是 stateless

---

## 3. 对账三维 + 阈值

### 3.1 三个 diff 维度

| 维度 | 数据源 | 容差口径 |
| --- | --- | --- |
| **D1 OrderIntent diff** | qe `orders.csv` vs paper `orders.jsonl`，按 `(trade_date, symbol, side)` 三元组对齐 | **零容差**（决策侧 byte-equal；如果 D1 fail → 立即 finding `decision_drift`） |
| **D2 NAV diff** | qe `nav.csv` vs paper `ledger_snapshots.jsonl` 的日终 NAV | **bp 级容差**（默认 1bp；可配置；超阈 → finding `nav_drift_exceeded`） |
| **D3 持仓 diff** | qe `positions.csv` vs paper `positions.jsonl`，按 `(trade_date, symbol)` 对齐 | **股数零容差**（A 股 100 股一手；股数差 → finding `holding_drift`） |

### 3.2 容差为何分层

- **D1 OrderIntent 应零容差**：策略包 frozen + 同 score → 同决策（这是 Mode G 已断言的）。如有差异说明执行栈漂移；属 P0-B 范畴
- **D2 NAV 容差 1bp**：撮合差异（QE Exchange vs LocalSim ledger）会产生微小 NAV 漂移，1bp 容许
- **D3 持仓零容差（股数）**：A 股 100 股最小单位；不应有非整手差异

### 3.3 阈值配置

```python
# 伪代码 — 实施期落 backend/services/validation/shadow_run/config.py
@dataclass(frozen=True)
class ReconcilerConfig:
    # D1: OrderIntent
    allow_order_diff: bool = False                    # 默认零容差
    # D2: NAV
    nav_bp_threshold: int = 1                         # 1 bp = 0.01%
    nav_compare_mode: Literal["per_day_max", "final"] = "per_day_max"
    # D3: 持仓
    allow_share_diff: bool = False
    # 通用
    align_window_start: date | None = None            # 对账窗口起始
    align_window_end: date | None = None              # 对账窗口结束
    severity_on_fail: Literal["CRITICAL", "HIGH", "MEDIUM"] = "HIGH"
```

无 yaml schema；纯 Python dataclass + 调 reconciler 时显式传入（没有运行时配置漂移问题）。

### 3.4 D4 换手率 diff（本轮新增）

**数据源**：从两侧 orders 数据按日聚合得到日内换手率
- QE 侧：`orders.csv` → `daily_turnover = sum(|fill_amount|) / portfolio_nav_at_open`
- Paper 侧：`orders.jsonl` → 同公式

**容差口径**：**relative 容差 5%**（默认；可配）
- `(qe_turnover - paper_turnover) / max(qe_turnover, paper_turnover) <= 0.05` 视为 PASS
- 超阈 → finding `turnover_drift_exceeded`

**为何独立成维**：
- 即便 D1（OrderIntent 决策）一致 + D2（NAV）一致，**换手率漂移**仍可能暴露撮合层偏差（如 QE 视为 fill 100 但 Paper 视为 fill 80 + 撤 20，最终 NAV 接近但行为有差）
- 换手率是策略产能 / 容量 / 费率压力的**先行指标**；漂移风险下游放大

**字段细化**：
```python
@dataclass(frozen=True)
class TurnoverDiffRow:
    trade_date: date
    qe_turnover_ratio: float           # daily_turnover / portfolio_nav
    paper_turnover_ratio: float
    relative_diff: float               # |qe - paper| / max(qe, paper)
    is_pass: bool


@dataclass(frozen=True)
class TurnoverDiffReport:
    per_day: list[TurnoverDiffRow]
    max_relative_diff: float
    threshold_relative: float          # 默认 0.05
    is_pass: bool
```

### 3.5 D5 费用 diff（本轮新增）

**数据源**：从两侧 orders / ledger 提取每笔成交的费用项
- QE 侧：`orders.csv` 列含 `commission` / `stamp_tax` / `transfer_fee`（按 audit §11.3.1 行业惯例）
- Paper 侧：`ledger_snapshots.jsonl` 含 `cumulative_fees`（per ledger snapshot）

**比对策略**：
- **总费用对账**：累计费用差异 = `|qe_total_fees - paper_total_fees|`，按累计 NAV 折算 bp
- **费用结构对账**（可选）：commission / stamp_tax / slippage_cost 各类目分别对账（slippage_cost = `|fill_price - reference_price| * fill_qty`）

**容差口径**：
- 总费用：**bp 级容差 0.5 bp**（默认；可配）— 比 NAV 严格，因为费用应几乎确定（费率参数已约定）
- 各类目：**relative 容差 10%**（commission / stamp_tax 应严格一致；slippage 允许较大相对差，因撮合差异本就反映在这里）

**字段细化**：
```python
@dataclass(frozen=True)
class FeesDiffReport:
    qe_total_fees: Decimal
    paper_total_fees: Decimal
    fees_diff_bp: float                # vs cumulative NAV
    by_category: dict[str, "FeeCategoryDiff"]   # commission / stamp_tax / slippage_cost
    is_pass: bool


@dataclass(frozen=True)
class FeeCategoryDiff:
    category: Literal["commission", "stamp_tax", "transfer_fee", "slippage_cost"]
    qe_amount: Decimal
    paper_amount: Decimal
    relative_diff: float
    is_pass: bool
```

**为何独立成维**：
- 费用是 **策略可投容量 + 实盘真实回报**的关键差距源（详见 audit §11.3.1）
- QE 用合约级费率（写在 backtest contract）/ Paper v2 用 ledger 实际累计——两者偏差直接反映"backtest 用了什么费率参数 vs 实际 paper 撮合配的什么费率参数"
- D2 NAV 已含费用影响，但 D5 单独提取费用维度让漂移**可定位**（如发现 NAV 漂移源于费用错算 vs 源于撮合差异）

### 3.6 容忍度阈值按维度分级（本轮新增）

汇总 D1-D5 容差，**按维度分级**显式：

| 维度 | 默认容差 | 严格度等级 | 严重性触发 |
| --- | --- | --- | --- |
| **D1 OrderIntent** | 零容差（任意条目差异） | **L0 决策侧 byte-equal** | `decision_drift` → severity = **CRITICAL** |
| **D2 NAV** | 1 bp（max_per_day） | L1 撮合可接受微漂移 | `nav_drift_exceeded` → severity = HIGH |
| **D3 持仓** | 零股数差异 | **L0 持仓物理一致** | `holding_drift` → severity = CRITICAL |
| **D4 换手率** | 5% relative | L2 行为指标先行 | `turnover_drift_exceeded` → severity = HIGH |
| **D5 总费用** | 0.5 bp（vs 累计 NAV） | L1 费率参数应一致 | `fees_drift_exceeded` → severity = HIGH |
| **D5 费用类目（slippage）** | 10% relative | L2 滑点维度 | `slippage_drift_exceeded` → severity = MEDIUM |
| **D5 费用类目（commission/stamp）** | 10% relative | **L0 应严格一致** | `commission_drift` → severity = CRITICAL（费率配置错） |

**ReconcilerConfig 增量**（v1 配置基础上加 D4/D5 字段）：

```python
@dataclass(frozen=True)
class ReconcilerConfig:
    # v1 字段（D1/D2/D3）保持不变
    allow_order_diff: bool = False
    nav_bp_threshold: int = 1
    nav_compare_mode: Literal["per_day_max", "final"] = "per_day_max"
    allow_share_diff: bool = False

    # 本轮新增 D4 / D5
    turnover_relative_threshold: float = 0.05       # D4
    fees_total_bp_threshold: float = 0.5             # D5 总费用
    fees_category_relative_threshold: float = 0.10   # D5 各类目（默认）
    fees_strict_categories: list[str] = field(
        default_factory=lambda: ["commission", "stamp_tax", "transfer_fee"]
    )                                                # D5 这些类目必须严格一致（任意差 → CRITICAL）

    align_window_start: date | None = None
    align_window_end: date | None = None
```

**禁止做法**（与 §6.3 v1 不变量保持一致）：
- ❌ 把 D1/D3 / strict_categories 的 CRITICAL 失败降级为 HIGH（语义上零容差就该 CRITICAL）
- ❌ 让 D4/D5 默认值为"任意通过"（必须显式给阈值）
- ❌ 用单一 `tolerance_threshold` 覆盖所有维度（语义不同，必须分级）

---

## 4. Reconciler API 契约

### 4.1 主入口

```python
# 伪代码 — backend/services/validation/shadow_run/reconciler.py

@dataclass(frozen=True)
class ReconciliationResult:
    qe_run_id: str
    paper_run_id: str
    manifest_sha256: str                  # 必须两边一致；不一致直接 fail
    started_at: datetime
    ended_at: datetime
    d1_order_diff: OrderDiffReport
    d2_nav_diff: NavDiffReport
    d3_position_diff: PositionDiffReport
    d4_turnover_diff: TurnoverDiffReport            # 本轮新增
    d5_fees_diff: FeesDiffReport                    # 本轮新增
    overall_status: Literal["pass", "fail_d1", "fail_d2", "fail_d3", "fail_d4", "fail_d5", "fail_multi"]
    findings_emitted: list[str]           # 写入 bug_root 的 bug_id 列表


def reconcile(
    qe_run_id: str,
    paper_run_id: str,
    config: ReconcilerConfig | None = None,
    *,
    bug_root: Path | None = None,         # 默认 finding_store 的 DEFAULT_BUG_ROOT
) -> ReconciliationResult:
    """End-to-end shadow run reconciliation.

    Steps:
        1. read_qe_run(qe_run_id) -> QeArtifacts
        2. read_paper_run(paper_run_id) -> PaperArtifacts
        3. assert qe.manifest_sha256 == paper.manifest_sha256
           OR raise ManifestMismatchError (typed; no fallback)
        4. align_window = intersect(qe.dates, paper.dates) ∩ config window
        5. d1 = compute_order_diff(qe.orders, paper.orders, align_window)
        6. d2 = compute_nav_diff(qe.nav, paper.nav, align_window, config.nav_bp_threshold)
        7. d3 = compute_position_diff(qe.positions, paper.positions, align_window)
        8. for each failed dimension: emit_finding(severity, type, context)
        9. return ReconciliationResult

    Errors:
        ManifestMismatchError — qe/paper 的 manifest_sha256 不一致；不允许
                                 跨 manifest 对账（语义无意义）
        ArtifactNotFoundError — qe_run_id 或 paper_run_id 对应目录不存在
        ArtifactSchemaError   — orders.csv 缺列 / nav.csv 日期不连续等
        NO silent fallback — 任一阶段失败立即抛
    """
```

### 4.2 三个 diff 子模块

```python
# backend/services/validation/shadow_run/diff_engine.py

@dataclass(frozen=True)
class OrderDiffReport:
    matched: int
    qe_only: list[OrderRecord]            # qe 有 paper 无
    paper_only: list[OrderRecord]
    field_diffs: list[OrderFieldDiff]     # 三元组对齐但字段差异
    is_pass: bool                          # 零容差下 = (qe_only==0 and paper_only==0 and field_diffs==0)


@dataclass(frozen=True)
class NavDiffReport:
    per_day: list[NavDiffRow]              # 每个对账日的 qe_nav / paper_nav / diff_bp
    max_diff_bp: float
    final_diff_bp: float
    threshold_bp: int
    is_pass: bool                          # max_diff_bp <= threshold


@dataclass(frozen=True)
class PositionDiffReport:
    matched: int
    qe_only: list[PositionRecord]
    paper_only: list[PositionRecord]
    share_diffs: list[PositionShareDiff]   # 同日同 symbol 但股数不同
    is_pass: bool                          # 零股数差异
```

### 4.3 Finding 写入契约

```python
# backend/services/validation/shadow_run/finding_writer.py

def emit_finding(
    *,
    bug_root: Path,
    qe_run_id: str,
    paper_run_id: str,
    failure_type: Literal["decision_drift", "nav_drift_exceeded", "holding_drift", "manifest_mismatch"],
    severity: Literal["CRITICAL", "HIGH", "MEDIUM"],
    context: dict[str, Any],
) -> str:
    """Write a single bug record under bug_root, conforming to BUG_SCHEMA.

    File path: <bug_root>/shadow_<qe_run_id>_<paper_run_id>_<failure_type>.json
    Returns: bug_id (the file stem)

    Schema fields populated (subset of BUG_SCHEMA = 'aistock_validation_bug_v1'):
        - bug_id            : "shadow_<qe>_<paper>_<failure_type>"
        - module            : "validation/shadow_run"
        - severity          : as given
        - status            : "open"
        - title             : "Shadow run mismatch: <failure_type>"
        - fingerprint       : sha256 of (qe_run_id|paper_run_id|failure_type|context_summary)
        - assigned_agent    : null  (Lead 后续派)
        - created_at / last_seen_at : utcnow
        - context           : full dict (含 sample diff rows，bounded size)
    """
```

**重要**：本 writer 写入的 bug 文件 `_load_bugs` 在 finding_store 中是已支持的路径（参 finding_store.py:188+）；schema_version="aistock_validation_bug_v1" 是 finding_store 已识别的常量。**不需要改 finding_store**。

---

## 5. 触发机制

### 5.1 触发入口

| # | 入口 | 形态 | 用途 |
| --- | --- | --- | --- |
| T1 | CLI 手动 | `python -m backend.services.validation.shadow_run.reconciler --qe <id> --paper <id>` | 开发期单次对账 |
| T2 | scheduled job | 定时扫描"近 N 天有 paper run 完成且关联 qe run 也已完成"的 pair | 每日固定时间跑 |
| T3 | post-Paper-replay hook | Paper v2 replay 结束后自动调（可选；增加耦合慎用） | 实时监测；本设计不强推 |
| T4 | CI smoke | CI 上跑 1 个固定 fixture（小数据 fixture pair）作为冒烟 | guardrail 防回归 |

**默认推荐**：T1 + T2（解耦性高）；T3 由 Lead 决定是否引入；T4 在 PR-shadow-run 合 main 时一并加。

### 5.2 关联 qe_run_id ↔ paper_run_id 的方式

QE backtest run 与 Paper v2 replay run 的配对**不在本设施职责内**——必须由调用方传入正确的 pair。原因：
- run_id 来源不同 schema（QE 是 `qe_<yyyymmdd>_xxx`，Paper 是 `pv2_<yyyymmdd>_xxx`）
- 一个 manifest 可能对应多次 backtest + 多次 replay；配对策略多样（按 manifest_sha256 + portfolio_id + 日期窗口等）
- 配对错误应该尽早 fail（manifest_sha256 不等抛 `ManifestMismatchError`）

**推荐配对策略**（在 T2 scheduled job 里实现）：
- Paper replay 结束时在 meta.json 写 `linked_qe_run_id`（由用户或自动从 manifest 选择）
- T2 job 扫此字段产生 pair list

但本设计**仅约束 reconcile() 接受 pair**，配对策略由调用方实现。

---

## 6. 错误传播契约

承接 `feedback_no_silent_errors`：

### 6.1 错误类型

```python
class ShadowRunReconcileError(Exception): ...

class ManifestMismatchError(ShadowRunReconcileError):
    """qe.manifest_sha256 != paper.manifest_sha256 — 无法跨 manifest 对账"""
    # context: { qe_run_id, paper_run_id, qe_sha, paper_sha }

class ArtifactNotFoundError(ShadowRunReconcileError):
    """qe_run_id 或 paper_run_id 对应目录不存在 / 关键文件缺失"""

class ArtifactSchemaError(ShadowRunReconcileError):
    """artifact 文件 schema 不符（缺列 / 类型不对 / 日期不连续）"""
```

### 6.2 错误 vs Finding 的区分

- **错误（exception）**：reconcile 输入有问题（artifact 缺失 / schema 错 / manifest 不一致）→ 立即抛；不写 finding（错的对账没意义）
- **Finding**：reconcile 成功跑完但发现真实漂移（D1/D2/D3/D4/D5 fail）→ 写入 bug_root；返回 ReconciliationResult.findings_emitted

**Finding 类型集合（含本轮新增 D4/D5）**：

| failure_type | 触发维度 | severity | 来源 |
| --- | --- | --- | --- |
| `decision_drift` | D1 OrderIntent 任意差异 | CRITICAL | v1 |
| `nav_drift_exceeded` | D2 NAV max diff > threshold | HIGH | v1 |
| `holding_drift` | D3 持仓股数任意差异 | CRITICAL | v1 |
| `manifest_mismatch` | reconcile 阶段（错误而非 finding） | — | v1 |
| `turnover_drift_exceeded` | D4 换手率 relative > threshold | HIGH | **本轮新增** |
| `fees_drift_exceeded` | D5 总费用 bp > threshold | HIGH | **本轮新增** |
| `commission_drift` | D5 commission/stamp/transfer 任意差异 | CRITICAL | **本轮新增** |
| `slippage_drift_exceeded` | D5 slippage_cost relative > threshold | MEDIUM | **本轮新增** |

### 6.3 禁止做法

- ❌ artifact schema 错时改用默认值继续比对（必须抛）
- ❌ manifest_sha256 不一致时打 warning 继续（必须抛 `ManifestMismatchError`）
- ❌ 把所有失败都包成"general_failure"（必须保留 typed error）

---

## 7. 与 Mode G 的协作

`qe_sota_strategy_package_asset_governance_design_20260508.md` §A.3.4 定义 Mode G Cross-Adapter Equivalence — **同 (manifest, scores, portfolio, seed)** 在 QE/Paper/Live 三 adapter 必产 byte-equal OrderIntent。

**Mode G 与本设施的分工**：

| 维度 | Mode G | Shadow Run Consistency |
| --- | --- | --- |
| 输入 | 固定 score + 固定 portfolio（fixture） | 真实 backtest run + 真实 replay run（含真实多日 score） |
| 比对面 | OrderIntent byte-equal（决策侧） | OrderIntent + NAV + 持仓（决策侧 + 执行侧） |
| 容差 | 零容差（D1） | D1 零，D2 1bp，D3 零股数 |
| 触发 | Engine PR 合 main 前的 gate | 真实 run 结束后旁路检测 |
| 失败处置 | PR 不合 main | 写 finding；现有 review 流程 triage |

**互补关系**：
- Mode G 失败 → engine 决策代码 bug（修代码）
- 本设施 D1 失败 + Mode G 通过 → 撮合层喂入的 score / portfolio 不一致（修上游 score / portfolio 数据流）
- 本设施 D2 失败 + D1 通过 → 撮合层差异（QE Exchange vs LocalSim ledger）；可接受或上调 nav_bp_threshold
- 本设施 D3 失败 + D1 通过 → 撮合层 fill 量计算 bug（撮合层 bug，不是决策 bug）

---

## 8. 实施依赖与归属

| 项 | 归属 | 状态 |
| --- | --- | --- |
| 本设计文档 | engine-design teammate（task #37 (2)） | 交付（本文档） |
| `backend/services/validation/shadow_run/` 实施 | 待派 impl | 依赖本设计 |
| QE backtest artifact 落盘 schema 文档化 | 由 QE 工作面（Codex 端）确认 | 本设计假设 orders.csv / nav.csv / positions.csv 字段 |
| Paper v2 replay artifact 落盘 schema 文档化 | impl-paper-v2 teammate | 本设计假设 orders.jsonl / ledger_snapshots.jsonl / positions.jsonl 字段 |
| 调度（T2 scheduled job） | 待派 impl + 运维 | 实施期 |
| CI smoke（T4） | 测试基础设施 | 实施期 |

**与 Codex Phase 4-7 衔接**：
- Phase 4 (Master Seed Contract)：本设施**强依赖** seed 一致性；如 qe 与 paper 用了不同 master_seed，对账无意义。reconcile() 应额外校验 `qe.meta.master_seed == paper.meta.master_seed`，否则抛 `SeedMismatchError`（task #37 实施期补）
- Phase 5 (Model Library)：本设施依赖 `qe.meta.model_artifact_id == paper.meta.model_artifact_id`；不一致也无对账意义
- Phase 6 (Runtime Variants)：variant_id / variant_hash 应相等
- Phase 7 (Latest-data + Rolling Validation)：滚动训练后新 ModelArtifact 进入 ORIGINAL_RETESTING；本设施可以作为 retest 通过后的"端到端漂移监控"

---

## 9. 测试策略

### 9.1 单元测试（`backend/tests/validation/shadow_run/`）

| # | 测试 | 验收 |
| --- | --- | --- |
| U1 | `compute_order_diff` 在零容差下：完全等价 → is_pass=True；缺一单 → is_pass=False + qe_only/paper_only 分类正确 | fixture pair |
| U2 | `compute_nav_diff` 容差边界：最大 diff = 0.99 bp 且 threshold=1 → is_pass=True；max=1.01 bp → is_pass=False | per_day_max 模式 |
| U3 | `compute_position_diff` 股数差 100 股 → 落入 share_diffs；symbol 缺失 → qe_only / paper_only | 同上 |
| U4 | `reconcile` manifest 不一致 → 抛 `ManifestMismatchError` | pytest.raises |
| U5 | `reconcile` artifact 文件缺失 → `ArtifactNotFoundError` | 同上 |
| U6 | `emit_finding` 写入 BUG_SCHEMA 文件 → finding_store.list_bugs 能检索到 | 集成测试桥 |
| U7 | `reconcile` 全过 → ReconciliationResult.overall_status="pass" + findings_emitted=[] | 等价 fixture |

### 9.2 集成测试

| # | 测试 | 验收 |
| --- | --- | --- |
| I1 | 跑真实小型 QE backtest fixture + Paper v2 replay fixture（同 manifest）→ reconcile → 全过 | 端到端 |
| I2 | 故意修改 paper fixture 的某日 OrderIntent → reconcile → 写 1 个 `decision_drift` finding | finding 文件落盘 |
| I3 | finding_store.list_bugs 检索到 I2 写入的 bug | 跨模块契约 |
| I4 | MCP server `validation.list_bugs` 检索到 I2 写入的 bug | 跨模块契约（与 task #37 (1) 联动） |

### 9.3 数据 fixture

实施期需要 1 对 minimal QE / Paper run artifact pair，建议放：
```
backend/tests/validation/shadow_run/fixtures/
├── qe_run_dev_001/
│   ├── manifest.json
│   ├── orders.csv
│   ├── nav.csv
│   ├── positions.csv
│   └── meta.json
└── paper_run_dev_001/
    ├── manifest_pointer.json
    ├── orders.jsonl
    ├── ledger_snapshots.jsonl
    ├── positions.jsonl
    └── meta.json
```

每个 fixture < 20 行；可手工编写不依赖真实 run。

---

## 10. 性能与扩展性

### 10.1 单次对账规模

- 典型 1 个 backtest = 250 trade days × 平均 20 持仓 ≈ 5000 行 positions / 1000 行 orders
- 内存：< 50 MB；CPU：< 5s（单进程）
- 不需要并发；不需要分块

### 10.2 长期 finding 文件累积

- 每次失败对账 → 写 1 个 bug 文件
- 假设每天 1-3 次对账 + 偶发失败：每月 < 50 个文件
- bug_root 已在 finding_store 中以文件枚举方式扫描（参 finding_store.py:188-）；几百个文件无性能问题
- 长期超 1000 个文件时：手工 archive 老 bug 到 `bugs/archive/`（不在本设施 scope）

---

## 11. 关键开放问题（实施期决定，本文档不裁决）

| # | 问题 | 默认建议 |
| --- | --- | --- |
| OQ1 | T3 post-Paper-replay hook 是否启用 | 默认**不启**（增加 paper v2 耦合风险）；用 T2 定时即可 |
| OQ2 | nav_bp_threshold 默认 1 bp 是否合理 | 实施期跑 5-10 对真实 run 看分布，再 calibrate；先用 1bp |
| OQ3 | 对账失败的 finding severity 默认 HIGH 还是 CRITICAL | 默认 HIGH；CRITICAL 留给 D1 decision_drift（最严重） |
| OQ4 | 是否对账 fill price / slippage 维度 | **不在本设施**；属于撮合层细节，与 `holding_drift` 重复 |
| OQ5 | 跨 manifest（同策略包不同版本）能否做对账 | **不允许**（语义无意义）；reconcile 强制等 manifest_sha256 |

---

## 12. 不在本设计范围

- 业绩指标对账（IC / Sharpe / max drawdown）— 那是 strategy_package validation 范畴
- 重写 QE backtest_executor 或 Paper v2 day_runner 的 artifact schema
- 实时（盘中）对账 — 仅日级 / 收盘后
- 自动修复偏移（agent 自动修代码）— 仅检测 + 报告
- 跨 portfolio 对账（多 portfolio 聚合 NAV）— 单 (qe_run, paper_run) 一对一
- finding_store 之外的 finding 投递路径（Slack / email 通知等）

---

## 13. 与 Validation Center MCP Server（task #37 (1)）的衔接

本设施产出的 finding 通过现有 finding_store 进入 MCP server 的查询面：

```
Reconciler 写 bug 文件
   ↓
ValidationFindingStore._load_bugs() 自动 pick up
   ↓
Validation Center MCP Server `validation.list_bugs` / `validation.get_bug`
   ↓
agent 在 session 内查到 shadow run finding，决定如何处置（修代码 / 调阈值 / 标 false_positive）
```

**关键流转**：
- 不需要新 MCP tool（既有 list_bugs / get_bug 即可消费）
- module 字段固定 `"validation/shadow_run"`，agent 用 `module=validation/shadow_run` 过滤即可只看 shadow run findings

---

## 15. 与 Codex Validation Mode A-F + 附录 A.3.4 Mode G 整合（本轮新增）

`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` §6.2 定义 Validation Mode A-F（QE 端验证模式集合），附录 §A.3.4 引入 Mode G Cross-Adapter Equivalence。本设施与每个 Mode 的具体协作关系：

### 15.1 Mode 编号速查（来自 Codex 主体 §6.2）

| Mode | 名称 | 范围 |
| --- | --- | --- |
| A | Original-config retest | 同 manifest + 同原始配置 / 数据快照重跑 — NAV vs 原 archive |
| B | Latest-data retest | 同 manifest + 最新数据窗口 |
| C | Rolling retrain | 滚动训练新 ModelArtifact，进 ORIGINAL_RETESTING |
| D | Out-of-sample retest | 划出 OOS 段重测 |
| E | Stress / regime overlay | 极端 regime / 黑天鹅段验证 |
| F | Cross-strategy互独立性 | 多策略组合下单策略稳定性 |
| G | Cross-Adapter Equivalence（附录 A.3.4） | 同 (manifest, scores, portfolio, seed) → QE/Paper/Live 三 adapter byte-equal OrderIntent |

### 15.2 本设施与 Mode A-G 的协作矩阵

| Mode | 本设施衔接方式 | 协作粒度 |
| --- | --- | --- |
| **A Original retest** | Mode A 跑出的 QE retest run + 对应 Paper v2 replay run → 本设施作为"retest pass 后的端到端漂移监控"（与主体 §A.6.4 滚动训练后流程一致） | 强协作；推荐 Mode A 通过后 24h 内自动跑本设施一次 |
| **B Latest-data retest** | Mode B 跑出的 latest-window QE run + 对应 Paper 同窗口 replay → 本设施验证最新数据下决策+撮合一致 | 强协作；窗口对齐是关键 |
| **C Rolling retrain** | Mode C 产出新 ModelArtifact 进 ORIGINAL_RETESTING；本设施在 retest pass 后跑（同 Mode A 流程） | 强协作；本设施是滚动训练流水线的"出门 gate" |
| **D OOS retest** | Mode D 是 QE 端单边验证；本设施需要 Paper v2 也跑同 OOS 段才能对账 — **本期不强制对接**（OOS 段往往是历史，Paper v2 重放不一定有此段） | 弱协作；视有无 Paper 历史决定 |
| **E Stress overlay** | 同 Mode D；regime 段 Paper v2 不一定跑过 | 弱协作 |
| **F Cross-strategy** | 多策略组合下的单策略行为稳定性 — 本设施可单独对账每个策略包；多策略聚合层不在本设施 scope | 中协作；逐包并行跑 |
| **G Cross-Adapter Equivalence**（最强协作） | Mode G 是**单元层 fixture 等价**；本设施是**端到端真实 run 等价**。Mode G PASS 是本设施 D1 PASS 的**必要前提**（决策代码一致才可能 OrderIntent 一致） | **互锁协作**：Mode G 失败 → 本设施 D1 几乎必然失败（先修代码，不跑本设施）；Mode G PASS + 本设施 D1 失败 → 喂入数据流不一致（不是代码 bug） |

### 15.3 与 §A.5 测试矩阵衔接

主体 §A.5 规定每个 Codex Phase PR 必须含测试矩阵 + ≥ 3 个 case。本设施对应 Phase 4-7 的测试矩阵中可作为 **L4 端到端验证项**：

| Phase | 测试矩阵文件 | 本设施位置 |
| --- | --- | --- |
| Phase 4 (Master Seed Contract) | `tests/aistock_validation/modules/qe_reproducibility.md` | 本设施作为 L4 reproducibility gate 的端到端补充（同 master_seed → 同 OrderIntent + 同 NAV ± 1bp） |
| Phase 5 (Model Library) | `tests/aistock_validation/modules/model_registry.md` | 本设施 D1/D5 失败 → artifact_id 不一致 / 费率参数错配 |
| Phase 6 (Runtime Variants) | `tests/aistock_validation/modules/strategy_package_v2.md` | variant 切换前后跑本设施验证 variant_id 一致 |
| Phase 7 (Latest-data + Rolling) | `tests/aistock_validation/modules/qe_validation_modes.md` | 本设施作为 Mode A/B/C 通过后的 gate |

### 15.4 触发顺序（推荐）

```
Codex Phase 4 PR 合 main：Master Seed Contract gate
   ↓
Mode G fixture 等价性测试 PASS（Engine 实施 PR）
   ↓
现有 paper_trading_v2 + LocalSim/MiniQMTSim 基础设施落地（task #20 / #29）
   ↓
本设施实施（reconciler 落地）
   ↓
循环：每个新 manifest 走 Mode A 通过 → 24h 内本设施扫一次 → 失败写 finding
```

**关键**：本设施**前置依赖** Mode G 能 PASS（否则 D1 必然失败，跑了无意义）。Mode G 是 fixture 单元层；本设施是真实 run 端到端层。

### 15.5 失败归因 decision tree（含本轮 D4/D5）

```
本设施失败：
├── D1 OrderIntent 失败
│   ├── Mode G 也失败 → engine 决策代码 bug（修代码；不动本设施）
│   └── Mode G PASS → score / portfolio 数据流不一致（修上游数据通路）
├── D2 NAV 失败但 D1 PASS
│   ├── D5 commission_drift / fees_drift 同时失败 → 费率参数错配（合约 vs 实际）
│   └── D5 PASS → 撮合层差异（QE Exchange vs LocalSim ledger 差异；可接受或上调阈值）
├── D3 持仓失败但 D1 PASS
│   └── 撮合层 fill 量计算 bug（撮合 bug，不是决策 bug）
├── D4 换手率失败但 D1+D2+D3 都 PASS
│   └── 行为指标先行预警；通常意味着撮合层有"等价但不同形状"的差异（如 split 单 vs 整单）
└── D5 单独失败
    ├── commission_drift（CRITICAL）→ 费率参数（backtest_contract vs paper config）不一致
    ├── slippage_drift_exceeded（MEDIUM）→ 撮合层滑点模型差异；可调阈值或接受
    └── fees_drift_exceeded（HIGH）→ 总费用累计错；查 ledger 计费路径
```

---

## 14. 一句话总结

**Shadow Run Consistency Reconciler = 旁路 + 只读对账器**：消费 QE backtest run 与 Paper v2 replay run 的 artifact，做**五维 diff**（D1 OrderIntent 零容差 / D2 NAV 1bp / D3 持仓零股数容差 / D4 换手率 5% relative / D5 费用 0.5bp 总 + 各类目分级容差），失败时写入 BUG_SCHEMA finding 让 ValidationFindingStore + MCP server 消费。**与 Codex Mode A-G 整合**（前置依赖 Mode G PASS；Mode A/B/C 通过后定时跑本设施作 gate）；不修改 schema、不改 QE/Paper 路径、fail-fast typed error；CLI / 定时 / CI 三种触发。

---

**End of Shadow Run Consistency Infrastructure design**.

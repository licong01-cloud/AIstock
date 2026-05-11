# rl_execution Import Error 审计 (T4) — 2026-05-10

> 状态：audit-only，无代码改动。承诺自 cross-tool drawer 7e272edf766538c9d531b5b4。
> 关联：Codex 原始报告 drawer 8b88cd3c9f19b67aaf3f2d5a；用户 pre-classify drawer 7e272edf。
> Created: 2026-05-10. Branch: claude/paper-v2-vnpy-mvp-20260508
> 不修任何代码 / schema / migration。仅诊断 + scope 分类。

## TL;DR — 核心结论

`backend.services.rl_execution` 不存在于 git 跟踪树中，是因为 `.gitignore` 第 116 行 `rl_execution/` 通配规则把 `backend/services/rl_execution/`、顶层 `rl_execution/`、以及任何路径下的 `rl_execution/` 目录全部排除。该模块在主开发副本 `F:\Dev\AIstock\backend\services\rl_execution\` 实际存在并被使用，但所有 git 副本（worktree / 远端分支 / clone）都看不到它。这不是 paper-v2 的问题，也不是 RDAgent 的问题——是仓库的 .gitignore 规则太宽，把生产代码当作了本地构件忽略掉了。

---

## §1 import 链定位

### 1.1 backend/main.py 端到端 grep

```
backend/main.py:63: from .routers import rl_execution
backend/main.py:489:     app.include_router(rl_execution.router, prefix="/api/v1")
```

仅这两处。Line 63 是顶层 import；line 489 是在 `create_app()` 工厂里挂路由。

### 1.2 仓库范围 grep（worktree git-tracked + on-disk）

```
backend/main.py:63                                from .routers import rl_execution
backend/main.py:489                               app.include_router(rl_execution.router, prefix="/api/v1")
backend/execution_algos/v24_plan_algo.py:36       from rl_execution.executor.v24_hybrid_executor import V24HybridExecutor   (lazy, in __init__)
backend/routers/rl_execution.py:9                 from ..models.rl_execution import (...)
backend/routers/rl_execution.py:15                from ..services.rl_execution.model_registry import model_registry
backend/routers/rl_execution.py:16                from ..services.rl_execution.scheduler import trigger_rolling_train
```

`backend/services/strategy_package/`、`backend/services/paper_trading_v2/`、`backend/services/trading_core/` 全部 0 命中。`live_inference.py` 同样 0 命中。

### 1.3 模块存在性检查

```
F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\services\
  -> 无 rl_execution/ 目录 (Glob "backend/services/rl_execution*" → 无结果)
```

但同一份仓库的非 worktree 主副本上：

```
F:\Dev\AIstock\backend\services\rl_execution\__init__.py        (41 B)
F:\Dev\AIstock\backend\services\rl_execution\deploy.py          (4.0 KB)
F:\Dev\AIstock\backend\services\rl_execution\model_registry.py  (9.9 KB)
F:\Dev\AIstock\backend\services\rl_execution\scheduler.py       (6.8 KB)
F:\Dev\AIstock\rl_execution\                                    (config/ executor/ interpreter/ network/ reward/ simulator/ strategy/ + __init__.py)
```

`F:\Dev\AIstock\backend\services\rl_execution\model_registry.py` 与 `scheduler.py` 提供 `model_registry` 单例和 `trigger_rolling_train` —— 正是 `backend/routers/rl_execution.py:15-16` 期待的两个符号。

### 1.4 .gitignore 决定性证据

```
.gitignore:116: rl_execution/
```

`rtk git check-ignore -v` 验证：

```
.gitignore:116: rl_execution/   backend/services/rl_execution/__init__.py
.gitignore:116: rl_execution/   rl_execution/
```

**两条都被同一行规则忽略。** `.gitignore` 中 `rl_execution/` 没有前导 `/`，意味着 git 在任何深度都把 `rl_execution/` 当忽略目录。当前规则块在 `.gitignore:113-116`：

```
# Logs … Local data … data/  qlib_bin/  *.db  *.sqlite
…
rl_execution/
```

把生产代码模块和本地 data/log/db 工件混在同一个忽略块里——这是规则起草时的语义误判。

### 1.5 Git 历史

`rtk git ls-files | grep rl_execution` 命中 25 个文件，全部是 `scripts/rl_execution/*.sh`、`docs/rl_execution_*.md`、`backend/models/rl_execution.py`、`backend/routers/rl_execution.py`。**`backend/services/rl_execution/` 在 git 整个历史里从未出现过**（`rtk git log --all --oneline -- "backend/services/rl_execution*"` 空输出；`--diff-filter=D` 同样空）—— 不是被删的，是从未提交。

`rtk git log --all -S "rl_execution" -- backend/main.py` 三个命中：

| Hash | Author | Date | Subject |
|---|---|---|---|
| `dc9262a` | licong01-cloud <licong01@gmail.com> | 2026-03-31 00:38 | feat: 图形化参数编辑、自定义策略 initial_cash、修复 TailTWAP 兼容性、更新 gitignore 排除数据目录 |
| `c00fee2` | licong01-cloud <licong01@gmail.com> | 2026-05-09 18:47 | fix(qe): allow governance dev backend without rl execution service |
| `f709c2f` | licong01-cloud <licong01@gmail.com> | 2026-05-09 18:48 | Revert "merge(qe): governance dev backend startup fix" |

`rtk git log --all -S "rl_execution" -- backend/routers/rl_execution.py` 唯一命中：`dc9262a`（同上）。

**`dc9262a` 同时新增 `backend/routers/rl_execution.py`、`backend/models/rl_execution.py`、main.py 的导入与 router 挂载，并修改了 .gitignore**——也就是说，引入 `rl_execution` 路由和把 `rl_execution/` 加进 `.gitignore` 是**同一次提交**。这是问题根源。

### 1.6 Commit 工作面分类

| Commit | 工作面归属 | 证据 |
|---|---|---|
| `dc9262a` | RDAgent main / AIstock main | 主分支大功能合入（268 行 main.py 改动 + 创建 RL 模型 schema 初始化 + 多个执行算法），author 用 `licong01-cloud@gmail.com`（非 codex / paper-v2 分支签名），消息无 `[Codex]`/`[T*]` 标签 |
| `c00fee2` | Codex governance | commit 主题 `fix(qe): allow governance dev backend without rl execution service`；diff 内容是 try/except `ModuleNotFoundError` 包裹 `from .routers import rl_execution`；与 drawer 8b88cd3c 描述完全吻合 |
| `f709c2f` | Codex governance（自我回退） | `Revert "merge(qe): governance dev backend startup fix"` |
| `db07d16` | Codex governance | merge 提交，把 `c00fee2` 合入主线，立即被 `f709c2f` 撤销 |

---

## §2 8012 启动失败点

### 2.1 8012 入口

worktree 内 8012 来源：

- `backend/services/validation/plan_catalog.py:29` — `ALLOWED_BACKEND_PORTS = {8011, 8012}`（白名单）
- `noxfile.py:224 / 648 / 801 / 1007` — `BACKEND_PORT` 缺省 `8012`
- `frontend/playwright.config.ts:3`、`frontend/e2e/qe-label-horizon.spec.ts:3` — playwright 默认指向 8012
- `backend/tests/test_validation_center_runner_smoke.py:48` — `api_base="http://127.0.0.1:8012/api/v1"`
- `.codex/skills/verify-aistock-feature/SKILL.md` — Codex 验证 skill 显式 `set BACKEND_PORT=8012` + `python scripts/aistock_validate.py services --backend-port 8012`

worktree 内**没有**专门的 8012 启动脚本；启动方式是一致的 `uvicorn backend.main:app --port 8012`（或 nox session 包装）。

### 2.2 启动失败链

```
uvicorn backend.main:app
  └─ import backend.main
       └─ backend/main.py:63    from .routers import rl_execution
            └─ backend/routers/rl_execution.py:15    from ..services.rl_execution.model_registry import model_registry
                 └─ ModuleNotFoundError: No module named 'backend.services.rl_execution'
```

**首个抛错点**：`backend/main.py:63` 顶层 import（不是延迟 import）。`create_app()` 永远到不了 line 489。在 worktree 这种从 git 干净 clone 的环境，`backend/services/rl_execution/` 因为 `.gitignore` 永远不存在，启动必定失败。

### 2.3 与 Codex drawer 8b88cd3c 描述对照

Codex 报告："Isolated 8012 startup exposed backend.main -> rl_execution -> missing backend.services.rl_execution"。

**完全吻合**。链路、错误模块名、错误位置都一致。Codex 的 `c00fee2` 临时补丁（`try/except ModuleNotFoundError` 在 main.py:63 处包住 import，并在 line 489 处条件挂路由）确实能让 8012 启动通过——但被 `f709c2f` 回退掉了。

---

## §3 与 Codex drawer 8b88cd3c 描述的吻合度

### 一致

- `backend.main -> rl_execution -> missing backend.services.rl_execution` —— 与 §2.2 链路一致。
- "temporary startup compatibility proved Validation Center readonly smoke can pass" —— `c00fee2` patch 在 line 489 同时把 `app.include_router(rl_execution.router, ...)` 改为条件挂载，整个应用只丢失 `/api/v1/rl-execution` 子树，其他 router 正常注册，validation 路由可用。
- "the backend/main change was reverted" —— 对应 `f709c2f`。
- "Final pushed branch clean with no production DB writes, no production 8001 touched" —— diff 范围确实只动了 main.py 12 行 + governance smoke 测试 + 文档。
- a314528 / db07d16 / 5a92ba7 / 069ae8b 四个提交全部能在 git log 中定位，时间戳与 Codex 自述顺序一致。

### 差异 / 需要纠正

- Codex 称"guardrail flags historical P0 findings in that file"——`rtk git log --all --oneline --grep="guardrail"` 命中 4 条（19bb7c9 / a772c8b / 8168d6d / 54b23fb），均与"development guardrail baseline scanner"和"parallel worktree guardrails"相关；`rtk git log --grep="P0"` 配合 backend/main.py 没有定位到具体的 P0 finding ticket。回退动机是"guardrail flags historical P0 findings in that file"——这一点**部分可信**（确实存在 guardrail 框架），但**根本动因没有在公开 commit 元数据里写明**。回退理由属于团队内规约，从代码层无法证伪；从工程层评估，回退是合理的：try/except 吞 ImportError 在生产环境被 guardrail 视为反模式（fallback / silent error），与 user 的 `feedback_no_silent_errors` 规则一致。

### 关键补充（Codex 报告未提）

`backend/services/rl_execution/` **从未提交过**。Codex 的报告语气暗示"模块缺失"是临时事件；实际是**仓库结构性事实**——任何全新 clone 都会缺失。这把问题从"环境同步"提升到"git 治理"。

---

## §4 替代方案分析（仅可行性，不实施）

### 选项 A：Graceful import fallback in `backend/main.py`

代码形态：

```
try:
    from .routers import rl_execution
except ModuleNotFoundError as exc:
    if not str(exc.name or "").startswith("backend.services.rl_execution"):
        raise
    rl_execution = None
…
if rl_execution is not None:
    app.include_router(rl_execution.router, prefix="/api/v1")
else:
    logger.warning("Skipping rl_execution router")
```

—— 这正是 `c00fee2` 的实现。

| 评估项 | 结论 |
|---|---|
| 可行性 | 高（Codex 已验证 8012 readonly smoke 通过） |
| 工作量 | trivial（10 行） |
| 工作面归属 | 基础设施 / Codex governance |
| 可逆性 | 完全可逆（git revert） |
| 风险 | 与 user `feedback_no_silent_errors` 规则冲突，因此被 `f709c2f` 回退；如果不修 `.gitignore`，错误会在每个全新 clone 复发 |
| paper-v2 影响 | 无影响——paper-v2 / live_inference / paper_trading_v2 / strategy_package 无任何 rl_execution 引用，graceful fallback 路径 paper-v2 永远走 `rl_execution is None` 分支，不影响 paper-v2 业务 |

### 选项 B：Stub 模块占位

需要的最小符号集（来自 `backend/routers/rl_execution.py:15-16`）：

- `backend.services.rl_execution.model_registry.model_registry`（带 `list_versions(dev_version, status)` / `activate(dev, roll)` / `deactivate(dev, roll)` / `compare_versions(version_tags)` / `get_dev_lineage()` 方法的实例）
- `backend.services.rl_execution.scheduler.trigger_rolling_train(dev_version, reference_date)` 函数

stub 形态（仅分析，**不创建**）：

```
# backend/services/rl_execution/__init__.py  (empty)
# backend/services/rl_execution/model_registry.py
class _StubRegistry:
    def list_versions(self, **kw): return []
    def activate(self, dev, roll): raise RuntimeError("rl_execution unavailable")
    …
model_registry = _StubRegistry()
# backend/services/rl_execution/scheduler.py
def trigger_rolling_train(**kw):
    raise RuntimeError("rl_execution unavailable in this deployment")
```

| 评估项 | 结论 |
|---|---|
| 可行性 | 中（需要镜像主副本里 9.9 KB + 6.8 KB 实现的公开签名） |
| 工作量 | moderate（约 60 行 stub + 必须从主副本反向抽取 API） |
| 工作面归属 | RDAgent main（拥有 rl_execution 真实实现的团队） |
| 可逆性 | 完全可逆 |
| 风险 | stub 必须随真实实现演进保持 ABI 兼容；调用 stub 路由会得到 500——前端需要分辨 `rl_execution unavailable` 错误码；如果 .gitignore 不修，stub 文件本身也会被忽略——**不可行**（致命缺陷）。除非同时调整 .gitignore，否则 stub 永远 commit 不进去 |

### 选项 C：将 import 移到端点层（lazy import）

代码形态：

```
# backend/routers/rl_execution.py
@router.get("/models", ...)
def list_models(...):
    from ..services.rl_execution.model_registry import model_registry
    …
```

main.py 也需把 `from .routers import rl_execution` 改成等价的延迟导入；但因为 router 对象需要在 `create_app()` 期间挂载，router module 本身仍必须在启动时 import 成功。除非把 router 创建也延迟到首次请求（FastAPI 的 mount/include_router 不支持），否则**不彻底**。

| 评估项 | 结论 |
|---|---|
| 可行性 | 低（FastAPI router 注册机制要求启动期 import） |
| 工作量 | moderate（routers/rl_execution.py 改 7 处 import） |
| 工作面归属 | RDAgent main（拥有 router 文件） |
| 可逆性 | 可逆 |
| 风险 | 路由注册仍需 router 对象本身可用——除非 routers/rl_execution.py 也用 try/except 退化为占位 router，否则等价于把 §A 的 fallback 下移一层。复杂度无收益 |

---

## §5 历史关联

### 5.1 live_inference.py

```
Grep "rl_execution" → backend/services/strategy_package/live_inference.py
  No matches
```

**确认用户假设**：`live_inference.py` 不依赖 `rl_execution`。

### 5.2 paper_trading_v2/

```
Grep "rl_execution" → backend/services/paper_trading_v2/
  No matches
```

**确认 paper-v2 runtime 独立性**。`backend/services/paper_trading_v2/` 全树 0 命中。

### 5.3 worktree 内的 rl_* 目录与 RL execution 工件位置

worktree 内：

- `backend/models/rl_execution.py` —— 是 Pydantic 数据模型（RLDevLineage / RLModelVersion / RLModelCompareItem / RLModelCompareRequest），仅 62 行，**无 backend.services.rl_execution 引用**，不阻塞启动。
- `backend/routers/rl_execution.py` —— 路由文件，是直接受害者。
- `backend/execution_algos/v24_plan_algo.py:36` —— 在类的 `__init__` 内 lazy `from rl_execution.executor.v24_hybrid_executor import V24HybridExecutor`（注意**不是** `backend.services.rl_execution`，而是**顶层** `rl_execution` 包）。这是另一个被 `.gitignore` 覆盖的目录（`F:\Dev\AIstock\rl_execution\`）。但因为是 lazy，启动期不会触发。
- `scripts/rl_execution/*.sh` —— 25 个训练脚本，内容指向 WSL/RD-Agent 工作流。
- `docs/rl_execution_v15_roadmap.md` / `v16_design.md` / `v17_design.md` —— 设计文档。

**自动记忆中提到的 RL Execution v13-v24 训练历史**对应 `F:\Dev\AIstock\rl_execution\`（顶层）和训练脚本，与 `backend/services/rl_execution/` 是不同物件：

- `rl_execution/`（顶层）：RL 训练框架本体（network/strategy/simulator/reward/...）
- `backend/services/rl_execution/`：**model_registry / scheduler 是 backend 服务层包装**，把训练框架的产物（模型版本 / dev 谱系）暴露为 FastAPI 端点

两者都被 `.gitignore:116` 同一规则忽略。

### 5.4 外部参考 `F:\Dev\AIstock\report\rl_execution_training_history.md`

文件存在。从命名与自动记忆条目（"v13-v22 完整训练记录"）推断，这是 RL 训练日志报告，**不是 backend service 模块路径文档**。它讨论的是训练实验序列（v13/v14/.../v22/v24），与 `backend.services.rl_execution.{model_registry,scheduler}` 这两个 9.9 KB + 6.8 KB 的 API 包装层是**不同关注点**。结论：自动记忆条目（rl_execution_evolution / rl_layer_b_progress）与本次缺失模块不直接对应——它们是 RL 训练系统本身的历史，缺失模块是 backend 暴露层。

---

## §6 修复 scope 分类

| 工作面 | 影响范围 | 修复路径建议 | 工作量估算 | 谁应执行 |
|---|---|---|---|---|
| `.gitignore:116 "rl_execution/"` 规则 | 任何 git clone / worktree 都看不到 `backend/services/rl_execution/` 与顶层 `rl_execution/` | 把宽通配 `rl_execution/` 收紧为针对运行时构件的精确路径（如 `rl_execution/runs/`、`rl_execution/checkpoints/`、`rl_execution/data/`），让 source 文件可被 commit | trivial（5 行 .gitignore 改动 + 一次 `git add backend/services/rl_execution/ rl_execution/{*.py,config/,executor/,...}` 决策） | RDAgent main（拥有 rl_execution 模块；需要对哪些是 source / 哪些是构件做出最终切分） |
| `backend/services/rl_execution/` 源文件首次入库 | 修完 .gitignore 后必须把 5 个 .py 提交进 git | `rtk git add backend/services/rl_execution/` + 同时审查 `rl_execution/` 顶层包内容决定哪些 commit、哪些保留为构件 | moderate（涉及大模块审查、license 标注、依赖声明） | RDAgent main |
| `backend/main.py:63` 顶层 import 模式 | 任何 import 失败都会让 8011/8012/8001 全部启动失败 | 短期：保留 try/except fallback（Codex `c00fee2` 形态）；长期：改用 conditional router registry（启动期发现可选模块） | trivial（已经有现成 patch） | 基础设施 / Codex governance（main.py 属于 backend 启动入口，不属任何业务团队独占） |
| dev-port 8012 smoke | 直到 .gitignore + import 都修好之前，8012 startup 持续 fail | 修复路径=上面两条的并集，无独立修复 | 0（依赖项） | 基础设施 |
| `backend/execution_algos/v24_plan_algo.py:36` 顶层 `rl_execution` import（lazy） | 仅在创建 V24PlanAlgo 实例时触发；启动期不阻塞 | 同步 .gitignore 修复后，顶层 `rl_execution/` 也应可见——同一根因 | 0（依赖项） | RDAgent main |

### 用户 pre-classification 验证

| 用户原话 | 审计结论 | 证据 |
|---|---|---|
| "rl_execution module itself: RDAgent main workspace, NOT paper-v2 D1 boundary" | **基本成立**；但更精确表述：rl_execution 模块是 **RDAgent main** 工作面拥有、被 **AIstock main backend** 使用、**未跟踪入 git**——三段事实合并才完整 | `dc9262a` 创建 router/model 时 author 是 licong01-cloud（主分支），无 codex/paper-v2 标签；模块物理存在于主副本但不在 git；`backend/services/rl_execution/` 源码内部 logger 名 `aistock.rl_execution.*`、注释指向 RD-Agent 训练流水线 |
| "backend/main.py: infrastructure entry, technically outside strict D1, but paper-v2 team will handle the audit" | **成立**；本审计已完成 | main.py 集成 23+ 路由、跨业务，单一团队拥有不合理 |
| "dev-port 8012 smoke: infrastructure" | **成立** | nox session、playwright config、SKILL.md 三处都把 8012 视为统一开发端口规约 |

### 推荐修复路径

**单一最佳路径：根因修复——RDAgent main 团队负责修 `.gitignore` 第 116 行并把 `backend/services/rl_execution/` 源文件首次提交入库；基础设施团队同步保留 main.py 的 graceful fallback 作为防御性兜底。**

理由：

1. 把 production 路径下的 5 个 .py 永久排除于 git 之外，是仓库治理的**结构性 bug**（与 user 全局规则 `feedback_no_silent_errors` / `feedback_no_empty_db_password` 同档次的"无声故障源"）。
2. 修 .gitignore + 提交源码后，所有副本一致，不再有"环境差异导致 8012 启动失败"的反复发作。
3. graceful fallback 不再是"修复"，而是"运行时降级保险"，与生产环境对可选 ML 依赖的常规处理一致。

**估算工作量**：RDAgent main 团队 1-2 小时（审 .gitignore 切分 + 提 PR）；基础设施 30 分钟（main.py fallback 已有 c00fee2 现成 patch）。

### 风险

- **风险 1（依赖膨胀）**：`backend/services/rl_execution/scheduler.py` 调用 `python scripts/rl_execution/train.py` 并依赖顶层 `rl_execution/` 包（PyTorch / Qlib / 重 ML 依赖）。如果 RDAgent 把整个 `rl_execution/` 顶层包都 commit 进 git，paper-v2 / 8012 readonly smoke 流水线安装时会被迫拉 ML 重依赖。**缓解**：仅 commit `backend/services/rl_execution/` API 层（5 个文件），保留顶层 `rl_execution/` 在 .gitignore 内（继续作为开发者本地训练目录），并把 `model_registry.py` / `scheduler.py` 中对顶层 `rl_execution.*` 的 import 局限在函数内部（lazy）。`scheduler.py:140` 的 subprocess 调用本身就是 lazy 的，不需要 import；`deploy.py:34` `from rl_execution.network.mlp_network import ExecutionMLP` 已在函数内 lazy。结论：API 层公开 5 文件 commit 不引入新 ML 依赖。
- **风险 2（治理 P0 复发）**：`c00fee2` 被 `f709c2f` 回退的根本动因是 try/except ImportError 触发 guardrail。如果只采纳 graceful fallback 而不修 .gitignore，每次回退/重启都会循环这一冲突。**缓解**：必须把 .gitignore 修复作为前置条件，fallback 仅作为防御层，不作为主修复。
- **风险 3（rl_execution 后续依赖扩散）**：若 RDAgent 后续向 `backend.services.rl_execution` 加更多符号且 paper-v2 路由意外引用，paper-v2 runtime 边界会被打破。**缓解**：在 paper-v2 D1 boundary 文档中显式标注 "rl_execution = RDAgent main owned, paper-v2 must not depend on" 守则；CI 加 import 静态扫描（grep `from.*rl_execution` in paper-v2 模块树）。
- **风险 4（model_registry DB 副作用）**：`backend/services/rl_execution/model_registry.py` 9.9 KB 的实现可能带 PG 写入。一旦 commit 入库，启动期 import 是否触发 DB 连接需要 RDAgent 团队复核。**缓解**：审 commit 时把任何模块级 DB 调用降级到方法级 lazy。

---

## 附录 A — 关键文件路径与行号速查

| 路径 | 角色 |
|---|---|
| `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\main.py:63` | 顶层 import（首发故障点） |
| `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\main.py:489` | router 挂载（永远到不了） |
| `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\routers\rl_execution.py:15-16` | 缺失符号引用点 |
| `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\models\rl_execution.py` | Pydantic schema（不阻塞） |
| `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\execution_algos\v24_plan_algo.py:36` | 顶层 rl_execution lazy import（间接受害） |
| `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\.gitignore:116` | **根因** |
| `F:\Dev\AIstock\backend\services\rl_execution\model_registry.py` | 真实实现（仅在主副本可见） |
| `F:\Dev\AIstock\backend\services\rl_execution\scheduler.py` | 真实实现（仅在主副本可见） |
| `F:\Dev\AIstock\backend\services\rl_execution\deploy.py` | 真实实现（仅在主副本可见） |
| `F:\Dev\AIstock\backend\services\rl_execution\__init__.py` | 真实实现（仅在主副本可见） |
| `F:\Dev\AIstock\rl_execution\` | RL 训练框架（顶层包，主副本可见） |
| `F:\Dev\AIstock\report\rl_execution_training_history.md` | RL v13-v22 训练历史报告（与本审计无直接对应） |

## 附录 B — 关键 commit 速查

| Hash | Author | Date | 角色 |
|---|---|---|---|
| `dc9262a` | licong01-cloud | 2026-03-31 | 引入 routers/rl_execution.py、main.py:63 import、修改 .gitignore（**根因 commit**） |
| `c00fee2` | licong01-cloud | 2026-05-09 18:47 | Codex 临时 fallback fix |
| `db07d16` | licong01-cloud | 2026-05-09 18:47 | merge c00fee2 |
| `f709c2f` | licong01-cloud | 2026-05-09 18:48 | revert c00fee2 |
| `a314528` | licong01-cloud | 2026-05-09 18:50 | governance smoke continuation checkpoint |
| `069ae8b` | licong01-cloud | (前) | governance migration DB smoke 加固 |
| `5a92ba7` | licong01-cloud | 2026-05-09 18:42 | merge governance smoke errors hardening |

worktree HEAD：`18fdcf12a6efcaef095bd2f234db4433450d86b6`（branch claude/paper-v2-vnpy-mvp-20260508）

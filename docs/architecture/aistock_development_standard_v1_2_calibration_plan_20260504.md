# AIstock 开发规范 v1.2 校准与历史治理方案

> 日期：2026-05-04
> 状态：评审草案；本文不是 active standard，不改变当前 v1.1 门禁
> 文档位置：`docs/architecture/aistock_development_standard_v1_2_calibration_plan_20260504.md`
> 当前生效规范：`docs/standards/aistock_development_standard_v1.1_20260504.md` 和 `docs/standards/aistock_development_standard_v1.1_20260504.yaml`

## 1. 目标

本方案用于指导 AIstock 开发规范 v1.2 的下一轮校准。v1.2 不应把 Python 社区规范简单堆叠到项目规范中，而应只吸收与 AIstock 直接相关、能降低真实工程和量化业务风险、能逐步自动化落地的规则。

核心目标：

1. 保持规范完整、内部一致、无冲突、无重复。
2. 明确区分运行时代码、文档、测试 fixture、`debug_tools/`、正式脚本和配置文件的不同约束。
3. 对新增/修改代码严格，对历史遗留问题建立 baseline/backlog 渐进治理。
4. 把历史遗留垃圾代码和疑似 dead code 纳入生命周期管理，但不自动删除。
5. 先做规则校准和只读盘点，再更新 active standard v1.2。


## 2. 质量治理总路线

当前已确认的路线是：先完成开发规范和历史 baseline，再建设自动化验证流水线 / Validation Center，最后用流水线逐步修复历史遗留问题。这个顺序本身是质量治理的前置约束，不应跳过。

```text
开发规范 v1.1 / v1.2 校准
  -> guardrail baseline + legacy/dead-code inventory + coverage baseline
  -> Validation Center / nox / aistock_validate 形成可观测、可追溯流水线
  -> 每个遗留问题按模块拆分修复
  -> 每次修复必须有复现、回归、证据和提交记录
```

### 2.1 当前状态

| 项目 | 状态 | 说明 |
|---|---|---|
| active standard v1.1 | 已完成 | 人类可读规范和同版本 YAML 已在 `docs/standards/` 生效，v1.0 已归档。 |
| guardrail scanner 与首批 baseline | 已完成 | 历史违规作为 baseline/backlog，不作为一次性全仓阻断。 |
| legacy/dead-code inventory 工具与 baseline | 已完成 | 输出是 advisory candidate，不是删除批准清单。 |
| v1.2 规则校准 | 待实施 | 需要先确认目录例外、误报、硬编码路径语义和新增 Python 规则。 |
| coverage baseline / coverage gate | 待实施 | 当前 run metadata 已预留 coverage 字段，但还需要真实 `pytest-cov` 采集、解析和阈值策略。 |
| Validation Center 第一阶段完整闭环 | 待实施 | 先实现只读计划、历史 run、coverage、evidence、guardrail/legacy findings 展示，再考虑受控执行；字段语义按长期架构设计。 |

### 2.2 执行原则

- 历史问题不阻塞流水线建设；流水线建设的目标之一就是把历史问题变成可追踪、可复现、可验收的 backlog。
- 不在 Validation Center 可验证前启动大规模遗留修复；否则修复质量不可观测，容易引入回归。
- active v1.1 在 v1.2 人类规范、机器 YAML、测试用例、baseline 和误报校准完成前不被替换。
- 新增/修改代码必须逐步遵守更严格规则；历史代码先 baseline、分级、分模块治理。
- 遗留修复必须最小化范围，禁止借修复名义重构无关模块、删除未确认资产或静默改变业务逻辑。
- 清理候选、guardrail findings、coverage findings 和真实 Bug 最终都应进入统一质量问题闭环，但其处置优先级和阻断策略不同。
- 后续实现不得采用“字段先少存、接口先凑合、后续再补”的简化版；阶段划分只限制接入范围，不降低数据模型、contract、证据链和测试完整性。

### 2.3 后续缺口

下一批工作应优先补齐：

1. `CONFIG-HARDCODE-001` / 绝对路径 / WSL 红线的误报校准和测试样例。
2. coverage contract、baseline、changed-files/diff coverage 的第一阶段完整闭环实现。
3. Validation Center 只读 API/UI 第一阶段完整闭环，展示 run metadata、evidence、coverage、guardrail 和 legacy inventory。
4. 质量问题状态机与 agent-context，为 Codex/Claude 后续修复提供机器可读上下文。
5. 在流水线可验证后，按 QE/Paper v2/Qlib/data/frontend/root pollution 等模块分批治理历史问题。

## 3. 权威资料取舍原则

参考资料只作为规则来源池，不直接全文搬运：

| 来源 | 采用边界 | 不采用边界 |
|---|---|---|
| PEP 8 | 导入、命名、可读性、避免隐式/复杂写法。 | 不把所有格式细节做阻断，不做全仓一次性格式化。 |
| PEP 257 | public API、service、复杂业务函数的 docstring。 | 不要求所有 private 小函数写 docstring。 |
| Python typing / mypy | API/service/repository 边界、核心数据结构类型化。 | 不一次性开启全仓 strict。 |
| Ruff | 作为机器规则执行工具，优先 bugbear/security/import/logging 类规则。 | 不一次性打开全部 rules。 |
| pytest good practices | 测试分层、fixture 隔离、参数化、长耗时测试分级。 | 不要求所有历史测试立即重构。 |
| Python logging/pathlib/contextlib | 日志、路径、资源释放的具体写法。 | 不把文档示例中的本机路径当运行时违规。 |
| Google Python Style Guide | 大型工程经验补充。 | 与 AIstock 分层、生产隔离、QE/API-only 红线冲突的部分不采用。 |

采纳一条规则前必须回答：

- 它解决 AIstock 哪个真实风险？
- 适用哪些目录和代码类型？
- 哪些目录必须例外？
- 新代码是否阻断？历史代码如何处理？
- 是否可自动检测？误报如何豁免？

## 4. `CONFIG-HARDCODE-001` 校准方案

v1.1 已包含硬编码路径和密钥规则，但 v1.2 需要细化，避免与用户明确指定的规范目录和文档示例冲突。

### 4.1 建议拆分语义

| 规则 | 严重级别 | 说明 | 执行策略 |
|---|---:|---|---|
| `CONFIG-HARDCODE-001` | P1 | 运行时代码禁止硬编码本机路径、生产端口、密钥。 | `block_new_only` |
| `CONFIG-ABS-PATH-001` | P1 | backend/service/scripts 默认值禁止绝对路径，必须来自 config/env/DB/manifest/request。 | 先 `warn_new_only`，校准后 P1 |
| `ARCH-WSL-001` | P0 | Windows 侧禁止直接读 WSL/远端 workspace。 | `block_new_only`，核心链路立即阻断 |

### 4.2 目录语义

| 目录/文件类型 | 绝对路径策略 |
|---|---|
| `backend/**/*.py` | 运行时代码禁止硬编码绝对路径；必须注入配置或读取 catalog/manifest。 |
| `scripts/**/*.py` | 正式复用脚本禁止把绝对路径作为默认业务路径；允许 CLI 参数传入本机路径。 |
| `frontend/src/**/*` | 禁止硬编码后端生产端口、本机文件路径、worker 地址。 |
| `backend/tests/**` | 允许 fixture 路径样例，但不得访问真实生产路径、WSL workspace 或远端 API。 |
| `debug_tools/**` | 允许本机路径用于诊断复现，但不得被生产服务、scheduler、正式 API 依赖。 |
| `docs/**` | 允许说明性路径和用户明确指定目录；不作为运行时硬编码违规。 |
| 配置文件 | 允许 dev-only/local-only 路径，但必须标注用途、环境和是否可提交。 |

### 4.3 推荐写法

禁止：

```python
ARTIFACT_ROOT = "F:\\Dev\\AIstock\\qe_archive\\artifacts"
```

推荐：

```python
from pathlib import Path


def resolve_artifact_root(configured_root: str | None) -> Path:
    if not configured_root:
        raise ValueError("artifact root is required")
    return Path(configured_root).expanduser().resolve()
```

禁止：

```python
worker_file = r"\\wsl$\\Ubuntu\\home\\lc999\\rdagent\\workspace\\result.pkl"
```

推荐：

```python
payload = worker_client.fetch_artifact_manifest(task_id=task_id, loop_index=loop_index)
```

## 5. v1.2 建议新增 Python 代码写法规则

以下规则是候选，不代表立即全部阻断。v1.2 应先写入人类规范并把机器规则设置为 `warn_new_only` 或 `manual_review_only`，再根据误报率决定升级。

| rule_id | 建议级别 | AIstock 风险 | 初始执行 |
|---|---:|---|---|
| `PY-MUTABLE-DEFAULT-001` | P1 | 默认 list/dict 跨请求共享，影响服务状态和实验配置。 | 新代码阻断 |
| `PY-BARE-EXCEPT-001` | P1 | 捕获系统退出/键盘中断或隐藏真实业务失败。 | 新代码阻断 |
| `PY-RAISE-FROM-001` | P2 | 包装异常丢失根因，不利于 agent 修复和 UI 报错。 | warning |
| `PY-ASSERT-RUNTIME-001` | P1 | `assert` 在优化模式可被移除，不能做业务校验。 | 核心模块阻断 |
| `PY-PRINT-RUNTIME-001` | P2 | backend/service 用 `print` 难以审计和关联 run_id。 | warning，核心服务逐步阻断 |
| `PY-LOGGING-FSTRING-001` | P2 | 日志 eager formatting、缺少结构化上下文。 | warning |
| `PY-SUBPROCESS-SHELL-001` | P1/P0 | shell 注入、不可控执行、误杀生产。 | 新代码阻断；涉及 8001 为 P0 |
| `PY-HTTP-TIMEOUT-001` | P2/P1 | 外部 API 或 worker 调用卡死长任务。 | 高风险模块 P1 |
| `PY-PICKLE-TRUST-001` | P1 | 不可信 pickle 执行风险，实验 artifact 不可追溯。 | 新代码阻断或强制 manifest |
| `PY-TYPE-PUBLIC-001` | P2 | API/service/repository 边界无类型，agent 修改易破坏 contract。 | warning，逐步提高 |
| `PY-PANDAS-ITERROWS-001` | P2 | 热路径低效，分钟线/因子计算性能风险。 | warning |
| `PY-PANDAS-MERGE-001` | P2/P1 | 大表 join 行数爆炸、PIT 口径错误。 | 数据/QE 模块 P1 |
| `PY-TEST-ISOLATION-001` | P1 | 单元测试误连生产端口、远端 API、WSL workspace。 | 新测试阻断 |

## 6. 历史遗留和 dead code 治理规范

AIstock 历史代码包含探索期脚本、旧页面、旧设计文档、未引用模块和一次性诊断代码。v1.2 应把这类代码纳入生命周期规范。

### 6.1 生命周期状态

| 状态 | 含义 | 允许行为 |
|---|---|---|
| `active` | 当前业务链路使用。 | 正常维护，必须有测试。 |
| `deprecated` | 仍可能被旧入口调用，但不再推荐。 | 禁止新增依赖，记录迁移计划。 |
| `legacy_readonly` | 仅保留历史参考或迁移对照。 | 不改业务逻辑，只允许归档/注释。 |
| `delete_candidate` | 疑似无引用，待确认删除。 | 必须有只读盘点和验证计划。 |
| `removed` | 已删除。 | 删除 commit 必须有验证证据。 |

### 6.2 删除前检查清单

删除或移动历史代码前必须确认：

- 是否被 FastAPI router、Next.js route、scheduler、nox、pytest、CLI、DB migration/bootstrap 引用。
- 是否被文档标为当前入口。
- 是否被配置文件、环境变量、前端 API client、测试 fixture 引用。
- 是否存在动态 import、字符串注册、反射调用。
- 是否属于受保护资产、实验 artifact、模型权重、历史 ledger。
- 是否已有替代路径和回滚方案。

### 6.3 自动化策略

- 第一阶段只读 inventory，不自动删除。
- 工具输出 `candidate` 而不是 `dead` 结论。
- 每个候选项必须有 `confidence`、`evidence`、`risk`、`recommended_action`。
- 删除必须走单独 commit，且只包含清理相关文件。
- 高风险模块删除必须有 targeted tests 和 import/API smoke。

## 7. 历史 baseline 执行策略

| 问题类型 | 新增/修改代码 | 历史代码 |
|---|---|---|
| P0 红线 | 阻断 | 立即建高优先级 issue，必要时隔离。 |
| P1 架构/数据风险 | 默认阻断 | baseline/backlog，按模块治理。 |
| P2 质量/性能风险 | warning，核心模块可阻断 | 记录趋势，不打断普通开发。 |
| P3 风格 | 不阻断 | 随重构顺手清理。 |
| dead code | 禁止新增孤儿代码 | 只读盘点，人工确认后删除。 |

推荐治理顺序：

1. QE/RD-Agent workspace、artifact、实验数据完整性。
2. Paper Trading v2、Selection Center、StrategyPackage。
3. Qlib exporter、数据管线、因子库、HMM。
4. DB schema/comment/migration。
5. Frontend UI 错误态和 raw JSON。
6. 根目录污染、历史脚本、历史文档。

## 8. v1.2 测试用例设计

规范 v1.2 更新时必须同步增加测试：

| 测试 | 目标 |
|---|---|
| YAML source/version sync | v1.2 YAML 指向 v1.2 MD。 |
| rule_id presence | 每条 enabled rule 在人类规范中有锚点。 |
| hardcode runtime path positive | backend/service 中硬编码 `F:\...` 被识别。 |
| docs path exception | docs 中说明性 `F:\Dev\AIstock\debug_tools` 不被阻断。 |
| tests fixture exception | backend/tests 中 fixture 路径不触发 P1。 |
| WSL path P0 | runtime 中 `\\wsl$` 仍触发 P0。 |
| mutable default | `def f(x=[])` 触发候选规则。 |
| bare except | `except:` 触发候选规则。 |
| debug_tools dependency | backend/service import `debug_tools` 触发违规。 |
| dead-code inventory parser | inventory 输出 schema、confidence、risk 字段。 |

## 9. 实施顺序

已完成基础：

- active v1.1 人类规范与机器 YAML 已生成并归档 v1.0。
- guardrail scanner、首批 guardrail baseline、legacy/dead-code inventory 工具和 baseline 已完成。
- 上述 baseline 只用于分级治理和趋势跟踪，不代表立即修复或删除授权。

后续实施顺序：

1. 评审本文档，确认 v1.2 只做校准和生命周期治理，不堆叠全部社区规范。
2. 校准 `CONFIG-HARDCODE-001`、绝对路径、WSL 红线、`debug_tools/` 例外和测试样例。
3. 增加 coverage contract / baseline / parsing / gate 的第一阶段完整闭环方案与验证记录，保证数据结构、失败语义和证据格式一次设计到位。
4. 起草 `docs/standards/aistock_development_standard_v1.2_YYYYMMDD.md`，归档 v1.1。
5. 同步生成 `docs/standards/aistock_development_standard_v1.2_YYYYMMDD.yaml`。
6. 补充 scanner tests、coverage tests、legacy inventory regression 和 targeted guardrail scan。
7. 重新生成 guardrail/legacy/coverage baseline，并把新增违规与历史 baseline 区分展示。
8. 建设 Validation Center 只读 API/UI 第一阶段完整闭环，先展示计划、run、evidence、coverage、guardrail 和 legacy findings。
9. 评审后再决定是否把 changed-files P0/P1 和 coverage 阈值接入更严格 L0。
10. 在流水线可验证后，才开始按模块修复历史遗留问题，并要求每个修复都有复现、回归、证据和提交记录。

## 10. 当前不执行

- 不修改 active v1.1 规范正文。
- 不启用新的阻断规则。
- 不全仓格式化。
- 不全仓 mypy strict。
- 不自动删除 dead code。
- 不移动历史文档或历史脚本。
- 不重启生产 `8001` 或远端 API。

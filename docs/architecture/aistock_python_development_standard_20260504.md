# AIstock Python 开发规范 v0

> 日期：2026-05-04
> 状态：v0，作为 AIstock 开发规范和 Guardrail 机器规则的基础；后续依据全仓 baseline scan 校准。
> 文档位置：`docs/architecture/aistock_python_development_standard_20260504.md`
> 关联文档：`docs/architecture/aistock_development_standards_and_guardrails_20260504.md`

## 1. 目标

本规范用于统一 AIstock Python 代码的可读性、可测试性、可维护性和运行安全。它不是一次性重构要求，而是“新增/修改代码必须遵守，历史问题通过 baseline 逐步治理”的工程基线。

核心目标：

1. 让 backend、scripts、data pipeline、QE/Paper/HMM 等 Python 代码有统一最低标准。
2. 把 P0/P1 红线转成 `development_guardrails.yaml` 和自动扫描。
3. 避免静默 fallback、资源泄漏、无界内存、路径硬编码、生产服务误操作。
4. 支持 Codex/Claude 等 agent 读取规范、复现问题、修复问题并回写验证证据。

## 2. 参考基线

| 来源 | AIstock 采用方式 |
|---|---|
| PEP 8 | 作为 Python 命名、import、空行、可读性的基础；项目已有风格优先于一次性重排。 |
| PEP 257 | 公共模块、类、函数的 docstring 采用简洁语义；复杂业务函数必须说明输入、输出、错误语义。 |
| PEP 484 | 新增业务代码应逐步加类型标注，尤其是 API contract、repository DTO、配置和 run metadata。 |
| Google Python Style Guide | 采用其异常、全局状态、线程/并发、类型和可维护性建议；避免魔法和隐式副作用。 |
| Black / Ruff / mypy / pytest | 作为后续自动格式、lint、类型和测试工具链，不在第一阶段全仓强制重排。 |
| Qlib code standard | 与 Qlib/QE 相关代码优先参考其 Black、lint、pre-commit 和开发测试习惯。 |

## 3. 风险等级

| 等级 | 处理方式 | Python 示例 |
|---|---|---|
| P0 | 新代码立即阻断；历史问题优先修复或隔离 | 静默返回成功、生产端口操作、直接访问 WSL/远端 workspace、交易 fallback 改变结果。 |
| P1 | 新代码阻断；历史问题进入高优治理 | DB schema 无 comment、artifact 只有路径无 manifest、缺 timeout、缺幂等性。 |
| P2 | 新代码 warning 或按模块阻断；历史分批治理 | 大函数、弱类型、未拆分 parser、raw dict 横传、测试覆盖不足。 |
| P3 | 记录和趋势治理 | 命名不一致、局部风格不统一、注释不足。 |

## 4. 代码组织

- 新业务代码放在已有模块边界内：router 只做 API contract，service 做业务逻辑，repository/data access 做持久化。
- 不允许在 service 中隐式执行 DDL；DB schema 由 migration/bootstrap 管理，并带表字段 comment。
- CLI 脚本必须把解析参数、业务执行、输出写入拆开，方便单元测试。
- 高风险业务逻辑必须可注入依赖，不直接绑定全局 DB、全局 HTTP client、全局 clock。
- 新增大型模块必须同步设计文档、测试矩阵、run evidence 策略。

## 5. 命名和类型

- 函数、变量、模块使用 `snake_case`；类使用 `PascalCase`；常量使用 `UPPER_SNAKE_CASE`。
- API request/response、DB DTO、config、manifest、run metadata 应使用 Pydantic model、dataclass 或明确 TypedDict。
- 禁止在核心业务中长期传递未定义 schema 的大 dict；如果必须使用 JSON，应有 schema/version/source/quality 字段。
- 新增公共函数应标注参数和返回类型；高风险分支的 `Any` 必须有注释或后续治理任务。
- 时间、金额、比例、收益率字段必须在命名或注释中说明单位和口径。

## 6. 错误处理

P0 禁止：

```python
try:
    do_business_work()
except Exception:
    return []  # forbidden
```

要求：

- 业务错误必须 fail-fast，返回结构化错误码和可操作上下文。
- 不能用空数组、`None`、`True`、默认 0、默认价格、默认资金伪装成功。
- 允许兜底时必须满足：显式配置、可审计日志、UI 可见、测试覆盖、不会改变交易/回测语义。
- catch broad exception 时必须重新抛出业务异常或记录 `partial/failed` 状态，不得吞掉。
- parser 可以收集多条错误，但最终必须给出 `complete/partial/failed`，不能默默丢字段。

## 7. 日志与可观测

- 长任务必须记录 `run_id/task_id/loop_index/step/status/duration`。
- 日志不得泄露 token、密码、数据库连接串。
- 高频循环日志要限流，避免日志爆炸和磁盘占满。
- 业务失败日志应包含输入摘要、缺失字段、来源 artifact、修复建议。
- 成功日志不得替代持久化状态，UI/API 必须从 DB 或 run metadata 读取权威状态。

## 8. 配置和路径

- 禁止硬编码本机路径、用户目录、生产端口、远端 worker 路径、密钥。
- 路径和端口必须来自配置、环境变量、DB catalog、manifest 或 API request。
- Windows 侧 backend 不得直接访问 WSL/远端 worker workspace；必须通过 API 或 AIstock-owned artifact store。
- 新增配置必须有默认值来源、覆盖顺序、有效值范围和快照记录。
- 交易/回测配置必须记录 effective config，确保可复现。

## 9. 资源和并发

- HTTP 请求、subprocess、DB 查询、文件读写必须有 timeout 或上下文管理。
- 大 DataFrame/CSV/parquet 处理必须考虑 chunk/batch；禁止无边界全量读大文件。
- 全局 cache 必须有 max size、TTL、clear 或生命周期说明。
- 后台任务必须支持 heartbeat、timeout、cancel、状态持久化和重复执行幂等。
- 子进程启动必须记录命令摘要、环境白名单、退出码和日志路径。

## 10. 测试要求

- 新增 parser/config/manifest/cost/ledger/position 逻辑必须有 L1/L2 测试。
- 修改 API contract 必须有 FastAPI/TestClient 或 service-level contract 测试。
- 修改数据 pipeline 必须有小样本 fixture、schema 检查、row_count/hash/quality oracle。
- 修复 bug 必须增加回归测试，run record 中记录失败和复测证据。
- 高风险代码 coverage 第一阶段目标：新增/修改 line >= 80%，branch >= 70%。

## 11. 提交前最低检查

开发者或 agent 在提交前至少执行：

```powershell
python -m compileall <changed-python-paths>
python -m pytest <targeted-tests> -q -p no:cacheprovider
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
```

涉及 UI/API/DB/QE/Paper 的变更还必须执行对应 nox session 和 run evidence。

## 12. 当前不做

- 不对全仓历史代码一次性 Black 格式化。
- 不要求历史代码立刻补满类型标注。
- 不把 P2/P3 历史问题作为第一版阻断项。
- 不用云端 CI 代替本地真实业务验证。

## 13. 测试设计

| 用例 | 层级 | 验证内容 | 自动化路径 |
|---|---|---|---|
| PY-STD-001 | L0 | 规范文档可读取、引用路径正确 | UTF-8/read check |
| PY-STD-002 | L0 | 机器规则能识别静默 fallback、路径红线 | `backend/tests/test_aistock_guardrail_scan.py` |
| PY-STD-003 | L0 | changed-files 扫描可输出 JSON/MD | `scripts/aistock_guardrail_scan.py` |
| PY-STD-004 | L0 | baseline scan 不写业务数据、不启动服务 | 只读命令和 evidence |

## 14. 外部参考

- PEP 8: https://peps.python.org/pep-0008/
- PEP 257: https://peps.python.org/pep-0257/
- PEP 484: https://peps.python.org/pep-0484/
- Google Python Style Guide: https://google.github.io/styleguide/pyguide.html
- Black code style: https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html
- Ruff linter: https://docs.astral.sh/ruff/linter/
- mypy: https://mypy.readthedocs.io/en/stable/
- pytest good integration practices: https://docs.pytest.org/en/stable/explanation/goodpractices.html
- Qlib code standard: https://github.com/microsoft/qlib/blob/main/docs/developer/code_standard_and_dev_guide.rst

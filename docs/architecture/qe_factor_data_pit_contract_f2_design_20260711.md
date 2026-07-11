# QE factor_data PIT 数据契约 + 运行路径 + 生产验收 F2 设计

- 文档类型:F2 跨模块 / 生产关键架构设计(FEATURE-WORKFLOW-001)
- 日期:2026-07-11
- 跨仓:AIstock(验证器 / Data Doctor / .env / QE 路径解析 / bundle 生成)+ RD-Agent(QE_FACTOR_DATA_DIR / factor_data_template / 无回退)+ WSL runtime + rdagent-node1
- 关联:`update-backtest-dataset` skill、`dc_signoff_candidate_20260630.md`、l2_code_id 链路(#1940/#1943/#1944/#1945)、`infra.compute_nodes`
- 分类理由:变更 QE 全体因子的日频资格口径 + 生产运行数据源路径 + 双节点部署,属跨模块、生产关键。

## 1. 背景 (Background)

候选源 `F:\Dev\AIstock\qlib_snapshots\qlib_st_pit_active_h5_daily_candidate_20180801_20260630` 已验证到 2026-06-30(daily_pv 8,416,951 行 / 5,119 股 / 零重复零 NaN;sector_data 23 列 l2_code_id int16;static_factors 121 数据列 + 2 索引列;static l2_code_id 无 -1 且与 sector 逐行一致;与 DB / 当前 WSL 共享数据数值一致)。

三类生产隐患:
1. **PIT 资格口径未强制**:`instruments/all.txt` 的多区间 PIT spans 是 QE 股票资格权威,但辅助 H5 未按其裁剪,存在越界数据,rolling/rank/entropy 在越界样本上计算会污染截面。
2. **候选辅助 H5 越界**:daily_basic 多 80 股(含 200xxx/900xxx B 股、000409.SZ、600289.SH,相对 daily_pv 多 4089 条)、bak_basic/cyq_perf 多股、moneyflow 多 2 条、sector/margin 存在有效 PIT 区间外记录。
3. **QE 运行路径碎片化**:`.env RDAGENT_FACTOR_DATA_WSL` 与 compute node `wsl2-5080` 均指向 `/mnt/f/...`(只有 04-30 数据),`rdagent-node1` 指向 `/home/lc999/data/factor_data`;存在静默 `/mnt` fallback 风险。

### 1.1 已接受限制(用户明确批准,不阻断,须记入 meta)
- 退市股全历史剔除 = 已知幸存者偏差限制;`300029.SZ` 不要求保留。(用户批准)
- `001248.SZ`、`301583.SZ` 为未满一年新股,不进入回测,不阻断部署。(用户批准)
- daily_basic 相对 daily_pv 缺 896 条**全部是停牌日**(volume=0、amount=0、896/896 存在于 `suspend_d`、`market.daily_basic` 亦无记录、正���交日漏数=0)。**允许缺失,保 NaN,禁填 0 / 禁伪造**。(用户批准)

## 2. Scope / 范围

### 2.1 Scope
建立 canonical PIT index 契约、生成 PIT 裁剪的版本化 QE bundle、补齐元数据/schema、修验证器与 Data Doctor、统一 QE 运行路径、版本化部署 + 回滚(含生产切换前 dry-run 回执)。

### 2.2 Non-goals / 非目标
- 不修退市 PIT(delist_pit)/停牌 PIT(pause_pit),仅记 `false` + accepted limitation。
- 不就地破坏源快照。
- 不触发全量因子指标 / 相关性 / 正式 QE 大回测。
- 无明确授权不覆盖生产目录、不重启服务、不改 `AGENTS.md`、不在 `F:\Dev\AIstock` main 直接开发。
- 分钟线不与日频辅助逐 bar 对齐(只共享同一日级 PIT 资格规则)。

## 3. Architecture / 架构与关键决策

1. **canonical PIT index 单一真源** = `instruments/all.txt`(多 span)。新增 PIT index 解析器:读 all.txt 展开为每日 `(datetime, instrument)` 集合(按 calendar 与 span 区间交集),供 bundle 生成与 Data Doctor 共用同一实现,避免口径漂移。
2. **PIT mask 在时序算子前**:bundle 生成先按 canonical index 裁剪,再做任何 rolling/rank/entropy;缺失保 NaN。static_factors 派生列若涉及 rolling,须在裁剪后面板上计算。
3. **版本化 bundle,不动源快照**:源 `..._candidate_...20260630` 只读;PIT 裁剪输出到版本目录 `..._l2_v1`。
4. **路径统一 + fail-fast**:AIstock QE 命令生成侧解析 effective factor_data_dir(node 查 `infra.compute_nodes.factor_data_dir`;本机查 `.env`),打印记录;RD-Agent 侧 `QE_FACTOR_DATA_DIR` 缺失/不符即 raise,禁 `/mnt` fallback。
5. **Data Doctor 权威 = WSL runtime**(symlink 指向的运行目录),Windows mirror 仅 staging。
6. **部署原子性**:`factor_data` 为 symlink 指向版本目录;切换 = 原子 `ln -sfn`;回滚 = 指回旧版本目录。

## 4. Contracts / 契约

### 4.1 数据契约(QE bundle 文件集与 schema)
- 文件集:`daily_pv.h5` / `daily_basic.h5` / `moneyflow.h5` / `bak_basic.h5` / `cyq_perf.h5` / `sector_data.h5` / `margin_detail.h5` / `static_factors.parquet` + `README.md` / `static_factors_schema.csv` / `static_factors_schema.json` / `metadata/aistock_field_map.csv` / `calendars/day.txt` / `instruments/all.txt` / `meta.json` + debug bundle。
- 列契约:`sector_data.h5` = 23 列(22 sw2_* + `l2_code_id` int16);`static_factors.parquet` = 123 物理列(121 数据 + 2 索引 datetime/instrument)。
- `l2_code_id`:int16;`unknown=-1`;稳定映射来源 `market.sw_index_classify` L2 `index_code ASC`(禁 factorize);static 与 sector 的 l2_code_id 逐行一致。
- 缺失语义:辅助 H5 index 为 canonical PIT index 的子集;缺失保 NaN,禁填 0;slow-static 仅显式 PIT as-of 才 ffill。

### 4.2 索引契约(canonical PIT index)
- 真源 = `instruments/all.txt` 多区间 PIT spans;一股可多 span;展开为每日 `(datetime, instrument)`。
- 所有日频辅助 H5 的 `(datetime, instrument)` index 为 canonical PIT index 的子集(不得越界)。

### 4.3 路径契约(runtime factor_data_dir)
- `wsl2-5080` 与 `rdagent-node1` 的 `factor_data_dir` 均 = 各自主机 `/home/lc999/data/factor_data`(symlink 指向版本目录)。
- AIstock `.env` `RDAGENT_FACTOR_DATA_WSL=/home/lc999/data/factor_data`。
- RD-Agent `QE_FACTOR_DATA_DIR` / API 默认不回退 repo Windows mirror;路径/版本/meta 不匹配 fail fast。
- QE 命令显式打印记录 effective factor_data_dir。

### 4.4 meta.json 契约
- `universe_key`、`rule_version`、`ST PIT`、`delist_pit=false`、`pause_pit=false`、accepted survivorship limitation、l2_code_id 映射来源与版本、cutoff、行数/文件数/SHA256。

## 5. Design Acceptance Index / 设计验收索引

### A. Canonical PIT index contract
- **F-001**:以 `instruments/all.txt` 多区间 PIT spans 为 QE 资格权威,解析为 canonical 每日 `(datetime, instrument)` PIT index(一股可多 span)。
- **F-002**:生成 QE 专用 PIT 裁剪 factor_data bundle;PIT mask 必须在 rolling / rank / entropy / rolling correlation 等计算之前应用。
- **F-003**:每个日频辅助 H5 的 index 为 canonical daily PIT index 的子集(不越界)。
- **F-004**:不要求辅助 H5 与 daily_pv 逐行相等;停牌/未披露/无融资融券资格等稀疏缺失允许存在,缺失保 NaN,禁填 0;slow-static 字段仅在明确 PIT as-of 规则下才允许 ffill。
- **F-005**:分钟线不和日频辅助逐 bar 对齐,只共享同一日级 PIT 资格规则。

### B. 清理候选辅助 H5 越界
- **F-006**:对 8 个文件统一应用 canonical PIT mask;原始 snapshot 保留不变,生成新的版本化 QE bundle。
- **F-007**:记录每文件清洗前后行数 + 越界行统计(含 B 股、区间外样本明细)。

### C. 元数据
- **F-008**:README + schema csv/json 覆盖全部 121 个非索引字段(非当前 89);记录 l2_code_id int16 / unknown=-1 / 稳定映射来源与版本。
- **F-009**:meta 记录 universe_key、rule_version、ST PIT、delist_pit=false、pause_pit=false、accepted survivorship limitation。
- **F-010**:最终 QE bundle 含 §4.1 全部文件(含 debug bundle)。
- **F-011**:同步更新所有 RD-Agent factor_data_template README 副本,避免 debug 重建覆盖。

### D. 验证器 / Data Doctor
- **F-012**:parquet 期望 123 物理 / 121 数据列;sector_data 期望 23 列。
- **F-013**:支持一股多 PIT span,不再把 5409 行 all.txt 误判为应有 4643 行。
- **F-014**:新增检查 —— auxiliary index 为 canonical PIT index 子集;正成交日关键源数据缺失(报错);停牌日稀疏缺失单独报 expected_sparse;l2_code_id dtype/range/unknown/映射一致性;static 与 sector l2 逐行一致;schema 字段覆盖完整。
- **F-015**:sw_index_member 告警拆分 no_member_record 与 first_coverage_gap_gt_30d 分开统计。
- **F-016**:Data Doctor 以 WSL runtime authority 为主,不把 Windows mirror 误报为生产权威。

### E. 统一 QE 运行路径
- **F-017**:wsl2-5080 factor_data_dir=/home/lc999/data/factor_data(改 infra.compute_nodes);rdagent-node1 用各自主机 /home/lc999/data/factor_data。
- **F-018**:AIstock .env RDAGENT_FACTOR_DATA_WSL=/home/lc999/data/factor_data。
- **F-019**:RD-Agent QE_FACTOR_DATA_DIR 及 API 默认路径不得回退 repo Windows mirror。
- **F-020**:生成的 QE 命令显式打印并记录 effective factor_data_dir。
- **F-021**:路径不存在 / 版本不一致 / meta 不匹配时 fail fast,禁止静默 /mnt fallback。
- **F-022**:Windows 目录 factor_implementation_source_data 暂不删除,切换验证完成后定义为 staging/mirror,不再是 runtime authority。

### F. 版本化部署 + 回滚
- **F-023**:本地 WSL 与 rdagent-node1 各建版本目录 factor_data_versions/qlib_st_pit_active_h5_daily_20180801_20260630_l2_v1。
- **F-024**:复制后校验 SHA256 + 文件数 + 行数 + 日期 + schema + meta。
- **F-025**:Data Doctor + 因子读取 smoke + 一个最小 QE 日频与分钟配置 preflight 全过。
- **F-026**:全过后才允许原子切换 factor_data symlink;保留旧版本;提供一条明确回滚命令。
- **F-027**:不触发全量因子指标 / 相关性 / 正式 QE 大回测。
- **F-028**:真正生产切换前先给 dry-run 回执;无明确授权不得覆盖生产目录或重启服务。

## 6. Implementation Plan / 实施方案

### 6.1 AIstock(本 worktree,独立 PR)
- PIT index 解析器 + PIT 裁剪 bundle 生成脚本(读源快照 → canonical mask → 版本目录)。
- 验证器 `scripts/validate_qe_qlib_candidate.py` + Data Doctor(F-012..F-016)。
- QE 路径解析 + 打印 + fail-fast(config_composer / qrun 命令生成侧;`.env`)。
- 元数据/schema 生成(F-008..F-010)。
- 版本化部署 + 回滚脚本(F-023..F-028,dry-run 优先)。

### 6.2 RD-Agent(独立 PR)
- QE_FACTOR_DATA_DIR 及 API 默认路径去 Windows mirror fallback + fail-fast(F-019/F-021)。
- factor_data_template README 副本同步(F-011)。

### 6.3 数据 ops(编排,dry-run 优先,生产切换待授权)
- 生成 `..._l2_v1` 版本目录(WSL + node1);SHA/行数/日期/schema/meta 校验;Data Doctor + smoke + preflight;dry-run 回执。

> 代码实现按 AIstock↔Codex 分工派发,Tier2 + 数据 ops 编排在本 session;广验证走 validation-delegation/nightly。

## 7. Verification Plan / 验证方案
- 变更文件 lint/compile + `git diff --check` + scope check(最小本地门)。
- Data Doctor 全量检查(F-012..F-016)输出完整结果。
- PIT 裁剪前后行数 + 越界行统计(F-007)。
- 因子读取 smoke(读版本 bundle,断言 auxiliary 子集、l2_code_id 一致、schema 覆盖)。
- 最小 QE 日频 + 分钟 preflight(打印 effective factor_data_dir;路径不符 fail-fast 验证)。
- 两节点 effective factor_data_dir 证据。
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_factor_data_pit_contract_f2_design_20260711.md --tier F2`。
- 广模块/跨仓回归 → validation-delegation/nightly。

## 8. Design Acceptance Matrix / 设计验收矩阵(pre-merge)

> 状态说明:本文档为**设计先行**产物;实现由 AIstock/RD-Agent 各自 PR 完成后,逐行填 `pass` + 证据。当前 `PLANNED` 表示尚未实现,`validate --tier F2` 会因此标红,属设计阶段预期;实现完成 + 证据齐后本矩阵转 `pass`,门禁在 PR/merge 前放行。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | PIT 解析器(待建) | 单测:多 span 展开每日集合 | PLANNED | |
| F-002 | bundle 生成脚本 | 单测:rolling 前裁剪 | PLANNED | |
| F-003 | bundle + Data Doctor | Data Doctor 检查 | PLANNED | |
| F-004 | bundle 生成 | 单测:停牌日 NaN 非 0 | PLANNED | |
| F-005 | 路径/规则文档 | preflight | PLANNED | |
| F-006 | bundle 生成 | 越界行=0(post) | PLANNED | |
| F-007 | bundle 报告 | 行数表 | PLANNED | |
| F-008 | schema 生成 | schema 行数=121 | PLANNED | |
| F-009 | meta.json 生成 | meta 校验 | PLANNED | |
| F-010 | bundle 打包 | 文件集检查 | PLANNED | |
| F-011 | RD-Agent PR | 副本一致 | PLANNED | |
| F-012 | validator | Data Doctor | PLANNED | |
| F-013 | validator | 5409 不误判 4643 | PLANNED | |
| F-014 | Data Doctor | 检查输出 | PLANNED | |
| F-015 | Data Doctor | 分开统计 | PLANNED | |
| F-016 | Data Doctor | 不误报 mirror | PLANNED | |
| F-017 | infra.compute_nodes | DB 值证据 | PLANNED | |
| F-018 | AIstock .env | .env diff | PLANNED | |
| F-019 | RD-Agent PR | 单测 fail-fast | PLANNED | |
| F-020 | QE 命令生成 | 命令日志 | PLANNED | |
| F-021 | 路径解析 | 单测:mismatch raise | PLANNED | |
| F-022 | 文档 | 定义记录 | PLANNED | |
| F-023 | 部署脚本 | 目录存在 | PLANNED | |
| F-024 | 部署脚本 | 校验回执 | PLANNED | |
| F-025 | 部署脚本 | 全过回执 | PLANNED | |
| F-026 | 部署脚本 | dry-run 回执 | PLANNED | |
| F-027 | 部署脚本 | 不含相关调用 | PLANNED | |
| F-028 | 部署脚本 | dry-run 回执 | PLANNED | |

## 9. Rollout & Rollback / 发布与回滚
- Rollout:生成版本目录 → 校验 → Data Doctor/smoke/preflight → dry-run 回执 → 用户授权后原子 `ln -sfn <version> /home/lc999/data/factor_data`(WSL + node1 各自)→ 冒烟。
- Rollback:`ln -sfn <prev_version> /home/lc999/data/factor_data`(保留旧版本目录,单命令回滚)。

## 10. Risks / 风险与失败模式
- **PIT 裁剪误删正成交样本**:若 canonical index 解析错误(span 边界/calendar 交集),可能误删有效样本。缓解:Data Doctor F-014 正成交日关键源缺失检查 + 清洗前后行数对账(F-007),越界应仅为 B 股/区间外。
- **静默 /mnt fallback 回归**:路径解析若有隐藏 fallback,会读到 04-30 旧数据而不报错。缓解:F-021 fail-fast + F-020 打印 effective dir + 单测覆盖 mismatch。
- **debug 重建覆盖 template README**:RD-Agent debug 重建可能覆盖 README。缓解:F-011 同步所有副本。
- **双节点数据不一致**:WSL 与 node1 版本目录若不同步,QE 跨节点结果漂移。缓解:F-024 SHA256 双节点对齐。
- **生产切换中断**:非原子切换可能留下半态。缓解:F-026 原子 symlink + 保留旧版本 + 单命令回滚 + F-028 dry-run 先行。
- **survivorship / 新股 / 停牌缺失**:已知限制,已用户批准,记入 meta(§1.1),不阻断。

## 11. Production Gates / 生产门禁
- `production_ddl_gate`: noop(无 DDL;`infra.compute_nodes` 为业务表 UPDATE,属生产写,待授权单独执行)。
- `production_frontend_dependency_gate`: noop。
- `production_backend_dependency_gate`: noop。
- Runtime/DB touch:设计阶段不写生产 DB、不覆盖生产数据目录、不重启服务;生产切换 + `infra.compute_nodes` 更新 + `.env` 生效均待用户明确授权,先给 dry-run 回执。

## 12. Deliverables / 交付与分开报告
AIstock + RD-Agent 各自 commit/PR;设计验收矩阵;清洗前后行数及越界统计;两节点 effective factor_data_dir 证据;Data Doctor 完整结果;metadata/schema 完整性;production activation dry-run;rollback 方案。分开报告:source merge / runtime config / WSL local deployment / rdagent-node1 deployment / backend·worker restart / production data activation。

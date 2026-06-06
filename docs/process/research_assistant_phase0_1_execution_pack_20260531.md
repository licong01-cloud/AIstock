# Research Assistant 执行 Pack 样板：Phase 0 + Phase 1

> 日期：2026-05-31
> 配套：`research_assistant_blueprint_execution_runbook_20260531.md`（执行环 + 防缩水三闸门 + 模板）、`docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md`（蓝图）
> 用途：Phase 0/1 的**填好实例**，作为 Phase 2–15 自生成的样板。
> 边界：本文 docs-only；验证在 8011/8012，不碰 8001/3000。

---

## Phase 0：基线锁定与脚手架（对应蓝图 §10 Phase 0）

### A. 任务卡

```yaml
card_id: RA-P0-01
phase: Phase 0
title: 锁定改造前基线，逐条复验蓝图 §1.1/§1.2/§16.1 的 文件:行号
target_files:
  - path: docs/process/research_assistant_baseline_verification_20260531.md
    change: create   # 复验报告：每个 DEF/资产锚点对齐当前 HEAD，记录 commit
acceptance_command: "rg -n \"DEF-0|DEF-1\" docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md"
consumption_assertion: "复验报告须对每条 §1.2(DEF-01~09)/§16.1(DEF-10~12) 给出当前 HEAD 的 文件:行号 实测，行号漂移须更新蓝图"
definition_of_done:
  - "DEF-01~12 + §1.1 资产逐条复验，附 commit 哈希"
  - "蓝图 §1.2/§16.1 与当前代码不一致处，开 PR 修正行号"
forbidden: "不得跳过任一 DEF；不得用旧快照充当当前 HEAD 证据"
traceability_row: "蓝图 §12 全表（基线是矩阵的事实锚点）"

card_id: RA-P0-02
phase: Phase 0
title: 建脚手架——测试目录 + 迁移脚本骨架（不执行生产 DDL）
target_files:
  - path: backend/tests/research_assistant/__init__.py
    change: create
  - path: backend/db/migrations/ra_upgrade/  # 迁移脚本骨架目录
    change: create
acceptance_command: "python -m py_compile backend/tests/research_assistant/__init__.py && git diff --check"
consumption_assertion: "脚手架仅占位；不得引入任何 mock 充当真实实现"
definition_of_done:
  - "测试目录可被 pytest 收集（0 用例不报错）"
  - "迁移骨架不执行、不连生产库"
forbidden: "不得在 Phase 0 提前塞入未验证实现"
traceability_row: "—"

card_id: RA-P0-03
phase: Phase 0
title: 注册本蓝图所有新模块到 module_registry + file_ownership（蓝图 L1.6/L1/L2/L3 等新件）
target_files:
  - path: tests/aistock_validation/catalog/module_registry.yaml
    change: modify
  - path: tests/aistock_validation/catalog/file_ownership.yaml
    change: modify
acceptance_command: "python -c \"import yaml,sys; yaml.safe_load(open('tests/aistock_validation/catalog/module_registry.yaml'))\""
consumption_assertion: "新模块(memory_tree/graph_context/external_research/agent_teams/...) 进入 registry，guardrail/质量摘要可纳管"
definition_of_done:
  - "蓝图新增模块全部登记，owner=claude_code_boundary"
forbidden: "不得遗漏任一新模块"
traceability_row: "—"
```

### B. 验证计划草案（实现时进 test_plans.yaml）

```yaml
plan_key: ra_phase0_baseline
title: Phase 0 基线锁定与脚手架验收
runner_enabled: true
commands:
  - "rg -n \"DEF-0|DEF-1\" docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md"
  - "pytest backend/tests/research_assistant -q -p no:cacheprovider"   # 0 用例不报错
  - "python -c \"import yaml; yaml.safe_load(open('tests/aistock_validation/catalog/module_registry.yaml'))\""
  - "git diff --check"
assertions:
  - "DEF-01~12 在蓝图中命中（共 12 项）"
  - "测试目录可被 pytest 收集，无 import 错误"
  - "module_registry/file_ownership 合法 YAML，含全部新模块"
gates: [G1]
ports: "8011/8012；不碰 8001"
```

### C. closure_requirements（DESIGN-COMPLIANCE-001 逐项）

```yaml
phase: Phase 0
design_ref: "蓝图 §10 Phase 0 + §1 + §16.1"
items:
  - id: CR-P0-01
    requirement: "DEF-01~12 + §1.1 资产逐条复验，行号对齐当前 HEAD，附 commit"
    done: false
    evidence: "<baseline_verification 文件 + run_id>"
  - id: CR-P0-02
    requirement: "测试脚手架可收集；迁移骨架不碰生产库"
    done: false
    evidence: "<run_id>"
  - id: CR-P0-03
    requirement: "全部新模块登记 module_registry + file_ownership"
    done: false
    evidence: "<diff>"
anti_shrink_checks:
  - "无 mock 冒充实现；无跳过 DEF"
sign_off: "ra_phase0_baseline 绿 run_id + 本表全 done + §12 锚点确认，方可合入"
```

---

## Phase 1：L1 记忆树（DDL + 树形召回 + curator）（对应蓝图 §4 / §10 Phase 1）

### A. 任务卡

```yaml
card_id: RA-P1-01
phase: Phase 1
title: research_memory_items 真树 + 治理列 DDL（蓝图 §4.2）
target_files:
  - path: backend/db/migrations/ra_upgrade/001_memory_tree.sql
    change: create
ddl: |
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS tree_path TEXT;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS parent_key TEXT;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS node_type TEXT NOT NULL DEFAULT 'fact';
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'project';
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS importance REAL NOT NULL DEFAULT 0.5;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS use_count INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS auto_created BOOLEAN NOT NULL DEFAULT FALSE;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS trust_level TEXT NOT NULL DEFAULT 'user_stated';
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb;
  ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS resident BOOLEAN NOT NULL DEFAULT FALSE;
  -- 索引 + COMMENT 见蓝图 §4.2
acceptance_command: "迁移在 8011/8012 验证库幂等执行两次无差异"
consumption_assertion: "所有新列有默认值；存量行回填 scope='project'、tree_path 由点分 memory_key 推导"
definition_of_done:
  - "DDL 幂等；COMMENT 齐全；production_ddl_gate 逐列报告"
  - "MEMORY_TYPES 扩 user_preference/directive/habit/analysis_note（models.py:61，保留旧值）"
forbidden: "不删旧列；不破坏现有读路径"
traceability_row: "蓝图 §12「记忆真树 DDL/个人维度类型」"

card_id: RA-P1-02
phase: Phase 1
title: 树形召回服务 memory_tree.py（无 RAG，复用 Prompt Tree 引擎）
target_files:
  - path: backend/services/research_assistant/memory_tree.py
    change: create
signatures:
  - "def select_memory_branches(user_message, intent, *, repo, runtime_config) -> MemoryRetrievalResult: ..."
acceptance_command: "pytest backend/tests/research_assistant/test_memory_tree_retrieval.py test_memory_scoring.py -p no:cacheprovider"
consumption_assertion: "build_context_pack 用 select_memory_branches 结果替换'按类型平铺取前N'；返回含 route_reason + 命中分支；directive/preference resident 必现"
definition_of_done:
  - "分类→collapsed 多分支命中；分支内 importance×recency 排序；无 embedding"
  - "build_context_pack(service.py:3093) 改造完成，旧平铺逻辑移除"
forbidden: "不得用向量/embedding；core 不 import AIstock 领域 service（ANTI-DRIFT-11）"
traceability_row: "蓝图 §12「树形召回（无 RAG）」"

card_id: RA-P1-03
phase: Phase 1
title: 自动扩展 curator memory_curator.py（MemTree 式 + Mem0 去重 + 信任分级 + provenance）
target_files:
  - path: backend/services/research_assistant/memory_curator.py
    change: create
acceptance_command: "pytest backend/tests/research_assistant/test_memory_autogrow.py test_memory_dedup_scope.py -p no:cacheprovider"
consumption_assertion: "新主题无分支时自动建 branch(auto_created=true)；冲突 self-edit；每条带 provenance_json，无来源不入"
definition_of_done:
  - "chat_turn 末尾异步触发 curator(cheap_worker)"
  - "personal.preference/habit 低风险自动入；project.rule/directive 改写须 approval"
  - "🔗 剥离：memory_tree/curator 不 import AIstock 业务 service（蓝图 §4.6）"
forbidden: "不得无 provenance 入库（防幻觉记忆）"
traceability_row: "蓝图 §12「自动扩展 curator」"
```

### B. 验证计划草案（实现时进 test_plans.yaml）

```yaml
plan_key: ra_phase1_memory_tree
title: Phase 1 L1 记忆树验收
runner_enabled: true
commands:
  - "pytest backend/tests/research_assistant/test_memory_tree_retrieval.py test_memory_autogrow.py test_memory_scoring.py test_memory_dedup_scope.py -p no:cacheprovider"
assertions:
  - "build_context_pack 返回 refs 来自命中分支且含 route_reason（消费断言：树召回真正接入推理）"
  - "directive/preference resident 项每轮必现"
  - "新主题自动建分支(auto_created=true)；冲突 self-edit 不堆叠"
  - "全链路无 embedding 依赖（grep 断言）"
  - "core 模块无 AIstock 领域 import（test_core_no_adapter_import.py）"
  - "DDL 在 8011/8012 幂等执行两次无差异"
gates: [G1]
ports: "8011/8012；不碰 8001"
```

### C. closure_requirements（DESIGN-COMPLIANCE-001 逐项）

```yaml
phase: Phase 1
design_ref: "蓝图 §4（含 §4.6 剥离考虑）+ §10 Phase 1"
items:
  - id: CR-P1-01
    requirement: "真树 + 治理列 DDL 幂等 + COMMENT + MEMORY_TYPES 扩充；存量回填"
    done: false
    evidence: "<run_id / 迁移日志>"
  - id: CR-P1-02
    requirement: "memory_tree.py 树形召回(无RAG)，build_context_pack 替换平铺并真正接入"
    done: false
    evidence: "<test_memory_tree_retrieval run_id>"
  - id: CR-P1-03
    requirement: "memory_curator.py 自动扩展 + 去重 + 信任分级 + provenance"
    done: false
    evidence: "<test_memory_autogrow run_id>"
  - id: CR-P1-04
    requirement: "🔗 剥离：core 经 Memory/Storage Provider，不 import AIstock 领域符号"
    done: false
    evidence: "<test_core_no_adapter_import run_id>"
anti_shrink_checks:
  - "不得只加列不接召回（建了不用）"
  - "不得用 embedding 偷换'纯分类'设计"
  - "不得 read-only 占位充当 curator 实现"
sign_off: "ra_phase1_memory_tree 绿 run_id + 本表全 done + §12 三行回填 commit，方可合入"
```

---

## 滚动说明

Phase 2–15 按 Runbook §5 的 A/B/C 模板**逐阶段生成同样的执行 Pack**（每阶段一份），并受三闸门（G1 绿 run_id / G2 逐项 done / G3 矩阵回填）约束。建议每个 Phase 实现 PR 同时：① 接入其验证计划到 `test_plans.yaml`；② 提交该 Phase 的 closure_requirements；③ 回填蓝图 §12 矩阵。任一缺失即视为缩水，合入阻断。

# Research Assistant React Flow Read-only Graph View

## Feature Card

本任务在 Research Assistant /research-assistant/graph 页面加入 React Flow 只读图谱视图，把现有 graph/summary 返回的实体与关系直观显示为可缩放、可平移、可拖动布局的向量图。节点拖动只改变本地展示布局，布局保存到浏览器 localStorage，不改变图谱事实、不写后端、不新增 DDL。

## Scope

- 前端引入 React Flow（@xyflow/react），用于只读图谱画布。
- 将现有 AssistantGraphSummary.entities 映射为 nodes，将 relations 映射为 edges；点击节点/边显示详情和证据。
- 支持缩放、平移、fit view、mini map、controls、节点拖动。
- 节点拖动布局按 namespace 保存到 localStorage；提供 重置布局按钮。
- 对 API 失败、关系端点缺失、localStorage 解析失败显示明确 degraded/告警，不静默丢边或假装成功。
- 保留现有图谱详情/实体列表作为辅助视图；不改图谱后端 API。

## Non-goals

- 不做图谱事实编辑：不新增、修改、删除实体或关系。
- 不把拖动布局写入后端或 DB。
- 不新增后端 API，不改 research_memory_entities / research_memory_relations / research_evolution_paths schema。
- 不执行 DDL，不连接生产 DB。
- 不启动或重启生产 8001/3000/19080。

## Design Acceptance Index

- F-001: /research-assistant/graph 默认展示 React Flow 图谱画布，实体为节点、关系为边，保留原有摘要指标和详情辅助信息。
- F-002: 图谱画布支持缩放、平移、fit view、mini map、节点拖动，并使用 shadcn-compatible RA 视觉 token，不扩散 Paper v2 视觉基座。
- F-003: 节点布局仅保存到浏览器 localStorage，按 namespace 隔离；刷新后恢复布局；重置布局清除本地布局并重新自动排布。
- F-004: 点击节点或边可查看原始实体/关系详情、source/evidence refs、confidence、approval status；不丢失现有可审计信息。
- F-005: 对缺失 source/target 端点的 relation 显式显示 degraded reason（如 graph_relation_endpoint_missing），不得静默丢弃；API/localStorage 失败也必须 loud。
- F-006: 本功能只读，不产生后端写入、不改 DB/DDL、不改变图谱事实源；新增前端依赖和生产门禁在 PR 中如实声明。

## Verification

- rtk proxy python scripts/aistock_feature_workflow.py validate --design docs/analysis/ra_graph_reactflow_readonly_layout_feature_20260701.md --tier F0
- cd frontend && rtk proxy npm exec -- tsc --noEmit --incremental false
- cd frontend && rtk proxy npm run lint
- cd frontend && rtk proxy npm run build
- Targeted Playwright or frontend test：验证图谱 tab/page 存在 React Flow 画布、节点/边详情、reset layout、缺失端点 degraded 文案。
- rtk proxy git diff --check

## Production Gates

- production_ddl_gate=noop：不改 DB schema，不执行 DDL。
- production_backend_dependency_gate=noop：不改后端依赖。
- production_frontend_dependency_gate=pending：新增 @xyflow/react 前端依赖；生产部署需执行前端依赖安装/镜像重建并重新部署前端。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | frontend/src/app/research-assistant/graph/page.tsx; frontend/src/app/research-assistant/graph/GraphFlowView.tsx | tsc/lint/build; Playwright graph page smoke | ready | - |
| F-002 | frontend/src/app/research-assistant/graph/GraphFlowView.tsx; frontend/src/app/research-assistant/research-assistant.css | tsc/lint/build; DOM check for controls/minimap/viewport | ready | - |
| F-003 | frontend/src/app/research-assistant/graph/GraphFlowView.tsx localStorage helpers | Playwright reload/reset layout checks | ready | - |
| F-004 | frontend/src/app/research-assistant/graph/GraphFlowView.tsx; DetailDrawer integration | Playwright click node/edge detail checks | ready | - |
| F-005 | frontend/src/app/research-assistant/graph/GraphFlowView.tsx degraded relation handling | targeted test with missing endpoint relation | ready | - |
| F-006 | frontend/package.json; frontend/package-lock.json; PR production gates | git diff --check; PR production gates | ready | - |


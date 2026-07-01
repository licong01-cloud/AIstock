"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type EdgeMouseHandler,
  type Node,
  type NodeMouseHandler,
  type OnConnect,
  type OnEdgesChange,
  type OnNodeDrag,
  type OnNodesChange,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";

import { DetailDrawer, EmptyState, StatusPill } from "@/components/research-assistant/AssistantShared";
import type { AssistantGraphSummary, JsonObject } from "@/lib/research-assistant/api";

type GraphFlowNodeData = {
  label: React.ReactNode;
  raw: JsonObject;
  entityId: string;
  entityType: string;
  approvalStatus: string;
  confidence: number | null;
};

type GraphFlowEdgeData = {
  raw: JsonObject;
  relationId: string;
  relationType: string;
  approvalStatus: string;
  confidence: number | null;
};

type GraphFlowNode = Node<GraphFlowNodeData>;
type GraphFlowEdge = Edge<GraphFlowEdgeData>;
type SavedPosition = { x: number; y: number };
type SavedLayout = Record<string, SavedPosition>;
type Selection = { kind: "entity" | "relation"; title: string; data: JsonObject } | null;

type InvalidRelation = {
  relation_id: string;
  relation_type: string;
  source_entity_id: string;
  target_entity_id: string;
  reason_code: "graph_relation_endpoint_missing";
};

const NODE_WIDTH = 230;
const NODE_HEIGHT = 92;
const STORAGE_PREFIX = "aistock.ra.graph.layout";

function asString(value: unknown, fallback = "-"): string {
  if (typeof value === "string" && value.trim()) return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return fallback;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asJsonObjectArray(value: unknown): JsonObject[] {
  return Array.isArray(value) ? value.filter((item): item is JsonObject => Boolean(item) && typeof item === "object" && !Array.isArray(item)) : [];
}

function storageKey(namespace: string): string {
  return `${STORAGE_PREFIX}.${namespace || "aistock"}.v1`;
}

function sanitizeLayout(raw: unknown): SavedLayout {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("graph_layout_local_storage_invalid: expected object");
  }
  const next: SavedLayout = {};
  for (const [id, position] of Object.entries(raw as Record<string, unknown>)) {
    if (!position || typeof position !== "object" || Array.isArray(position)) continue;
    const x = (position as SavedPosition).x;
    const y = (position as SavedPosition).y;
    if (typeof x === "number" && Number.isFinite(x) && typeof y === "number" && Number.isFinite(y)) {
      next[id] = { x, y };
    }
  }
  return next;
}

function computePosition(index: number, total: number): SavedPosition {
  if (total <= 1) return { x: 120, y: 140 };
  const ring = Math.floor(index / 10);
  const slot = index % 10;
  const ringSize = Math.min(10, total - ring * 10);
  const radiusX = 320 + ring * 210;
  const radiusY = 180 + ring * 130;
  const angle = (2 * Math.PI * slot) / Math.max(1, ringSize) - Math.PI / 2;
  return {
    x: 420 + Math.cos(angle) * radiusX,
    y: 260 + Math.sin(angle) * radiusY,
  };
}

function nodeColor(entityType: string): string {
  const key = entityType.toLowerCase();
  if (key.includes("experiment") || key.includes("qe")) return "#326fa8";
  if (key.includes("paper") || key.includes("doc")) return "#8a5a0a";
  if (key.includes("factor")) return "#7d3f98";
  if (key.includes("module") || key.includes("service")) return "#1f4f3a";
  return "#c66b42";
}

function NodeLabel({ entity }: { entity: JsonObject }) {
  const entityId = asString(entity.entity_id || entity.entity_key, "unknown");
  const entityType = asString(entity.entity_type, "entity");
  const title = asString(entity.title || entity.entity_key || entity.entity_id, "未命名实体");
  const summary = asString(entity.summary, "暂无摘要");
  const confidence = asNumber(entity.confidence);
  const accent = nodeColor(entityType);
  return (
    <div className="ra-graph-node" data-testid="ra-graph-node" data-entity-id={entityId} style={{ borderColor: accent }}>
      <div className="ra-graph-node-top">
        <span style={{ background: accent }} />
        <strong title={title}>{title}</strong>
      </div>
      <p title={summary}>{summary}</p>
      <div className="ra-graph-node-meta">
        <small>{entityType}</small>
        {confidence !== null ? <small>conf {confidence.toFixed(2)}</small> : null}
      </div>
    </div>
  );
}

function buildGraph(graph: AssistantGraphSummary, savedLayout: SavedLayout): { nodes: GraphFlowNode[]; edges: GraphFlowEdge[]; invalidRelations: InvalidRelation[] } {
  const entities = asJsonObjectArray(graph.entities);
  const relations = asJsonObjectArray(graph.relations);
  const entityIds = new Set<string>();
  const nodes = entities.map((entity, index): GraphFlowNode => {
    const entityId = asString(entity.entity_id || entity.entity_key, `entity-${index}`);
    entityIds.add(entityId);
    const entityType = asString(entity.entity_type, "entity");
    const approvalStatus = asString(entity.approval_status, "unknown");
    return {
      id: entityId,
      type: "default",
      position: savedLayout[entityId] || computePosition(index, entities.length),
      data: {
        label: <NodeLabel entity={{ ...entity, entity_id: entityId }} />,
        raw: entity,
        entityId,
        entityType,
        approvalStatus,
        confidence: asNumber(entity.confidence),
      },
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
      className: "ra-graph-flow-node-shell",
    };
  });

  const invalidRelations: InvalidRelation[] = [];
  const edges: GraphFlowEdge[] = [];
  relations.forEach((relation, index) => {
    const relationId = asString(relation.relation_id, `relation-${index}`);
    const source = asString(relation.source_entity_id, "");
    const target = asString(relation.target_entity_id, "");
    const relationType = asString(relation.relation_type, "relates_to");
    if (!source || !target || !entityIds.has(source) || !entityIds.has(target)) {
      invalidRelations.push({
        relation_id: relationId,
        relation_type: relationType,
        source_entity_id: source || "<missing>",
        target_entity_id: target || "<missing>",
        reason_code: "graph_relation_endpoint_missing",
      });
      return;
    }
    edges.push({
      id: relationId,
      source,
      target,
      label: relationType,
      type: "smoothstep",
      animated: asString(relation.approval_status, "") !== "approved",
      data: {
        raw: relation,
        relationId,
        relationType,
        approvalStatus: asString(relation.approval_status, "unknown"),
        confidence: asNumber(relation.confidence),
      },
      style: { stroke: "#567264", strokeWidth: 2 },
      labelStyle: { fill: "#18211d", fontWeight: 800, fontSize: 12 },
      labelBgStyle: { fill: "#fffaf0", fillOpacity: 0.9 },
      markerEnd: { type: "arrowclosed", color: "#567264" },
    });
  });

  return { nodes, edges, invalidRelations };
}

function persistLayout(namespace: string, nodes: GraphFlowNode[]) {
  if (typeof window === "undefined") return;
  const payload: SavedLayout = {};
  for (const node of nodes) payload[node.id] = { x: node.position.x, y: node.position.y };
  window.localStorage.setItem(storageKey(namespace), JSON.stringify(payload));
}

export default function GraphFlowView({ graph }: { graph: AssistantGraphSummary | null }) {
  const namespace = asString(graph?.namespace, "aistock");
  const [savedLayout, setSavedLayout] = useState<SavedLayout>({});
  const [layoutLoaded, setLayoutLoaded] = useState(false);
  const [storageWarning, setStorageWarning] = useState<JsonObject | null>(null);
  const [selection, setSelection] = useState<Selection>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;
    setLayoutLoaded(false);
    setStorageWarning(null);
    const key = storageKey(namespace);
    try {
      const raw = window.localStorage.getItem(key);
      if (!raw) {
        setSavedLayout({});
        return;
      }
      const parsed = sanitizeLayout(JSON.parse(raw));
      setSavedLayout(parsed);
      setLayoutLoaded(Object.keys(parsed).length > 0);
    } catch (exc) {
      window.localStorage.removeItem(key);
      setSavedLayout({});
      setStorageWarning({
        reason_code: "graph_layout_local_storage_invalid",
        operator_action: "已清除损坏的本地图谱布局，请重新拖动节点保存布局。",
        error_summary: exc instanceof Error ? exc.message : String(exc),
      });
    }
  }, [namespace]);

  const { nodes: initialNodes, edges: initialEdges, invalidRelations } = useMemo(
    () => buildGraph(graph || { namespace, entities: [], relations: [], evolution_paths: [] }, savedLayout),
    [graph, namespace, savedLayout],
  );

  const [nodes, setNodes, onNodesChangeBase] = useNodesState<GraphFlowNode>(initialNodes);
  const [edges, setEdges, onEdgesChangeBase] = useEdgesState<GraphFlowEdge>(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
    setSelection(null);
  }, [initialEdges, initialNodes, setEdges, setNodes]);

  const onNodesChange = useCallback<OnNodesChange<GraphFlowNode>>((changes) => onNodesChangeBase(changes), [onNodesChangeBase]);
  const onEdgesChange = useCallback<OnEdgesChange<GraphFlowEdge>>((changes) => onEdgesChangeBase(changes), [onEdgesChangeBase]);
  const onConnect = useCallback<OnConnect>(() => {
    setStorageWarning({
      reason_code: "graph_view_read_only",
      operator_action: "当前图谱视图只读；连线不会写入图谱事实源。",
    });
  }, []);

  const onNodeClick = useCallback<NodeMouseHandler<GraphFlowNode>>((_event, node) => {
    setSelection({ kind: "entity", title: `实体：${asString(node.data.raw.title || node.id)}`, data: node.data.raw });
  }, []);

  const onEdgeClick = useCallback<EdgeMouseHandler<GraphFlowEdge>>((event, edge) => {
    event.stopPropagation();
    setSelection({ kind: "relation", title: `关系：${edge.data?.relationType || edge.id}`, data: edge.data?.raw || {} });
  }, []);

  const onNodeDragStop = useCallback<OnNodeDrag<GraphFlowNode>>(
    (_event, movedNode) => {
      const nextNodes = nodes.map((node) => (node.id === movedNode.id ? movedNode : node));
      persistLayout(namespace, nextNodes);
      setSavedLayout(Object.fromEntries(nextNodes.map((node) => [node.id, { x: node.position.x, y: node.position.y }])));
      setLayoutLoaded(true);
    },
    [namespace, nodes],
  );

  const resetLayout = useCallback(() => {
    if (typeof window !== "undefined") window.localStorage.removeItem(storageKey(namespace));
    setSavedLayout({});
    setLayoutLoaded(false);
    setStorageWarning(null);
  }, [namespace]);

  if (!graph) {
    return <EmptyState title="图谱数据未加载" hint="等待 graph/summary 返回实体和关系后显示向量图。" />;
  }

  return (
    <section className="ra-graph-flow-card" data-testid="ra-graph-flow-section">
      <div className="ra-graph-toolbar">
        <div>
          <div className="ra-kicker">React Flow / read-only graph</div>
          <h2>图谱向量视图</h2>
          <p>拖动节点只保存本地展示布局；不会修改实体、关系或图谱事实源。</p>
        </div>
        <div className="ra-graph-toolbar-actions">
          <StatusPill status={layoutLoaded ? "restored" : "auto_layout"}>{layoutLoaded ? "本地布局已恢复" : "自动布局"}</StatusPill>
          <button className="ra-secondary-button" onClick={resetLayout} type="button">重置布局</button>
        </div>
      </div>

      {storageWarning ? (
        <div className="ra-graph-degraded" data-testid="ra-graph-storage-warning">
          <strong>{asString(storageWarning.reason_code)}</strong>
          <span>{asString(storageWarning.operator_action || storageWarning.error_summary)}</span>
        </div>
      ) : null}
      {invalidRelations.length ? (
        <div className="ra-graph-degraded" data-testid="ra-graph-degraded-relations">
          <strong>graph_relation_endpoint_missing</strong>
          <span>有 {invalidRelations.length} 条关系缺少可定位 source/target 端点，已在告警中列出，未静默绘制成假边。</span>
          <DetailDrawer title="缺失端点关系详情" data={invalidRelations} />
        </div>
      ) : null}

      <div className="ra-graph-flow-shell" data-testid="ra-graph-flow">
        <ReactFlow<GraphFlowNode, GraphFlowEdge>
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={onNodeClick}
          onEdgeClick={onEdgeClick}
          onNodeDragStop={onNodeDragStop}
          fitView
          fitViewOptions={{ padding: 0.22 }}
          minZoom={0.18}
          maxZoom={2.2}
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
          proOptions={{ hideAttribution: true }}
        >
          <MiniMap pannable zoomable nodeColor={(node) => nodeColor(asString(node.data?.entityType, "entity"))} />
          <Controls showInteractive={false} />
          <Background gap={22} size={1.4} color="rgba(31, 79, 58, 0.18)" />
        </ReactFlow>
      </div>

      <div className="ra-graph-inspector" data-testid="ra-graph-inspector">
        {selection ? (
          <>
            <div>
              <div className="ra-kicker">{selection.kind === "entity" ? "node detail" : "edge detail"}</div>
              <h3>{selection.title}</h3>
            </div>
            <DetailDrawer title="图谱可审计详情" data={selection.data} />
          </>
        ) : (
          <EmptyState title="选择节点或关系查看详情" hint="点击画布中的实体节点或关系边，可查看 source_refs、evidence_refs、confidence 与审批状态。" />
        )}
      </div>
    </section>
  );
}

"use client";

import { DetailDrawer, EmptyState, StatusPill, display } from "@/components/research-assistant/AssistantShared";
import { EvidenceCard, evidenceCompleteness, normalizeEvidenceRef } from "@/components/research-assistant/EvidenceCard";
import type {
  AssistantContextPack,
  AssistantEvidenceCard,
  AssistantEvidenceRef,
  AssistantMemory,
  AssistantMemoryTreeNode,
  JsonObject,
} from "@/lib/research-assistant/api";

function asObject(value: unknown): JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as JsonObject) : {};
}

function memoryScope(memory: AssistantMemory): "personal" | "project" {
  const raw = String(memory.scope || memory.namespace || memory.subject_key || "").toLowerCase();
  return raw.includes("personal") ? "personal" : "project";
}

function memoryPath(memory: AssistantMemory): string {
  return String(memory.tree_path || memory.subject_key || `${memoryScope(memory)}/missing_tree_path/${memory.memory_id}`);
}

function titleFromPath(path: string): string {
  return path.split(/[/.]/).filter(Boolean).pop() || path;
}

function evidenceRefsFromMemory(memory: AssistantMemory): AssistantEvidenceRef[] {
  const refs = Array.isArray(memory.evidence_refs) ? memory.evidence_refs.map(normalizeEvidenceRef) : [];
  if (!refs.length && memory.source_ref) refs.push({ source_ref: memory.source_ref });
  return refs;
}

function emptyNode(node_id: string, tree_path: string): AssistantMemoryTreeNode {
  return {
    node_id,
    tree_path,
    title: titleFromPath(tree_path),
    memory_ids: [],
    children: [],
    evidence_refs: [],
  };
}

export function buildMemoryTree(items: AssistantMemory[]): AssistantMemoryTreeNode[] {
  const roots = new Map<string, AssistantMemoryTreeNode>();
  for (const memory of items) {
    const scope = memoryScope(memory);
    const path = memoryPath(memory);
    const parts = [scope, ...path.split(/[/.]/).filter(Boolean).filter((part) => part !== scope)];
    let currentPath = "";
    let parentChildren: AssistantMemoryTreeNode[] | null = null;
    for (const part of parts) {
      currentPath = currentPath ? `${currentPath}/${part}` : part;
      const nodeId = `${scope}:${currentPath}`;
      let node: AssistantMemoryTreeNode | undefined;
      if (!parentChildren) {
        node = roots.get(nodeId);
        if (!node) {
          node = emptyNode(nodeId, currentPath);
          roots.set(nodeId, node);
        }
      } else {
        node = parentChildren.find((candidate) => candidate.node_id === nodeId);
        if (!node) {
          node = emptyNode(nodeId, currentPath);
          parentChildren.push(node);
        }
      }
      parentChildren = node.children;
      if (part === parts[parts.length - 1]) {
        node.memory_ids.push(memory.memory_id);
        node.evidence_refs.push(...evidenceRefsFromMemory(memory));
        node.title = memory.title || node.title;
      }
    }
  }
  return Array.from(roots.values()).sort((left, right) => left.tree_path.localeCompare(right.tree_path));
}

function contextPackRoutes(pack: AssistantContextPack): JsonObject {
  const packJson = asObject(pack.pack_json);
  return {
    context_pack_id: pack.context_pack_id,
    matched_branches: packJson.matched_branches || packJson.route_matched_branches || [],
    route_reason: packJson.route_reason || packJson.selection_reason || pack.pack_summary || "",
    omitted_relevant_refs: packJson.omitted_relevant_refs || [],
    graph_relation_refs: packJson.graph_relation_refs || [],
  };
}

function nodeEvidenceCard(node: AssistantMemoryTreeNode): AssistantEvidenceCard {
  const refs = node.evidence_refs.map(normalizeEvidenceRef);
  const completeness = evidenceCompleteness({
    card_id: `memory-node-${node.node_id}`,
    title: node.title,
    summary: `Memory node ${node.tree_path} carries ${node.memory_ids.length} memory record(s).`,
    evidence_refs: refs,
    status: "supported",
  });
  return {
    card_id: `memory-node-${node.node_id}`,
    title: node.title,
    summary: `Memory node ${node.tree_path} carries ${node.memory_ids.length} memory record(s).`,
    evidence_refs: refs,
    status: completeness.ok ? "supported" : "insufficient",
  };
}

function TreeNode({ node, depth = 0 }: { node: AssistantMemoryTreeNode; depth?: number }) {
  const card = nodeEvidenceCard(node);
  return (
    <li className="ra-memory-node" data-testid="ra-memory-tree-node" style={{ marginLeft: depth * 14 }}>
      <div className="ra-memory-node-head">
        <strong>{node.title}</strong>
        <StatusPill status={card.status}>{card.status}</StatusPill>
      </div>
      <p className="ra-muted">{node.tree_path} / memories: {node.memory_ids.join(", ") || "none"}</p>
      <EvidenceCard card={card} />
      {node.children.length ? (
        <ul className="ra-memory-tree-list">
          {node.children.map((child) => <TreeNode node={child} depth={depth + 1} key={child.node_id} />)}
        </ul>
      ) : null}
    </li>
  );
}

export function MemoryTreeView({ items, packs }: { items: AssistantMemory[]; packs: AssistantContextPack[] }) {
  const roots = buildMemoryTree(items);
  const projectRoots = roots.filter((node) => node.tree_path.startsWith("project"));
  const personalRoots = roots.filter((node) => node.tree_path.startsWith("personal"));
  const routeRows = packs.map(contextPackRoutes);

  if (!items.length) {
    return <EmptyState title="Memory tree is empty" hint="The API returned no memory records; create approved memories before expecting tree recall." />;
  }

  return (
    <section className="ra-phase7-panel" data-testid="ra-memory-tree-view">
      <div className="ra-card-headline">
        <span className="ra-chat-eyebrow">Phase 7 Memory Tree</span>
        <StatusPill status="ready">project + personal trees</StatusPill>
      </div>
      <div className="ra-memory-tree-grid">
        <div>
          <h3>Project tree</h3>
          {projectRoots.length ? <ul className="ra-memory-tree-list">{projectRoots.map((node) => <TreeNode node={node} key={node.node_id} />)}</ul> : <EmptyState title="No project tree records" hint="Project memory scope is absent in the current API result." />}
        </div>
        <div>
          <h3>Personal tree</h3>
          {personalRoots.length ? <ul className="ra-memory-tree-list">{personalRoots.map((node) => <TreeNode node={node} key={node.node_id} />)}</ul> : <EmptyState title="No personal tree records" hint="Personal memory scope is absent in the current API result." />}
        </div>
      </div>
      <div className="ra-context-pack-routes" data-testid="ra-context-pack-routes">
        <h3>Context Pack route consumption</h3>
        {routeRows.length ? routeRows.map((row) => (
          <div className="ra-list-card" key={String(row.context_pack_id)}>
            <strong>{display(row.context_pack_id)}</strong>
            <p>route_reason: {display(row.route_reason)}</p>
            <p>matched_branches: {Array.isArray(row.matched_branches) ? row.matched_branches.map(display).join(" / ") : display(row.matched_branches)}</p>
            <p>omitted_relevant_refs: {Array.isArray(row.omitted_relevant_refs) ? row.omitted_relevant_refs.map(display).join(" / ") || "-" : display(row.omitted_relevant_refs)}</p>
            <DetailDrawer title="context pack route detail" data={row} />
          </div>
        )) : <EmptyState title="No context packs returned" hint="The tree view is loaded, but no context pack route evidence is available yet." />}
      </div>
    </section>
  );
}

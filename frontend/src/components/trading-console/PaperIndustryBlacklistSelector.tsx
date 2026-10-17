"use client";

import { useEffect, useMemo, useState } from "react";
import NoticePanel from "./NoticePanel";
import { selectionCenterApi } from "@/lib/paper-v2/api";
import type { JsonObject } from "@/lib/paper-v2/types";

export type Sw2Entry = {
  l1_code: string;
  l1_name: string;
  l2_code: string;
  l2_name: string;
};

type Sw1Group = {
  l1_code: string;
  l1_name: string;
  children: Array<{ l2_code: string; l2_name: string }>;
};

type Props = {
  selected: Sw2Entry[];
  onChange: (items: Sw2Entry[]) => void;
};

export default function PaperIndustryBlacklistSelector({ selected, onChange }: Props) {
  const [tree, setTree] = useState<Sw1Group[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [error, setError] = useState("");

  useEffect(() => {
    let alive = true;
    selectionCenterApi.industryTree()
      .then((rows) => {
        if (alive) setTree(rows as Sw1Group[]);
      })
      .catch((exc) => {
        if (alive) setError(exc instanceof Error ? exc.message : String(exc));
      });
    return () => { alive = false; };
  }, []);

  const selectedCodes = useMemo(() => new Set(selected.map((item) => item.l2_code)), [selected]);

  function add(group: Sw1Group, child: { l2_code: string; l2_name: string }) {
    if (selectedCodes.has(child.l2_code)) return;
    onChange([
      ...selected,
      {
        l1_code: group.l1_code,
        l1_name: group.l1_name,
        l2_code: child.l2_code,
        l2_name: child.l2_name,
      },
    ]);
  }

  function remove(l2Code: string) {
    onChange(selected.filter((item) => item.l2_code !== l2Code));
  }

  return (
    <div className="pv2-card" data-testid="paper-industry-blacklist-selector">
      <div className="pv2-row-actions" style={{ justifyContent: "space-between", marginBottom: 10 }}>
        <div>
          <strong>行业黑名单</strong>
          <div className="pv2-muted">Paper v2 策略包运行配置；只写入本次 Selection/Paper runtime profile，不读写 QE 全局黑名单。</div>
        </div>
        <span className="pv2-chip">{selected.length} 个行业已忽略</span>
      </div>
      {error ? <NoticePanel title="行业树加载失败" tone="warning">{error}</NoticePanel> : null}
      <div className="pv2-grid pv2-grid-2">
        <div className="pv2-card" style={{ maxHeight: 260, overflowY: "auto" }}>
          {tree.length === 0 && !error ? <span className="pv2-muted">加载申万行业树...</span> : null}
          {tree.map((group) => (
            <div key={group.l1_code}>
              <button
                className="pv2-link-button"
                data-testid={`paper-industry-group-${group.l1_code}`}
                type="button"
                onClick={() => setExpanded((current) => {
                  const next = new Set(current);
                  next.has(group.l1_code) ? next.delete(group.l1_code) : next.add(group.l1_code);
                  return next;
                })}
              >
                {expanded.has(group.l1_code) ? "▼" : "▶"} {group.l1_name}
              </button>
              {expanded.has(group.l1_code) ? group.children.map((child) => (
                <div key={child.l2_code} className="pv2-row-actions" data-testid={`paper-industry-child-${child.l2_code}`} style={{ justifyContent: "space-between", padding: "4px 0 4px 18px" }}>
                  <span>{child.l2_name}</span>
                  <button className="pv2-button-ghost" data-testid={`paper-industry-add-${child.l2_code}`} type="button" disabled={selectedCodes.has(child.l2_code)} onClick={() => add(group, child)}>
                    {selectedCodes.has(child.l2_code) ? "已选择" : "忽略"}
                  </button>
                </div>
              )) : null}
            </div>
          ))}
        </div>
        <div className="pv2-card" style={{ maxHeight: 260, overflowY: "auto" }}>
          {selected.length === 0 ? <span className="pv2-muted">未选择忽略行业。</span> : null}
          {selected.map((item) => (
            <div key={item.l2_code} className="pv2-row-actions" data-testid={`paper-industry-selected-${item.l2_code}`} style={{ justifyContent: "space-between", marginBottom: 8 }}>
              <span><strong>{item.l2_name}</strong><br /><span className="pv2-muted">{item.l1_name} / {item.l2_code}</span></span>
              <button className="pv2-button-ghost" data-testid={`paper-industry-remove-${item.l2_code}`} type="button" onClick={() => remove(item.l2_code)}>移除</button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function selectedIndustryCodes(items: Sw2Entry[]): string[] {
  return items.map((item) => item.l2_code).filter(Boolean);
}

export function selectedIndustryTrace(items: Sw2Entry[]): JsonObject[] {
  return items.map((item) => ({ ...item }));
}

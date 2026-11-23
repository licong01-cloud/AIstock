"use client";
import { useEffect, useRef, useState } from "react";
import { read, type DailyPage } from "@/lib/api/advisoryWorkbench";
import { useRead } from "./shared";

const pageUrl = (url: string, offset: number) => {
  const [pathname, query] = url.split("?");
  const params = new URLSearchParams(query);
  params.set("limit", "100"); params.set("offset", String(offset));
  return `${pathname}?${params}`;
};
const identityKeys = ["program_id", "binding_version_id", "list_version_id", "snapshot_id", "target_date", "status", "total_count"] as const;
type State = { first: DailyPage; data: DailyPage; complete: boolean; error: string | null };

export function useDailyItems(url: string | null, refresh: number, program: string) {
  const first = useRead<DailyPage>(url ? pageUrl(url, 0) : null, refresh, program);
  const [all, setAll] = useState<State | null>(null);
  useEffect(() => {
    if (!url || !first.data || first.loading || first.error) return;
    const base = first.data, controller = new AbortController();
    const timeout = setTimeout(() => controller.abort("timeout"), 45000);
    const seen = new Set<string>();
    const validate = (page: DailyPage, offset: number) => {
      if (identityKeys.some(key => page[key] !== base[key])) throw new Error("名单身份或总数已变化，请刷新后重新排序。");
      if (!Number.isInteger(page.total_count) || page.total_count < 0 || page.offset !== offset || page.limit !== 100
          || !Array.isArray(page.items) || page.items.length !== Math.min(100, page.total_count - offset)) {
        throw new Error("完整名单分页数据不完整，请刷新重试。");
      }
      for (const row of page.items) {
        if (!row || typeof row.symbol !== "string" || !row.symbol) throw new Error("名单包含无效记录，请刷新重试。");
        const key = `${row.symbol}:${row.episode_id || ""}`;
        if (seen.has(key)) throw new Error("名单包含重复记录，请刷新重试。");
        seen.add(key);
      }
    };
    void (async () => {
      try {
        validate(base, 0);
        let items = base.items;
        setAll({ first: base, data: base, complete: items.length === base.total_count, error: null });
        while (items.length < base.total_count && !controller.signal.aborted) {
          const next = await read<DailyPage>(pageUrl(url, items.length), controller.signal);
          if (controller.signal.aborted) return;
          validate(next, items.length);
          items = [...items, ...next.items];
          setAll({ first: base, data: { ...base, items }, complete: items.length === base.total_count, error: null });
        }
      } catch (error) {
        if (controller.signal.aborted && controller.signal.reason !== "timeout") return;
        const message = controller.signal.reason === "timeout" ? "完整名单读取超时，请刷新重试。"
          : error instanceof Error ? error.message : "完整名单读取失败，请刷新重试。";
        setAll(previous => ({ first: base, data: previous?.first === base ? previous.data : { ...base, items: [] }, complete: false, error: message }));
      } finally { clearTimeout(timeout); }
    })();
    return () => { controller.abort(); clearTimeout(timeout); };
  }, [url, first.data, first.loading, first.error]);
  const matching = first.data && all?.first === first.data ? all : null;
  const error = first.error || matching?.error || null;
  return { url, data: matching?.data || null, error,
    complete: !!matching?.complete && !first.loading && !error,
    loading: first.loading || !!(first.data && !matching?.complete && !error) };
}

export function useExpandedGroup() {
  const ref = useRef<HTMLElement>(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const ancestors: HTMLDetailsElement[] = [];
    for (let parent = ref.current?.parentElement; parent; parent = parent.parentElement) {
      if (parent instanceof HTMLDetailsElement) ancestors.push(parent);
    }
    const update = () => setVisible(ancestors.every(details => details.open));
    update(); ancestors.forEach(details => details.addEventListener("toggle", update));
    return () => ancestors.forEach(details => details.removeEventListener("toggle", update));
  }, []);
  return { ref, visible };
}

"use client";

import { useEffect, useRef } from "react";

export const QE_REFRESH_MS = 60_000;
export const isExecutingTask = (task: { status?: string; running_count?: number; reconciling_count?: number; queued_count?: number } | null | undefined) =>
  !!task && !["completed", "succeeded", "failed", "partial_failed", "partial_recovered", "cancelled", "paused", "stopped"].includes(String(task.status || "").toLowerCase())
    && (["running", "processing", "reconciling", "submitting"].includes(String(task.status || "").toLowerCase())
    || Number(task.running_count) > 0 || Number(task.reconciling_count) > 0
    || (task.status === "pending" && Number(task.queued_count) > 0));

/** No idle polling, no overlapping callbacks, and no catch-up storm after tab sleep. */
export function useActiveRefresh(refresh: () => Promise<unknown>, enabled: boolean, identity: string, interval = QE_REFRESH_MS) {
  const callback = useRef(refresh);
  callback.current = refresh;
  const inFlight = useRef(false);
  const lastStarted = useRef(0);
  useEffect(() => {
    if (!enabled) return;
    let disposed = false;
    let failures = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const delay = Math.max(QE_REFRESH_MS, interval);
    const schedule = () => {
      clearTimeout(timer);
      if (disposed || document.visibilityState !== "visible") return;
      timer = setTimeout(run, Math.max(delay * Math.min(2 ** failures, 8), lastStarted.current + delay - Date.now()));
    };
    const run = async () => {
      if (disposed || document.visibilityState !== "visible") return;
      if (inFlight.current) { schedule(); return; }
      inFlight.current = true;
      lastStarted.current = Date.now();
      try { await callback.current(); failures = 0; }
      catch (error) { failures += 1; console.error("[QE] Active status refresh failed", error); }
      finally { inFlight.current = false; schedule(); }
    };
    schedule();
    document.addEventListener("visibilitychange", schedule);
    return () => {
      disposed = true;
      clearTimeout(timer);
      document.removeEventListener("visibilitychange", schedule);
    };
  }, [enabled, identity, interval]);
}

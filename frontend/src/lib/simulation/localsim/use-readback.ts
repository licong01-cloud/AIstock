"use client";

import { useCallback, useEffect, useRef, useState } from "react";

const POLL_INTERVAL_MS = 5_000;
const REQUEST_TIMEOUT_MS = 12_000;
const STALE_AFTER_MS = 20_000;

type Loader<T> = (signal: AbortSignal) => Promise<T>;

export type LocalSimReadback<T> = {
  data: T | null;
  error: string | null;
  updatedAt: number | null;
  refreshing: boolean;
  stale: boolean;
  refresh: () => Promise<void>;
};

type Snapshot<T> = Omit<LocalSimReadback<T>, "refresh"> & { loader: Loader<T> };

const emptySnapshot = <T,>(loader: Loader<T>): Snapshot<T> => ({
  loader, data: null, error: null, updatedAt: null, refreshing: false, stale: true,
});

/** Serial, visible-page readback. Every resource owns its own error and freshness. */
export function useLocalSimReadback<T>(loader: Loader<T>): LocalSimReadback<T> {
  const [snapshot, setSnapshot] = useState<Snapshot<T>>(() => emptySnapshot(loader));
  const refreshRef = useRef<() => Promise<void>>(async () => {});
  const refresh = useCallback(() => refreshRef.current(), []);

  useEffect(() => {
    let alive = true;
    let pending: Promise<void> | null = null;
    let controller: AbortController | null = null;
    let pollTimer: number | undefined;
    let staleTimer: number | undefined;
    setSnapshot(emptySnapshot(loader));

    function read(): Promise<void> {
      if (!alive || document.visibilityState === "hidden") return Promise.resolve();
      if (pending) return pending;
      window.clearTimeout(pollTimer);
      setSnapshot((previous) => ({ ...previous, refreshing: true }));
      const requestController = new AbortController();
      controller = requestController;
      let timedOut = false;
      const timeout = window.setTimeout(() => {
        timedOut = true;
        requestController.abort();
      }, REQUEST_TIMEOUT_MS);

      pending = (async () => {
        try {
          const data = await Promise.resolve().then(() => loader(requestController.signal));
          if (!alive) return;
          setSnapshot({ loader, data, error: null, updatedAt: Date.now(), refreshing: false, stale: false });
          window.clearTimeout(staleTimer);
          staleTimer = window.setTimeout(() => {
            if (alive) setSnapshot((previous) => ({ ...previous, stale: true }));
          }, STALE_AFTER_MS);
        } catch (error) {
          if (!alive) return;
          const code = error && typeof error === "object" && "code" in error && typeof error.code === "string" ? error.code : null;
          const message = timedOut ? "读取超时，请重试"
            : `${error instanceof Error ? error.message : String(error)}${code ? ` [${code}]` : ""}`;
          setSnapshot((previous) => ({ ...previous, error: message, refreshing: false, stale: true }));
        } finally {
          window.clearTimeout(timeout);
          controller = null;
          pending = null;
          if (alive) pollTimer = window.setTimeout(() => { void read(); }, POLL_INTERVAL_MS);
        }
      })();
      return pending;
    }

    const onVisible = () => {
      if (document.visibilityState !== "hidden") {
        setSnapshot((previous) => ({ ...previous, stale: true }));
        void read();
      }
    };
    refreshRef.current = read;
    void read();
    window.addEventListener("focus", onVisible);
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      alive = false;
      controller?.abort();
      window.clearTimeout(pollTimer);
      window.clearTimeout(staleTimer);
      window.removeEventListener("focus", onVisible);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loader]);

  // A route/loader change must not expose the previous account's data or CAS version.
  const current = snapshot.loader === loader ? snapshot : emptySnapshot(loader);
  return { ...current, refresh };
}

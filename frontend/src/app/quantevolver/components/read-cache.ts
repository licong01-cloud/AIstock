/** Bounded, page-owned read cache. Never shared across tasks or persisted to disk. */
export class QEReadCache {
  private values = new Map<string, any>();
  private pending = new Map<string, { promise: Promise<any>; controller: AbortController }>();

  constructor(private readonly capacity = 24) {}

  read(url: string, key = url, cache = true): Promise<any> {
    if (cache && this.values.has(key)) {
      const value = this.values.get(key);
      this.values.delete(key);
      this.values.set(key, value);
      return Promise.resolve(value);
    }
    const existing = this.pending.get(key);
    if (existing) return existing.promise;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30_000);
    const promise = fetch(url, { signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const value = await response.json();
        if (controller.signal.aborted) throw new DOMException("Read cancelled", "AbortError");
        if (value?.status === "error" || value?.status === "failed") {
          throw new Error(value.detail || value.message || "QE read failed");
        }
        if (cache) {
          this.values.set(key, value);
          while (this.values.size > this.capacity) this.values.delete(this.values.keys().next().value!);
        }
        return value;
      })
      .finally(() => {
        clearTimeout(timeout);
        if (this.pending.get(key)?.controller === controller) this.pending.delete(key);
      });
    this.pending.set(key, { promise, controller });
    return promise;
  }

  cancelPending(): void {
    for (const entry of this.pending.values()) entry.controller.abort();
    this.pending.clear();
  }

  clear(): void {
    this.values.clear();
    this.cancelPending();
  }
}

export function mergeTaskUpdates<T extends { task_id: string }>(previous: T[], updates: Partial<T>[]): T[] {
  const byId = new Map(updates.map(row => [row.task_id, row]));
  let changed = false;
  const next = previous.map(row => {
    const update = byId.get(row.task_id);
    if (!update) return row;
    const merged = { ...row, ...update };
    if (JSON.stringify(row) === JSON.stringify(merged)) return row;
    changed = true;
    return merged;
  });
  return changed ? next : previous;
}

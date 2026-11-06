/** Commit each section as it arrives; abort/timeout cannot publish late results. */
export async function loadSection<T>(
  request: () => Promise<T>, commit: (value: T) => void,
  fail: (message: string) => void, signal: AbortSignal,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const value = await Promise.race([
      request(),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("分区读取超过 12 秒，请重试")), 12_000); }),
    ]);
    if (!signal.aborted) commit(value);
  } catch (error) {
    if (!signal.aborted) fail(error instanceof Error ? error.message : String(error));
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

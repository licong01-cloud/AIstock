import { Badge } from "@/components/ui/badge";
import { chinaTime, replayPresentation } from "@/lib/simulation/localsim/presentation";
import type { LocalSimReplay } from "@/lib/simulation/localsim/types";
import styles from "../localsim.module.css";

export function LocalSimReplayStatus({ replay }: { replay: LocalSimReplay }) {
  const view = replayPresentation(replay);
  return <div className={styles.replayStatus}>
    <Badge className={`${styles.replayBadge} ${view.blocked ? styles.replayBlocked : ""}`} variant={view.blocked ? "destructive" : "secondary"}>{view.label}</Badge>
    {view.message && <p>{view.message}</p>}
    {view.blocked && replay.failure_code && <p>错误码：<code>{replay.failure_code}</code></p>}
    <p className={styles.statusMeta}>任务更新时间：{chinaTime(replay.updated_at)}</p>
    <details className={styles.statusDetails}><summary>回放诊断详情</summary><pre>{JSON.stringify({
      status: replay.status, failure_code: replay.failure_code, failure_context: replay.failure_context, version: replay.version,
    }, null, 2)}</pre></details>
  </div>;
}

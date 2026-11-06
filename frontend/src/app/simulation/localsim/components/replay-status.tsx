import { Badge } from "@/components/ui/badge";
import { chinaTime, replayPresentation } from "@/lib/simulation/localsim/presentation";
import type { LocalSimReplay } from "@/lib/simulation/localsim/types";
import styles from "../localsim.module.css";

export function LocalSimReplayStatus({ replay }: { replay: LocalSimReplay }) {
  const view = replayPresentation(replay);
  return <div className={styles.replayStatus}>
    <Badge className={`${styles.replayBadge} ${view.blocked ? styles.replayBlocked : ""}`} variant={view.blocked ? "destructive" : "secondary"}>{view.label}</Badge>
    {view.message && <p>{view.message}</p>}
    <p className={styles.statusMeta}>任务更新时间：{chinaTime(replay.updated_at)}</p>
  </div>;
}

import type { ReactNode } from "react";
import HMMResearchNavigation from "./HMMResearchNavigation";
import styles from "./hmm-research.module.css";

export default function HMMResearchShell({ children }: { children: ReactNode }) {
  return (
    <div className={styles.researchRoot}>
      <header className={styles.topbar}>
        <div className={styles.brand}>
          <div className={styles.brandMark} aria-hidden="true">H</div>
          <div>
            <div className={styles.brandTitle}>HMM Research Workspace</div>
            <div className={styles.brandSubtitle}>演进 · 风险 · 研究训练</div>
          </div>
        </div>
        <HMMResearchNavigation />
        <div className={styles.topActions}>
          <span className={styles.modeChip}>
            <span className={styles.modeDot} aria-hidden="true" />
            研究分析 · 不影响 QE 与模拟盘
          </span>
          <div className={styles.profile} aria-label="Research Analyst">RA</div>
        </div>
      </header>
      {children}
    </div>
  );
}

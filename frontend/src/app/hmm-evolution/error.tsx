"use client";

import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import styles from "@/components/hmm-research/hmm-research.module.css";

export default function HMMEvolutionError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <HMMResearchShell>
      <main className={styles.page}>
        <VisibleErrorState error={error} title="HMM 演进页面渲染失败" onRetry={reset} />
      </main>
    </HMMResearchShell>
  );
}

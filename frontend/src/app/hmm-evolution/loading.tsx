import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import styles from "@/components/hmm-research/hmm-research.module.css";

export default function HMMEvolutionLoading() {
  return (
    <HMMResearchShell>
      <main className={styles.page}>
        <div className={styles.loadingState} role="status" aria-live="polite">
          正在加载 HMM 演进实验室；请求超时会转为可见错误，不会永久等待。
        </div>
      </main>
    </HMMResearchShell>
  );
}

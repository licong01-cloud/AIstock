import Link from "next/link";
import styles from "./hmm-research.module.css";

export default function HMMResearchNavigation({ active = "evolution" }: { active?: "evolution" }) {
  return (
    <nav className={styles.workspaceNav} aria-label="HMM 研究模块导航">
      <Link
        href="/hmm-evolution"
        className={`${styles.navLink} ${active === "evolution" ? styles.navLinkActive : ""}`}
        aria-current={active === "evolution" ? "page" : undefined}
      >
        演进实验室
      </Link>
    </nav>
  );
}

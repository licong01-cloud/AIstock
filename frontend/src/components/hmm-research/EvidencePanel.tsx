import styles from "./hmm-research.module.css";

export type EvidenceSection = {
  title: string;
  rows: Array<{ label: string; value: string }>;
};

export default function EvidencePanel({ sections }: { sections: EvidenceSection[] }) {
  return (
    <div>
      {sections.map((section) => (
        <section key={section.title} className={styles.evidenceSection}>
          <h3 className={styles.evidenceHeading}>{section.title}</h3>
          <div className={styles.evidenceList}>
            {section.rows.map((row) => (
              <div className={styles.evidenceRow} key={`${section.title}-${row.label}`}>
                <span className={styles.evidenceKey}>{row.label}</span>
                <span className={styles.evidenceValue}>{row.value}</span>
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

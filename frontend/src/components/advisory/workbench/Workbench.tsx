"use client";
import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Overview from "./Overview";
import ProgramDetail from "./ProgramDetail";
import styles from "./workbench.module.css";

export default function Workbench() {
  const search = useSearchParams();
  const programId = search.get("program_id");
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    // Detail pagination is an explicit snapshot: only the overview refreshes periodically.
    if (programId) return;
    const update = () => { if (!document.hidden) setRefresh(v => v + 1); };
    const timer = setInterval(update, 60000);
    window.addEventListener("focus", update);
    return () => { clearInterval(timer); window.removeEventListener("focus", update); };
  }, [programId]);
  return <div className={styles.shell} data-testid="advisory-workbench">{programId
    ? <ProgramDetail key={programId} programId={programId} initialDate={search.get("target_date") || ""} />
    : <Overview refresh={refresh} onRefresh={() => setRefresh(v => v + 1)} />}</div>;
}

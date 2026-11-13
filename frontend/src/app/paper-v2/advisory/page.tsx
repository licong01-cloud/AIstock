import { Suspense } from "react";
import Workbench from "@/components/advisory/workbench/Workbench";

export default function AdvisoryPage() {
  return <Suspense fallback={<p>正在读取荐股工作台…</p>}><Workbench /></Suspense>;
}

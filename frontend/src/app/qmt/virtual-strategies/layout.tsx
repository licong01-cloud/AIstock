import type { ReactNode } from "react";
import "../../paper-v2/paper-v2.css";

export default function QmtVirtualStrategiesLayout({ children }: { children: ReactNode }) {
  return <div className="pv2-shell">{children}</div>;
}

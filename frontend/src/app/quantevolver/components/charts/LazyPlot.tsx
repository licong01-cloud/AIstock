"use client";

import React, { useEffect, useRef, useState } from "react";

let plotModule: Promise<React.ComponentType<any>> | undefined;
function loadPlot() {
  if (!plotModule) {
    plotModule = Promise.all([
      // These installed vendor packages do not ship TypeScript declarations.
      // @ts-expect-error untyped vendor factory
      import("react-plotly.js/factory"),
      // @ts-expect-error untyped basic distribution
      import("plotly.js-basic-dist"),
    ]).then(([factory, plotly]) => factory.default(plotly.default))
      .catch(error => { plotModule = undefined; throw error; });
  }
  return plotModule;
}

/** Keep off-screen charts unmounted; Plotly's component purges its graph on unmount. */
export default React.memo(function LazyPlot(props: any) {
  const container = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);
  const [Plot, setPlot] = useState<React.ComponentType<any> | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!container.current) return;
    if (typeof IntersectionObserver === "undefined") { setVisible(true); return; }
    const observer = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting)) { setVisible(true); observer.disconnect(); }
    }, { rootMargin: "150px" });
    observer.observe(container.current);
    return () => observer.disconnect();
  }, []);
  useEffect(() => {
    if (!visible) return;
    let cancelled = false;
    loadPlot().then(component => { if (!cancelled) setPlot(() => component); })
      .catch(reason => { if (!cancelled) setError(String(reason)); });
    return () => { cancelled = true; };
  }, [visible]);
  return <div ref={container} style={{ minHeight: props?.layout?.height ?? 260 }}>
    {error ? <div role="alert">图表加载失败：{error}</div>
      : Plot ? <Plot {...props} /> : <div style={{ padding: 16, color: "#64748b" }}>图表按需加载…</div>}
  </div>;
});

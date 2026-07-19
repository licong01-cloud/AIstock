"use client";

import styles from "@/components/hmm-research/hmm-research.module.css";

export type DailyMetricPoint = {
  date: string;
  daily_net_label: number | null;
  daily_net_db_10d: number | null;
};

const WIDTH = 900;
const HEIGHT = 260;
const MARGIN = { top: 22, right: 28, bottom: 42, left: 64 };

export default function DailyMetricChart({
  rows,
  horizon,
}: {
  rows: DailyMetricPoint[];
  horizon: number;
}) {
  const values = rows.flatMap((row) => [row.daily_net_label, row.daily_net_db_10d])
    .filter((value): value is number => value !== null && Number.isFinite(value));
  if (rows.length < 2 || values.length === 0) {
    return <div className={styles.emptyState}>至少需要两个有效交易日才能绘制逐日收益曲线。</div>;
  }

  const minValue = Math.min(0, ...values);
  const maxValue = Math.max(0, ...values);
  const span = maxValue - minValue || 1;
  const plotWidth = WIDTH - MARGIN.left - MARGIN.right;
  const plotHeight = HEIGHT - MARGIN.top - MARGIN.bottom;
  const x = (index: number) => MARGIN.left + (index / (rows.length - 1)) * plotWidth;
  const y = (value: number) => MARGIN.top + ((maxValue - value) / span) * plotHeight;
  const labelSegments = lineSegments(rows, "daily_net_label", x, y);
  const dbSegments = lineSegments(rows, "daily_net_db_10d", x, y);
  const zeroY = y(0);
  const tickIndexes = Array.from(new Set([0, Math.floor((rows.length - 1) / 2), rows.length - 1]));

  return (
    <div className={styles.chartWrap}>
      <div className={styles.chartLegend}>
        <span><i className={styles.chartLegendLabel} />净标签收益 · {horizon}D</span>
        <span><i className={styles.chartLegendDb} />Net DB 10D</span>
      </div>
      <svg
        className={styles.metricChart}
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`逐日净标签 ${horizon} 日收益与 DB 10 日收益折线图`}
      >
        <line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={zeroY} y2={zeroY} className={styles.chartZero} />
        {[minValue, maxValue].map((value) => (
          <g key={value}>
            <line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={y(value)} y2={y(value)} className={styles.chartGrid} />
            <text x={MARGIN.left - 10} y={y(value) + 4} textAnchor="end" className={styles.chartAxisText}>
              {(value * 100).toFixed(2)}%
            </text>
          </g>
        ))}
        {labelSegments.map((points) => <polyline key={`label-${points}`} points={points} className={styles.chartLineLabel} />)}
        {dbSegments.map((points) => <polyline key={`db-${points}`} points={points} className={styles.chartLineDb} />)}
        {tickIndexes.map((index) => (
          <text key={index} x={x(index)} y={HEIGHT - 12} textAnchor="middle" className={styles.chartAxisText}>
            {rows[index].date}
          </text>
        ))}
      </svg>
    </div>
  );
}

function lineSegments(
  rows: DailyMetricPoint[],
  key: "daily_net_label" | "daily_net_db_10d",
  x: (index: number) => number,
  y: (value: number) => number,
): string[] {
  const segments: string[] = [];
  let current: string[] = [];
  rows.forEach((row, index) => {
    const value = row[key];
    if (value === null) {
      if (current.length > 1) segments.push(current.join(" "));
      current = [];
      return;
    }
    current.push(`${x(index).toFixed(2)},${y(value).toFixed(2)}`);
  });
  if (current.length > 1) segments.push(current.join(" "));
  return segments;
}

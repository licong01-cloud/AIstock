export const QE_LABEL_HORIZONS = [1, 3, 5, 10, 20, 30, 40, 60, 120, 180] as const;

export type QELabelHorizon = (typeof QE_LABEL_HORIZONS)[number];

export function isQELabelHorizon(value: unknown): value is QELabelHorizon {
  return typeof value === "number"
    && Number.isInteger(value)
    && QE_LABEL_HORIZONS.some((item) => item === value);
}

export function normalizeQELabelHorizon(
  value: unknown,
  fallback: QELabelHorizon = 1,
): QELabelHorizon {
  const parsed = Number(value);
  return isQELabelHorizon(parsed) ? parsed : fallback;
}

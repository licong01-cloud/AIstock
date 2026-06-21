# BUG-443 Fix Smoke Validation

- Collected at: 2026-06-21T03:44:37+08:00
- No 575-factor full recompute was triggered.
- No production runtime was restarted or touched.
- Default worker metric precompute: `False`
- Factor effective_workers with 20GB available memory: `4`
- Factor effective_workers with 14.173GB available memory: `2`
- Parent metric concurrency peak: `2`
- Computed factors in smoke: `factor_a`, `factor_b`
- Result: `passed`

See `summary.json` for structured evidence.

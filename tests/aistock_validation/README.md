# AIstock Validation

This directory stores the reusable local validation assets for AIstock.

The first implementation target is the unified Paper Trading v2 + Selection Center
slice because those pages and backend flows are tightly coupled. Later phases should
promote the same runner, templates, and evidence format to StrategyPackage, HMM,
Tushare data, QE, RD-Agent, and release-candidate validation.

Rules:

- Keep authoritative test matrices in this repository, not in a separate test repo.
- Store lightweight Markdown evidence in `history/`; store bulky logs, traces, and
  screenshots outside Git and reference their paths/hashes in run records.
- Never restart or manage production backend port `8001` during validation.
- Use development ports `8011`/`8012` and `3011`/`3012`.
- Do not modify protected strategy/model/HMM/QE assets as part of framework tests.

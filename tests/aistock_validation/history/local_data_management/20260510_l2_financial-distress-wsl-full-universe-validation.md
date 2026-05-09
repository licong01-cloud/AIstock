# 2026-05-10 L2 Validation - Financial Distress WSL Full-Universe Rerun

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | Phase 19                                                            |
| validation level | L2 research full-universe rerun validation                           |
| source loop      | qe_20260507_132049_d4e7 / Loop2                                     |
| runtime impact   | no QE/Paper/Selection/QMT/runtime integration                        |
| production 8001  | not touched                                                         |
| report           | docs/analysis/event_signal_financial_distress_true_qe_wsl_full_universe_result_20260510.md |
+------------------+---------------------------------------------------------------------+
```

## Checks Run

```bash
wsl bash -lc 'source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2 && python qrun_limit_minute.py conf.yaml --pred-backtest /mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/mlruns/455819547877124274/b78e832bfd634afbbc770bcafe2e33ca/artifacts/pred.pkl'

wsl bash -lc 'source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2 && python qrun_limit_minute.py conf.yaml --pred-backtest /mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/mlruns/455819547877124274/26db7c74fd024a82b803866c235ec519/artifacts/pred.pkl'
```

## Results

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| WSL adjusted full-universe rerun      | pass: recorder 59eaf3f33f864ade97b79ce561a13f2a             |
| WSL baseline full-universe rerun      | pass: recorder 7b57828280ad40b988e6574c9a083da6             |
| PortAnaRecord                         | pass: both runs completed 442 backtest steps                 |
| same-environment comparison           | pass: both runs used source Loop2 full-universe conf.yaml    |
| production 8001                       | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## Business Outcome

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| outcome                              | status                     | evidence                                                     |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| full-universe feasibility             | PASS                       | WSL completed baseline and adjusted full-universe reruns     |
| current financial-distress candidate  | WEAK_POSITIVE              | +0.147pp ann excess return and +0.0276pp max-DD relief       |
| runtime promotion                     | REJECTED                   | effect is one-loop and too small for deployment              |
| next research step                    | SELECTIVE_TRUE_RERUN       | screen cheaply, then WSL true-rerun shortlisted candidates   |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Metrics

```text
+--------------------------------+----------------+----------------+------------------+
| metric                         | baseline       | adjusted       | delta            |
+--------------------------------+----------------+----------------+------------------+
| IC                             | 0.0613135033   | 0.0613167250   | +0.0000032217    |
| Rank IC                        | 0.0999630204   | 0.0999631593   | +0.0000001390    |
| annualized excess return cost  | 0.4763182376   | 0.4777874746   | +0.0014692370    |
| information ratio cost         | 2.2159536500   | 2.2222781396   | +0.0063244896    |
| max drawdown cost              | -0.1754268423  | -0.1751511308  | +0.0002757115    |
+--------------------------------+----------------+----------------+------------------+
```

## Residual Risks

```text
+------------------------------+---------------------------------------------------------------+
| risk                         | mitigation                                                     |
+------------------------------+---------------------------------------------------------------+
| one-loop dependence          | do not promote until multi-loop evidence is material          |
| WSL runtime cost             | run full true rerun only for shortlisted candidates           |
| source workspace drift       | preserve recorder IDs and external logs                       |
| no runtime audit table       | keep research artifacts outside DB until policy justified     |
+------------------------------+---------------------------------------------------------------+
```

## Post-Documentation Checks

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| git diff --check                      | pass: only LF/CRLF warnings                                  |
| focused event_signal pytest           | pass: 35 passed                                              |
| full event_signal pytest              | pass: 164 passed                                             |
| production 8001                       | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

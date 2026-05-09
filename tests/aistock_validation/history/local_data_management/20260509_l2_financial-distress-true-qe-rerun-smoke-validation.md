# 2026-05-09 L2 Validation - Financial Distress True QE Smoke Rerun

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | Phase 18                                                            |
| validation level | L2 research harness smoke validation                                 |
| runtime impact   | no QE/Paper/Selection/QMT/runtime integration                        |
| production 8001  | not touched                                                         |
| report           | docs/analysis/event_signal_financial_distress_true_qe_rerun_smoke_result_20260509.md |
+------------------+---------------------------------------------------------------------+
```

## Checks Run

```powershell
@'
from pathlib import Path
from ruamel.yaml import YAML
from qrun_limit_minute import render_yaml_template
p = Path(r'F:\Dev\AIstock_artifacts\event_signal_true_qe_rerun_20260509_qe20260507_loop2\conf.yaml')
text = render_yaml_template(str(p))
yaml = YAML(typ='safe', pure=True)
config = yaml.load(text)
print('qlib_init', 'qlib_init' in config, config.get('qlib_init') is not None)
'@ | C:\Users\lc999\miniconda3\envs\AIstock\python.exe -

C:\Users\lc999\miniconda3\envs\AIstock\python.exe qrun_limit_minute.py conf.yaml --pred-backtest event_signal_pred_backtest\adjusted_pred.pkl
```

## Results

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| copied conf parse                    | pass: qlib_init key loaded after BOM cleanup                |
| copied-loop smoke rerun              | pass: qrun_limit_minute.py completed                        |
| SigAnaRecord                         | pass: IC/ICIR and Rank IC/Rank ICIR recorded               |
| PortAnaRecord                        | pass: portfolio analysis completed                          |
| baseline-vs-adjusted comparison      | rejected: narrowed quote universe is not comparable        |
| production 8001                      | not touched                                                 |
+--------------------------------------+--------------------------------------------------------------+
```

## Business Outcome

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| outcome                              | status                     | evidence                                                     |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| true QE smoke path validated          | PASS                       | copied-loop pred-backtest completed end-to-end              |
| full-universe true rerun              | FAIL_CURRENT_MACHINE       | full copied universe reproduced MemoryError                 |
| financial distress overlay effect     | NOT_EVALUATED              | narrowed-universe PnL is not valid evidence                 |
| runtime promotion                     | REJECTED                   | Phase 18 validates only the technical harness path          |
| next research step                    | FEASIBILITY_REDESIGN       | build memory-safe or parity-controlled rerun before batch   |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+------------------------------+---------------------------------------------------------------+
| risk                         | mitigation                                                     |
+------------------------------+---------------------------------------------------------------+
| environment drift            | require baseline parity before interpreting adjusted metrics  |
| full-universe memory blowup  | redesign copied-loop true rerun before multi-loop expansion   |
| narrowed quote universe      | treat completed portfolio metrics as technical smoke only     |
| score-weighted sizing drift  | use the trace to inspect changed symbols and TopK drops       |
| no runtime audit table       | keep trace.csv/meta.json until policy persistence is justified|
+------------------------------+---------------------------------------------------------------+
```

## Post-Handoff Consistency Checks

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| documentation consistency scan        | pass: no stale return/drawdown-improvement promotion wording |
| git diff --check                      | pass: only LF/CRLF warnings                                  |
| pred materializer py_compile          | pass                                                         |
| focused event_signal pytest           | pass: 35 passed                                              |
| full event_signal pytest              | pass: 164 passed                                             |
| production 8001                       | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

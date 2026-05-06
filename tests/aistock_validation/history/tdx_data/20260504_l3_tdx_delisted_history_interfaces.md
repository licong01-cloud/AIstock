# TDX Delisted Historical Interface Validation

- Run at: 2026-05-04T17:34:43
- Base URL: `http://localhost:19080`
- Samples: 000001.SZ, 000979.SZ, 002477.SZ, 300379.SZ, 603056.SH
- Full missing PIT delisted daily check: 197 stocks

## Summary

| Endpoint group | Calls | Nonzero | Zero | Errors |
|---|---:|---:|---:|---:|
| kline | 40 | 7 | 21 | 12 |
| kline_all_alias | 5 | 1 | 4 | 0 |
| kline_all_tdx | 50 | 10 | 40 | 0 |
| kline_all_ths | 15 | 3 | 0 | 12 |
| kline_history | 40 | 8 | 20 | 12 |
| minute_trade_all | 4 | 1 | 0 | 3 |
| trade_history | 4 | 1 | 0 | 3 |
| trade_history_full | 4 | 1 | 0 | 3 |
| full197_kline_all_tdx_day | 197 | 0 | 197 | 0 |

## Sample Detail Counts

| Group | 000001.SZ | 000979.SZ | 002477.SZ | 300379.SZ | 603056.SH |
|---|---:|---:|---:|---:|---:|
| kline nonzero/calls | 7/8 | 0/8 | 0/8 | 0/8 | 0/8 |
| kline_all_alias nonzero/calls | 1/1 | 0/1 | 0/1 | 0/1 | 0/1 |
| kline_all_tdx nonzero/calls | 10/10 | 0/10 | 0/10 | 0/10 | 0/10 |
| kline_all_ths nonzero/calls | 3/3 | 0/3 | 0/3 | 0/3 | 0/3 |
| kline_history nonzero/calls | 8/8 | 0/8 | 0/8 | 0/8 | 0/8 |
| minute_trade_all nonzero/calls | 1/1 | 0/1 | 0/0 | 0/1 | 0/1 |
| trade_history nonzero/calls | 1/1 | 0/1 | 0/0 | 0/1 | 0/1 |
| trade_history_full nonzero/calls | 1/1 | 0/1 | 0/0 | 0/1 | 0/1 |

## Evidence JSON
- `tests/aistock_validation/history/tdx_data/20260504_l3_tdx_delisted_history_interfaces.json`

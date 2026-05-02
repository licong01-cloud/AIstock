# QE Price and Tradability Audit: qe_20260501_011054_c90a

Scope: read-only comparison of Qlib warning evidence, DB daily/minute/suspend/limit records, and Qlib price basis samples.
Warnings parsed: rows=1642 unique_total=1642 unique_audited=1642 minute_audited=206.

## P0 Qlib `$close=None` Warning Classification

```text
DBState                                                  UniqueWarnings
-------------------------------------------------------  --------------
DB_DAILY_LIMIT_PRESENT_NOT_SUSPENDED_MINUTE_NOT_AUDITED  1023          
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED              100           
DB_DAILY_PRESENT_MINUTE_NOT_AUDITED                      35            
SUSPEND_D_PRESENT_NO_DAILY_PRICE_MINUTE_NOT_AUDITED      378           
SUSPEND_D_PRESENT_NO_DB_PRICE                            106           
```

## P0 Warning Examples

```text
Loop  Stock      Date        DBState                                      Daily  MinRows  Suspend  Limit
----  ---------  ----------  -------------------------------------------  -----  -------  -------  -----
19    300489.SZ  2024-10-09  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    300489.SZ  2024-10-10  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2024-12-30  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2024-12-31  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-02  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-03  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-06  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-07  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-08  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-09  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-10  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002494.SZ  2025-01-13  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002798.SZ  2025-06-04  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    002798.SZ  2025-06-05  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    301178.SZ  2025-06-13  SUSPEND_D_PRESENT_NO_DB_PRICE                False  0        True     True 
19    603185.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    603421.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    688607.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    688051.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    603396.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    603898.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    605177.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    002486.SZ  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    603385.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    002817.SZ  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    300440.SZ  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    000751.SZ  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    300787.SZ  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    688680.SH  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
19    300006.SZ  2025-07-08  DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  True   240      False    True 
```

## P0 Warning Rows With DB Minute Audit

```text
DBState                                      MinuteAuditedRows
-------------------------------------------  -----------------
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  100              
SUSPEND_D_PRESENT_NO_DB_PRICE                106              
```

## P0 Price Basis Classification

```text
Basis             DaySamples  MinuteSamples
----------------  ----------  -------------
close_div_factor  12          11           
close_raw         8           9            
```

## P0 Price Precision Summary

```text
Samples  MaxDayDiff  MaxMinDiff  MaxUpDiff   MaxDownDiff
-------  ----------  ----------  ----------  -----------
20         0.000002    0.000002    0.000002    0.000000 
```

## P0 Price Basis Examples

```text
Loop  Stock      Date        Side  DayBasis          DayDiff     MinBasis          MinDiff     MinRows
----  ---------  ----------  ----  ----------------  ----------  ----------------  ----------  -------
19    002607.SZ  2024-07-02  buy   close_raw           0.000000  close_raw           0.000000  241    
19    688265.SH  2024-12-12  buy   close_raw           0.000000  close_raw           0.000000  241    
20    688069.SH  2025-10-20  sell  close_raw           0.000000  close_raw           0.000000  240    
20    002694.SZ  2026-03-09  sell  close_raw           0.000000  close_raw           0.000000  240    
21    300923.SZ  2026-02-25  buy   close_raw           0.000001  close_raw           0.000001  240    
21    301081.SZ  2025-04-21  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
22    688607.SH  2025-02-19  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
22    603727.SH  2025-02-28  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
23    688296.SH  2026-02-06  buy   close_raw           0.000001  close_raw           0.000001  240    
23    300176.SZ  2025-04-29  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
24    688592.SH  2024-08-28  sell  close_div_factor    0.000000  close_div_factor    0.000000  241    
24    600855.SH  2025-05-07  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
25    002295.SZ  2025-04-25  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
25    301272.SZ  2025-09-19  buy   close_div_factor    0.000002  close_div_factor    0.000002  240    
26    603116.SH  2024-10-21  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
26    600395.SH  2025-11-28  sell  close_raw           0.000000  close_raw           0.000000  240    
27    300692.SZ  2024-10-31  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
27    601007.SH  2026-04-24  buy   close_raw           0.000000  close_raw           0.000000  240    
28    002224.SZ  2024-11-20  sell  close_div_factor    0.000000  close_div_factor    0.000000  241    
28    002691.SZ  2026-04-27  buy   close_div_factor    0.000000  close_raw           0.000000  240    
```

## Evidence Notes

- Warning classification is factual: it reports whether DB daily/minute/limit/suspend records exist for the same stock/date where Qlib logged `$close=None`.
- Price basis classification compares DB raw close against Qlib `$close`, `$close/$factor`, and `$close*$factor`; the smallest absolute difference determines the basis label.
- If Qlib warning rows are classified as DB-present, this audit proves those rows are not caused by complete DB daily/minute absence for that stock/date.

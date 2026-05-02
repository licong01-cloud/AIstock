# QE Price and Tradability Audit: qe_20260501_011054_c90a

Scope: read-only comparison of Qlib warning evidence, DB daily/minute/suspend/limit records, and Qlib price basis samples.
Warnings parsed: rows=1642 unique_total=1642 unique_audited=1642 minute_audited=1642.

## P0 Qlib `$close=None` Warning Classification

```text
DBState                                         UniqueWarnings
----------------------------------------------  --------------
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED     1123          
SUSPEND_D_PRESENT_DAILY_PRESENT_MINUTE_MISSING  35            
SUSPEND_D_PRESENT_NO_DB_PRICE                   484           
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
DBState                                         MinuteAuditedRows
----------------------------------------------  -----------------
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED     1123             
SUSPEND_D_PRESENT_DAILY_PRESENT_MINUTE_MISSING  35               
SUSPEND_D_PRESENT_NO_DB_PRICE                   484              
```

## P0 Price Basis Classification

```text
Basis             DaySamples  MinuteSamples
----------------  ----------  -------------
close_div_factor  126         123          
close_raw         174         177          
```

## P0 Price Precision Summary

```text
Samples  MaxDayDiff  MaxMinDiff  MaxUpDiff   MaxDownDiff
-------  ----------  ----------  ----------  -----------
300        0.000004    0.000004    0.000007    0.000004 
```

## P0 Price Basis Examples

```text
Loop  Stock      Date        Side  DayBasis          DayDiff     MinBasis          MinDiff     MinRows
----  ---------  ----------  ----  ----------------  ----------  ----------------  ----------  -------
19    002607.SZ  2024-07-02  buy   close_raw           0.000000  close_raw           0.000000  241    
19    000882.SZ  2024-07-04  sell  close_raw           0.000000  close_raw           0.000000  241    
19    603489.SH  2024-07-02  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    600981.SH  2025-09-19  buy   close_raw           0.000000  close_raw           0.000000  240    
19    600128.SH  2024-07-18  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    688069.SH  2025-05-12  sell  close_div_factor    0.000001  close_div_factor    0.000001  241    
19    688315.SH  2024-12-12  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    002084.SZ  2025-08-15  buy   close_raw           0.000000  close_raw           0.000000  240    
19    603458.SH  2025-01-08  sell  close_div_factor    0.000000  close_div_factor    0.000000  241    
19    688329.SH  2025-06-20  sell  close_raw           0.000000  close_raw           0.000000  241    
19    300614.SZ  2025-04-29  sell  close_div_factor    0.000000  close_div_factor    0.000000  241    
19    002769.SZ  2025-06-04  buy   close_raw           0.000000  close_raw           0.000000  241    
19    600076.SH  2025-12-31  buy   close_raw           0.000000  close_raw           0.000000  240    
19    603898.SH  2025-08-29  buy   close_raw           0.000000  close_raw           0.000000  240    
19    000096.SZ  2024-11-06  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    603903.SH  2025-04-22  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    000590.SZ  2024-12-02  buy   close_raw           0.000000  close_raw           0.000000  241    
19    600744.SH  2024-12-23  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    300266.SZ  2025-01-17  sell  close_raw           0.000000  close_raw           0.000000  241    
19    002558.SZ  2025-05-16  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    001211.SZ  2025-03-05  sell  close_div_factor    0.000001  close_div_factor    0.000001  241    
19    603316.SH  2025-03-28  buy   close_div_factor    0.000000  close_div_factor    0.000000  241    
19    688113.SH  2025-12-26  buy   close_raw           0.000000  close_raw           0.000000  240    
19    002821.SZ  2025-06-16  sell  close_div_factor    0.000001  close_div_factor    0.000001  241    
19    002080.SZ  2025-07-02  sell  close_raw           0.000000  close_raw           0.000000  241    
19    002390.SZ  2026-03-31  buy   close_raw           0.000000  close_raw           0.000000  240    
19    605266.SH  2025-12-01  buy   close_raw           0.000001  close_raw           0.000001  240    
19    600233.SH  2025-10-20  buy   close_raw           0.000000  close_raw           0.000000  240    
19    688393.SH  2025-12-15  sell  close_raw           0.000001  close_raw           0.000001  240    
19    603098.SH  2026-03-03  sell  close_raw           0.000000  close_raw           0.000000  240    
```

## Evidence Notes

- Warning classification is factual: it reports whether DB daily/minute/limit/suspend records exist for the same stock/date where Qlib logged `$close=None`.
- Price basis classification compares DB raw close against Qlib `$close`, `$close/$factor`, and `$close*$factor`; the smallest absolute difference determines the basis label.
- If Qlib warning rows are classified as DB-present, this audit proves those rows are not caused by complete DB daily/minute absence for that stock/date.

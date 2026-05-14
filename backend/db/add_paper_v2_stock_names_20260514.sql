-- Add display/audit stock-name columns for Paper Trading v2.
--
-- These nullable fields are metadata only. They must not be used for
-- selection, scoring, risk checks, execution decisions, or matching.

ALTER TABLE paper_v2.orders
    ADD COLUMN IF NOT EXISTS stock_name TEXT;

ALTER TABLE paper_v2.fills
    ADD COLUMN IF NOT EXISTS stock_name TEXT;

ALTER TABLE paper_v2.cash_ledger
    ADD COLUMN IF NOT EXISTS stock_name TEXT;

ALTER TABLE paper_v2.positions
    ADD COLUMN IF NOT EXISTS stock_name TEXT;

COMMENT ON COLUMN paper_v2.orders.stock_name IS
    'Display/audit-only stock name resolved from market reference data; never used for trading logic.';
COMMENT ON COLUMN paper_v2.fills.stock_name IS
    'Display/audit-only stock name resolved from market reference data; never used for trading logic.';
COMMENT ON COLUMN paper_v2.cash_ledger.stock_name IS
    'Display/audit-only stock name resolved from market reference data; never used for trading logic.';
COMMENT ON COLUMN paper_v2.positions.stock_name IS
    'Display/audit-only stock name resolved from market reference data; never used for trading logic.';

DO $$
BEGIN
    IF to_regclass('market.stock_basic') IS NOT NULL THEN
        UPDATE paper_v2.orders o
        SET stock_name = NULLIF(TRIM(sb.name), '')
        FROM market.stock_basic sb
        WHERE o.symbol = sb.ts_code
          AND NULLIF(TRIM(sb.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(o.stock_name, '')), '') IS NULL;

        UPDATE paper_v2.fills f
        SET stock_name = NULLIF(TRIM(sb.name), '')
        FROM market.stock_basic sb
        WHERE f.symbol = sb.ts_code
          AND NULLIF(TRIM(sb.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(f.stock_name, '')), '') IS NULL;

        UPDATE paper_v2.cash_ledger c
        SET stock_name = NULLIF(TRIM(sb.name), '')
        FROM market.stock_basic sb
        WHERE c.symbol = sb.ts_code
          AND NULLIF(TRIM(sb.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(c.stock_name, '')), '') IS NULL;

        UPDATE paper_v2.positions p
        SET stock_name = NULLIF(TRIM(sb.name), '')
        FROM market.stock_basic sb
        WHERE p.symbol = sb.ts_code
          AND NULLIF(TRIM(sb.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(p.stock_name, '')), '') IS NULL;
    END IF;

    IF to_regclass('market.symbol_dim') IS NOT NULL THEN
        UPDATE paper_v2.orders o
        SET stock_name = NULLIF(TRIM(sd.name), '')
        FROM market.symbol_dim sd
        WHERE o.symbol = sd.ts_code
          AND NULLIF(TRIM(sd.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(o.stock_name, '')), '') IS NULL;

        UPDATE paper_v2.fills f
        SET stock_name = NULLIF(TRIM(sd.name), '')
        FROM market.symbol_dim sd
        WHERE f.symbol = sd.ts_code
          AND NULLIF(TRIM(sd.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(f.stock_name, '')), '') IS NULL;

        UPDATE paper_v2.cash_ledger c
        SET stock_name = NULLIF(TRIM(sd.name), '')
        FROM market.symbol_dim sd
        WHERE c.symbol = sd.ts_code
          AND NULLIF(TRIM(sd.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(c.stock_name, '')), '') IS NULL;

        UPDATE paper_v2.positions p
        SET stock_name = NULLIF(TRIM(sd.name), '')
        FROM market.symbol_dim sd
        WHERE p.symbol = sd.ts_code
          AND NULLIF(TRIM(sd.name), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(p.stock_name, '')), '') IS NULL;
    END IF;
END $$;

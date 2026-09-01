BEGIN;

CREATE TABLE IF NOT EXISTS market.dividend (
    ts_code TEXT NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    div_proc TEXT NOT NULL,
    stk_div NUMERIC,
    stk_bo_rate NUMERIC,
    stk_co_rate NUMERIC,
    cash_div NUMERIC,
    cash_div_tax NUMERIC,
    record_date DATE,
    ex_date DATE NOT NULL,
    pay_date DATE,
    div_listdate DATE,
    imp_ann_date DATE,
    base_date DATE,
    base_share NUMERIC,
    PRIMARY KEY (ts_code, end_date, ann_date, div_proc)
);

CREATE INDEX IF NOT EXISTS idx_market_dividend_ex_date_symbol
    ON market.dividend (ex_date, ts_code);

CREATE INDEX IF NOT EXISTS idx_market_dividend_knowledge_date
    ON market.dividend (imp_ann_date, ex_date);

COMMENT ON TABLE market.dividend IS
    'Tushare dividend implementation schedule used by Advisory M4B decision-cutoff price projection';
COMMENT ON COLUMN market.dividend.ts_code IS
    'Canonical Tushare A-share symbol';
COMMENT ON COLUMN market.dividend.end_date IS
    'Dividend plan reporting period end date supplied by Tushare';
COMMENT ON COLUMN market.dividend.ann_date IS
    'Plan announcement date supplied by Tushare; part of the decision-time knowledge contract';
COMMENT ON COLUMN market.dividend.div_proc IS
    'Dividend process state supplied by Tushare; M4B consumes only implemented rows';
COMMENT ON COLUMN market.dividend.stk_div IS
    'Total stock distribution per share; checked against stock bonus plus capitalization ratios';
COMMENT ON COLUMN market.dividend.stk_bo_rate IS
    'Stock bonus shares per share supplied by Tushare';
COMMENT ON COLUMN market.dividend.stk_co_rate IS
    'Capitalization shares per share supplied by Tushare';
COMMENT ON COLUMN market.dividend.cash_div IS
    'After-tax cash dividend per share supplied by Tushare; not substituted for the pre-tax reference-price input';
COMMENT ON COLUMN market.dividend.cash_div_tax IS
    'Pre-tax cash dividend per share used in the exchange reference-price adjustment';
COMMENT ON COLUMN market.dividend.record_date IS
    'Equity registration date supplied by Tushare';
COMMENT ON COLUMN market.dividend.ex_date IS
    'Ex-right/ex-dividend business date used for exact target-date refresh';
COMMENT ON COLUMN market.dividend.pay_date IS
    'Cash dividend payment date supplied by Tushare';
COMMENT ON COLUMN market.dividend.div_listdate IS
    'Distributed share listing date supplied by Tushare';
COMMENT ON COLUMN market.dividend.imp_ann_date IS
    'Implementation announcement date supplied by Tushare; required decision-time knowledge date for implemented actions';
COMMENT ON COLUMN market.dividend.base_date IS
    'Dividend calculation base date supplied by Tushare';
COMMENT ON COLUMN market.dividend.base_share IS
    'Dividend calculation base share count supplied by Tushare';

DO $migration$
DECLARE
    actual_pk TEXT[];
BEGIN
    SELECT array_agg(att.attname ORDER BY key_columns.ordinality)
      INTO actual_pk
      FROM pg_constraint con
      JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinality)
        ON TRUE
      JOIN pg_attribute att
        ON att.attrelid = con.conrelid AND att.attnum = key_columns.attnum
     WHERE con.conrelid = 'market.dividend'::regclass
       AND con.contype = 'p';

    IF actual_pk IS DISTINCT FROM ARRAY['ts_code', 'end_date', 'ann_date', 'div_proc']::TEXT[] THEN
        RAISE EXCEPTION 'market.dividend primary key mismatch: %', actual_pk;
    END IF;

    IF EXISTS (
        SELECT 1
          FROM (VALUES
              ('ts_code'), ('end_date'), ('ann_date'), ('div_proc'),
              ('stk_div'), ('stk_bo_rate'), ('stk_co_rate'),
              ('cash_div'), ('cash_div_tax'), ('record_date'), ('ex_date'),
              ('pay_date'), ('div_listdate'), ('imp_ann_date'), ('base_date'), ('base_share')
          ) AS required(column_name)
         WHERE NOT EXISTS (
             SELECT 1
               FROM information_schema.columns c
              WHERE c.table_schema = 'market'
                AND c.table_name = 'dividend'
                AND c.column_name = required.column_name
         )
    ) THEN
        RAISE EXCEPTION 'market.dividend required column set is incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_attribute attr
          LEFT JOIN pg_description description
            ON description.objoid = attr.attrelid
           AND description.objsubid = attr.attnum
         WHERE attr.attrelid = 'market.dividend'::regclass
           AND attr.attnum > 0
           AND NOT attr.attisdropped
           AND NULLIF(BTRIM(description.description), '') IS NULL
    ) THEN
        RAISE EXCEPTION 'market.dividend column comments are incomplete';
    END IF;
END
$migration$;

COMMIT;

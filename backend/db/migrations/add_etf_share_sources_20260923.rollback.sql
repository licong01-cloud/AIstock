BEGIN;

DELETE FROM market.data_stats_config
WHERE data_kind IN ('etf_share_size', 'etf_basic_snapshots');

DROP TABLE IF EXISTS market.etf_basic_snapshots;
DROP TABLE IF EXISTS market.etf_share_size;

COMMIT;

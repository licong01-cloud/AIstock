package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/injoyai/tdx/protocol"
	"github.com/jackc/pgx/v5"
	"strings"
	"time"
)

func sourceCode(tsCode string) (string, error) {
	p := strings.Split(tsCode, ".")
	if len(p) != 2 || len(p[0]) != 6 {
		return "", fmt.Errorf("invalid canonical symbol")
	}
	switch p[1] {
	case "SZ", "SH", "BJ":
		return strings.ToLower(p[1]) + p[0], nil
	}
	return "", fmt.Errorf("invalid symbol exchange")
}

func boundedEnd(value string, start time.Time) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	end, err := parseTimeOrDate(value)
	if err != nil {
		return time.Time{}, err
	}
	if len(value) == 10 {
		end = end.AddDate(0, 0, 1).Add(-time.Nanosecond)
	}
	if end.Before(start) {
		return time.Time{}, fmt.Errorf("end_time before start_time")
	}
	return end, nil
}

func rawRows(ks []*protocol.Kline, symbol string, start, end time.Time, minute bool, source string) ([][]any, error) {
	rows := make([][]any, 0, len(ks))
	seen := map[int64]bool{}
	for _, k := range ks {
		if k == nil {
			return nil, fmt.Errorf("nil raw fact")
		}
		if k.Time.Before(start) || (!end.IsZero() && k.Time.After(end)) {
			continue
		}
		t := k.Time.In(protocol.ExchangeLocation)
		if t.IsZero() || seen[t.Unix()] {
			return nil, fmt.Errorf("zero/duplicate raw fact time")
		}
		seen[t.Unix()] = true
		if k.Open <= 0 || k.Low <= 0 || k.High < k.Low || k.Open < k.Low || k.Open > k.High || k.Close < k.Low || k.Close > k.High || k.Volume < 0 || k.Amount < 0 {
			return nil, fmt.Errorf("invalid raw OHLCV at %s", t.Format(time.RFC3339))
		}
		if minute {
			m := t.Hour()*60 + t.Minute()
			// Collection-auction bars are not one of the 240 continuous-session bars.
			if m <= 9*60+30 {
				continue
			}
			if (m > 11*60+30 && m < 13*60+1) || m > 15*60 || t.Second() != 0 {
				return nil, fmt.Errorf("invalid continuous-session timestamp %s", t.Format(time.RFC3339))
			}
		}
		var shares any
		var pin any
		var preciseSource any
		if k.VolumeShares != nil {
			if *k.VolumeShares < 0 || *k.VolumeShares/100 != k.Volume {
				return nil, fmt.Errorf("share/hand units differ")
			}
			shares = *k.VolumeShares
			preciseSource = "tdx_wire_shares"
			b, err := json.Marshal(struct {
				Symbol     string
				Time       time.Time
				VolumeWire uint32
			}{symbol, t, k.VolumeWire})
			if err != nil {
				return nil, err
			}
			pin = fmt.Sprintf("%x", sha256.Sum256(b))
		}
		row := []any{t, symbol}
		if minute {
			row = append(row, "1m")
		}
		row = append(row, int64(k.Open), int64(k.High), int64(k.Low), int64(k.Close), k.Volume, int64(k.Amount), "none", source, shares, preciseSource, pin)
		rows = append(rows, row)
	}
	return rows, nil
}

// Suppress an entire provider placeholder day only when independent daily
// suspension facts prove it. S/R mixed events, missing evidence and any raw
// turnover remain fail-closed; 13:00 is never shifted or accepted as a quote.
func filterSuspendedMinutePlaceholders(ctx context.Context, query func(context.Context, string, ...any) pgx.Row,
	symbol string, ks []*protocol.Kline, start, end time.Time) ([]*protocol.Kline, error) {
	days := map[string][]*protocol.Kline{}
	boundary := map[string]bool{}
	for _, k := range ks {
		if k == nil {
			return nil, fmt.Errorf("nil raw fact")
		}
		if k.Time.Before(start) || (!end.IsZero() && k.Time.After(end)) {
			continue
		}
		t := k.Time.In(protocol.ExchangeLocation)
		d := t.Format("2006-01-02")
		days[d] = append(days[d], k)
		if t.Hour() == 13 && t.Minute() == 0 {
			boundary[d] = true
		}
	}
	suppressed := map[string]bool{}
	for day := range boundary {
		empty := true
		for _, k := range days[day] {
			if k.Volume != 0 || k.Amount != 0 || k.VolumeWire != 0 ||
				(k.VolumeShares != nil && *k.VolumeShares != 0) ||
				k.Open != k.High || k.High != k.Low || k.Low != k.Close || k.Close < 0 {
				empty = false
			}
		}
		if !empty {
			continue // rawRows reports the invalid session, not fabricated facts.
		}
		var proven bool
		err := query(ctx, `SELECT
			EXISTS(SELECT 1 FROM market.suspend_d WHERE ts_code=$1 AND trade_date=$2::date
			 AND suspend_type='S' AND NULLIF(BTRIM(COALESCE(suspend_timing,'')),'') IS NULL)
			AND NOT EXISTS(SELECT 1 FROM market.suspend_d WHERE ts_code=$1 AND trade_date=$2::date AND suspend_type='R')
			AND NOT EXISTS(SELECT 1 FROM market.kline_daily_raw WHERE ts_code=$1 AND trade_date=$2::date
			 AND adjust_type='none' AND (volume_hand IS NULL OR amount_li IS NULL OR volume_hand<>0 OR amount_li<>0 OR volume_shares<>0))
			AND NOT EXISTS(SELECT 1 FROM market.kline_minute_raw WHERE ts_code=$1 AND freq='1m'
			 AND trade_time >= $2::date::timestamp AT TIME ZONE 'Asia/Shanghai'
			 AND trade_time < ($2::date+1)::timestamp AT TIME ZONE 'Asia/Shanghai'
			 AND (volume_hand IS NULL OR amount_li IS NULL OR volume_hand<>0 OR amount_li<>0 OR volume_shares<>0))`, symbol, day).Scan(&proven)
		if err != nil {
			return nil, fmt.Errorf("suspension evidence query failed: %w", err)
		}
		suppressed[day] = proven
	}
	var filtered []*protocol.Kline
	for _, k := range ks {
		if !suppressed[k.Time.In(protocol.ExchangeLocation).Format("2006-01-02")] {
			filtered = append(filtered, k)
		}
	}
	return filtered, nil
}

// Insert new keys first. Only keys rejected by ON CONFLICT remain in staging
// for fact comparison or NULL-only precision enrichment. The caller owns the
// transaction and must roll it back on any error, including a replay conflict.
func copyRawRows(ctx context.Context, tx pgx.Tx, table pgx.Identifier, columns []string, rows [][]any, chunkSize int) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if chunkSize <= 0 {
		return 0, fmt.Errorf("raw stage chunk size must be positive")
	}
	name := table.Sanitize()
	if _, err := tx.Exec(ctx, "CREATE TEMP TABLE tdx_raw_stage (LIKE "+name+" INCLUDING DEFAULTS) ON COMMIT DROP"); err != nil {
		return 0, err
	}
	for off := 0; off < len(rows); off += chunkSize {
		end := off + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tdx_raw_stage"}, columns, pgx.CopyFromRows(rows[off:end])); err != nil {
			return 0, err
		}
	}
	key := "a.ts_code=b.ts_code AND a.trade_date=b.trade_date"
	keyCols := "ts_code,trade_date"
	dateCol := "a.trade_date"
	if table[1] == "kline_minute_raw" {
		key = "a.ts_code=b.ts_code AND a.trade_time=b.trade_time AND a.freq=b.freq"
		keyCols = "ts_code,trade_time,freq"
		dateCol = "a.trade_time"
	}
	cols := make([]string, len(columns))
	for i, col := range columns {
		cols[i] = pgx.Identifier{col}.Sanitize()
	}
	c := strings.Join(cols, ",")
	// RETURNING drives deletion of exactly the newly inserted stage keys.
	// This also handles mixed new/replayed batches without re-reading new facts.
	result, err := tx.Exec(ctx, "WITH inserted AS (INSERT INTO "+name+" ("+c+") SELECT "+c+" FROM tdx_raw_stage ON CONFLICT DO NOTHING RETURNING "+keyCols+") DELETE FROM tdx_raw_stage b USING inserted a WHERE "+key)
	if err != nil {
		return 0, err
	}
	inserted := int(result.RowsAffected())
	if inserted == len(rows) {
		return inserted, nil // ordinary daily ingestion: no target-table checks/updates
	}
	// Only duplicate keys require statistics and a lookup of existing facts.
	if _, err := tx.Exec(ctx, "ANALYZE tdx_raw_stage"); err != nil {
		return 0, err
	}
	lo, hi := rows[0][0].(time.Time), rows[0][0].(time.Time)
	for _, row := range rows {
		d := row[0].(time.Time)
		if d.Before(lo) {
			lo = d
		}
		if d.After(hi) {
			hi = d
		}
	}
	// The lateral primary-key equality already restricts the symbol. Retain
	// only the date bound for up-front hypertable chunk pruning.
	bound := " AND " + dateCol + " BETWEEN $1 AND $2"
	var lower, upper any = lo, hi
	if table[1] == "kline_daily_raw" {
		lower = lo.In(protocol.ExchangeLocation).Format("2006-01-02")
		upper = hi.In(protocol.ExchangeLocation).Format("2006-01-02")
	}
	var existing int
	var conflict, needsPrecision bool
	// LIMIT 1 keeps the lookup parameterized by each exact staged primary key
	// rather than planning a large hypertable join for a few replayed minutes.
	q := "SELECT count(*),COALESCE(bool_or(ROW(a.open_li,a.high_li,a.low_li,a.close_li,a.volume_hand,a.amount_li,a.adjust_type) IS DISTINCT FROM ROW(b.open_li,b.high_li,b.low_li,b.close_li,b.volume_hand,b.amount_li,b.adjust_type) OR (a.volume_shares IS NOT NULL AND b.volume_shares IS NOT NULL AND a.volume_shares<>b.volume_shares)),false),COALESCE(bool_or(a.volume_shares IS NULL AND a.volume_shares_source IS NULL AND a.volume_shares_sha256 IS NULL AND b.volume_shares IS NOT NULL),false) FROM tdx_raw_stage b CROSS JOIN LATERAL (SELECT a.* FROM " + name + " a WHERE " + key + bound + " LIMIT 1) a"
	if err := tx.QueryRow(ctx, q, lower, upper).Scan(&existing, &conflict, &needsPrecision); err != nil {
		return 0, err
	}
	if existing != len(rows)-inserted {
		return 0, fmt.Errorf("existing raw key readback differs from staged duplicate keys")
	}
	if conflict {
		return 0, fmt.Errorf("existing raw facts conflict; explicit bounded repair required")
	}
	if !needsPrecision {
		return inserted, nil // exact replay: no no-op UPDATE
	}
	// Conflict resolution uses primary-key probes, not another hypertable join.
	// Valid existing precision and all original OHLCV/amount remain untouched.
	_, err = tx.Exec(ctx, "INSERT INTO "+name+" AS a ("+c+") SELECT "+c+" FROM tdx_raw_stage WHERE volume_shares IS NOT NULL ON CONFLICT ("+keyCols+") DO UPDATE SET volume_shares=EXCLUDED.volume_shares,volume_shares_source=EXCLUDED.volume_shares_source,volume_shares_sha256=EXCLUDED.volume_shares_sha256 WHERE a.volume_shares IS NULL AND a.volume_shares_source IS NULL AND a.volume_shares_sha256 IS NULL")
	return inserted, err
}

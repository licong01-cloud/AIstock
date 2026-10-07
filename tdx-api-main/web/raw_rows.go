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

// COPY into a transaction-local staging table, reject conflicting existing
// facts, then insert new keys and fill only NULL precision provenance.
func copyRawRows(ctx context.Context, tx pgx.Tx, table pgx.Identifier, columns []string, rows [][]any, chunkSize int) (int, error) {
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
	dateCol := "a.trade_date"
	if table[1] == "kline_minute_raw" {
		key = "a.ts_code=b.ts_code AND a.trade_time=b.trade_time AND a.freq=b.freq"
		dateCol = "a.trade_time"
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
	bound := " AND " + dateCol + " BETWEEN $1 AND $2"
	var lower, upper any = lo, hi
	if table[1] == "kline_daily_raw" {
		lower = lo.In(protocol.ExchangeLocation).Format("2006-01-02")
		upper = hi.In(protocol.ExchangeLocation).Format("2006-01-02")
	}
	var conflict bool
	q := "SELECT EXISTS(SELECT 1 FROM " + name + " a JOIN tdx_raw_stage b ON " + key + " WHERE (ROW(a.open_li,a.high_li,a.low_li,a.close_li,a.volume_hand,a.amount_li,a.adjust_type) IS DISTINCT FROM ROW(b.open_li,b.high_li,b.low_li,b.close_li,b.volume_hand,b.amount_li,b.adjust_type) OR (a.volume_shares IS NOT NULL AND b.volume_shares IS NOT NULL AND a.volume_shares<>b.volume_shares))" + bound + ")"
	if err := tx.QueryRow(ctx, q, lower, upper).Scan(&conflict); err != nil {
		return 0, err
	}
	if conflict {
		return 0, fmt.Errorf("existing raw facts conflict; explicit bounded repair required")
	}
	cols := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	c := strings.Join(cols, ",")
	result, err := tx.Exec(ctx, "INSERT INTO "+name+" ("+c+") SELECT "+c+" FROM tdx_raw_stage ON CONFLICT DO NOTHING")
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, "UPDATE "+name+" a SET volume_shares=b.volume_shares,volume_shares_source=b.volume_shares_source,volume_shares_sha256=b.volume_shares_sha256 FROM tdx_raw_stage b WHERE "+key+" AND a.volume_shares IS NULL AND a.volume_shares_source IS NULL AND a.volume_shares_sha256 IS NULL AND b.volume_shares IS NOT NULL"+bound, lower, upper)
	return int(result.RowsAffected()), err
}

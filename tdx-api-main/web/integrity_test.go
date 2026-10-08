package main

import (
	"context"
	"github.com/injoyai/tdx/protocol"
	"github.com/jackc/pgx/v5"
	"os"
	"testing"
	"time"
)

func TestImportHasNoNetworkOrDatabaseSideEffects(t *testing.T) {
	if client != nil || manager != nil || pgPool != nil {
		t.Fatal("package import bootstrapped production dependencies")
	}
}

func TestExplicitConfigAndIsolatedTarget(t *testing.T) {
	for _, key := range []string{"TDX_DB_DSN", "TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"} {
		t.Setenv(key, "")
	}
	if _, err := explicitDatabaseDSN(); err == nil {
		t.Fatal("implicit production database accepted")
	}
	t.Setenv("TDX_RUNTIME_ENV", "dev")
	t.Setenv("TDX_HTTP_HOST", "127.0.0.1")
	t.Setenv("TDX_HTTP_PORT", "19081")
	t.Setenv("TDX_DB_NAME", "aistock_dev")
	if _, err := listenAddress(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TDX_HTTP_PORT", "19080")
	if _, err := listenAddress(); err == nil {
		t.Fatal("isolated service accepted production port")
	}
	t.Setenv("TDX_HTTP_PORT", "19bad081")
	if _, err := listenAddress(); err == nil {
		t.Fatal("malformed port silently sanitized")
	}
}

func TestWeekUsesISOYear(t *testing.T) {
	r := &protocol.KlineResp{Count: 2, List: []*protocol.Kline{{Time: time.Date(2019, 12, 30, 15, 0, 0, 0, time.UTC)}, {Time: time.Date(2019, 12, 31, 15, 0, 0, 0, time.UTC)}}}
	if got := convertToWeekKline(r); got.Count != 1 {
		t.Fatalf("same ISO week split: %d", got.Count)
	}
}

func TestBoundedRowsRejectDuplicatesAndPreserveUnits(t *testing.T) {
	start, _ := parseTimeOrDate("2026-09-01")
	end, _ := boundedEnd("2026-09-30", start)
	shares := int64(1)
	k := &protocol.Kline{Time: time.Date(2026, 9, 10, 11, 21, 0, 0, protocol.ExchangeLocation), Open: 12800, High: 12800, Low: 12800, Close: 12800, Amount: 13000, VolumeShares: &shares, VolumeWire: 0x3f800000}
	r, err := rawRows([]*protocol.Kline{k}, "688526.SH", start, end, true, "tdx_api")
	if err != nil || len(r) != 1 || r[0][8] != int64(13000) || r[0][11] != shares {
		t.Fatalf("raw fields: %#v %v", r, err)
	}
	if _, err := rawRows([]*protocol.Kline{k, k}, "688526.SH", start, end, true, "tdx_api"); err == nil {
		t.Fatal("duplicate accepted")
	}
	k.Time = time.Date(2026, 9, 10, 13, 0, 0, 0, protocol.ExchangeLocation)
	if _, err := rawRows([]*protocol.Kline{k}, "688526.SH", start, end, true, "tdx_api"); err == nil {
		t.Fatal("ambiguous 13:00 relabelled or accepted")
	}
	if _, err := sourceCode("688526.SH"); err != nil {
		t.Fatal(err)
	}
}

func TestTaskPanicAndSnapshotAreNotSilent(t *testing.T) {
	tm := NewTaskManager()
	id := tm.Run("test", func(context.Context) error { panic("decoder error") })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s, _ := tm.Get(id)
		if s.Status == TaskStatusFailed {
			s.Status = TaskStatusSuccess
			again, _ := tm.Get(id)
			if again.Status != TaskStatusFailed {
				t.Fatal("snapshot mutates live task")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("panic task did not fail")
}

func TestDEVRawWriteReadbackAndIdempotence(t *testing.T) {
	dsn := os.Getenv("TDX_DEV_TEST_DSN")
	if dsn == "" {
		t.Skip("explicit existing DEV configuration required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil || cfg.Database != "aistock_dev" {
		t.Fatal("DEV target guard")
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("DEV unavailable")
	}
	defer conn.Close(ctx)
	// No persistent market rows or schema are modified. This session-local
	// target inherits the real DEV schema and keys, then disappears on close.
	if _, err = conn.Exec(ctx, "CREATE TEMP TABLE kline_minute_raw (LIKE market.kline_minute_raw INCLUDING ALL)"); err != nil {
		t.Fatal(err)
	}
	start, _ := parseTimeOrDate("2026-09-01")
	end, _ := boundedEnd("2026-09-30", start)
	shares := int64(1)
	k := &protocol.Kline{Time: time.Date(2026, 9, 10, 11, 21, 0, 0, protocol.ExchangeLocation), Open: 12800, High: 12800, Low: 12800, Close: 12800, Amount: 13000, VolumeShares: &shares, VolumeWire: 0x3f800000}
	rows, err := rawRows([]*protocol.Kline{k}, "688526.SH", start, end, true, "tdx_api")
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{"trade_time", "ts_code", "freq", "open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li", "adjust_type", "source", "volume_shares", "volume_shares_source", "volume_shares_sha256"}
	for iteration := 0; iteration < 2; iteration++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n, err := copyRawRows(ctx, tx, pgx.Identifier{"pg_temp", "kline_minute_raw"}, cols, rows, 1)
		if err != nil || n != 1-iteration {
			tx.Rollback(ctx)
			t.Fatalf("write/replay %d: count=%d error=%v", iteration, n, err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var count, amount, exact int64
	var pin string
	err = conn.QueryRow(ctx, "SELECT count(*),max(amount_li),max(volume_shares)::bigint,max(volume_shares_sha256) FROM pg_temp.kline_minute_raw").Scan(&count, &amount, &exact, &pin)
	if err != nil || count != 1 || amount != 13000 || exact != 1 || len(pin) != 64 {
		t.Fatalf("DEV readback: %d %d %d %v", count, amount, exact, err)
	}
	rows[0][8] = int64(1288000)
	tx, _ := conn.Begin(ctx)
	defer tx.Rollback(ctx)
	if _, err = copyRawRows(ctx, tx, pgx.Identifier{"pg_temp", "kline_minute_raw"}, cols, rows, 1); err == nil {
		t.Fatal("conflicting original amount overwritten")
	}
}

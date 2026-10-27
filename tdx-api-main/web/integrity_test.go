package main

import (
	"context"
	"fmt"
	"github.com/injoyai/tdx/protocol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"os"
	"strings"
	"testing"
	"time"
)

// Embedding the interface makes any unexpected database operation fail the
// test, rather than silently emulating an expensive query.
type rawWriteTrace struct {
	pgx.Tx
	inserted       int64
	existing       int
	conflict       bool
	needsPrecision bool
	execs          []string
	queries        []string
}

func (tx *rawWriteTrace) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return tx.inserted, nil
}

func (tx *rawWriteTrace) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, sql)
	if strings.Contains(sql, "INSERT INTO") {
		return pgconn.NewCommandTag("DELETE " + fmt.Sprint(tx.inserted)), nil
	}
	return pgconn.NewCommandTag("CREATE TABLE"), nil
}

func (tx *rawWriteTrace) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.queries = append(tx.queries, sql)
	return rawWriteEvidenceRow{tx}
}

type rawWriteEvidenceRow struct{ trace *rawWriteTrace }

func (r rawWriteEvidenceRow) Scan(dest ...any) error {
	if len(dest) == 1 { // original pre-insert query, retained for the RED test
		*dest[0].(*bool) = r.trace.conflict
		return nil
	}
	*dest[0].(*int) = r.trace.existing
	*dest[1].(*bool) = r.trace.conflict
	*dest[2].(*bool) = r.trace.needsPrecision
	return nil
}

func TestNewRawKeysSkipExistingFactScansAndPrecisionUpdates(t *testing.T) {
	for _, table := range []string{"kline_minute_raw", "kline_daily_raw"} {
		t.Run(table, func(t *testing.T) {
			tx := &rawWriteTrace{inserted: 1}
			rows := [][]any{{time.Date(2026, 10, 19, 9, 31, 0, 0, protocol.ExchangeLocation), "000001.SZ"}}
			n, err := copyRawRows(context.Background(), tx, pgx.Identifier{"market", table}, []string{"trade_time", "ts_code"}, rows, 240)
			if err != nil || n != 1 {
				t.Fatalf("new-key path: rows=%d error=%v", n, err)
			}
			if len(tx.execs) != 4 || len(tx.queries) != 0 || strings.Contains(strings.Join(tx.execs, " "), "ANALYZE") {
				t.Fatalf("new keys performed redundant database work: %v", tx.execs)
			}
			if !strings.Contains(tx.execs[2], "ON CONFLICT DO NOTHING") || !strings.Contains(tx.execs[2], "RETURNING") {
				t.Fatal("insert must retain key-based idempotence and identify only duplicate keys")
			}
			if !strings.Contains(tx.execs[2], "FROM inserted") || !strings.Contains(tx.execs[3], "mark_data_integrity_changed") {
				t.Fatal("only actually changed partition dates must be marked inside the raw transaction")
			}
		})
	}
}

func TestRawReplayOnlyEnrichesMissingPrecision(t *testing.T) {
	for _, tc := range []struct {
		name           string
		needsPrecision bool
		conflict       bool
		existing       int
		wantError      bool
		wantExecs      int
	}{
		{"exact_replay", false, false, 1, false, 4},
		{"legacy_precision", true, false, 1, false, 6},
		{"conflicting_fact", true, true, 1, true, 4},
		{"missing_existing_key", false, false, 0, true, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &rawWriteTrace{existing: tc.existing, needsPrecision: tc.needsPrecision, conflict: tc.conflict}
			rows := [][]any{{time.Date(2026, 10, 19, 9, 31, 0, 0, protocol.ExchangeLocation), "000001.SZ"}}
			_, err := copyRawRows(context.Background(), tx, pgx.Identifier{"market", "kline_minute_raw"}, []string{"trade_time", "ts_code"}, rows, 240)
			if (err != nil) != tc.wantError || len(tx.execs) != tc.wantExecs || len(tx.queries) != 1 {
				t.Fatalf("replay operations differ: error=%v execs=%d queries=%d", err, len(tx.execs), len(tx.queries))
			}
			if !strings.Contains(tx.queries[0], "LATERAL") || !strings.Contains(tx.queries[0], "LIMIT 1") {
				t.Fatal("replay must use staged-key point lookups")
			}
			if tc.needsPrecision && !tc.wantError && !strings.Contains(tx.execs[4], "ON CONFLICT (ts_code,trade_time,freq) DO UPDATE") {
				t.Fatal("precision enrichment must use primary-key conflict resolution")
			}
			if !tc.needsPrecision && strings.Contains(strings.Join(tx.execs, " "), "mark_data_integrity_changed") {
				t.Fatal("unchanged raw replay must not advance the partition revision")
			}
		})
	}
}

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

type booleanEvidenceRow struct{ value bool }

func (r booleanEvidenceRow) Scan(dest ...any) error { *dest[0].(*bool) = r.value; return nil }

func TestEmptyLunchBoundaryRequiresIndependentSuspensionEvidence(t *testing.T) {
	start, _ := parseTimeOrDate("2026-10-15")
	end, _ := boundedEnd("2026-10-15", start)
	for _, price := range []protocol.Price{0, 10000} {
		k := &protocol.Kline{Time: time.Date(2026, 10, 15, 13, 0, 0, 0, protocol.ExchangeLocation),
			Open: price, High: price, Low: price, Close: price}
		for _, proven := range []bool{false, true} {
			query := func(_ context.Context, sql string, args ...any) pgx.Row {
				if len(args) != 2 || args[0] != "002388.SZ" || args[1] != "2026-10-15" {
					t.Fatal("evidence scope differs")
				}
				if !strings.Contains(sql, "suspend_type='R'") || strings.Count(sql, "OR volume_shares<>0") != 2 {
					t.Fatal("mixed events or sub-hand trades must contradict full-day suspension")
				}
				return booleanEvidenceRow{proven}
			}
			filtered, err := filterSuspendedMinutePlaceholders(context.Background(), query, "002388.SZ", []*protocol.Kline{k}, start, end)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := rawRows(filtered, "002388.SZ", start, end, true, "tdx_api")
			if proven && (err != nil || len(rows) != 0) {
				t.Fatalf("proven placeholder not suppressed: %v", err)
			}
			if !proven && err == nil {
				t.Fatal("unproven boundary accepted")
			}
		}
		oneShare := int64(1)
		k.VolumeShares = &oneShare
		if _, err := rawRows([]*protocol.Kline{k}, "002388.SZ", start, end, true, "tdx_api"); err == nil {
			t.Fatal("sub-hand economic trade at invalid time was silently discarded")
		}
	}
}

func TestDEVRawWriteReadbackAndIdempotence(t *testing.T) {
	dsn := os.Getenv("TDX_DEV_TEST_DSN")
	if dsn == "" {
		t.Skip("explicit existing DEV configuration required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil || cfg.Database != "aistock_dev" || cfg.Port != 5433 || (cfg.Host != "localhost" && cfg.Host != "127.0.0.1") {
		t.Fatal("DEV target guard")
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("DEV unavailable")
	}
	defer conn.Close(ctx)
	// Exercise the exact Go marker against the installed DEV migration. The
	// synthetic ledger key and temp staging dates are rolled back together.
	t.Run("atomic_integrity_marker", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM market.dataset_date_refresh_audit WHERE dataset='kline_minute_raw' AND trade_date='2099-01-03')").Scan(&exists); err != nil || exists {
			t.Fatal("synthetic DEV ledger key is not isolated")
		}
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE tdx_integrity_dates (day date PRIMARY KEY) ON COMMIT DROP; INSERT INTO tdx_integrity_dates VALUES ('2099-01-03')"); err != nil {
			t.Fatal(err)
		}
		if err := markRawIntegrity(ctx, tx, pgx.Identifier{"market", "kline_minute_raw"}); err != nil {
			t.Fatal(err)
		}
		var revision int64
		var status string
		if err := tx.QueryRow(ctx, "SELECT data_revision,validation_status FROM market.dataset_date_refresh_audit WHERE dataset='kline_minute_raw' AND trade_date='2099-01-03' AND data_source='canonical_db'").Scan(&revision, &status); err != nil || revision != 1 || status != "UNVERIFIED" {
			t.Fatalf("Go marker readback: revision=%d status=%s error=%v", revision, status, err)
		}
	})
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
	if _, err = copyRawRows(ctx, tx, pgx.Identifier{"pg_temp", "kline_minute_raw"}, cols, rows, 1); err == nil {
		tx.Rollback(ctx)
		t.Fatal("conflicting original amount overwritten")
	}
	tx.Rollback(ctx)

	// Exercise both schemas with real DEV SQL, including mixed new/replayed
	// batches, NULL-only enrichment and rollback of new rows on a conflict.
	for _, minute := range []bool{true, false} {
		name := "kline_daily_raw"
		columns := append([]string(nil), cols...)
		if minute {
			name = "kline_minute_raw"
		} else {
			columns = append([]string{"trade_date", "ts_code"}, cols[3:]...)
			if _, err := conn.Exec(ctx, "CREATE TEMP TABLE kline_daily_raw (LIKE market.kline_daily_raw INCLUDING ALL)"); err != nil {
				t.Fatal(err)
			}
		}
		makeRows := func(day int, precision bool) [][]any {
			fact := &protocol.Kline{Time: time.Date(2026, 9, day, 11, 21, 0, 0, protocol.ExchangeLocation), Open: 12800, High: 12800, Low: 12800, Close: 12800, Amount: 13000}
			if precision {
				fact.VolumeShares, fact.VolumeWire = &shares, 0x3f800000
			}
			r, err := rawRows([]*protocol.Kline{fact}, "000001.SZ", start, end, minute, "tdx_api")
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		apply := func(batch [][]any, want int, wantError bool) {
			t.Helper()
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			started := time.Now()
			n, err := copyRawRows(ctx, tx, pgx.Identifier{"pg_temp", name}, columns, batch, 240)
			if (err != nil) != wantError || (!wantError && n != want) {
				t.Fatalf("%s expected rows=%d error=%v: rows=%d error=%v", name, want, wantError, n, err)
			}
			if wantError {
				return
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s input=%d inserted=%d elapsed=%s", name, len(batch), n, time.Since(started))
		}
		apply(makeRows(11, false), 1, false)
		mixed := append(makeRows(11, true), makeRows(14, true)...)
		apply(mixed, 1, false)
		apply(mixed, 0, false)
		var enriched int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_temp."+name+" WHERE ts_code='000001.SZ' AND volume_shares=1 AND length(volume_shares_sha256)=64").Scan(&enriched); err != nil || enriched != 2 {
			t.Fatalf("%s enrichment readback=%d error=%v", name, enriched, err)
		}
		bad := makeRows(11, true)
		amountColumn := 8
		if !minute {
			amountColumn--
		}
		bad[0][amountColumn] = int64(99999)
		apply(append(bad, makeRows(15, true)...), 0, true)
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_temp."+name+" WHERE ts_code='000001.SZ'").Scan(&enriched); err != nil || enriched != 2 {
			t.Fatalf("%s mixed conflict did not roll back new keys: rows=%d error=%v", name, enriched, err)
		}
	}

	// Also execute the duplicate-key path against the real DEV hypertable,
	// using only its existing facts and rolling the entire transaction back.
	actual, err := conn.Query(ctx, "SELECT "+strings.Join(cols, ",")+" FROM market.kline_minute_raw WHERE ts_code='301569.SZ' AND trade_time >= '2026-10-15'::timestamptz AND trade_time < '2026-10-16'::timestamptz LIMIT 240")
	if err != nil {
		t.Fatal(err)
	}
	var existingRows [][]any
	for actual.Next() {
		values, err := actual.Values()
		if err != nil {
			t.Fatal(err)
		}
		existingRows = append(existingRows, values)
	}
	actual.Close()
	if err := actual.Err(); err != nil {
		t.Fatal(err)
	}
	if len(existingRows) == 0 {
		t.Fatal("existing DEV October15 minute facts required for hypertable replay")
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	started := time.Now()
	n, err := copyRawRows(ctx, tx, pgx.Identifier{"market", "kline_minute_raw"}, cols, existingRows, 240)
	if err != nil || n != 0 {
		t.Fatalf("real DEV hypertable replay rows=%d error=%v", n, err)
	}
	t.Logf("DEV real hypertable replay rows=%d elapsed=%s (transaction rolled back)", len(existingRows), time.Since(started))
}

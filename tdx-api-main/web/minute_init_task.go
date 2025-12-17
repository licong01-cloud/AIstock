package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/injoyai/tdx/protocol"
)

var pgPool *pgxpool.Pool

type MinuteInitOptions struct {
	TruncateBefore  bool   `json:"truncate_before"`
	MaxRowsPerChunk int    `json:"max_rows_per_chunk"`
	Source          string `json:"source"`
}

type MinuteInitRequest struct {
	JobID     string           `json:"job_id"`
	Codes     []string         `json:"codes"`
	StartTime string           `json:"start_time"`
	Workers   int              `json:"workers"`
	Options   MinuteInitOptions `json:"options"`
}

// DailyRawInitRequest 复用与分钟初始化一致的选项结构，仅目标表与K线类型不同。
type DailyRawInitRequest struct {
	JobID     string           `json:"job_id"`
	Codes     []string         `json:"codes"`
	StartTime string           `json:"start_time"`
	Workers   int              `json:"workers"`
	Options   MinuteInitOptions `json:"options"`
}

// DailyQfqInitRequest: 前复权日线初始化（Go 直连版）。
// 与 DailyRawInitRequest 非常相似，但不再需要前端指定时间范围，由 Go 端拉取所有可用 QFQ 日线。
type DailyQfqInitRequest struct {
	JobID   string           `json:"job_id"`
	Codes   []string         `json:"codes"`
	Workers int              `json:"workers"`
	Options MinuteInitOptions `json:"options"`
}

type symbolResult struct {
	TsCode       string `json:"ts_code"`
	InsertedRows int    `json:"inserted_rows"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
}

func init() {
	dsn := os.Getenv("TDX_DB_DSN")
	if dsn == "" {
		host := os.Getenv("TDX_DB_HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv("TDX_DB_PORT")
		if port == "" {
			port = "5432"
		}
		name := os.Getenv("TDX_DB_NAME")
		if name == "" {
			name = "aistock"
		}
		user := os.Getenv("TDX_DB_USER")
		if user == "" {
			user = "postgres"
		}
		pass := os.Getenv("TDX_DB_PASSWORD")
		if pass == "" {
			pass = "lc78080808"
		}
		u := url.QueryEscape(user)
		p := url.QueryEscape(pass)
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", u, p, host, port, name)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Printf("init pg pool parse config failed: %v", err)
		return
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Printf("init pg pool failed: %v", err)
		return
	}
	pgPool = pool
}

func handleCreateMinuteRawInitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "只支持POST请求")
		return
	}
	if client == nil {
		errorResponse(w, "TDX客户端未初始化")
		return
	}
	if pgPool == nil {
		errorResponse(w, "数据库连接池未初始化")
		return
	}

	var req MinuteInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "请求参数错误: "+err.Error())
		return
	}
	if req.JobID == "" {
		errorResponse(w, "job_id 不能为空")
		return
	}
	start, err := parseTimeOrDate(req.StartTime)
	if err != nil {
		errorResponse(w, "start_time 格式错误，应为 RFC3339 或 YYYY-MM-DD")
		return
	}
	workers := req.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	if req.Options.MaxRowsPerChunk <= 0 {
		req.Options.MaxRowsPerChunk = 500000
	}
	if req.Options.Source == "" {
		req.Options.Source = "tdx_api"
	}

	jobUUID, err := uuid.Parse(req.JobID)
	if err != nil {
		errorResponse(w, "job_id 格式错误")
		return
	}

	taskID := taskManager.Run("minute_init_raw", func(ctx context.Context) error {
		return runMinuteInitTask(ctx, jobUUID, req.Codes, start, workers, req.Options)
	})

	successResponse(w, map[string]string{
		"task_id": taskID,
	})
}

// handleCreateDailyQfqInitTask: 初始化前复权日线 kline_daily_qfq（Go 直连版）。
// 不再要求前端提供时间范围，由 Go 端直接拉取该标的全部可用 QFQ 日线并入库。
func handleCreateDailyQfqInitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "只支持POST请求")
		return
	}
	if client == nil {
		errorResponse(w, "TDX客户端未初始化")
		return
	}
	if pgPool == nil {
		errorResponse(w, "数据库连接池未初始化")
		return
	}

	var req DailyQfqInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "请求参数错误: "+err.Error())
		return
	}
	if req.JobID == "" {
		errorResponse(w, "job_id 不能为空")
		return
	}

	workers := req.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	if req.Options.MaxRowsPerChunk <= 0 {
		req.Options.MaxRowsPerChunk = 500000
	}
	if req.Options.Source == "" {
		req.Options.Source = "tdx_api"
	}

	jobUUID, err := uuid.Parse(req.JobID)
	if err != nil {
		errorResponse(w, "job_id 格式错误")
		return
	}

	taskID := taskManager.Run("daily_qfq_init", func(ctx context.Context) error {
		return runDailyQfqInitTask(ctx, jobUUID, req.Codes, workers, req.Options)
	})

	successResponse(w, map[string]string{
		"task_id": taskID,
	})
}

func parseTimeOrDate(value string) (time.Time, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return time.Time{}, errors.New("empty")
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02", v, time.Local); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %s", v)
}

// handleCreateDailyRawInitTask: 初始化未复权日线 kline_daily_raw（Go 直连版）。
// 行为与分钟初始化保持一致，仅目标表与使用的 K 线类型不同（使用 GetKlineDayAll）。
func handleCreateDailyRawInitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "只支持POST请求")
		return
	}
	if client == nil {
		errorResponse(w, "TDX客户端未初始化")
		return
	}
	if pgPool == nil {
		errorResponse(w, "数据库连接池未初始化")
		return
	}

	var req DailyRawInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "请求参数错误: "+err.Error())
		return
	}
	if req.JobID == "" {
		errorResponse(w, "job_id 不能为空")
		return
	}
	start, err := parseTimeOrDate(req.StartTime)
	if err != nil {
		errorResponse(w, "start_time 格式错误，应为 RFC3339 或 YYYY-MM-DD")
		return
	}
	workers := req.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	if req.Options.MaxRowsPerChunk <= 0 {
		req.Options.MaxRowsPerChunk = 500000
	}
	if req.Options.Source == "" {
		req.Options.Source = "tdx_api"
	}

	jobUUID, err := uuid.Parse(req.JobID)
	if err != nil {
		errorResponse(w, "job_id 格式错误")
		return
	}

	taskID := taskManager.Run("daily_raw_init", func(ctx context.Context) error {
		return runDailyRawInitTask(ctx, jobUUID, req.Codes, start, workers, req.Options)
	})

	successResponse(w, map[string]string{
		"task_id": taskID,
	})
}

func runMinuteInitTask(ctx context.Context, jobID uuid.UUID, codes []string, start time.Time, workers int, opt MinuteInitOptions) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}

	baseSummary, err := loadJobSummary(ctx, jobID)
	if err != nil {
		return err
	}
	if baseSummary == nil {
		baseSummary = map[string]any{}
	}
	if _, ok := baseSummary["dataset"]; !ok {
		baseSummary["dataset"] = "kline_minute_raw"
	}
	if _, ok := baseSummary["datasets"]; !ok {
		baseSummary["datasets"] = []string{"kline_minute_raw"}
	}

	if err := markJobStarted(ctx, jobID, baseSummary); err != nil {
		return err
	}

	if opt.TruncateBefore {
		if err := truncateMinuteRaw(ctx); err != nil {
			merged := mergeSummary(baseSummary, map[string]any{"error": err.Error(), "phase": "truncate"})
			_ = markJobFinished(ctx, jobID, "failed", merged)
			return err
		}
	}

	var allCodes []string
	if len(codes) > 0 {
		allCodes = normalizeTSCodes(codes)
	} else {
		allCodes, err = collectAllStockTSCodes()
		if err != nil {
			merged := mergeSummary(baseSummary, map[string]any{"error": err.Error(), "phase": "collect_codes"})
			_ = markJobFinished(ctx, jobID, "failed", merged)
			return err
		}
	}
	if len(allCodes) == 0 {
		merged := mergeSummary(baseSummary, map[string]any{"error": "no codes", "phase": "collect_codes"})
		_ = markJobFinished(ctx, jobID, "failed", merged)
		return fmt.Errorf("no codes to process")
	}

	sort.Strings(allCodes)
	total := len(allCodes)

	mu := &sync.Mutex{}
	var successCodes int64
	var failedCodes int64
	var insertedRows int64
	results := make([]symbolResult, 0, total)

	mu.Lock()
	baseSummary["total_codes"] = total
	baseSummary["success_codes"] = successCodes
	baseSummary["failed_codes"] = failedCodes
	baseSummary["inserted_rows"] = insertedRows
	_ = saveJobSummary(ctx, jobID, baseSummary)
	mu.Unlock()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	errCh := make(chan error, total)

	for _, tsCode := range allCodes {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(code string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			localInserted, e := ingestSingleSymbol(ctx, code, start, opt)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				failedCodes++
				results = append(results, symbolResult{
					TsCode: code,
					Status: "failed",
					Error:  e.Error(),
				})
				errCh <- e
			} else {
				if localInserted > 0 {
					results = append(results, symbolResult{
						TsCode:       code,
						Status:       "success",
						InsertedRows: localInserted,
					})
				}
				if localInserted > 0 {
					insertedRows += int64(localInserted)
				}
				successCodes++
			}
			baseSummary["total_codes"] = total
			baseSummary["success_codes"] = successCodes
			baseSummary["failed_codes"] = failedCodes
			baseSummary["inserted_rows"] = insertedRows
			_ = saveJobSummary(ctx, jobID, baseSummary)
		}(tsCode)
	}

	wg.Wait()
	close(errCh)

	status := "success"
	if ctx.Err() == context.Canceled {
		status = "cancelled"
	} else if failedCodes > 0 {
		status = "failed"
	}

	payload := map[string]any{
		"summary": map[string]any{
			"dataset":       baseSummary["dataset"],
			"datasets":      baseSummary["datasets"],
			"mode":          "init",
			"total_codes":   total,
			"success_codes": successCodes,
			"failed_codes":  failedCodes,
			"inserted_rows": insertedRows,
		},
		"symbols": results,
	}
	level := "INFO"
	if status == "failed" {
		level = "ERROR"
	}
	if err := insertIngestionLog(ctx, jobID, level, payload); err != nil {
		log.Printf("insert ingestion log failed: %v", err)
	}

	if err := markJobFinished(ctx, jobID, status, baseSummary); err != nil {
		return err
	}

	if status == "failed" {
		for e := range errCh {
			if e != nil {
				return e
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// runDailyQfqInitTask 前复权日线初始化：
// - dataset 固定为 kline_daily_qfq
// - 使用 getQfqKlineDay 拉取该标的全部可用前复权日线
// - 由本地 COPY 写入 TimescaleDB。
func runDailyQfqInitTask(ctx context.Context, jobID uuid.UUID, codes []string, workers int, opt MinuteInitOptions) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}

	baseSummary, err := loadJobSummary(ctx, jobID)
	if err != nil {
		return err
	}
	if baseSummary == nil {
		baseSummary = map[string]any{}
	}
	if _, ok := baseSummary["dataset"]; !ok {
		baseSummary["dataset"] = "kline_daily_qfq"
	}
	if _, ok := baseSummary["datasets"]; !ok {
		baseSummary["datasets"] = []string{"kline_daily_qfq"}
	}

	if err := markJobStarted(ctx, jobID, baseSummary); err != nil {
		return err
	}

	if opt.TruncateBefore {
		if err := truncateDailyQfq(ctx); err != nil {
			merged := mergeSummary(baseSummary, map[string]any{"error": err.Error(), "phase": "truncate"})
			_ = markJobFinished(ctx, jobID, "failed", merged)
			return err
		}
	}

	var allCodes []string
	if len(codes) > 0 {
		allCodes = normalizeTSCodes(codes)
	} else {
		allCodes, err = collectAllStockTSCodes()
		if err != nil {
			merged := mergeSummary(baseSummary, map[string]any{"error": err.Error(), "phase": "collect_codes"})
			_ = markJobFinished(ctx, jobID, "failed", merged)
			return err
		}
	}
	if len(allCodes) == 0 {
		merged := mergeSummary(baseSummary, map[string]any{"error": "no codes", "phase": "collect_codes"})
		_ = markJobFinished(ctx, jobID, "failed", merged)
		return fmt.Errorf("no codes to process")
	}

	sort.Strings(allCodes)
	total := len(allCodes)

	mu := &sync.Mutex{}
	var successCodes int64
	var failedCodes int64
	var insertedRows int64
	results := make([]symbolResult, 0, total)

	mu.Lock()
	baseSummary["total_codes"] = total
	baseSummary["success_codes"] = successCodes
	baseSummary["failed_codes"] = failedCodes
	baseSummary["inserted_rows"] = insertedRows
	_ = saveJobSummary(ctx, jobID, baseSummary)
	mu.Unlock()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	errCh := make(chan error, total)

	for _, tsCode := range allCodes {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(code string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			localInserted, e := ingestDailyQfqSingleSymbol(ctx, code, opt)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				failedCodes++
				results = append(results, symbolResult{
					TsCode: code,
					Status: "failed",
					Error:  e.Error(),
				})
				errCh <- e
			} else {
				if localInserted > 0 {
					results = append(results, symbolResult{
						TsCode:       code,
						Status:       "success",
						InsertedRows: localInserted,
					})
				}
				if localInserted > 0 {
					insertedRows += int64(localInserted)
				}
				successCodes++
			}
			baseSummary["total_codes"] = total
			baseSummary["success_codes"] = successCodes
			baseSummary["failed_codes"] = failedCodes
			baseSummary["inserted_rows"] = insertedRows
			_ = saveJobSummary(ctx, jobID, baseSummary)
		}(tsCode)
	}

	wg.Wait()
	close(errCh)

	status := "success"
	if ctx.Err() == context.Canceled {
		status = "cancelled"
	} else if failedCodes > 0 {
		status = "failed"
	}

	payload := map[string]any{
		"summary": map[string]any{
			"dataset":       baseSummary["dataset"],
			"datasets":      baseSummary["datasets"],
			"mode":          "init",
			"total_codes":   total,
			"success_codes": successCodes,
			"failed_codes":  failedCodes,
			"inserted_rows": insertedRows,
		},
		"symbols": results,
	}
	level := "INFO"
	if status == "failed" {
		level = "ERROR"
	}
	if err := insertIngestionLog(ctx, jobID, level, payload); err != nil {
		log.Printf("insert ingestion log failed: %v", err)
	}

	if err := markJobFinished(ctx, jobID, status, baseSummary); err != nil {
		return err
	}

	if status == "failed" {
		for e := range errCh {
			if e != nil {
				return e
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// runDailyRawInitTask 未复权日线初始化：
// - dataset 固定为 kline_daily_raw
// - 使用 TDX GetKlineDayAll 拉取该标的上市以来全部日线
// - 由本地 COPY 写入 TimescaleDB。
func runDailyRawInitTask(ctx context.Context, jobID uuid.UUID, codes []string, start time.Time, workers int, opt MinuteInitOptions) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}

	baseSummary, err := loadJobSummary(ctx, jobID)
	if err != nil {
		return err
	}
	if baseSummary == nil {
		baseSummary = map[string]any{}
	}
	if _, ok := baseSummary["dataset"]; !ok {
		baseSummary["dataset"] = "kline_daily_raw"
	}
	if _, ok := baseSummary["datasets"]; !ok {
		baseSummary["datasets"] = []string{"kline_daily_raw"}
	}

	if err := markJobStarted(ctx, jobID, baseSummary); err != nil {
		return err
	}

	if opt.TruncateBefore {
		if err := truncateDailyRaw(ctx); err != nil {
			merged := mergeSummary(baseSummary, map[string]any{"error": err.Error(), "phase": "truncate"})
			_ = markJobFinished(ctx, jobID, "failed", merged)
			return err
		}
	}

	var allCodes []string
	if len(codes) > 0 {
		allCodes = normalizeTSCodes(codes)
	} else {
		allCodes, err = collectAllStockTSCodes()
		if err != nil {
			merged := mergeSummary(baseSummary, map[string]any{"error": err.Error(), "phase": "collect_codes"})
			_ = markJobFinished(ctx, jobID, "failed", merged)
			return err
		}
	}
	if len(allCodes) == 0 {
		merged := mergeSummary(baseSummary, map[string]any{"error": "no codes", "phase": "collect_codes"})
		_ = markJobFinished(ctx, jobID, "failed", merged)
		return fmt.Errorf("no codes to process")
	}

	sort.Strings(allCodes)
	total := len(allCodes)

	mu := &sync.Mutex{}
	var successCodes int64
	var failedCodes int64
	var insertedRows int64
	results := make([]symbolResult, 0, total)

	mu.Lock()
	baseSummary["total_codes"] = total
	baseSummary["success_codes"] = successCodes
	baseSummary["failed_codes"] = failedCodes
	baseSummary["inserted_rows"] = insertedRows
	_ = saveJobSummary(ctx, jobID, baseSummary)
	mu.Unlock()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	errCh := make(chan error, total)

	for _, tsCode := range allCodes {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(code string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			localInserted, e := ingestDailyRawSingleSymbol(ctx, code, start, opt)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				failedCodes++
				results = append(results, symbolResult{
					TsCode: code,
					Status: "failed",
					Error:  e.Error(),
				})
				errCh <- e
			} else {
				if localInserted > 0 {
					results = append(results, symbolResult{
						TsCode:       code,
						Status:       "success",
						InsertedRows: localInserted,
					})
				}
				if localInserted > 0 {
					insertedRows += int64(localInserted)
				}
				successCodes++
			}
			baseSummary["total_codes"] = total
			baseSummary["success_codes"] = successCodes
			baseSummary["failed_codes"] = failedCodes
			baseSummary["inserted_rows"] = insertedRows
			_ = saveJobSummary(ctx, jobID, baseSummary)
		}(tsCode)
	}

	wg.Wait()
	close(errCh)

	status := "success"
	if ctx.Err() == context.Canceled {
		status = "cancelled"
	} else if failedCodes > 0 {
		status = "failed"
	}

	payload := map[string]any{
		"summary": map[string]any{
			"dataset":       baseSummary["dataset"],
			"datasets":      baseSummary["datasets"],
			"mode":          "init",
			"total_codes":   total,
			"success_codes": successCodes,
			"failed_codes":  failedCodes,
			"inserted_rows": insertedRows,
		},
		"symbols": results,
	}
	level := "INFO"
	if status == "failed" {
		level = "ERROR"
	}
	if err := insertIngestionLog(ctx, jobID, level, payload); err != nil {
		log.Printf("insert ingestion log failed: %v", err)
	}

	if err := markJobFinished(ctx, jobID, status, baseSummary); err != nil {
		return err
	}

	if status == "failed" {
		for e := range errCh {
			if e != nil {
				return e
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func loadJobSummary(ctx context.Context, jobID uuid.UUID) (map[string]any, error) {
	if pgPool == nil {
		return nil, errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	var raw []byte
	err = conn.QueryRow(ctx, "SELECT summary FROM market.ingestion_jobs WHERE job_id=$1", jobID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]any{}, nil
	}
	return m, nil
}

func saveJobSummary(ctx context.Context, jobID uuid.UUID, summary map[string]any) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	b, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "UPDATE market.ingestion_jobs SET summary=$1 WHERE job_id=$2", b, jobID)
	return err
}

func markJobStarted(ctx context.Context, jobID uuid.UUID, summary map[string]any) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	b, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "UPDATE market.ingestion_jobs SET status='running', started_at=NOW(), summary=$1 WHERE job_id=$2", b, jobID)
	return err
}

func markJobFinished(ctx context.Context, jobID uuid.UUID, status string, summary map[string]any) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	b, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "UPDATE market.ingestion_jobs SET status=$1, finished_at=NOW(), summary=$2 WHERE job_id=$3", status, b, jobID)
	return err
}

func truncateMinuteRaw(ctx context.Context) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "TRUNCATE TABLE market.kline_minute_raw")
	return err
}

func truncateDailyRaw(ctx context.Context) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "TRUNCATE TABLE market.kline_daily_raw")
	return err
}

func truncateDailyQfq(ctx context.Context) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "TRUNCATE TABLE market.kline_daily_qfq")
	return err
}

func normalizeTSCodes(codes []string) []string {
	result := make([]string, 0, len(codes))
	seen := map[string]struct{}{}
	for _, c := range codes {
		code := strings.TrimSpace(c)
		if code == "" {
			continue
		}
		code = strings.ToUpper(code)
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		result = append(result, code)
	}
	return result
}

func collectAllStockTSCodes() ([]string, error) {
	models, err := getAllCodeModels()
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(models))
	seen := map[string]struct{}{}
	for _, m := range models {
		full := m.FullCode()
		if !protocol.IsStock(full) {
			continue
		}
		ex := strings.ToUpper(m.Exchange)
		if ex != "SH" && ex != "SZ" && ex != "BJ" {
			continue
		}
		code := fmt.Sprintf("%s.%s", m.Code, ex)
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		result = append(result, code)
	}
	return result, nil
}

func ingestSingleSymbol(ctx context.Context, tsCode string, start time.Time, opt MinuteInitOptions) (int, error) {
	base := tsCode
	if idx := strings.Index(tsCode, "."); idx > 0 {
		base = tsCode[:idx]
	}
	resp, err := client.GetKlineMinuteAll(base)
	if err != nil {
		return 0, fmt.Errorf("获取分钟K线失败: %w", err)
	}
	if resp == nil || len(resp.List) == 0 {
		return 0, nil
	}

	source := opt.Source
	if source == "" {
		source = "tdx_api"
	}
	if len(source) > 16 {
		source = source[:16]
	}

	rows := make([][]any, 0, len(resp.List))
	for _, k := range resp.List {
		if k == nil {
			continue
		}
		// 当 TruncateBefore=false 时，start 作为增量阈值：仅保留 >= start 的分钟线；
		// 当 TruncateBefore=true 时，仍保留全量行为，不按 start 过滤。
		if !opt.TruncateBefore && !k.Time.IsZero() && k.Time.Before(start) {
			continue
		}
		rows = append(rows, []any{
			k.Time,
			tsCode,
			"1m",
			int64(k.Open),
			int64(k.High),
			int64(k.Low),
			int64(k.Close),
			k.Volume,
			int64(k.Amount),
			"none",
			source,
		})
	}
	if len(rows) == 0 {
		return 0, nil
	}

	if pgPool == nil {
		return 0, errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	table := pgx.Identifier{"market", "kline_minute_raw"}
	columns := []string{"trade_time", "ts_code", "freq", "open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li", "adjust_type", "source"}

	maxRows := opt.MaxRowsPerChunk
	if maxRows <= 0 {
		maxRows = 500000
	}

	inserted := 0
	for offset := 0; offset < len(rows); offset += maxRows {
		endIdx := offset + maxRows
		if endIdx > len(rows) {
			endIdx = len(rows)
		}
		chunk := rows[offset:endIdx]
		n, err := tx.CopyFrom(ctx, table, columns, pgx.CopyFromRows(chunk))
		if err != nil {
			return 0, err
		}
		inserted += int(n)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return inserted, nil
}

// ingestDailyQfqSingleSymbol 使用前复权日线数据填充 kline_daily_qfq。
// 数据来源为 getQfqKlineDay（通过 THS 前复权接口获得全量 QFQ 日线）。
func ingestDailyQfqSingleSymbol(ctx context.Context, tsCode string, opt MinuteInitOptions) (int, error) {
	base := tsCode
	if idx := strings.Index(tsCode, "."); idx > 0 {
		base = tsCode[:idx]
	}
	resp, err := getQfqKlineDay(base)
	if err != nil {
		return 0, fmt.Errorf("获取前复权日线K线失败: %w", err)
	}
	if resp == nil || len(resp.List) == 0 {
		return 0, nil
	}

	source := opt.Source
	if source == "" {
		source = "tdx_api"
	}
	if len(source) > 16 {
		source = source[:16]
	}

	rows := make([][]any, 0, len(resp.List))
	for _, k := range resp.List {
		if k == nil {
			continue
		}
		rows = append(rows, []any{
			k.Time,
			tsCode,
			int64(k.Open),
			int64(k.High),
			int64(k.Low),
			int64(k.Close),
			k.Volume,
			int64(k.Amount),
			"qfq",
			source,
		})
	}
	if len(rows) == 0 {
		return 0, nil
	}

	if pgPool == nil {
		return 0, errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	table := pgx.Identifier{"market", "kline_daily_qfq"}
	columns := []string{"trade_date", "ts_code", "open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li", "adjust_type", "source"}

	maxRows := opt.MaxRowsPerChunk
	if maxRows <= 0 {
		maxRows = 500000
	}

	inserted := 0
	for offset := 0; offset < len(rows); offset += maxRows {
		endIdx := offset + maxRows
		if endIdx > len(rows) {
			endIdx = len(rows)
		}
		chunk := rows[offset:endIdx]
		n, err := tx.CopyFrom(ctx, table, columns, pgx.CopyFromRows(chunk))
		if err != nil {
			return 0, err
		}
		inserted += int(n)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return inserted, nil
}

// ingestDailyRawSingleSymbol 使用 TDX 日线未复权数据填充 kline_daily_raw。
func ingestDailyRawSingleSymbol(ctx context.Context, tsCode string, start time.Time, opt MinuteInitOptions) (int, error) {
	base := tsCode
	if idx := strings.Index(tsCode, "."); idx > 0 {
		base = tsCode[:idx]
	}
	resp, err := client.GetKlineDayAll(base)
	if err != nil {
		return 0, fmt.Errorf("获取日线K线失败: %w", err)
	}
	if resp == nil || len(resp.List) == 0 {
		return 0, nil
	}

	source := opt.Source
	if source == "" {
		source = "tdx_api"
	}
	if len(source) > 16 {
		source = source[:16]
	}

	rows := make([][]any, 0, len(resp.List))
	for _, k := range resp.List {
		if k == nil {
			continue
		}
		// 当 TruncateBefore=false 时，start 作为增量阈值：仅保留 >= start 的日线；
		// 当 TruncateBefore=true 时，仍保留全量行为，不按 start 过滤。
		if !opt.TruncateBefore && !k.Time.IsZero() && k.Time.Before(start) {
			continue
		}
		rows = append(rows, []any{
			k.Time,
			tsCode,
			int64(k.Open),
			int64(k.High),
			int64(k.Low),
			int64(k.Close),
			k.Volume,
			int64(k.Amount),
			"none",
			source,
		})
	}
	if len(rows) == 0 {
		return 0, nil
	}

	if pgPool == nil {
		return 0, errors.New("database not configured")
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	table := pgx.Identifier{"market", "kline_daily_raw"}
	columns := []string{"trade_date", "ts_code", "open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li", "adjust_type", "source"}

	maxRows := opt.MaxRowsPerChunk
	if maxRows <= 0 {
		maxRows = 500000
	}

	inserted := 0
	for offset := 0; offset < len(rows); offset += maxRows {
		endIdx := offset + maxRows
		if endIdx > len(rows) {
			endIdx = len(rows)
		}
		chunk := rows[offset:endIdx]
		n, err := tx.CopyFrom(ctx, table, columns, pgx.CopyFromRows(chunk))
		if err != nil {
			return 0, err
		}
		inserted += int(n)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return inserted, nil
}

func insertIngestionLog(ctx context.Context, jobID uuid.UUID, level string, payload map[string]any) error {
	if pgPool == nil {
		return errors.New("database not configured")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES ($1, NOW(), $2, $3)", jobID, strings.ToUpper(level), b)
	return err
}

func mergeSummary(base map[string]any, patch map[string]any) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	for k, v := range patch {
		base[k] = v
	}
	return base
}

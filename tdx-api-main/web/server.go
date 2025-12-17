package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"time"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/extend"
	"github.com/injoyai/tdx/protocol"
)

var (
	client      *tdx.Client
	manager     *tdx.Manage
	taskManager = NewTaskManager()
)

func init() {
	var err error
	// 杩炴帴閫氳揪淇℃湇鍔″櫒
	client, err = tdx.DialDefault(tdx.WithDebug(false))
	if err != nil {
		log.Fatalf("杩炴帴鏈嶅姟鍣ㄥけ璐? %v", err)
	}
	log.Println("鎴愬姛杩炴帴鍒伴€氳揪淇℃湇鍔″櫒")

	// 鍒濆鍖栦唬鐮佺紦瀛?
	if err = os.MkdirAll(tdx.DefaultDatabaseDir, 0755); err != nil {
		log.Printf("鍒涘缓鏁版嵁鐩綍澶辫触: %v", err)
	}
	if codes, err := tdx.NewCodesSqlite(client); err != nil {
		log.Printf("鍒濆鍖栦唬鐮佸簱澶辫触: %v", err)
	} else {
		tdx.DefaultCodes = codes
		if err := tdx.DefaultCodes.Update(); err != nil {
			log.Printf("鏇存柊浠ｇ爜搴撳け璐? %v", err)
		} else {
			log.Printf("已加载股票代码，共%d条", len(tdx.DefaultCodes.Map))
		}
	}

	manager, err = tdx.NewManage(&tdx.ManageConfig{
		Number: 4,
	})
	if err != nil {
		log.Fatalf("鍒濆鍖栨暟鎹鐞嗗櫒澶辫触: %v", err)
	}
	if err := manager.Codes.Update(); err != nil {
		log.Printf("鏇存柊绠＄悊鍣ㄤ唬鐮佸簱澶辫触: %v", err)
	}
	if err := manager.Workday.Update(); err != nil {
		log.Printf("鏇存柊浜ゆ槗鏃ユ暟鎹け璐? %v", err)
	}
	manager.Cron.Start()
}

// Response 缁熶竴鍝嶅簲缁撴瀯
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// 杩斿洖鎴愬姛鍝嶅簲
func successResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(Response{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

// 杩斿洖閿欒鍝嶅簲
func errorResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(Response{
		Code:    -1,
		Message: message,
		Data:    nil,
	})
}

// 鑾峰彇浜旀。琛屾儏
func handleGetQuote(w http.ResponseWriter, r *http.Request) {
	codeParam := r.URL.Query().Get("code")
	if codeParam == "" {
		errorResponse(w, "鑲＄エ浠ｇ爜涓嶈兘涓虹┖")
		return
	}

	codes := splitCodes(codeParam)
	if len(codes) == 0 {
		errorResponse(w, "鑲＄エ浠ｇ爜涓嶈兘涓虹┖")
		return
	}

	quotes, err := client.GetQuote(codes...)
	if err != nil {
		errorResponse(w, fmt.Sprintf("鑾峰彇琛屾儏澶辫触: %v", err))
		return
	}

	successResponse(w, quotes)
}

// 获取K线数据（日线默认使用前复权）
func handleGetKline(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	klineType := r.URL.Query().Get("type") // minute1/minute5/minute15/minute30/hour/day/week/month
	if code == "" {
		errorResponse(w, "鑲＄エ浠ｇ爜涓嶈兘涓虹┖")
		return
	}

	var resp *protocol.KlineResp
	var err error

	switch klineType {
	case "minute1":
		// 鍒嗛挓K绾夸笉闇€瑕佸鏉?		resp, err = client.GetKlineMinuteAll(code)
	case "minute5":
		resp, err = client.GetKline5MinuteAll(code)
	case "minute15":
		resp, err = client.GetKline15MinuteAll(code)
	case "minute30":
		resp, err = client.GetKline30MinuteAll(code)
	case "hour":
		resp, err = client.GetKlineHourAll(code)
	case "week":
		// 鍛↘绾夸娇鐢ㄥ墠澶嶆潈锛堜粠鏃绾胯浆鎹級
		resp, err = getQfqKlineDay(code)
		if err == nil && len(resp.List) > 0 {
			// 灏嗘棩K绾胯浆鎹负鍛↘绾匡紙绠€鍖栫増锛氭瘡5涓氦鏄撴棩鍚堝苟锛?			resp = convertToWeekKline(resp)
		}
	case "month":
		// 鏈圞绾夸娇鐢ㄥ墠澶嶆潈锛堜粠鏃绾胯浆鎹級
		resp, err = getQfqKlineDay(code)
		if err == nil && len(resp.List) > 0 {
			// 灏嗘棩K绾胯浆鎹负鏈圞绾?			resp = convertToMonthKline(resp)
		}
	case "day":
		fallthrough
	default:
		// 鏃绾夸娇鐢ㄥ墠澶嶆潈鏁版嵁
		resp, err = getQfqKlineDay(code)
	}

	if err != nil {
		errorResponse(w, fmt.Sprintf("鑾峰彇K绾垮け璐? %v", err))
		return
	}

	successResponse(w, resp)
}

// getQfqKlineDay 鑾峰彇鍓嶅鏉冩棩K绾挎暟鎹?
func getQfqKlineDay(code string) (*protocol.KlineResp, error) {
	// 浣跨敤鍚岃姳椤篈PI鑾峰彇鍓嶅鏉冩暟鎹?
	klines, err := extend.GetTHSDayKline(code, extend.THS_QFQ)
	if err != nil {
		return nil, fmt.Errorf("鑾峰彇鍓嶅鏉冩暟鎹け璐? %w", err)
	}

	if len(klines) == 0 {
		return nil, fmt.Errorf("鍚岃姳椤哄墠澶嶆潈鏁版嵁涓虹┖")
	}

	// 杞崲涓?protocol.KlineResp 鏍煎紡
	resp := &protocol.KlineResp{
		Count: uint16(len(klines)),
		List:  make([]*protocol.Kline, 0, len(klines)),
	}

	for i, k := range klines {
		pk := &protocol.Kline{
			Time:   time.Unix(k.Date, 0),
			Open:   k.Open,
			High:   k.High,
			Low:    k.Low,
			Close:  k.Close,
			Volume: k.Volume,
			Amount: k.Amount,
		}
		// 璁剧疆鏄ㄦ敹浠凤紙浣跨敤涓婁竴鏉绾跨殑鏀剁洏浠凤級
		if i > 0 {
			pk.Last = klines[i-1].Close
		}
		resp.List = append(resp.List, pk)
	}

	return resp, nil
}

// convertToWeekKline 灏嗘棩K绾胯浆鎹负鍛↘绾匡紙绠€鍖栫増锛?
func convertToWeekKline(dayKline *protocol.KlineResp) *protocol.KlineResp {
	if len(dayKline.List) == 0 {
		return dayKline
	}

	weekResp := &protocol.KlineResp{
		List: make([]*protocol.Kline, 0),
	}

	var currentWeek *protocol.Kline
	var lastWeekDay time.Time

	for _, k := range dayKline.List {
		year, week := k.Time.ISOWeek()

		// 鍒ゆ柇鏄惁鏄柊鐨勪竴鍛?
		if currentWeek == nil || lastWeekDay.Year() != year || getISOWeek(lastWeekDay) != week {
			// 淇濆瓨涓婁竴鍛ㄧ殑鏁版嵁
			if currentWeek != nil {
				weekResp.List = append(weekResp.List, currentWeek)
			}
			// 鍒涘缓鏂板懆
			currentWeek = &protocol.Kline{
				Time:   k.Time,
				Last:   k.Last,
				Open:   k.Open,
				High:   k.High,
				Low:    k.Low,
				Close:  k.Close,
				Volume: k.Volume,
				Amount: k.Amount,
			}
		} else {
			// 绱Н褰撳懆鏁版嵁
			if k.High > currentWeek.High {
				currentWeek.High = k.High
			}
			if k.Low < currentWeek.Low || currentWeek.Low == 0 {
				currentWeek.Low = k.Low
			}
			currentWeek.Close = k.Close
			currentWeek.Volume += k.Volume
			currentWeek.Amount += k.Amount
			currentWeek.Time = k.Time // 浣跨敤鏈€鍚庝竴澶╃殑鏃堕棿
		}
		lastWeekDay = k.Time
	}

	// 娣诲姞鏈€鍚庝竴鍛?
	if currentWeek != nil {
		weekResp.List = append(weekResp.List, currentWeek)
	}

	weekResp.Count = uint16(len(weekResp.List))
	return weekResp
}

// convertToMonthKline 灏嗘棩K绾胯浆鎹负鏈圞绾?
func convertToMonthKline(dayKline *protocol.KlineResp) *protocol.KlineResp {
	if len(dayKline.List) == 0 {
		return dayKline
	}

	monthResp := &protocol.KlineResp{
		List: make([]*protocol.Kline, 0),
	}

	var currentMonth *protocol.Kline
	var lastMonthKey string

	for _, k := range dayKline.List {
		monthKey := k.Time.Format("200601") // YYYYMM

		// 鍒ゆ柇鏄惁鏄柊鐨勪竴鏈?
		if currentMonth == nil || lastMonthKey != monthKey {
			// 淇濆瓨涓婁竴鏈堢殑鏁版嵁
			if currentMonth != nil {
				monthResp.List = append(monthResp.List, currentMonth)
			}
			// 鍒涘缓鏂版湀
			currentMonth = &protocol.Kline{
				Time:   k.Time,
				Last:   k.Last,
				Open:   k.Open,
				High:   k.High,
				Low:    k.Low,
				Close:  k.Close,
				Volume: k.Volume,
				Amount: k.Amount,
			}
		} else {
			// 绱Н褰撴湀鏁版嵁
			if k.High > currentMonth.High {
				currentMonth.High = k.High
			}
			if k.Low < currentMonth.Low || currentMonth.Low == 0 {
				currentMonth.Low = k.Low
			}
			currentMonth.Close = k.Close
			currentMonth.Volume += k.Volume
			currentMonth.Amount += k.Amount
			currentMonth.Time = k.Time // 浣跨敤鏈€鍚庝竴澶╃殑鏃堕棿
		}
		lastMonthKey = monthKey
	}

	// 娣诲姞鏈€鍚庝竴鏈?
	if currentMonth != nil {
		monthResp.List = append(monthResp.List, currentMonth)
	}

	monthResp.Count = uint16(len(monthResp.List))
	return monthResp
}

// getISOWeek 鑾峰彇ISO鍛ㄦ暟
func getISOWeek(t time.Time) int {
	_, week := t.ISOWeek()
	return week
}

// 鑾峰彇鍒嗘椂鏁版嵁
func handleGetMinute(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	date := r.URL.Query().Get("date")
	if code == "" {
		errorResponse(w, "鑲＄エ浠ｇ爜涓嶈兘涓虹┖")
		return
	}

	resp, usedDate, err := getMinuteWithFallback(code, date)
	if err != nil {
		errorResponse(w, fmt.Sprintf("鑾峰彇鍒嗘椂鏁版嵁澶辫触: %v", err))
		return
	}

	if resp == nil {
		successResponse(w, map[string]interface{}{
			"date":  usedDate,
			"Count": 0,
			"List":  []interface{}{},
		})
		return
	}

	successResponse(w, map[string]interface{}{
		"date":  usedDate,
		"Count": resp.Count,
		"List":  resp.List,
	})
}

// 鑾峰彇鍒嗘椂鎴愪氦
func handleGetTrade(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	date := r.URL.Query().Get("date")
	if code == "" {
		errorResponse(w, "鑲＄エ浠ｇ爜涓嶈兘涓虹┖")
		return
	}

	var resp *protocol.TradeResp
	var err error

	if date == "" {
		// 鑾峰彇浠婃棩鍒嗘椂鎴愪氦锛堟渶杩?800鏉★級
		resp, err = client.GetMinuteTrade(code, 0, 1800)
	} else {
		// 鑾峰彇鍘嗗彶鏌愬ぉ鐨勫垎鏃舵垚浜?		resp, err = client.GetHistoryMinuteTradeDay(date, code)
	}

	if err != nil {
		errorResponse(w, fmt.Sprintf("鑾峰彇鍒嗘椂鎴愪氦澶辫触: %v", err))
		return
	}

	successResponse(w, resp)
}

// 鎼滅储鑲＄エ浠ｇ爜
func handleSearchCode(w http.ResponseWriter, r *http.Request) {
	keyword := r.URL.Query().Get("keyword")
	if keyword == "" {
		errorResponse(w, "搜索关键词不能为空")
		return
	}

	keywordUpper := strings.ToUpper(keyword)
	results := []map[string]string{}
	seen := map[string]struct{}{}

	codeModels, err := getAllCodeModels()
	if err != nil {
		errorResponse(w, "搜索失败: "+err.Error())
		return
	}

	for _, model := range codeModels {
		fullCode := model.FullCode()
		if !protocol.IsStock(fullCode) {
			continue
		}
		if _, ok := seen[model.Code]; ok {
			continue
		}

		codeUpper := strings.ToUpper(model.Code)
		nameUpper := strings.ToUpper(model.Name)
		if strings.Contains(codeUpper, keywordUpper) || strings.Contains(nameUpper, keywordUpper) {
			results = append(results, map[string]string{
				"code":     model.Code,
				"name":     model.Name,
				"exchange": strings.ToLower(model.Exchange),
			})
			seen[model.Code] = struct{}{}
		}

		if len(results) >= 50 {
			break
		}
	}

	successResponse(w, results)
}

// 鑾峰彇鑲＄エ鍩烘湰淇℃伅锛堟暣鍚堝涓帴鍙ｏ級
func handleGetStockInfo(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	if code == "" {
		errorResponse(w, "鑲＄エ浠ｇ爜涓嶈兘涓虹┖")
		return
	}

	// 鏁村悎澶氫釜鏁版嵁婧?
	result := make(map[string]interface{})

	// 1. 鑾峰彇浜旀。琛屾儏
	quotes, err := client.GetQuote(code)
	if err == nil && len(quotes) > 0 {
		result["quote"] = quotes[0]
	}

	// 2. 鑾峰彇鏈€杩?0澶╃殑鏃绾匡紙浣跨敤鍓嶅鏉冿級
	kline, err := getQfqKlineDay(code)
	if err == nil && len(kline.List) > 30 {
		// 鍙繑鍥炴渶杩?0鏉?		kline.List = kline.List[len(kline.List)-30:]
		kline.Count = 30
	}
	if err == nil {
		result["kline_day"] = kline
	}

	// 3. 鑾峰彇浠婃棩鍒嗘椂鏁版嵁
	minute, minuteDate, err := getMinuteWithFallback(code, "")
	if err == nil && minute != nil {
		result["minute"] = map[string]interface{}{
			"date":  minuteDate,
			"Count": minute.Count,
			"List":  minute.List,
		}
	}

	successResponse(w, result)
}

func handleCreatePullKlineTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "鍙敮鎸丳OST璇锋眰")
		return
	}
	if manager == nil {
		errorResponse(w, "鏁版嵁绠＄悊鍣ㄦ湭鍒濆鍖?")
		return
	}

	var req struct {
		Codes     []string `json:"codes"`
		Tables    []string `json:"tables"`
		Dir       string   `json:"dir"`
		Limit     int      `json:"limit"`
		StartDate string   `json:"start_date"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "璇锋眰鍙傛暟閿欒: "+err.Error())
		return
	}

	tables := req.Tables
	if len(tables) == 0 {
		tables = []string{extend.Day}
	} else {
		valid := make([]string, 0, len(tables))
		for _, v := range tables {
			if _, ok := extend.KlineTableMap[v]; ok {
				valid = append(valid, v)
			}
		}
		if len(valid) == 0 {
			errorResponse(w, "tables鍙傛暟鏃犳晥")
			return
		}
		tables = valid
	}

	dir := req.Dir
	if dir == "" {
		dir = filepath.Join(tdx.DefaultDatabaseDir, "kline")
	}

	startAt := time.Unix(0, 0)
	if req.StartDate != "" {
		var parsed bool
		for _, layout := range []string{"2006-01-02", "20060102"} {
			if t, err := time.ParseInLocation(layout, req.StartDate, time.Local); err == nil {
				startAt = t
				parsed = true
				break
			}
		}
		if !parsed {
			errorResponse(w, "start_date鏍煎紡閿欒锛屽簲涓篩YYY-MM-DD鎴朰YYYMMDD")
			return
		}
	}

	cfg := extend.PullKlineConfig{
		Codes:   req.Codes,
		Tables:  tables,
		Dir:     dir,
		Limit:   req.Limit,
		StartAt: startAt,
	}

	puller := extend.NewPullKline(cfg)

	taskID := taskManager.Run("pull_kline", func(ctx context.Context) error {
		return puller.Run(ctx, manager)
	})

	successResponse(w, map[string]string{
		"task_id": taskID,
	})
}

func handleCreatePullTradeTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "鍙敮鎸丳OST璇锋眰")
		return
	}
	if manager == nil {
		errorResponse(w, "鏁版嵁绠＄悊鍣ㄦ湭鍒濆鍖?")
		return
	}

	var req struct {
		Code      string `json:"code"`
		Dir       string `json:"dir"`
		StartYear int    `json:"start_year"`
		EndYear   int    `json:"end_year"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "璇锋眰鍙傛暟閿欒: "+err.Error())
		return
	}

	if req.Code == "" {
		errorResponse(w, "code涓嶈兘涓虹┖")
		return
	}

	dir := req.Dir
	if dir == "" {
		dir = filepath.Join(tdx.DefaultDatabaseDir, "trade")
	}

	puller := extend.NewPullTrade(dir)
	puller.StartYear = req.StartYear
	puller.EndYear = req.EndYear

	taskID := taskManager.Run("pull_trade", func(ctx context.Context) error {
		return puller.Pull(ctx, manager, req.Code)
	})

	successResponse(w, map[string]string{
		"task_id": taskID,
	})
}

func handleListTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, "鍙敮鎸丟ET璇锋眰")
		return
	}

	tasks := taskManager.List()
	successResponse(w, tasks)
}

func handleTaskOperations(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}

	parts := strings.Split(path, "/")
	id := parts[0]

	if len(parts) == 2 && parts[1] == "cancel" {
		if r.Method != http.MethodPost {
			errorResponse(w, "鍙栨秷浠诲姟浠呮敮鎸丳OST")
			return
		}
		if ok := taskManager.Cancel(id); !ok {
			errorResponse(w, "浠诲姟涓嶅瓨鍦ㄦ垨宸茬粨鏉?")
			return
		}
		successResponse(w, map[string]string{
			"task_id": id,
			"status":  string(TaskStatusCancelled),
		})
		return
	}

	if r.Method != http.MethodGet {
		errorResponse(w, "鍙敮鎸丟ET璇锋眰")
		return
	}

	if task, ok := taskManager.Get(id); ok {
		successResponse(w, task)
		return
	}

	errorResponse(w, "浠诲姟涓嶅瓨鍦?")
}

func splitCodes(param string) []string {
	parts := strings.Split(param, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		code := strings.TrimSpace(p)
		if code != "" {
			result = append(result, code)
		}
	}
	return result
}

func getMinuteWithFallback(code, date string) (*protocol.MinuteResp, string, error) {
	target := strings.TrimSpace(date)
	if target == "" {
		target = time.Now().Format("20060102")
		resp, err := client.GetMinute(code)
		return resp, target, err
	}

	resp, err := client.GetHistoryMinute(target, code)
	return resp, target, err
}

func main() {
	// 闈欐€佹枃浠舵湇鍔?
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// API璺敱
	http.HandleFunc("/api/quote", handleGetQuote)
	http.HandleFunc("/api/kline", handleGetKline)
	http.HandleFunc("/api/minute", handleGetMinute)
	http.HandleFunc("/api/trade", handleGetTrade)
	http.HandleFunc("/api/search", handleSearchCode)
	http.HandleFunc("/api/stock-info", handleGetStockInfo)
	http.HandleFunc("/api/codes", handleGetCodes)
	http.HandleFunc("/api/batch-quote", handleBatchQuote)
	http.HandleFunc("/api/kline-history", handleGetKlineHistory)
	http.HandleFunc("/api/index", handleGetIndex)
	http.HandleFunc("/api/index/all", handleGetIndexAll)
	http.HandleFunc("/api/market-stats", handleGetMarketStats)
	http.HandleFunc("/api/market-count", handleGetMarketCount)
	http.HandleFunc("/api/stock-codes", handleGetStockCodes)
	http.HandleFunc("/api/etf-codes", handleGetETFCodes)
	http.HandleFunc("/api/server-status", handleGetServerStatus)
	http.HandleFunc("/api/health", handleHealthCheck)
	http.HandleFunc("/api/etf", handleGetETFList)
	http.HandleFunc("/api/trade-history", handleGetTradeHistory)
	http.HandleFunc("/api/trade-history/full", handleGetTradeHistoryFull)
	http.HandleFunc("/api/minute-trade-all", handleGetMinuteTradeAll)
	http.HandleFunc("/api/kline-all", handleGetKlineAllTDX)
	http.HandleFunc("/api/kline-all/tdx", handleGetKlineAllTDX)
	http.HandleFunc("/api/kline-all/ths", handleGetKlineAllTHS)
	http.HandleFunc("/api/workday", handleGetWorkday)
	http.HandleFunc("/api/workday/range", handleGetWorkdayRange)
	http.HandleFunc("/api/income", handleGetIncome)
	http.HandleFunc("/api/tasks/pull-kline", handleCreatePullKlineTask)
	http.HandleFunc("/api/tasks/pull-trade", handleCreatePullTradeTask)
	http.HandleFunc("/api/tasks/ingest-minute-raw-init", handleCreateMinuteRawInitTask)
	http.HandleFunc("/api/tasks/ingest-daily-raw-init", handleCreateDailyRawInitTask)
	http.HandleFunc("/api/tasks/ingest-daily-qfq-init", handleCreateDailyQfqInitTask)
	http.HandleFunc("/api/tasks", handleListTasks)
	http.HandleFunc("/api/tasks/", handleTaskOperations)

	port := os.Getenv("TDX_HTTP_PORT")
	port = strings.TrimSpace(port)
	if port == "" {
		port = "8080"
	} else {
		// allow values like "tcp/19080", ":19080", "http://localhost:19080"
		port = strings.TrimPrefix(port, "tcp/")
		port = strings.TrimPrefix(port, ":")
		if i := strings.LastIndex(port, ":"); i >= 0 {
			port = port[i+1:]
		}
		// keep digits only
		b := strings.Builder{}
		for _, r := range port {
			if unicode.IsDigit(r) {
				b.WriteRune(r)
			}
		}
		port = b.String()
		if port == "" {
			port = "8080"
		}
	}
	addr := ":" + port
	log.Printf("鏈嶅姟鍚姩鎴愬姛锛岃闂?http://localhost:%s\n", port)
	log.Fatal(http.ListenAndServe(addr, nil))
}

# server.go 文件修复说明

## 问题总结

1. **文件编码问题**：文件包含大量NUL字符（16166个），导致Go编译器无法读取
2. **字符串未闭合**：第44行、376行、463行等
3. **注释和代码在同一行**：第34行、113行、164行、165行、215行、270行等

## 修复步骤

### 1. 清理NUL字符
```powershell
cd web
$bytes = [System.IO.File]::ReadAllBytes("server.go")
$cleanBytes = $bytes | Where-Object { $_ -ne 0 }
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
$text = $utf8NoBom.GetString($cleanBytes)
[System.IO.File]::WriteAllText("server.go", $text, $utf8NoBom)
```

### 2. 修复关键语法错误

#### 第44行：字符串未闭合
```go
// 修复前：
log.Printf("宸插姞杞借偂绁ㄤ唬鐮侊紝鍏?%d 鏉?, len(tdx.DefaultCodes.Map))

// 修复后：
log.Printf("已加载股票代码，共%d条", len(tdx.DefaultCodes.Map))
```

#### 第113行：注释和函数定义在同一行
```go
// 修复前：
// 鑾峰彇K绾挎暟鎹紙鏃绾块粯璁や娇鐢ㄥ墠澶嶆潈锛?func handleGetKline(w http.ResponseWriter, r *http.Request) {

// 修复后：
// 获取K线数据（日线默认使用前复权）
func handleGetKline(w http.ResponseWriter, r *http.Request) {
```

#### 第164-165行：注释和代码在同一行
```go
// 修复前：
// getQfqKlineDay 鑾峰彇鍓嶅鍒冩棩K绾挎暟鎹?func getQfqKlineDay(code string) (*protocol.KlineResp, error) {
	// 浣跨敤鍚岃姳椤篈PI鑾峰彇鍓嶅鍒冩暟鎹?	klines, err := extend.GetTHSDayKline(code, extend.THS_QFQ)

// 修复后：
// getQfqKlineDay 获取前复权日K线数据
func getQfqKlineDay(code string) (*protocol.KlineResp, error) {
	// 使用同花顺API获取前复权数据
	klines, err := extend.GetTHSDayKline(code, extend.THS_QFQ)
```

#### 第376行：字符串未闭合
```go
// 修复前：
errorResponse(w, "鎼滅储鍏抽敭璇嶄笉鑳戒负绌?)

// 修复后：
errorResponse(w, "搜索关键词不能为空")
```

#### 第463行：字符串未闭合
```go
// 修复前：
errorResponse(w, "鏁版嵁绠＄悊鍣ㄦ湭鍒濆鍖?)

// 修复后：
errorResponse(w, "数据管理器未初始化")
```

### 3. 编译测试
```powershell
$env:GOPROXY="https://goproxy.cn,direct"
$env:CGO_ENABLED=0
go build -ldflags="-s -w" -o stock-api.exe .
```

## 推荐方案

**最简单的方法**：从GitHub恢复文件，然后只修复必要的语法错误：

```powershell
cd web
git checkout HEAD -- server.go
# 然后手动修复第44行和第113行的语法错误
```

## 本地与GitHub的区别

- **本地文件**：包含编码问题（NUL字符、字符串未闭合、注释格式错误）
- **GitHub文件**：也可能包含NUL字符，但语法基本正确

## 确保编译成功的修改清单

1. ✅ 清理所有NUL字符
2. ✅ 修复第44行：字符串闭合
3. ✅ 修复第113行：注释和函数定义分行
4. ✅ 修复第164-165行：注释和代码分行
5. ✅ 修复第376行：字符串闭合
6. ✅ 修复第463行：字符串闭合
7. ✅ 修复第215行、270行：注释和代码分行（如果存在）


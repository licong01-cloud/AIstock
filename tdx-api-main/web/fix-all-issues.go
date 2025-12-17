//go:build tools
// +build tools

package tools

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func main() {
	// 读取文件
	content, err := ioutil.ReadFile("server.go")
	if err != nil {
		fmt.Printf("读取文件失败: %v\n", err)
		os.Exit(1)
	}

	// 清理NUL字符
	content = []byte(strings.ReplaceAll(string(content), "\x00", ""))

	text := string(content)

	// 修复注释和函数定义在同一行的问题
	text = regexp.MustCompile(`(?m)^(//[^\n]*?)(func\s+\w+)`).ReplaceAllString(text, "$1\n$2")

	// 修复注释和代码在同一行的问题（tab开头）
	text = regexp.MustCompile(`(?m)^(\t//[^\n]*?)(\t[^\t])`).ReplaceAllString(text, "$1\n$2")

	// 修复字符串未闭合的问题
	text = strings.ReplaceAll(text, `errorResponse(w, "鎼滅储澶辫触: "+err.Error()")`, `errorResponse(w, "搜索失败: "+err.Error())`)
	text = strings.ReplaceAll(text, `errorResponse(w, "璇锋眰鍙傛暟閿欒: "+err.Error()")`, `errorResponse(w, "请求参数错误: "+err.Error())`)
	text = strings.ReplaceAll(text, `errorResponse(w, "鏁版嵁绠＄悊鍣ㄦ湭鍒濆鍖?)`, `errorResponse(w, "数据管理器未初始化")`)
	text = strings.ReplaceAll(text, `log.Printf("宸插姞杞借偂绁ㄤ唬鐮侊紝鍏?%d 鏉?, len(tdx.DefaultCodes.Map))`, `log.Printf("已加载股票代码，共%d条", len(tdx.DefaultCodes.Map))`)

	// 写入文件
	err = ioutil.WriteFile("server.go", []byte(text), 0644)
	if err != nil {
		fmt.Printf("写入文件失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("修复完成！")
}


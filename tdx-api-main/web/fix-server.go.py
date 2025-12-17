#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复server.go文件的编码和语法错误
"""
import re
import sys

def fix_server_go(input_file, output_file):
    with open(input_file, 'rb') as f:
        content = f.read()
    
    # 清理NUL字符
    content = content.replace(b'\x00', b'')
    
    # 转换为UTF-8字符串
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            text = content.decode('gb2312')
        except UnicodeDecodeError:
            text = content.decode('utf-8', errors='ignore')
    
    # 修复第34行：注释缺少换行
    text = re.sub(r'(\t// [^\n]*?)(\tif err = os\.MkdirAll)', r'\1\n\2', text)
    
    # 修复第44行：字符串未闭合
    text = re.sub(r'log\.Printf\("宸插姞杞借偂绁ㄤ唬鐮侊紝鍏\?%d 鏉\?, len\(tdx\.DefaultCodes\.Map\)\)', 
                  r'log.Printf("已加载股票代码，共%d条", len(tdx.DefaultCodes.Map))', text)
    
    # 修复第113行：注释和函数定义在同一行
    text = re.sub(r'// 鑾峰彇K绾挎暟鎹.*?func handleGetKline', 
                  r'// 获取K线数据（日线默认使用前复权）\nfunc handleGetKline', text)
    
    # 修复第376行：字符串未闭合
    text = re.sub(r'errorResponse\(w, "鎼滅储鍏抽敭璇嶄笉鑳戒负绌\?\)', 
                  r'errorResponse(w, "搜索关键词不能为空")', text)
    
    # 修复第463行：字符串未闭合
    text = re.sub(r'errorResponse\(w, "鏁版嵁绠＄悊鍣ㄦ湭鍒濆鍖\?\)', 
                  r'errorResponse(w, "数据管理器未初始化")', text)
    
    # 修复所有其他未闭合的字符串（通用模式）
    lines = text.split('\n')
    fixed_lines = []
    for i, line in enumerate(lines, 1):
        # 检查是否有未闭合的字符串
        if 'errorResponse(w, "' in line and not line.rstrip().endswith('")'):
            # 尝试修复
            if line.rstrip().endswith(')'):
                line = line.rstrip()[:-1] + '")'
            elif ')' in line:
                line = line.replace(')', '")', 1)
        fixed_lines.append(line)
    
    text = '\n'.join(fixed_lines)
    
    # 写入文件
    with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
        f.write(text)
    
    print(f"已修复文件: {input_file} -> {output_file}")

if __name__ == '__main__':
    input_file = 'server.go'
    output_file = 'server.go.fixed'
    fix_server_go(input_file, output_file)


#!/usr/bin/env python3
"""
通用分段推送脚本 - 支持盘前/盘中/盘后报告
"""

import requests
import time
import sys

FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f70974f2-da1a-47d7-8f48-2bc855dad067"

def send_feishu_message(content, title=None):
    """发送单条飞书消息"""
    if title:
        text = f"{title}\n\n{content}"
    else:
        text = content
    
    payload = {
        "msg_type": "text",
        "content": {"text": text[:3500]}  # 飞书限制3500字符
    }
    
    try:
        response = requests.post(FEISHU_WEBHOOK, json=payload, timeout=30)
        result = response.json()
        return result.get('code') == 0
    except Exception as e:
        print(f"❌ 发送失败: {e}")
        return False

def split_report_into_sections(report_content):
    """将报告按章节分割"""
    sections = []
    lines = report_content.split('\n')
    current_section = []
    current_title = ""
    
    for line in lines:
        # 检测到新的二级标题
        if line.startswith('## '):
            if current_section:
                sections.append((current_title, '\n'.join(current_section)))
            current_title = line.replace('## ', '').strip()
            current_section = [line]
        else:
            current_section.append(line)
    
    # 添加最后一个章节
    if current_section:
        sections.append((current_title, '\n'.join(current_section)))
    
    return sections

def send_report_in_segments(report_path, report_title):
    """分段发送报告"""
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            report_content = f.read()
    except Exception as e:
        print(f"❌ 读取报告失败: {e}")
        return False
    
    print(f"📄 正在分段发送: {report_title}")
    
    # 1. 发送标题
    send_feishu_message(f"═══════════════════════\n{report_title}\n═══════════════════════")
    time.sleep(0.3)
    
    # 2. 按章节分割发送
    sections = split_report_into_sections(report_content)
    success_count = 0
    
    for i, (title, content) in enumerate(sections, 1):
        if not title or not content.strip():
            continue
        
        # 跳过空章节
        if len(content.strip()) < 50:
            continue
        
        # 如果内容太长，再分割
        if len(content) > 3000:
            chunks = []
            lines = content.split('\n')
            current_chunk = []
            current_len = 0
            
            for line in lines:
                if current_len + len(line) > 2800:
                    chunks.append('\n'.join(current_chunk))
                    current_chunk = [line]
                    current_len = len(line)
                else:
                    current_chunk.append(line)
                    current_len += len(line) + 1
            
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
            
            # 发送分块
            for j, chunk in enumerate(chunks):
                suffix = f" (续{j+1})" if j > 0 else ""
                success = send_feishu_message(chunk, f"{title}{suffix}")
                if success:
                    success_count += 1
                    print(f"✅ {title}{suffix}")
                else:
                    print(f"❌ {title}{suffix} 发送失败")
                time.sleep(0.3)
        else:
            # 直接发送
            success = send_feishu_message(content, title)
            if success:
                success_count += 1
                print(f"✅ {title}")
            else:
                print(f"❌ {title} 发送失败")
            time.sleep(0.3)
    
    # 3. 发送结束标记
    send_feishu_message(f"═══════════════════════\n{report_title} - 发送完成\n═══════════════════════")
    print(f"✅ 共发送 {success_count} 段消息")
    return True

if __name__ == "__main__":
    # 参数: 报告路径 报告标题
    if len(sys.argv) >= 3:
        report_path = sys.argv[1]
        report_title = sys.argv[2]
        send_report_in_segments(report_path, report_title)
    else:
        # 默认发送最新的盘前和盘中报告
        import glob
        import os
        
        # 找最新的盘前报告
        premarket_files = glob.glob("/workspace/projects/workspace/股票分析项目/daily_reports/premarket/20260325*.md")
        if premarket_files:
            latest_premarket = max(premarket_files, key=os.path.getmtime)
            send_report_in_segments(latest_premarket, "🌅 盘前选股报告 1.0")
            time.sleep(1)
        
        # 找最新的盘中报告
        intraday_files = glob.glob("/workspace/projects/workspace/股票分析项目/daily_reports/intraday/20260325*.md")
        if intraday_files:
            latest_intraday = max(intraday_files, key=os.path.getmtime)
            send_report_in_segments(latest_intraday, "📊 午间简报 1.2")

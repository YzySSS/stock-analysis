#!/usr/bin/env python3
"""
分段推送盘前报告到飞书
解决飞书消息长度限制问题
"""

import requests
import time

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
    with open(report_path, 'r', encoding='utf-8') as f:
        report_content = f.read()
    
    print(f"📄 正在发送 {report_title}...")
    
    # 1. 发送标题
    send_feishu_message(f"═══════════════════\n{report_title}\n═══════════════════")
    time.sleep(0.5)
    
    # 2. 按章节分割发送
    sections = split_report_into_sections(report_content)
    
    for i, (title, content) in enumerate(sections, 1):
        if not title or not content.strip():
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
                    print(f"✅ 第{i}章-{j+1}段发送成功")
                else:
                    print(f"❌ 第{i}章-{j+1}段发送失败")
                time.sleep(0.5)
        else:
            # 直接发送
            success = send_feishu_message(content, title)
            if success:
                print(f"✅ 第{i}章发送成功: {title}")
            else:
                print(f"❌ 第{i}章发送失败: {title}")
            time.sleep(0.5)
    
    # 3. 发送结束标记
    send_feishu_message("═══════════════════\n报告结束\n═══════════════════")
    print(f"✅ {report_title} 发送完成")

# 发送盘前报告
send_report_in_segments(
    "/workspace/projects/workspace/股票分析项目/daily_reports/premarket/20260325_114421_premarket_v10.md",
    "🌅 盘前选股报告 1.0（3月25日）"
)

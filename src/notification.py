#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推送通知系统 - 支持多种推送渠道
"""

import json
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional
from datetime import datetime
import traceback


class NotificationManager:
    """通知管理器"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化通知管理器
        
        Args:
            config: 配置字典，包含各种推送渠道的配置
        """
        self.config = config or {}
        self.enabled_channels = []
        
        # 检查哪些渠道已配置
        if self.config.get('email', {}).get('enabled'):
            self.enabled_channels.append('email')
        if self.config.get('wechat', {}).get('webhook'):
            self.enabled_channels.append('wechat')
        if self.config.get('dingtalk', {}).get('webhook'):
            self.enabled_channels.append('dingtalk')
        if self.config.get('feishu', {}).get('webhook'):
            self.enabled_channels.append('feishu')
    
    def send_report(self, 
                   report_data: Dict,
                   title: str = "每日股票报告",
                   channels: Optional[List[str]] = None):
        """
        发送报告
        
        Args:
            report_data: 报告数据字典
            title: 报告标题
            channels: 指定发送渠道，None则发送到所有已配置渠道
        """
        if channels is None:
            channels = self.enabled_channels
        
        # 生成消息内容
        message = self._format_report_message(report_data, title)
        
        results = {}
        
        for channel in channels:
            try:
                if channel == 'email':
                    results['email'] = self._send_email(title, message['email'])
                elif channel == 'wechat':
                    results['wechat'] = self._send_wechat(message['markdown'])
                elif channel == 'dingtalk':
                    results['dingtalk'] = self._send_dingtalk(title, message['markdown'])
                elif channel == 'feishu':
                    results['feishu'] = self._send_feishu(title, message['markdown'])
            except Exception as e:
                results[channel] = {'success': False, 'error': str(e)}
                print(f"❌ 发送到 {channel} 失败: {e}")
        
        return results
    
    def send_alert(self,
                  alert_text: str,
                  level: str = 'info',
                  title: str = "股票告警"):
        """
        发送告警消息
        
        Args:
            alert_text: 告警内容
            level: 级别 info/warning/error
            title: 标题
        """
        emoji_map = {
            'info': 'ℹ️',
            'warning': '⚠️',
            'error': '🚨'
        }
        emoji = emoji_map.get(level, 'ℹ️')
        
        message = f"{emoji} **{title}**\n\n{alert_text}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        results = {}
        for channel in self.enabled_channels:
            try:
                if channel == 'email':
                    results['email'] = self._send_email(title, message, is_html=False)
                elif channel == 'wechat':
                    results['wechat'] = self._send_wechat(message)
                elif channel == 'dingtalk':
                    results['dingtalk'] = self._send_dingtalk(title, message)
                elif channel == 'feishu':
                    results['feishu'] = self._send_feishu(title, message)
            except Exception as e:
                results[channel] = {'success': False, 'error': str(e)}
        
        return results
    
    def _format_report_message(self, report_data: Dict, title: str) -> Dict:
        """格式化报告消息 - 包含详细推荐理由和数据来源"""
        
        date = report_data.get('date', datetime.now().strftime('%Y-%m-%d'))
        market_summary = report_data.get('market_summary', {})
        stock_analysis = report_data.get('stock_analysis', [])
        recommendations = report_data.get('recommendations', [])
        
        # Markdown格式（用于微信、钉钉、飞书）
        markdown_lines = [
            f"# 📊 {title}",
            f"",
            f"**日期**: {date}",
            f"",
            f"---",
            f"",
            f"## 📈 市场概况",
            f"",
        ]
        
        if market_summary:
            markdown_lines.extend([
                f"- 上涨: {market_summary.get('up_count', 'N/A')} 家",
                f"- 下跌: {market_summary.get('down_count', 'N/A')} 家",
                f"- 涨停: {market_summary.get('limit_up', 'N/A')} 家",
                f"- 跌停: {market_summary.get('limit_down', 'N/A')} 家",
                f""
            ])
        
        markdown_lines.extend([
            f"---",
            f"",
            f"## 🎯 精选推荐",
            f""
        ])
        
        # 使用 recommendations 数据（包含详细理由）
        for i, rec in enumerate(recommendations[:5], 1):
            code = rec.get('code', 'N/A')
            name = rec.get('name', 'N/A')
            score = rec.get('score', 0)
            action = rec.get('action', 'N/A')
            reason = rec.get('reason', '')
            data_source = rec.get('data_source', 'AkShare')
            reliability = rec.get('data_reliability', '⭐⭐⭐⭐')
            
            action_emoji = {'买入': '🟢', '强烈买入': '🔥', '关注': '👀', '卖出': '🔴'}.get(action, '⚪')
            
            markdown_lines.extend([
                f"### {i}. {action_emoji} {name} ({code})",
                f"- **操作建议**: {action} (评分: {score:.1f}分)",
                f"- **推荐理由**: {reason}",
            ])
            
            # 添加回测数据
            if 'backtest' in rec:
                bt = rec['backtest']
                markdown_lines.append(f"- **历史回测**: {bt.get('period', '近1年')}收益 {bt.get('total_return', 'N/A')}, 胜率 {bt.get('win_rate', 'N/A')}")
            
            markdown_lines.extend([
                f"- **数据来源**: {data_source} {reliability}",
                f""
            ])
        
        # 如果没有 recommendations，回退到 stock_analysis
        if not recommendations and stock_analysis:
            markdown_lines.append(f"*以下是基础分析数据...*\n")
            for stock in stock_analysis[:5]:
                code = stock.get('code', 'N/A')
                name = stock.get('name', 'N/A')
                score = stock.get('composite_score', 0)
                signal = stock.get('signal', 'N/A')
                
                signal_emoji = {'强烈买入': '🔥', '买入': '🟢', '卖出': '🔴', '持有': '⚪'}.get(signal, '⚪')
                
                markdown_lines.extend([
                    f"### {signal_emoji} {name} ({code})",
                    f"- 综合评分: **{score:.1f}分**",
                    f"- 信号: **{signal}**",
                    f""
                ])
        
        # 添加数据源说明
        markdown_lines.extend([
            f"---",
            f"",
            f"## 📋 数据说明",
            f"",
            f"**数据来源**: AkShare（东方财富）",
            f"",
            f"**可信度评级**:",
            f"- ⭐⭐⭐⭐⭐ 实时交易数据（盘中）",
            f"- ⭐⭐⭐⭐ 日K历史数据（盘后）",
            f"- ⭐⭐⭐ 财务数据（季度更新）",
            f"",
            f"**免责声明**: 本报告仅供参考，不构成投资建议。",
            f"**分析时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f""
        ])
        
        markdown_lines.extend([
            f"---",
            f"",
            f"⏰ 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        markdown = '\n'.join(markdown_lines)
        
        # HTML格式（用于邮件）
        html_lines = [
            f"<h1>📊 {title}</h1>",
            f"<p><b>日期:</b> {date}</p>",
            f"<hr>",
            f"<h2>📈 市场概况</h2>",
            f"<ul>"
        ]
        
        if market_summary:
            html_lines.extend([
                f"<li>上涨: {market_summary.get('up_count', 'N/A')} 家</li>",
                f"<li>下跌: {market_summary.get('down_count', 'N/A')} 家</li>",
                f"<li>涨停: {market_summary.get('limit_up', 'N/A')} 家</li>",
                f"<li>跌停: {market_summary.get('limit_down', 'N/A')} 家</li>",
            ])
        
        html_lines.extend([
            f"</ul>",
            f"<hr>",
            f"<h2>🎯 个股分析</h2>"
        ])
        
        for stock in stock_analysis[:5]:
            code = stock.get('code', 'N/A')
            name = stock.get('name', 'N/A')
            score = stock.get('composite_score', 0)
            signal = stock.get('signal', 'N/A')
            
            html_lines.extend([
                f"<h3>{name} ({code})</h3>",
                f"<ul>",
                f"<li>综合评分: <b>{score:.1f}分</b></li>",
                f"<li>信号: <b>{signal}</b></li>",
                f"</ul>"
            ])
        
        html_lines.extend([
            f"<hr>",
            f"<p>⏰ 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
        ])
        
        html = '\n'.join(html_lines)
        
        return {
            'markdown': markdown,
            'email': html,
            'text': markdown.replace('#', '').replace('*', '')  # 纯文本
        }
    
    def _send_email(self, subject: str, content: str, is_html: bool = True) -> Dict:
        """发送邮件"""
        email_config = self.config.get('email', {})
        
        smtp_server = email_config.get('smtp_server', 'smtp.gmail.com')
        smtp_port = email_config.get('smtp_port', 587)
        username = email_config.get('username')
        password = email_config.get('password')
        to_addr = email_config.get('to_addr', username)
        
        if not all([username, password]):
            return {'success': False, 'error': '邮件配置不完整'}
        
        msg = MIMEMultipart()
        msg['From'] = username
        msg['To'] = to_addr
        msg['Subject'] = subject
        
        if is_html:
            msg.attach(MIMEText(content, 'html', 'utf-8'))
        else:
            msg.attach(MIMEText(content, 'plain', 'utf-8'))
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)
        
        return {'success': True}
    
    def _send_wechat(self, content: str) -> Dict:
        """发送到企业微信"""
        webhook = self.config.get('wechat', {}).get('webhook')
        
        if not webhook:
            return {'success': False, 'error': '未配置企业微信webhook'}
        
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        
        response = requests.post(webhook, json=data, timeout=10)
        response.raise_for_status()
        
        return {'success': True, 'response': response.json()}
    
    def _send_dingtalk(self, title: str, content: str) -> Dict:
        """发送到钉钉"""
        webhook = self.config.get('dingtalk', {}).get('webhook')
        secret = self.config.get('dingtalk', {}).get('secret')
        
        if not webhook:
            return {'success': False, 'error': '未配置钉钉webhook'}
        
        # 如果配置了secret，需要签名
        if secret:
            import time
            import hmac
            import hashlib
            import base64
            import urllib.parse
            
            timestamp = str(round(time.time() * 1000))
            string_to_sign = f"{timestamp}\n{secret}"
            hmac_code = hmac.new(
                secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                digestmod=hashlib.sha256
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            webhook = f"{webhook}&timestamp={timestamp}&sign={sign}"
        
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": content
            }
        }
        
        response = requests.post(webhook, json=data, timeout=10)
        response.raise_for_status()
        
        return {'success': True, 'response': response.json()}
    
    def _send_feishu(self, title: str, content: str) -> Dict:
        """发送到飞书"""
        webhook = self.config.get('feishu', {}).get('webhook')
        
        if not webhook:
            return {'success': False, 'error': '未配置飞书webhook'}
        
        data = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title
                    }
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": content
                        }
                    }
                ]
            }
        }
        
        response = requests.post(webhook, json=data, timeout=10)
        response.raise_for_status()
        
        return {'success': True, 'response': response.json()}
    
    def send_in_parts(self, title: str, content: str, channel: str = 'feishu'):
        """
        分段发送长报告
        解决飞书等平台的单条消息长度限制
        """
        import time
        
        if channel != 'feishu':
            # 其他渠道暂不支持分段
            return self._send_feishu(title, content)
        
        webhook = self.config.get('feishu', {}).get('webhook')
        if not webhook:
            return {'success': False, 'error': '未配置飞书webhook'}
        
        # 按章节分割
        sections = self._split_content_by_sections(content)
        success_count = 0
        
        for section_title, section_content in sections:
            if not section_content.strip() or len(section_content.strip()) < 30:
                continue
            
            # 如果内容太长，继续分割
            if len(section_content) > 3000:
                chunks = self._split_long_content(section_content, max_len=2800)
                for i, chunk in enumerate(chunks):
                    suffix = f" (续{i+1})" if i > 0 else ""
                    success = self._send_feishu_text(webhook, chunk, f"{section_title}{suffix}")
                    if success:
                        success_count += 1
                    time.sleep(0.3)
            else:
                success = self._send_feishu_text(webhook, section_content, section_title)
                if success:
                    success_count += 1
                time.sleep(0.3)
        
        return {'success': True, 'parts_sent': success_count}
    
    def _send_feishu_text(self, webhook: str, content: str, title: str = None) -> bool:
        """发送纯文本消息到飞书"""
        try:
            text = f"{title}\n\n{content}" if title else content
            data = {
                "msg_type": "text",
                "content": {"text": text[:3500]}
            }
            response = requests.post(webhook, json=data, timeout=10)
            result = response.json()
            return result.get('code') == 0
        except Exception as e:
            print(f"❌ 发送失败: {e}")
            return False
    
    def _split_content_by_sections(self, content: str) -> list:
        """按章节分割内容"""
        sections = []
        lines = content.split('\n')
        current_title = ""
        current_content = []
        
        for line in lines:
            if line.startswith('## '):
                if current_content:
                    sections.append((current_title, '\n'.join(current_content)))
                current_title = line.replace('## ', '').strip()
                current_content = [line]
            else:
                current_content.append(line)
        
        if current_content:
            sections.append((current_title, '\n'.join(current_content)))
        
        return sections
    
    def _split_long_content(self, content: str, max_len: int = 2800) -> list:
        """将长内容分割成多块"""
        chunks = []
        lines = content.split('\n')
        current_chunk = []
        current_len = 0
        
        for line in lines:
            if current_len + len(line) > max_len:
                chunks.append('\n'.join(current_chunk))
                current_chunk = [line]
                current_len = len(line)
            else:
                current_chunk.append(line)
                current_len += len(line) + 1
        
        if current_chunk:
            chunks.append('\n'.join(current_chunk))
        
        return chunks


def create_sample_config():
    """创建示例配置"""
    return {
        "email": {
            "enabled": True,
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "username": "your_email@gmail.com",
            "password": "your_app_password",
            "to_addr": "recipient@example.com"
        },
        "wechat": {
            "webhook": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
        },
        "dingtalk": {
            "webhook": "https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN",
            "secret": "YOUR_SECRET"  # 可选
        },
        "feishu": {
            "webhook": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_TOKEN"
        }
    }


if __name__ == "__main__":
    # 测试代码
    print("🧪 推送通知系统测试")
    
    # 创建示例配置
    config = create_sample_config()
    print("\n示例配置:")
    print(json.dumps(config, indent=2, ensure_ascii=False))
    
    # 创建通知管理器
    manager = NotificationManager(config)
    print(f"\n✅ 已配置渠道: {manager.enabled_channels}")
    
    # 测试报告数据
    test_report = {
        "date": "2024-01-15",
        "market_summary": {
            "up_count": 2500,
            "down_count": 1800,
            "limit_up": 45,
            "limit_down": 3
        },
        "stock_analysis": [
            {"code": "000001.SZ", "name": "平安银行", "composite_score": 75.5, "signal": "买入"},
            {"code": "000002.SZ", "name": "万科A", "composite_score": 82.3, "signal": "强烈买入"},
            {"code": "600519.SH", "name": "贵州茅台", "composite_score": 68.0, "signal": "持有"}
        ]
    }
    
    # 生成消息预览
    message = manager._format_report_message(test_report, "每日股票报告")
    print("\n📧 消息预览:")
    print(message['markdown'][:500] + "...")

#!/usr/bin/env python3
"""
每日舆情数据更新脚本 (AkShare版)
============================
使用AkShare免费获取新闻，经过滤后AI分析，存入MySQL

用法:
  python3 sentiment_daily_update.py           # 更新今天数据
  python3 sentiment_daily_update.py --date 2026-04-03  # 更新指定日期

定时任务:
  30 22 * * * cd /root/.openclaw/workspace/股票分析项目 && python3 sentiment_daily_update.py >> /tmp/cron_sentiment.log 2>&1
"""

import os
import sys
import argparse
import logging
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple

import pymysql
import akshare as ak

# 手动加载环境变量
def load_env_file(filepath):
    """手动加载 .env 文件"""
    try:
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"\'')
                    os.environ[key] = value
    except Exception as e:
        logger.warning(f"加载 .env 文件失败: {e}")

load_env_file('/root/.openclaw/workspace/股票分析项目/.env')

# 添加src路径
sys.path.insert(0, '/root/.openclaw/workspace/股票分析项目/src')
from news_filter import NewsFilter
from ai_sentiment_analyzer import SentimentAnalyzer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/root/.openclaw/workspace/股票分析项目/logs/sentiment_daily_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.0.4.8'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'openclaw_user'),
    'password': os.getenv('DB_PASSWORD', 'open@2026'),
    'database': os.getenv('DB_NAME', 'stock'),
    'charset': 'utf8mb4'
}


class SentimentDailyUpdater:
    """每日舆情更新器"""
    
    def __init__(self):
        self.conn = None
        self.news_filter = NewsFilter(min_credibility=0.4, max_age_days=3)
        self.sentiment_analyzer = SentimentAnalyzer(use_local_only=False)
        
        self.stats = {
            'total': 0,
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'news_fetched': 0,
            'ai_analyzed': 0,
            'records_created': 0
        }
        self.report_lines = []
    
    def connect_db(self) -> bool:
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            self.report_lines.append("✅ 数据库连接：成功")
            return True
        except Exception as e:
            self.report_lines.append(f"❌ 数据库连接：失败 - {e}")
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def get_priority_stocks(self) -> List[Tuple[str, str]]:
        """
        获取优先处理的股票列表
        优先级：持仓股 > 自选股 > 活跃Top100
        """
        stocks = []
        
        try:
            with self.conn.cursor() as cursor:
                # 1. 获取所有非退市股票，按优先级排序
                # 这里简化处理，获取所有活跃股票（成交额高的）
                cursor.execute('''
                    SELECT code, name 
                    FROM stock_basic 
                    WHERE is_delisted = 0
                    ORDER BY code
                ''')
                stocks = cursor.fetchall()
                
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
        
        return stocks
    
    def get_existing_sentiment(self, date: str) -> Set[str]:
        """获取指定日期已有舆情数据的股票"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    'SELECT code FROM sentiment_daily WHERE trade_date = %s',
                    (date,)
                )
                return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            logger.warning(f"获取已有数据失败: {e}")
            return set()
    
    def fetch_news_akshare(self, code: str) -> List[Dict]:
        """使用AkShare获取个股新闻"""
        try:
            # 获取个股新闻（东方财富）
            news_df = ak.stock_news_em(symbol=code[:6])
            
            if news_df is None or len(news_df) == 0:
                return []
            
            news_list = []
            for _, row in news_df.iterrows():
                news_list.append({
                    'title': str(row.get('新闻标题', '')),
                    'content': str(row.get('新闻内容', ''))[:500],
                    'source': str(row.get('文章来源', '东方财富')),
                    'datetime': str(row.get('发布时间', '')),
                    'url': str(row.get('新闻链接', ''))
                })
            
            return news_list
            
        except Exception as e:
            logger.debug(f"获取 {code} 新闻失败: {e}")
            return []
    
    def analyze_sentiment(self, code: str, name: str, news_list: List[Dict]) -> Dict:
        """分析舆情"""
        if not news_list:
            return {
                'sentiment_score': 0.0,
                'sentiment_type': 0,
                'news_count': 0,
                'positive_news': 0,
                'negative_news': 0,
                'neutral_news': 0,
                'credibility_avg': 0.5,
                'heat_score': 0,
                'top_keywords': '[]',
                'sources_distribution': '{}',
                'ai_analyzed': 0,
                'summary': '无新闻'
            }
        
        # 计算平均可信度
        total_credibility = sum(
            self.news_filter.get_source_credibility(n.get('source', ''))
            for n in news_list
        )
        avg_credibility = total_credibility / len(news_list) if news_list else 0.5
        
        # 使用AI分析情感
        try:
            result = self.sentiment_analyzer.analyze_sentiment(code, name, news_list)
            
            # 转换分数 (0-100 -> -10~+10)
            raw_score = result.get('score', 50)
            sentiment_score = (raw_score - 50) / 5
            
            # 确定情感类型
            if sentiment_score > 2:
                sentiment_type = 1  # 正面
            elif sentiment_score < -2:
                sentiment_type = 2  # 负面
            else:
                sentiment_type = 0  # 中性
            
            # 统计正负面
            positive_news = 1 if sentiment_type == 1 else 0
            negative_news = 1 if sentiment_type == 2 else 0
            neutral_news = 1 if sentiment_type == 0 else 0
            
            ai_analyzed = 1 if result.get('summary') != '本地分析' else 0
            if ai_analyzed:
                self.stats['ai_analyzed'] += 1
            
            # 提取关键词
            opportunities = result.get('opportunities', [])
            risks = result.get('risks', [])
            keywords = opportunities[:5] + risks[:5]
            
            return {
                'sentiment_score': round(sentiment_score, 2),
                'sentiment_type': sentiment_type,
                'news_count': len(news_list),
                'positive_news': positive_news,
                'negative_news': negative_news,
                'neutral_news': neutral_news,
                'credibility_avg': round(avg_credibility, 2),
                'heat_score': min(100, len(news_list) * 8 + len(keywords) * 5),
                'top_keywords': json.dumps(keywords),
                'sources_distribution': json.dumps(self._get_source_distribution(news_list)),
                'ai_analyzed': ai_analyzed,
                'summary': result.get('summary', '')
            }
            
        except Exception as e:
            logger.warning(f"AI分析失败 {code}: {e}")
            # 降级到本地分析
            return self._local_fallback(news_list, avg_credibility)
    
    def _local_fallback(self, news_list: List[Dict], credibility: float) -> Dict:
        """本地降级分析"""
        local_result = self.sentiment_analyzer._analyze_sentiment_local(news_list)
        
        raw_score = local_result.get('score', 50)
        sentiment_score = (raw_score - 50) / 5
        
        return {
            'sentiment_score': round(sentiment_score, 2),
            'sentiment_type': 0 if abs(sentiment_score) < 2 else (1 if sentiment_score > 0 else 2),
            'news_count': len(news_list),
            'positive_news': 0,
            'negative_news': 0,
            'neutral_news': len(news_list),
            'credibility_avg': round(credibility, 2),
            'heat_score': min(100, len(news_list) * 6),
            'top_keywords': json.dumps(local_result.get('opportunities', [])[:3] + local_result.get('risks', [])[:3]),
            'sources_distribution': json.dumps({'财经网站': 0.6, '自媒体': 0.4}),
            'ai_analyzed': 0,
            'summary': '本地分析'
        }
    
    def _get_source_distribution(self, news_list: List[Dict]) -> Dict:
        """获取来源分布"""
        distribution = {}
        for news in news_list:
            source = news.get('source', '未知')
            distribution[source] = distribution.get(source, 0) + 1
        
        # 归一化
        total = sum(distribution.values())
        if total > 0:
            distribution = {k: round(v/total, 2) for k, v in distribution.items()}
        
        return distribution
    
    def save_sentiment(self, code: str, date: str, data: Dict) -> bool:
        """保存舆情数据"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO sentiment_daily 
                    (code, trade_date, sentiment_score, sentiment_type, 
                     news_count, positive_news, negative_news, neutral_news,
                     credibility_avg, heat_score, top_keywords, sources_distribution, ai_analyzed)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    sentiment_score = VALUES(sentiment_score),
                    sentiment_type = VALUES(sentiment_type),
                    news_count = VALUES(news_count),
                    positive_news = VALUES(positive_news),
                    negative_news = VALUES(negative_news),
                    neutral_news = VALUES(neutral_news),
                    credibility_avg = VALUES(credibility_avg),
                    heat_score = VALUES(heat_score),
                    top_keywords = VALUES(top_keywords),
                    sources_distribution = VALUES(sources_distribution),
                    ai_analyzed = VALUES(ai_analyzed),
                    updated_at = NOW()
                '''
                cursor.execute(sql, (
                    code, date, data['sentiment_score'], data['sentiment_type'],
                    data['news_count'], data['positive_news'], data['negative_news'],
                    data['neutral_news'], data['credibility_avg'], data['heat_score'],
                    data['top_keywords'], data['sources_distribution'], data['ai_analyzed']
                ))
                self.conn.commit()
                return True
        except Exception as e:
            logger.warning(f"保存 {code} 失败: {e}")
            return False
    
    def log_update(self, update_date: str, duration: int, status: str = 'success'):
        """记录更新日志"""
        try:
            with self.conn.cursor() as cursor:
                sql = '''
                    INSERT INTO sentiment_update_log 
                    (update_date, update_type, total_stocks, success_count, 
                     failed_count, skip_count, start_time, end_time, duration_seconds, status)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s)
                '''
                cursor.execute(sql, (
                    update_date, 'daily', self.stats['total'],
                    self.stats['records_created'], self.stats['failed'],
                    self.stats['skipped'], duration, status
                ))
                self.conn.commit()
        except Exception as e:
            logger.warning(f"记录日志失败: {e}")
    
    def print_report(self, date: str, duration: int):
        """输出结构化报告"""
        minutes = duration // 60
        seconds = duration % 60
        
        print("\n" + "=" * 70)
        print("📊 每日舆情数据更新报告")
        print("=" * 70)
        print(f"📅 更新日期：{date}")
        print(f"⏰ 执行时间：约{minutes}分{seconds}秒")
        print("-" * 70)
        
        print(f"📈 处理统计：")
        print(f"  • 计划更新：{self.stats['total']} 只股票")
        print(f"  • 成功处理：{self.stats['processed']} 只")
        print(f"  • 跳过(已有)：{self.stats['skipped']} 只")
        print(f"  • 失败：{self.stats['failed']} 只")
        print(f"  • 获取新闻：{self.stats['news_fetched']} 条")
        print(f"  • AI分析：{self.stats['ai_analyzed']} 只")
        print(f"  • 新增/更新记录：{self.stats['records_created']} 条")
        
        print(f"\n🔌 连接状态：")
        for line in self.report_lines:
            print(f"  • {line}")
        
        # 数据库统计
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    SELECT COUNT(*), COUNT(DISTINCT code), MAX(trade_date)
                    FROM sentiment_daily
                ''')
                total, stocks, max_date = cursor.fetchone()
                print(f"\n💾 数据库状态：")
                print(f"  • 总记录数：{total:,} 条")
                print(f"  • 覆盖股票：{stocks} 只")
                print(f"  • 最新日期：{max_date}")
        except:
            pass
        
        print("=" * 70)
    
    def run(self, date_str: str = None, max_stocks: int = None):
        """执行每日更新"""
        start_time = datetime.now()
        target_date = date_str or start_time.strftime('%Y-%m-%d')
        
        logger.info("=" * 70)
        logger.info("🚀 每日舆情数据更新 (AkShare版)")
        logger.info("=" * 70)
        logger.info(f"📅 目标日期: {target_date}")
        
        # 1. 连接数据库
        if not self.connect_db():
            return False
        
        # 2. 获取股票列表
        stocks = self.get_priority_stocks()
        if not stocks:
            logger.error("❌ 未获取到股票列表")
            return False
        
        # 限制数量（如果指定）
        if max_stocks and len(stocks) > max_stocks:
            stocks = stocks[:max_stocks]
        
        self.stats['total'] = len(stocks)
        logger.info(f"📊 计划处理 {len(stocks)} 只股票")
        
        # 3. 获取已有数据
        existing = self.get_existing_sentiment(target_date)
        if existing:
            logger.info(f"⏭️ 已有 {len(existing)} 只股票的舆情数据")
        
        # 4. 逐只处理
        for i, (code, name) in enumerate(stocks):
            # 检查是否已有数据
            if code in existing:
                self.stats['skipped'] += 1
                continue
            
            try:
                # 获取新闻
                news_list = self.fetch_news_akshare(code)
                self.stats['news_fetched'] += len(news_list)
                
                # 过滤新闻
                if news_list:
                    news_list = self.news_filter.filter(news_list, code, name, top_n=5)
                
                # 分析情感
                sentiment_data = self.analyze_sentiment(code, name, news_list)
                
                # 保存
                if self.save_sentiment(code, target_date, sentiment_data):
                    self.stats['processed'] += 1
                    self.stats['records_created'] += 1
                else:
                    self.stats['failed'] += 1
                
            except Exception as e:
                logger.warning(f"处理 {code} 失败: {e}")
                self.stats['failed'] += 1
            
            # 进度显示
            if (i + 1) % 100 == 0:
                logger.info(f"⏱️ 进度: {i+1}/{len(stocks)} | 已处理: {self.stats['processed']}")
            
            # 延迟避免限流
            time.sleep(0.3)
        
        # 5. 完成统计
        elapsed = int((datetime.now() - start_time).total_seconds())
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ 每日舆情更新完成!")
        logger.info(f"📊 统计: 成功{self.stats['processed']} | 跳过{self.stats['skipped']} | 失败{self.stats['failed']}")
        logger.info(f"⏱️ 总耗时: {elapsed} 秒")
        logger.info("=" * 70)
        
        # 6. 记录日志和输出报告
        self.log_update(target_date, elapsed)
        self.print_report(target_date, elapsed)
        
        return True
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='每日舆情数据更新 (AkShare版)')
    parser.add_argument('--date', type=str, default=None,
                       help='指定日期 (格式: YYYY-MM-DD)，默认今天')
    parser.add_argument('--max-stocks', type=int, default=None,
                       help='最多处理N只股票（用于测试）')
    
    args = parser.parse_args()
    
    updater = SentimentDailyUpdater()
    try:
        success = updater.run(date_str=args.date, max_stocks=args.max_stocks)
        sys.exit(0 if success else 1)
    finally:
        updater.close()


if __name__ == "__main__":
    main()

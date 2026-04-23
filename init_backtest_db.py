#!/usr/bin/env python3
"""
回测数据库初始化脚本
=====================
创建回测所需的额外数据表

用法:
  python3 init_backtest_db.py
"""

import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_backtest_database(db_path: str = None):
    """初始化回测数据库"""
    
    if db_path is None:
        db_path = os.path.join(os.path.dirname(__file__), 'src', 'data_cache', 'backtest.db')
    
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    
    # 1. 回测记录表 - 记录每次选股结果
    conn.execute('''
        CREATE TABLE IF NOT EXISTS backtest_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,                    -- 回测批次ID
            run_date TEXT NOT NULL,                  -- 回测日期
            strategy_version TEXT NOT NULL,          -- 策略版本
            mode TEXT NOT NULL,                      -- premarket/noon/postmarket
            
            -- 选股信息
            stock_code TEXT NOT NULL,                -- 股票代码
            stock_name TEXT,                         -- 股票名称
            rank INTEGER,                            -- 选股排名
            
            -- 选股时的因子得分
            total_score REAL,                        -- 总评分
            technical_score REAL,                    -- 技术因子
            sentiment_score REAL,                    -- 情绪因子
            sector_score REAL,                       -- 板块因子
            money_flow_score REAL,                   -- 资金因子
            risk_score REAL,                         -- 风险因子
            consensus_score REAL,                    -- 一致预期
            news_sentiment_score REAL,               -- 舆情因子
            
            -- 选股时的市场数据
            entry_price REAL,                        -- 入选价格
            entry_change_pct REAL,                   -- 入选时涨跌幅
            market_status TEXT,                      -- 市场环境
            
            -- 所属板块
            sector TEXT,                             -- 所属板块
            is_sector_leader INTEGER,                -- 是否板块龙头
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(run_id, stock_code, mode)
        )
    ''')
    
    # 2. 回测绩效表 - 记录选股后的实际收益
    conn.execute('''
        CREATE TABLE IF NOT EXISTS backtest_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,                    -- 回测批次ID
            run_date TEXT NOT NULL,                  -- 回测日期
            stock_code TEXT NOT NULL,                -- 股票代码
            
            -- 持仓价格
            entry_price REAL,                        -- 买入价（收盘价）
            
            -- 次日表现
            next_day_open REAL,                      -- 次日开盘价
            next_day_high REAL,                      -- 次日最高价
            next_day_low REAL,                       -- 次日最低价
            next_day_close REAL,                     -- 次日收盘价
            next_day_change_pct REAL,                -- 次日涨跌幅
            
            -- 5日表现
            day5_close REAL,                         -- 5日后收盘价
            day5_change_pct REAL,                    -- 5日涨跌幅
            
            -- 最大回撤
            max_drawdown REAL,                       -- 期间最大回撤
            
            -- 相对大盘表现
            index_change_pct REAL,                   -- 同期大盘涨跌幅
            alpha REAL,                              -- 超额收益
            
            -- 胜率标记
            is_win INTEGER,                          -- 是否盈利（1=是, 0=否）
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(run_id, stock_code)
        )
    ''')
    
    # 3. 策略版本表 - 记录策略变更
    conn.execute('''
        CREATE TABLE IF NOT EXISTS strategy_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL UNIQUE,            -- 版本号
            version_name TEXT,                       -- 版本名称
            description TEXT,                        -- 版本描述
            
            -- 因子权重配置（JSON格式）
            weights TEXT,                            -- {"technical": 0.2, ...}
            
            -- 策略参数
            threshold INTEGER,                       -- 选股阈值
            max_picks INTEGER,                       -- 最大选股数
            
            -- 变更说明
            changes TEXT,                            -- 本次变更内容
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 4. 回测统计汇总表 - 按批次统计绩效
    conn.execute('''
        CREATE TABLE IF NOT EXISTS backtest_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,             -- 回测批次ID
            run_date TEXT NOT NULL,                  -- 回测日期
            strategy_version TEXT,                   -- 策略版本
            mode TEXT,                               -- premarket/noon/postmarket
            
            -- 选股统计
            total_picks INTEGER,                     -- 选股总数
            avg_score REAL,                          -- 平均得分
            
            -- 次日胜率
            next_day_win_rate REAL,                  -- 次日胜率
            next_day_avg_return REAL,                -- 次日平均收益
            next_day_max_return REAL,                -- 次日最大收益
            next_day_min_return REAL,                -- 次日最小收益
            
            -- 5日胜率
            day5_win_rate REAL,                      -- 5日胜率
            day5_avg_return REAL,                    -- 5日平均收益
            
            -- 相对大盘
            avg_alpha REAL,                          -- 平均超额收益
            
            -- 风险指标
            avg_max_drawdown REAL,                   -- 平均最大回撤
            
            -- 板块分布
            sector_distribution TEXT,                -- JSON格式板块分布
            
            -- 市场环境
            market_status TEXT,                      -- 市场环境
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 创建索引优化查询
    conn.execute('CREATE INDEX IF NOT EXISTS idx_backtest_run_id ON backtest_records(run_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_backtest_date ON backtest_records(run_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_backtest_code ON backtest_records(stock_code)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_perf_run_id ON backtest_performance(run_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_perf_date ON backtest_performance(run_date)')
    
    conn.commit()
    conn.close()
    
    logger.info(f"✅ 回测数据库初始化完成: {db_path}")
    return db_path


def show_tables(db_path: str):
    """显示所有表结构"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    print(f"\n{'='*60}")
    print(f"📊 回测数据库: {db_path}")
    print(f"{'='*60}")
    
    for table in tables:
        table_name = table[0]
        print(f"\n【表: {table_name}】")
        print("-"*60)
        
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]:20s} {col[2]:10s}")
        
        # 统计记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  [记录数: {count}]")
    
    conn.close()


if __name__ == "__main__":
    db_path = init_backtest_database()
    show_tables(db_path)
    
    print("\n" + "="*60)
    print("✅ 回测数据库准备就绪！")
    print("="*60)
    print("\n使用方式:")
    print("  1. 每日选股结果自动存入 backtest_records 表")
    print("  2. 次日获取实际收益数据存入 backtest_performance 表")
    print("  3. 自动生成汇总统计存入 backtest_summary 表")
    print("  4. 对比不同 strategy_version 的绩效表现")

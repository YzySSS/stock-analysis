#!/usr/bin/env python3
"""
基本面数据聚合模块 - 优化版
fail-open设计、多候选接口、部分容错
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import re

logger = logging.getLogger(__name__)


@dataclass
class ValuationMetrics:
    """估值指标"""
    pe_ttm: Optional[float] = None          # 市盈率TTM
    pe_lyr: Optional[float] = None          # 市盈率(静态)
    pb: Optional[float] = None              # 市净率
    ps: Optional[float] = None              # 市销率
    market_cap: Optional[float] = None      # 总市值(亿)
    
    def to_dict(self) -> Dict:
        return {
            'pe_ttm': self.pe_ttm,
            'pe_lyr': self.pe_lyr,
            'pb': self.pb,
            'ps': self.ps,
            'market_cap': self.market_cap
        }


@dataclass
class GrowthMetrics:
    """成长指标"""
    revenue_growth_yoy: Optional[float] = None      # 营收同比增长
    profit_growth_yoy: Optional[float] = None       # 净利润同比增长
    revenue_growth_qoq: Optional[float] = None      # 营收环比增长
    profit_growth_qoq: Optional[float] = None       # 净利润环比增长
    
    def to_dict(self) -> Dict:
        return {
            'revenue_growth_yoy': self.revenue_growth_yoy,
            'profit_growth_yoy': self.profit_growth_yoy,
            'revenue_growth_qoq': self.revenue_growth_qoq,
            'profit_growth_qoq': self.profit_growth_qoq
        }


@dataclass
class ProfitabilityMetrics:
    """盈利能力指标"""
    roe: Optional[float] = None             # 净资产收益率
    roa: Optional[float] = None             # 总资产收益率
    gross_margin: Optional[float] = None    # 毛利率
    net_margin: Optional[float] = None      # 净利率
    eps: Optional[float] = None             # 每股收益
    
    def to_dict(self) -> Dict:
        return {
            'roe': self.roe,
            'roa': self.roa,
            'gross_margin': self.gross_margin,
            'net_margin': self.net_margin,
            'eps': self.eps
        }


@dataclass
class InstitutionMetrics:
    """机构持仓指标"""
    fund_holdings: Optional[float] = None       # 基金持仓比例
    fund_count: Optional[int] = None            # 持仓基金数
    institution_holdings: Optional[float] = None # 机构总持仓比例
    
    def to_dict(self) -> Dict:
        return {
            'fund_holdings': self.fund_holdings,
            'fund_count': self.fund_count,
            'institution_holdings': self.institution_holdings
        }


@dataclass
class FundamentalData:
    """完整基本面数据"""
    code: str
    name: str
    valuation: ValuationMetrics
    growth: GrowthMetrics
    profitability: ProfitabilityMetrics
    institution: InstitutionMetrics
    industry: Optional[str] = None          # 所属行业
    report_date: Optional[str] = None       # 最新财报日期
    source_chain: List[str] = None          # 数据来源链
    errors: List[str] = None                # 错误记录
    
    def __post_init__(self):
        if self.source_chain is None:
            self.source_chain = []
        if self.errors is None:
            self.errors = []
    
    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'name': self.name,
            'industry': self.industry,
            'report_date': self.report_date,
            'valuation': self.valuation.to_dict(),
            'growth': self.growth.to_dict(),
            'profitability': self.profitability.to_dict(),
            'institution': self.institution.to_dict(),
            'source_chain': self.source_chain,
            'errors': self.errors
        }
    
    def get_summary(self) -> str:
        """获取基本面摘要"""
        parts = []
        
        # 估值
        if self.valuation.pe_ttm:
            pe_status = "低估" if self.valuation.pe_ttm < 20 else "合理" if self.valuation.pe_ttm < 40 else "偏高"
            parts.append(f"PE:{self.valuation.pe_ttm:.1f}({pe_status})")
        
        if self.valuation.pb:
            parts.append(f"PB:{self.valuation.pb:.2f}")
        
        # 成长
        if self.growth.profit_growth_yoy:
            growth_status = "高增长" if self.growth.profit_growth_yoy > 30 else "稳健" if self.growth.profit_growth_yoy > 0 else "下滑"
            parts.append(f"净利增:{self.growth.profit_growth_yoy:.1f}%({growth_status})")
        
        # 盈利
        if self.profitability.roe:
            roe_status = "优秀" if self.profitability.roe > 15 else "良好" if self.profitability.roe > 10 else "一般"
            parts.append(f"ROE:{self.profitability.roe:.1f}%({roe_status})")
        
        return " | ".join(parts) if parts else "基本面数据不足"
    
    @property
    def is_complete(self) -> bool:
        """检查数据是否完整"""
        return (
            self.valuation.pe_ttm is not None and
            self.growth.profit_growth_yoy is not None and
            self.profitability.roe is not None
        )


class FundamentalDataAdapter:
    """
    基本面数据适配器 - 优化版
    
    特性:
    - fail-open: 部分失败也返回可用数据
    - 多候选接口: 一个失败自动试下一个
    - 错误记录: 记录哪些接口失败了
    """
    
    def __init__(self):
        self.enabled = True
        self.ak = None
        self._init_akshare()
    
    def _init_akshare(self):
        """初始化AkShare"""
        try:
            import akshare as ak
            self.ak = ak
            logger.info("✅ 基本面数据适配器初始化成功")
        except ImportError:
            logger.error("❌ 请先安装 akshare")
            self.enabled = False
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全的浮点数转换"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        s = str(value).strip().replace(",", "").replace("%", "")
        if not s or s in ("-", "nan", "None"):
            return None
        try:
            return float(s)
        except (TypeError, ValueError):
            return None
    
    def _normalize_code(self, raw: Any) -> str:
        """标准化股票代码"""
        s = str(raw).strip().upper()
        if "." in s:
            s = s.split(".", 1)[0]
        s = re.sub(r"^(SH|SZ|BJ)", "", s)
        return s
    
    def _call_akshare_candidates(self, candidates: List[Tuple[str, Dict]]) -> Tuple[Optional[pd.DataFrame], Optional[str], List[str]]:
        """
        尝试多个AkShare接口候选
        
        Returns:
            (df, 成功接口名, 错误列表)
        """
        errors = []
        
        if not self.enabled or self.ak is None:
            return None, None, ["akshare_not_available"]
        
        for func_name, kwargs in candidates:
            fn = getattr(self.ak, func_name, None)
            if fn is None:
                errors.append(f"{func_name}:not_found")
                continue
            
            try:
                df = fn(**kwargs)
                if isinstance(df, pd.Series):
                    df = df.to_frame().T
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return df, func_name, errors
            except Exception as exc:
                errors.append(f"{func_name}:{type(exc).__name__}")
                continue
        
        return None, None, errors
    
    def get_fundamental_data(self, code: str) -> FundamentalData:
        """
        获取完整基本面数据 - fail-open设计
        
        即使部分接口失败，也会返回已获取的数据
        """
        code = self._normalize_code(code)
        
        # 初始化结果（空数据）
        result = FundamentalData(
            code=code,
            name=code,
            valuation=ValuationMetrics(),
            growth=GrowthMetrics(),
            profitability=ProfitabilityMetrics(),
            institution=InstitutionMetrics()
        )
        
        if not self.enabled:
            result.errors.append("akshare_not_enabled")
            return result
        
        # 1. 获取股票名称和基本信息
        try:
            name = self._get_stock_name(code)
            if name:
                result.name = name
                result.source_chain.append("name:basic_info")
        except Exception as e:
            result.errors.append(f"name:{type(e).__name__}")
        
        # 2. 获取估值数据
        try:
            valuation, source = self._get_valuation(code)
            if valuation:
                result.valuation = valuation
                result.source_chain.append(f"valuation:{source}")
        except Exception as e:
            result.errors.append(f"valuation:{type(e).__name__}")
        
        # 3. 获取成长数据
        try:
            growth, source = self._get_growth(code)
            if growth:
                result.growth = growth
                result.source_chain.append(f"growth:{source}")
        except Exception as e:
            result.errors.append(f"growth:{type(e).__name__}")
        
        # 4. 获取盈利数据
        try:
            profitability, source = self._get_profitability(code)
            if profitability:
                result.profitability = profitability
                result.source_chain.append(f"profitability:{source}")
        except Exception as e:
            result.errors.append(f"profitability:{type(e).__name__}")
        
        # 5. 获取机构数据
        try:
            institution, source = self._get_institution(code)
            if institution:
                result.institution = institution
                result.source_chain.append(f"institution:{source}")
        except Exception as e:
            result.errors.append(f"institution:{type(e).__name__}")
        
        # 6. 获取行业
        try:
            industry = self._get_industry(code)
            if industry:
                result.industry = industry
                result.source_chain.append("industry:basic_info")
        except Exception as e:
            result.errors.append(f"industry:{type(e).__name__}")
        
        # 记录日志
        if result.errors:
            logger.warning(f"[基本面] {code} 部分接口失败: {result.errors}")
        
        if result.source_chain:
            logger.info(f"[基本面] {code} 数据来源: {result.source_chain}")
        
        return result
    
    def _get_stock_name(self, code: str) -> Optional[str]:
        """获取股票名称"""
        try:
            df = self.ak.stock_individual_info_em(symbol=code)
            if not df.empty:
                name_row = df[df['item'] == '股票简称']
                if not name_row.empty:
                    return str(name_row['value'].values[0])
        except Exception as e:
            logger.debug(f"获取股票名称失败: {e}")
        return None
    
    def _get_valuation(self, code: str) -> Tuple[Optional[ValuationMetrics], Optional[str]]:
        """获取估值数据 - 多候选"""
        candidates = [
            ('stock_zh_a_spot_em', {}),  # 从全市场数据中筛选
            ('stock_individual_info_em', {'symbol': code}),
        ]
        
        df, source, errors = self._call_akshare_candidates(candidates)
        
        if df is not None:
            # 尝试找到对应股票的数据
            if '代码' in df.columns:
                stock_row = df[df['代码'].astype(str).str.zfill(6) == code.zfill(6)]
            elif '股票代码' in df.columns:
                stock_row = df[df['股票代码'].astype(str).str.zfill(6) == code.zfill(6)]
            else:
                stock_row = df.iloc[0:1] if not df.empty else None
            
            if stock_row is not None and not stock_row.empty:
                row = stock_row.iloc[0]
                return ValuationMetrics(
                    pe_ttm=self._safe_float(row.get('市盈率-动态') or row.get('市盈率')),
                    pb=self._safe_float(row.get('市净率')),
                    market_cap=self._safe_float(row.get('总市值'))
                ), source
        
        return None, None
    
    def _get_growth(self, code: str) -> Tuple[Optional[GrowthMetrics], Optional[str]]:
        """获取成长数据 - 多候选"""
        candidates = [
            ('stock_financial_analysis_indicator', {'symbol': code}),
            ('stock_financial_abstract', {'symbol': code}),
            ('stock_financial_analysis_indicator', {}),  # 兜底
        ]
        
        df, source, errors = self._call_akshare_candidates(candidates)
        
        if df is not None and not df.empty:
            # 找到对应股票的行
            row = None
            for col in df.columns:
                if any(k in str(col) for k in ('代码', '股票代码', '证券代码')):
                    try:
                        matched = df[df[col].astype(str).str.zfill(6) == code.zfill(6)]
                        if not matched.empty:
                            row = matched.iloc[0]
                            break
                    except:
                        continue
            
            if row is None:
                row = df.iloc[0]
            
            # 提取成长数据
            revenue_yoy = None
            profit_yoy = None
            
            for col in df.columns:
                col_str = str(col)
                if revenue_yoy is None and any(k in col_str for k in ('营业收入同比', '营收同比', '收入同比')):
                    revenue_yoy = self._safe_float(row.get(col))
                if profit_yoy is None and any(k in col_str for k in ('净利润同比', '净利同比', '归母净利润同比')):
                    profit_yoy = self._safe_float(row.get(col))
            
            return GrowthMetrics(
                revenue_growth_yoy=revenue_yoy,
                profit_growth_yoy=profit_yoy
            ), source
        
        return None, None
    
    def _get_profitability(self, code: str) -> Tuple[Optional[ProfitabilityMetrics], Optional[str]]:
        """获取盈利数据 - 多候选"""
        candidates = [
            ('stock_financial_analysis_indicator', {'symbol': code}),
            ('stock_financial_abstract', {'symbol': code}),
            ('stock_financial_analysis_indicator', {}),
        ]
        
        df, source, errors = self._call_akshare_candidates(candidates)
        
        if df is not None and not df.empty:
            row = None
            for col in df.columns:
                if any(k in str(col) for k in ('代码', '股票代码', '证券代码')):
                    try:
                        matched = df[df[col].astype(str).str.zfill(6) == code.zfill(6)]
                        if not matched.empty:
                            row = matched.iloc[0]
                            break
                    except:
                        continue
            
            if row is None:
                row = df.iloc[0]
            
            # 提取盈利数据
            roe = None
            gross_margin = None
            eps = None
            
            for col in df.columns:
                col_str = str(col)
                if roe is None and any(k in col_str for k in ('净资产收益率', 'ROE')):
                    roe = self._safe_float(row.get(col))
                if gross_margin is None and '毛利率' in col_str:
                    gross_margin = self._safe_float(row.get(col))
                if eps is None and any(k in col_str for k in ('每股收益', 'EPS')):
                    eps = self._safe_float(row.get(col))
            
            return ProfitabilityMetrics(
                roe=roe,
                gross_margin=gross_margin,
                eps=eps
            ), source
        
        return None, None
    
    def _get_institution(self, code: str) -> Tuple[Optional[InstitutionMetrics], Optional[str]]:
        """获取机构持仓数据"""
        try:
            df = self.ak.stock_institute_hold_detail(stock=code)
            if df is not None and not df.empty:
                # 计算基金持仓
                fund_df = df[df['机构类型'] == '基金'] if '机构类型' in df.columns else df
                fund_holdings = fund_df['持股比例'].sum() if '持股比例' in fund_df.columns else 0
                fund_count = len(fund_df)
                
                return InstitutionMetrics(
                    fund_holdings=round(fund_holdings, 2) if fund_holdings > 0 else None,
                    fund_count=fund_count if fund_count > 0 else None
                ), "institute_hold"
        except Exception as e:
            logger.debug(f"获取机构数据失败: {e}")
        
        return None, None
    
    def _get_industry(self, code: str) -> Optional[str]:
        """获取所属行业"""
        try:
            df = self.ak.stock_individual_info_em(symbol=code)
            if not df.empty:
                for _, row in df.iterrows():
                    if row['item'] == '行业':
                        return str(row['value'])
        except Exception as e:
            logger.debug(f"获取行业数据失败: {e}")
        return None
    
    def get_industry_comparison(self, code: str) -> Optional[Dict]:
        """获取行业对比数据"""
        try:
            fundamental = self.get_fundamental_data(code)
            if not fundamental or not fundamental.industry:
                return None
            
            industry = fundamental.industry
            stock_pe = fundamental.valuation.pe_ttm
            
            # 获取行业PE数据
            try:
                df = self.ak.stock_zh_a_spot_em()
                if '所处行业' in df.columns and '市盈率-动态' in df.columns:
                    industry_stocks = df[df['所处行业'] == industry]
                    if not industry_stocks.empty:
                        industry_pe_median = industry_stocks['市盈率-动态'].median()
                        
                        return {
                            'industry': industry,
                            'industry_pe_median': round(industry_pe_median, 2) if pd.notna(industry_pe_median) else None,
                            'stock_pe': round(stock_pe, 2) if stock_pe else None,
                            'is_cheaper_than_industry': stock_pe < industry_pe_median if stock_pe and pd.notna(industry_pe_median) else None,
                            'pe_discount': round((industry_pe_median - stock_pe) / industry_pe_median * 100, 1) if stock_pe and pd.notna(industry_pe_median) and industry_pe_median > 0 else None
                        }
            except Exception as e:
                logger.debug(f"获取行业对比失败: {e}")
                
        except Exception as e:
            logger.warning(f"行业对比失败: {e}")
        
        return None
    
    def get_risk_alerts(self, code: str) -> List[str]:
        """获取基本面风险警报"""
        risks = []
        
        try:
            fundamental = self.get_fundamental_data(code)
            if not fundamental:
                return risks
            
            # 估值风险
            if fundamental.valuation.pe_ttm:
                if fundamental.valuation.pe_ttm > 100:
                    risks.append(f"PE(TTM)过高({fundamental.valuation.pe_ttm:.1f}),估值泡沫风险")
                elif fundamental.valuation.pe_ttm < 0:
                    risks.append("亏损状态,PE为负")
            
            # 成长风险
            if fundamental.growth.profit_growth_yoy and fundamental.growth.profit_growth_yoy < -30:
                risks.append(f"净利润大幅下滑({fundamental.growth.profit_growth_yoy:.1f}%)")
            
            if fundamental.growth.revenue_growth_yoy and fundamental.growth.revenue_growth_yoy < -20:
                risks.append(f"营收大幅下滑({fundamental.growth.revenue_growth_yoy:.1f}%)")
            
            # 盈利风险
            if fundamental.profitability.roe and fundamental.profitability.roe < 5:
                risks.append(f"ROE过低({fundamental.profitability.roe:.1f}%),盈利能力弱")
            
            # 机构风险
            if fundamental.institution.fund_holdings is not None and fundamental.institution.fund_holdings < 1:
                risks.append("机构持仓比例极低,缺乏机构关注")
            
        except Exception as e:
            logger.error(f"获取风险警报失败: {e}")
        
        return risks


# 全局适配器实例
fundamental_adapter = FundamentalDataAdapter()


if __name__ == "__main__":
    print("🧪 基本面数据聚合测试 - 优化版")
    print("=" * 60)
    
    adapter = FundamentalDataAdapter()
    
    if adapter.enabled:
        # 测试平安银行
        code = "000001"
        print(f"\n📊 分析股票: {code}")
        
        fundamental = adapter.get_fundamental_data(code)
        
        print(f"\n股票: {fundamental.name} ({fundamental.code})")
        print(f"行业: {fundamental.industry or '未知'}")
        print(f"数据来源: {fundamental.source_chain}")
        
        if fundamental.errors:
            print(f"错误记录: {fundamental.errors}")
        
        print(f"\n📈 估值指标:")
        print(f"  PE(TTM): {fundamental.valuation.pe_ttm}")
        print(f"  PB: {fundamental.valuation.pb}")
        print(f"  市值: {fundamental.valuation.market_cap}亿")
        
        print(f"\n📊 成长指标:")
        print(f"  营收增长: {fundamental.growth.revenue_growth_yoy}%")
        print(f"  净利增长: {fundamental.growth.profit_growth_yoy}%")
        
        print(f"\n💰 盈利指标:")
        print(f"  ROE: {fundamental.profitability.roe}%")
        print(f"  毛利率: {fundamental.profitability.gross_margin}%")
        print(f"  EPS: {fundamental.profitability.eps}")
        
        print(f"\n🏦 机构持仓:")
        print(f"  基金持仓: {fundamental.institution.fund_holdings}%")
        print(f"  持仓基金数: {fundamental.institution.fund_count}")
        
        print(f"\n📝 摘要: {fundamental.get_summary()}")
        print(f"数据完整: {'是' if fundamental.is_complete else '否'}")
        
        # 风险警报
        risks = adapter.get_risk_alerts(code)
        if risks:
            print(f"\n⚠️ 风险警报:")
            for risk in risks:
                print(f"  - {risk}")
        else:
            print("\n✅ 无明显基本面风险")
        
        # 行业对比
        comparison = adapter.get_industry_comparison(code)
        if comparison:
            print(f"\n🏭 行业对比:")
            print(f"  行业: {comparison.get('industry')}")
            print(f"  行业中位数PE: {comparison.get('industry_pe_median')}")
            print(f"  个股PE: {comparison.get('stock_pe')}")
            print(f"  相对行业: {'低估' if comparison.get('is_cheaper_than_industry') else '高估'}")
    else:
        print("⚠️ 适配器未启用")
    
    print("\n" + "=" * 60)
    print("✅ 测试完成")

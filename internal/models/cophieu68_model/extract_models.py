from dataclasses import dataclass, field
from typing import List, Dict, Optional
import pandas as pd

# ============================================================
# 📌 DATA MODELS
# ============================================================

@dataclass
class StockBasicInfo:
    """Thông tin cơ bản của cổ phiếu"""
    symbol: str
    company_name: str = ""
    current_price: str = ""
    price_change: str = ""
    percent_change: str = ""
    reference_price: str = ""
    open_price: str = ""
    high_price: str = ""
    low_price: str = ""
    volume: str = ""
    timestamp: str = ""


@dataclass
class StockFinancialRatios:
    """Các chỉ số tài chính"""
    symbol: str
    book_value: str = ""       # Giá sổ sách
    eps: str = ""
    pe_ratio: str = ""
    pb_ratio: str = ""
    roa: str = ""
    roe: str = ""
    beta: str = ""
    market_cap: str = ""       # Vốn thị trường
    listed_volume: str = ""    # KL niêm yết
    avg_volume_52w: str = ""   # KLGD 52w
    high_low_52w: str = ""     # Cao-thấp 52w


@dataclass
class StockPowerRatings:
    """Sức mạnh các chỉ số"""
    symbol: str
    eps_power: str = ""
    roe_power: str = ""
    investment_efficiency: str = ""
    pb_power: str = ""
    price_growth_power: str = ""


@dataclass
class TradingData:
    """Dữ liệu giao dịch"""
    symbol: str
    buy_orders: List[Dict] = field(default_factory=list)
    sell_orders: List[Dict] = field(default_factory=list)
    foreign_buy: str = ""
    foreign_sell: str = ""


@dataclass
class FinancialStatement:
    """Báo cáo tài chính (tóm tắt)"""
    symbol: str
    period: str = ""
    revenue: str = ""
    profit_before_tax: str = ""
    net_profit: str = ""
    parent_profit: str = ""
    total_assets: str = ""
    total_debt: str = ""
    owner_equity: str = ""


@dataclass
class BusinessPlan:
    """Kế hoạch kinh doanh"""
    symbol: str
    year: str = ""
    revenue_plan: str = ""
    revenue_achievement: str = ""
    profit_plan: str = ""
    profit_achievement: str = ""


@dataclass
class IndustryInfo:
    """Thông tin ngành"""
    symbol: str
    industry_name: str = ""
    market_name: str = ""
    industry_influence_percent: str = ""


@dataclass
class CompanyProfile:
    """Thông tin chi tiết công ty"""
    symbol: str
    full_name: str = ""
    english_name: str = ""
    short_name: str = ""
    address: str = ""
    phone: str = ""
    fax: str = ""
    website: str = ""
    email: str = ""
    established_date: str = ""
    listed_date: str = ""
    chartered_capital: str = ""
    business_license: str = ""
    tax_code: str = ""


@dataclass
class CompleteStockData:
    """Dữ liệu tổng hợp của một cổ phiếu"""
    basic_info: Optional[StockBasicInfo] = None
    financial_ratios: Optional[StockFinancialRatios] = None
    power_ratings: Optional[StockPowerRatings] = None
    trading_data: Optional[TradingData] = None
    financial_statements: List[FinancialStatement] = field(default_factory=list)
    business_plans: List[BusinessPlan] = field(default_factory=list)
    industry_info: Optional[IndustryInfo] = None
    company_profile: Optional[CompanyProfile] = None


# ============================================================
# 📌 FINANCIAL REPORTS (raw DataFrame format)
# ============================================================

@dataclass
class StockFinancialReport:
    """Báo cáo tài chính (raw DataFrame, generic)"""
    symbol: str
    report_type: str       # "income" | "balance" | "cashflow"
    table_index: int       # bảng số mấy trong page
    data: pd.DataFrame     # dữ liệu báo cáo


@dataclass
class IncomeStatementReport:
    """Báo cáo KQKD"""
    symbol: str
    period: Optional[str]
    table_index: int
    data: pd.DataFrame


@dataclass
class BalanceSheetReport:
    """Báo cáo Bảng cân đối kế toán"""
    symbol: str
    period: Optional[str]
    table_index: int
    data: pd.DataFrame


@dataclass
class CashflowStatementReport:
    """Báo cáo Lưu chuyển tiền tệ"""
    symbol: str
    period: Optional[str]
    table_index: int
    data: pd.DataFrame


# ============================================================
# 📌 PARSING CONFIGS
# ============================================================

CRAWL_MARKET_LIST_CONFIG = {
    "link_pattern": r'/quote/summary\.php\?id=',
    "symbol_regex": r'id=([A-Z0-9]+)',
}

CRAWL_INDUSTRY_LIST_CONFIG = {
    "link_pattern": r'category',
    "limit": 20,   # số ngành tối đa crawl
}

FINANCIAL_MAPPING = {
    r"giá sổ sách": "book_value",
    r"eps": "eps",
    r"\bpe\b": "pe_ratio",
    r"\bpb\b": "pb_ratio",
    r"roa": "roa",
    r"roe": "roe",
    r"beta": "beta",
    r"vốn thị trường": "market_cap",
    r"kl niêm yết": "listed_volume",
    r"klgd 52w": "avg_volume_52w",
    r"cao.*thấp 52w": "high_low_52w",
}

CRAWL_TRADING_DATA_CONFIG = {
    "table_identifiers": ["MUA", "BÁN"],   # keyword nhận diện bảng
    "max_rows": 5,
    "foreign_buy_selector": "#foreigner_buy_volume",
    "foreign_sell_selector": "#foreigner_sell_volume",
}

CRAWL_BUSINESS_PLAN_CONFIG = {
    "container_id": "business_plan",
    "min_columns": 5,
}

CRAWL_INDUSTRY_INFO_CONFIG = {
    "header_text": "Ngành/Nhóm/Họ",
    "strip_parentheses": True,
}

CRAWL_COMPANY_PROFILE_CONFIG = {
    "field_map": {
        "tên đầy đủ": "full_name",
        "tên công ty": "full_name",
        "tên tiếng anh": "english_name",
        "tên viết tắt": "short_name",
        "địa chỉ": "address",
        "điện thoại": "phone",
        "fax": "fax",
        "website": "website",
        "email": "email",
        "ngày thành lập": "established_date",
        "ngày niêm yết": "listed_date",
        "vốn điều lệ": "chartered_capital",
        "giấy phép kinh doanh": "business_license",
        "mã số thuế": "tax_code",
    }
}

CRAWL_COMPLETE_STOCK_CONFIG = {
    "summary_submodules": [
        "crawl_basic_info",
        "crawl_financial_ratios",
        "crawl_balance_sheet",
        "crawl_power_ratings",
        "crawl_trading_data",
        "crawl_financial_statements",
        "crawl_business_plan",
        "crawl_industry_info",
    ],
    "profile_module": "crawl_company_profile",
}

CRAWL_MULTIPLE_STOCKS_CONFIG = {
    "max_workers_default": 5
}

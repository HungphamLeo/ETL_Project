from dataclasses import dataclass, field
from typing import List, Dict, Optional
from dataclasses import asdict
import pandas as pd
import json

# ============================================================
# 📌 DATA MODELS
# ============================================================
@dataclass
class PriceInfo:
    """Thông tin giá cổ phiếu"""
    current_price: str = ""
    price_change: str = "#stockname_price_change"
    percent_change: str = "#stockname_percent_change"
    reference_price: str = ""
    open_price: str = ""
    high_price: str = "#stockname_price_highest"
    low_price: str = "#stockname_price_lowest"
    close_price: str = "#stockname_close"
    volume: str = "#stockname_volume"
    timestamp: str = ""



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
    """Chỉ tiêu tóm tắt đầu trang của cổ phiếu (summary.php?id=xxx)."""
    symbol: str
    reference_price: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    volume: Optional[int] = None
    book_value: Optional[str] = None
    eps: Optional[str] = None
    pe: Optional[str] = None
    pb: Optional[str] = None
    roa_roe: Optional[str] = None
    beta: Optional[float] = None
    market_cap: Optional[str] = None
    listed_volume: Optional[str] = None
    avg_volume_52w: Optional[int] = None
    high_low_52w: Optional[str] = None
    debt: Optional[str] = None
    equity: Optional[str] = None
    debt_to_equity: Optional[str] = None
    equity_to_assets: Optional[str] = None
    cash: Optional[str] = None
    eps_power: Optional[str] = None
    roe_power: Optional[str] = None
    invest_efficiency: Optional[str] = None
    pb_power: Optional[str] = None
    price_growth_power: Optional[str] = None

    def to_df(self) -> pd.DataFrame:
        """Convert toàn bộ dataclass thành 1 DataFrame hàng duy nhất"""
        return pd.DataFrame([self.__dict__])



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
class DetailsMatchRow:
    Time_match: str
    Price_match: float
    Increase_decrease: str
    Volume: int
    Accum_volume: int


@dataclass
class DetailsMatchReport:
    symbol: str
    data: pd.DataFrame


@dataclass
class BusinessPlanRow:
    Year: str
    Plan_revenue: float
    Pass_revenue: float
    Plan_profit: float
    Pass_profit: float


@dataclass
class BusinessPlanReport:
    symbol: str
    data: pd.DataFrame



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




# ============================================================
# 📌 FINANCIAL REPORTS (raw DataFrame format)
# ============================================================

@dataclass
class StockFinancialReport:
    """Generic report wrapper"""
    symbol: str
    report_type: str       # "income" | "balance" | "cashflow" | "business_plan" | "details_match"
    table_index: int
    data: pd.DataFrame

@dataclass
class IncomeStatementReport:
    symbol: str
    data: pd.DataFrame

@dataclass
class BalanceSheetReport:
    symbol: str
    data: pd.DataFrame

@dataclass
class CashflowStatementReport:
    symbol: str
    data: pd.DataFrame

@dataclass
class BusinessPlanReport:
    symbol: str
    data: pd.DataFrame

@dataclass
class DetailsMatchReport:
    symbol: str
    data: pd.DataFrame

@dataclass
class IndustrySummaryInfo:
    industry_code: str
    industry_name: str
    industry_url: str
    index: Optional[str] = None
    change: Optional[str] = None
    liquidity: Optional[str] = None
    capital: Optional[str] = None

@dataclass
class IndustryFinancialInfo:
    industry_code: str
    industry_name: str
    industry_url: str
    avg_price: Optional[str] = None
    book_value: Optional[str] = None
    eps: Optional[str] = None
    pe: Optional[str] = None
    roa: Optional[str] = None
    roe: Optional[str] = None

@dataclass
class IndustryCapitalInfo:
    industry_code: str
    industry_name: str
    industry_url: str
    total_asset: Optional[str] = None
    total_equity: Optional[str] = None
    total_liabilities: Optional[str] = None
    percentage_debt_on_equity: Optional[str] = None
    percentage_equity_on_assets: Optional[str] = None
    revenue: Optional[str] = None
    profit_before_tax: Optional[str] = None

@dataclass
class TradingRecord:
    date: str
    close_price: float
    volume: int
    open_price: float
    high_price: float
    low_price: float
    foreign_buy: int
    foreign_sell: int
    foreign_value: float

@dataclass
class TradingData:
    symbol: str
    records: List[TradingRecord] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "symbol": self.symbol,
            "records": [asdict(r) for r in self.records]
        }, ensure_ascii=False, indent=2)
# ============================================================
# 📌 PARSING CONFIGS
# ============================================================

CRAWL_MARKET_LIST_CONFIG = {
    "VNINDEX" : "vnindex",
    "HNX": "hastc",
    "UPCOM": "upcom",
    "VN30": "vn30"


}

CRAWL_INDUSTRY_LIST_CONFIG = {
    "Bán buôn": "^bb",
    "Bất động sản": "^bds",
    "Bảo hiểm": "^bh",
    "Bán lẻ": "^bl",
    "Chế biến Thủy sản": "^cbts",
    "Chứng khoán": "^ck",
    "Công nghệ và Thông tin": "^cntt",
    "Chăm sóc sức khỏe": "^cssk",
    "Dịch vụ lưu trú, ăn uống, giải trí": "^dvltaugt",
    "Dịch vụ tư vấn, hỗ trợ": "^dvtvht",
    "Khai khoáng": "^kk",
    "Ngân hàng": "^nh",
    "Nông - Lâm - Ngư nghiệp": "^nln",
    "Sản phẩm cao su": "^spcs",
    "Sản xuất Hàng gia dụng": "^sxhgd",
    "Sản xuất Nhựa - Hóa chất": "^sxnhc",
    "Sản xuất Phụ trợ": "^sxpt",
    "Sản xuất Thiết bị, máy móc": "^sxtbmm",
    "Thiết bị điện": "^tbd",
    "Tài chính khác": "^tck",
    "Tiện ích": "^ti",
    "Thực phẩm - Đồ uống": "^tpdu",
    "Vật liệu xây dựng": "^vlxd",
    "Vận tải - kho bãi": "^vtkb",
    "Xây dựng": "^xd",
    "Cao su": "^caosu",
    "Nhóm Dầu khí": "^daukhi",
    "Dược phẩm / Y tế / Hóa chất": "^duocpham",
    "Giáo dục": "^giaoduc",
    "Hàng không": "^hk",
    "Năng lượng (Điện/Khí/...)": "^nangluong",
    "Nhựa - Bao bì": "^nhua",
    "Phân bón": "^phanbon",
    "Ngành Thép": "^thep",
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


INDUSTRIAL_INFO_TYPE = {"summary_info": 0, "financial_info": 2, "fund_info": 3}

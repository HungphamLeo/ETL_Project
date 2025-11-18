from dataclasses import dataclass, field
from typing import List, Dict, Optional
from dataclasses import asdict
import pandas as pd
import json

# ============================================================
# 📌 DATA MODELS
# ============================================================

@dataclass
class StockFinancialReport:
    """Báo cáo tài chính cổ phiếu"""
    symbol: str
    report_type: str
    table_index: int
    data: pd.DataFrame

@dataclass
class IndustrySummaryInfo:
    index: Optional[str] = None
    change: Optional[str] = None
    liquidity: Optional[str] = None
    capital: Optional[str] = None


@dataclass
class IndustryFinancialInfo:
    avg_price: Optional[str] = None
    book_value: Optional[str] = None
    eps: Optional[str] = None
    pe: Optional[str] = None
    roa: Optional[str] = None
    roe: Optional[str] = None


@dataclass
class IndustryCapitalInfo:
    supply_volumn: Optional[str] = None
    total_asset: Optional[str] = None
    total_equity: Optional[str] = None
    total_liabilities: Optional[str] = None
    percentage_debt_on_equity: Optional[str] = None
    percentage_equity_on_assets: Optional[str] = None
    revenue: Optional[str] = None
    profit_before_tax: Optional[str] = None

@dataclass
class BalanceSheet:
    """Bảng cân đối kế toán (tóm tắt)"""
    symbol: str
    report_type: str
    data: pd.DataFrame


@dataclass
class IncomeStatement:
    """Báo cáo kết quả hoạt động / Báo cáo thu nhập (tóm tắt)"""
    symbol: str
    report_type: str
    data: pd.DataFrame

@dataclass
class StockBasicInfo:
    """Thông tin cơ bản cổ phiếu"""
    symbol: str
    company_name: str = ""
    current_price: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    reference_price: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    volume: Optional[int] = None
    timestamp: str = ""


@dataclass
class StockFinancialRatios:
    """Chỉ tiêu tóm tắt đầu trang"""
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
    avg_volume_52w: Optional[str] = None
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
        return pd.DataFrame([asdict(self)])


@dataclass
class DetailsMatchRow:
    """Dòng dữ liệu khớp lệnh"""
    Time_match: str
    Price_match: float
    Increase_decrease: str
    Volume: int
    Accum_volume: int


@dataclass
class DetailsMatchReport:
    """Chi tiết khớp lệnh"""
    symbol: str
    data: list


@dataclass
class BusinessPlanRow:
    """Dòng kế hoạch kinh doanh"""
    Year: str
    Plan_revenue: float
    Pass_revenue: float
    Plan_profit: float
    Pass_profit: float


@dataclass
class BusinessPlanReport:
    """Kế hoạch kinh doanh"""
    symbol: str
    data: pd.DataFrame


@dataclass
class FinancialStatementReport:
    """Báo cáo tài chính (bảng HTML)"""
    symbol: str
    report_type: str
    data: pd.DataFrame


@dataclass
class CompanyProfile:
    """Thông tin công ty"""
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
class TradingRecord:
    """Dòng giao dịch"""
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
    """Dữ liệu giao dịch"""
    symbol: str
    records: List[TradingRecord] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "symbol": self.symbol,
            "records": [asdict(r) for r in self.records]
        }, ensure_ascii=False, indent=2)




INDUSTRIAL_INFO_TYPE = {"summary_info": 0, "financial_info": 2, "fund_info": 3}

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
CRAWL_MARKET_LIST_CONFIG = { "VNINDEX" : "vnindex", 
                            "HNX": "hastc", 
                            "UPCOM": "upcom", 
                            "VN30": "vn30" }
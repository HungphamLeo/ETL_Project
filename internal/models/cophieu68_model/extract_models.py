from dataclasses import dataclass, asdict, field

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
    book_value: str = ""  # Giá sổ sách
    eps: str = ""
    pe_ratio: str = ""
    pb_ratio: str = ""
    roa: str = ""
    roe: str = ""
    beta: str = ""
    market_cap: str = ""  # Vốn thị trường
    listed_volume: str = ""  # KL niêm yết
    avg_volume_52w: str = ""  # KLGD 52w
    high_low_52w: str = ""  # Cao-thấp 52w


@dataclass
class StockBalanceSheet:
    """Cấu trúc tài chính"""
    symbol: str
    total_debt: str = ""  # Nợ
    owner_equity: str = ""  # Vốn CSH
    debt_equity_ratio: str = ""  # %Nợ/VốnCSH
    equity_asset_ratio: str = ""  # %Vốn CSH/TàiSản
    cash: str = ""  # Tiền mặt


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
    """Báo cáo tài chính"""
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
    """Dữ liệu hoàn chỉnh của một cổ phiếu"""
    basic_info: Optional[StockBasicInfo] = None
    financial_ratios: Optional[StockFinancialRatios] = None
    balance_sheet: Optional[StockBalanceSheet] = None
    power_ratings: Optional[StockPowerRatings] = None
    trading_data: Optional[TradingData] = None
    financial_statements: List[FinancialStatement] = field(default_factory=list)
    business_plans: List[BusinessPlan] = field(default_factory=list)
    industry_info: Optional[IndustryInfo] = None
    company_profile: Optional[CompanyProfile] = None

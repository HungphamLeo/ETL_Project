from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Iterable
import pandas as pd
import json
from datetime import datetime


def _df_to_records(obj: Any) -> List[Dict[str, Any]]:
    """Convert pd.DataFrame / list / dict / scalar -> list[dict] safe for JSON storage."""
    if obj is None:
        return []
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")
    if isinstance(obj, list):
        out = []
        for it in obj:
            if isinstance(it, pd.DataFrame):
                out.extend(_df_to_records(it))
            elif isinstance(it, dict):
                out.append({k: (None if v is pd.NA else v) for k, v in it.items()})
            else:
                out.append({"value": it})
        return out
    if isinstance(obj, dict):
        return [obj]
    return [{"value": obj}]


@dataclass
class BaseDoc:
    symbol: Optional[str] = None
    data: List[Dict[str, Any]] = field(default_factory=list)
    report_type: Optional[str] = None
    update_time: Optional[str] = None

    def to_mongo_dict(self) -> Dict[str, Any]:
        d = {"symbol": self.symbol, "data": self.data}
        if self.report_type:
            d["report_type"] = self.report_type
        d["update_time"] = self.update_time or datetime.utcnow().isoformat()
        return d

    @classmethod
    def from_extract(cls, payload: Any) -> "BaseDoc":
        # payload may be dataclass from extract_models or raw dict
        if payload is None:
            return cls()
        if hasattr(payload, "__dict__"):
            src = payload.__dict__
        elif isinstance(payload, dict):
            src = payload
        else:
            return cls()
        symbol = src.get("symbol")
        report_type = src.get("report_type")
        data = src.get("data") or src.get("records") or src.get("metrics") or src.get("table") or []
        normalized = _df_to_records(data)
        return cls(symbol=symbol, data=normalized, report_type=report_type)


@dataclass
class TradingDataDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "trade_datetime": r.get("date") or r.get("datetime") or r.get("trade_datetime"),
                "close_price": r.get("close_price") or r.get("close") or r.get("price"),
                "volume": r.get("volume"),
                "open_price": r.get("open_price"),
                "high_price": r.get("high_price"),
                "low_price": r.get("low_price"),
                "foreign_buy": r.get("foreign_buy"),
                "foreign_sell": r.get("foreign_sell"),
                "foreign_net_value": r.get("foreign_value")
            })
        return rows


@dataclass
class FinancialInfoDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "symbol": r.get("symbol"),
                "reference_price": r.get("reference_price"),
                "open_price": r.get("open_price"),
                "high_price": r.get("high_price"),
                "low_price": r.get("low_price"),
                "volume": r.get("volume"),
                "book_value": r.get("book_value"),
                "pe": r.get("pe") or r.get("pe_ratio"),
                "pb": r.get("pb"),
                "roe": r.get("roe"),
                "roa": r.get("roa"),
                "beta": r.get("beta"),
                "market_cap": r.get("market_cap") or r.get("market_cap"),
                "listed_volume": r.get("listed_volume") or r.get("listed_volume"),
                "avg_volume_52w": r.get("avg_volume_52w"),
                "high_low_52w": r.get("high_low_52w"),
                "debt": r.get("debt"),
                "equity": r.get("equity"),
                "debt_to_equity": r.get("debt_to_equity"),
                "equity_to_assets": r.get("equity_to_assets"),
                "cash": r.get("cash"),
                "eps_power": r.get("eps_power"),
                "roe_power": r.get("roe_power"),
                "invest_efficiency": r.get("invest_efficiency"),
                "pb_power": r.get("pb_power"),
                "price_growth_power": r.get("price_growth_power")
               
            })
        return rows


@dataclass
class IncomeStatementDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "period": r.get("period") or r.get("report_date"),
                "revenue": r.get("revenue") or r.get("total_revenue"),
                "operating_profit": r.get("operating_profit"),
                "net_income": r.get("net_income") or r.get("netprofit"),
                "eps": r.get("eps"),
                "source_json": r
            })
        return rows


@dataclass
class BalanceSheetDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "period": r.get("period") or r.get("report_date"),
                "total_assets": r.get("total_assets") or r.get("assets"),
                "total_liabilities": r.get("total_liabilities") or r.get("liabilities"),
                "shareholder_equity": r.get("shareholder_equity") or r.get("equity"),
                "cash": r.get("cash"),
                "inventory": r.get("inventory"),
                "source_json": r
            })
        return rows


@dataclass
class MatchDetailsDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "match_datetime": r.get("Time_match") or r.get("time") or r.get("match_datetime"),
                "price": r.get("Price_match") or r.get("price"),
                "volume": r.get("Volume") or r.get("volume"),
                "fluctuation_range": r.get("Increase_decrease") or r.get("fluctuate_range"),
                "accum_volume": r.get("Accum_volume") or r.get("accum_volume")
                
            })
        return rows


@dataclass
class BusinessPlanDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "year": r.get("Year") or r.get("year") or r.get("period"),
                "target_revenue": r.get("Plan_revenue") or r.get("target_revenue"),
                "target_profit": r.get("Plan_profit") or r.get("target_profit"),
                "capex_plan": r.get("Capex") or r.get("capex_plan"),
                "source_json": r
            })
        return rows

@dataclass
class IndustrialDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        
        for r in self.data:
            rows.append({
                "year": r.get("Year") or r.get("year") or r.get("period"),
                "target_revenue": r.get("Plan_revenue") or r.get("target_revenue"),
                "target_profit": r.get("Plan_profit") or r.get("target_profit"),
                "capex_plan": r.get("Capex") or r.get("capex_plan"),
                "source_json": r
            })
        return rows
# helpers for factory selection
_DOC_TYPE_MAP = {
    "trading": TradingDataDoc,
    "financial_info": FinancialInfoDoc,
    "income_statement": IncomeStatementDoc,
    "balance_sheet": BalanceSheetDoc,
    "match_details": MatchDetailsDoc,
    "business_plan": BusinessPlanDoc,
    "default": BaseDoc
}

index_information = {
    "VNINDEX": "VNINDEX là chỉ số phản ánh biến động giá của toàn bộ cổ phiếu niêm yết trên Sở Giao dịch Chứng khoán TP. Hồ Chí Minh (HOSE), ngoại trừ các cổ phiếu thuộc diện bị hạn chế giao dịch.",
    "HNX":"HNX Index là chỉ số phản ánh biến động giá của toàn bộ cổ phiếu niêm yết trên Sở Giao dịch Chứng khoán Hà Nội (HNX), ngoại trừ các cổ phiếu thuộc diện bị hạn chế giao dịch.",
    "UPCOM":"UPCOM Index là chỉ số phản ánh biến động giá của các cổ phiếu đăng ký giao dịch trên thị trường UPCOM do Sở Giao dịch Chứng khoán Hà Nội quản lý.Bao gồm nhiều doanh nghiệp nhà nước cổ phần hóa, doanh nghiệp quy mô nhỏ hoặc đang trong giai đoạn chuyển tiếp",
    "VN30":"VN30 Index là chỉ số phản ánh biến động giá của 30 cổ phiếu có giá trị vốn hóa lớn nhất và thanh khoản cao nhất trên Sở Giao dịch Chứng khoán TP. Hồ Chí Minh (HOSE)."
}
def doc_from_extract(kind: str, payload: Any) -> BaseDoc:
    cls = _DOC_TYPE_MAP.get(kind, BaseDoc)
    return cls.from_extract(payload)

@dataclass
class Pattern_IncomeStatementStandardLoadToDW():
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_name: Optional[str] = None
    year:Optional[str] = None
    period:Optional[str] = None
    metric_code:Optional[str] = None
    metric_name_en:Optional[str] = None
    metric_group:Optional[str] = None
    metric_value:Optional[float] = None
    update_time:Optional[str] = None

    METRIC_MAPPING = {

    # timeline
    "Quý":{
        "Q1": "Quarter_1",
        "Q2": "Quarter_2",
        "Q3": "Quarter_3",
        "Q4": "Quarter_4"
    },
    # --- Core Income & Expense ---
    "Thu nhập lãi thuần": {
        "metric_code": "NET_INTEREST_INCOME",
        "metric_name_en": "Net Interest Income",
        "metric_group": "CORE_INCOME_EXPENSE"
    },
    "Thu nhập từ lãi và các khoản thu nhập tương tự": {
        "metric_code": "INTEREST_AND_SIMILAR_INCOME",
        "metric_name_en": "Interest and Similar Income",
        "metric_group": "CORE_INCOME_EXPENSE"
    },
    "Chi phí lãi và các chi phí tương tự": {
        "metric_code": "INTEREST_AND_SIMILAR_EXPENSES",
        "metric_name_en": "Interest and Similar Expenses",
        "metric_group": "CORE_INCOME_EXPENSE"
    },
    "Lãi/Lỗ thuần từ hoạt động dịch vụ": {
        "metric_code": "NET_FEE_COMMISSION_INCOME",
        "metric_name_en": "Net Fee and Commission Income",
        "metric_group": "CORE_INCOME_EXPENSE"
    },
    "Thu nhập từ hoạt động dịch vụ": {
        "metric_code": "FEE_COMMISSION_INCOME",
        "metric_name_en": "Fee and Commission Income",
        "metric_group": "CORE_INCOME_EXPENSE"
    },
    "Chi phí hoạt động dịch vụ": {
        "metric_code": "FEE_COMMISSION_EXPENSES",
        "metric_name_en": "Fee and Commission Expenses",
        "metric_group": "CORE_INCOME_EXPENSE"
    },

    # --- Financial Trading ---
    "Lãi/Lỗ thuần từ hoạt động kinh doanh ngoại hối": {
        "metric_code": "NET_FX_TRADING_INCOME",
        "metric_name_en": "Net Foreign Exchange Trading Income",
        "metric_group": "FINANCIAL_TRADING"
    },
    "Lãi/Lỗ thuần từ mua bán chứng khoán kinh doanh": {
        "metric_code": "NET_TRADING_SECURITIES_INCOME",
        "metric_name_en": "Net Trading Securities Income",
        "metric_group": "FINANCIAL_TRADING"
    },
    "Lãi/Lỗ thuần từ mua bán chứng khoán đầu tư": {
        "metric_code": "NET_INVESTMENT_SECURITIES_INCOME",
        "metric_name_en": "Net Investment Securities Income",
        "metric_group": "FINANCIAL_TRADING"
    },

    # --- Other Operating Activities ---
    "Lãi/Lỗ thuần từ hoạt động khác": {
        "metric_code": "NET_OTHER_OPERATING_INCOME",
        "metric_name_en": "Net Other Operating Income",
        "metric_group": "OTHER_OPERATING"
    },
    "Thu nhập từ hoạt động khác": {
        "metric_code": "OTHER_OPERATING_INCOME",
        "metric_name_en": "Other Operating Income",
        "metric_group": "OTHER_OPERATING"
    },
    "Chi phí hoạt động khác": {
        "metric_code": "OTHER_OPERATING_EXPENSES",
        "metric_name_en": "Other Operating Expenses",
        "metric_group": "OTHER_OPERATING"
    },
    "Thu nhập từ hoạt động góp vốn mua cổ phần": {
        "metric_code": "INCOME_FROM_EQUITY_INVESTMENTS",
        "metric_name_en": "Income from Equity Investments",
        "metric_group": "OTHER_OPERATING"
    },

    # --- Expenses & Profit ---
    "Chi phí hoạt động": {
        "metric_code": "OPERATING_EXPENSES",
        "metric_name_en": "Operating Expenses",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Lợi nhuận từ HĐKD trước chi phí dự phòng rủi ro tín dụng": {
        "metric_code": "OPERATING_PROFIT_BEFORE_CREDIT_PROVISION",
        "metric_name_en": "Operating Profit before Credit Risk Provision",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Chi phí dự phòng rủi ro tín dụng": {
        "metric_code": "CREDIT_RISK_PROVISION_EXPENSES",
        "metric_name_en": "Credit Risk Provision Expenses",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Tổng lợi nhuận trước thuế": {
        "metric_code": "PROFIT_BEFORE_TAX",
        "metric_name_en": "Profit before Tax",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Chi phí thuế TNDN": {
        "metric_code": "CORPORATE_INCOME_TAX_EXPENSE",
        "metric_name_en": "Corporate Income Tax Expense",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Chi phí thuế thu nhập hiện hành": {
        "metric_code": "CURRENT_INCOME_TAX_EXPENSE",
        "metric_name_en": "Current Income Tax Expense",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Chi phí thuế TNDN hoãn lại": {
        "metric_code": "DEFERRED_INCOME_TAX_EXPENSE",
        "metric_name_en": "Deferred Income Tax Expense",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Lợi nhuận sau thuế thu nhập doanh nghiệp": {
        "metric_code": "PROFIT_AFTER_TAX",
        "metric_name_en": "Profit after Tax",
        "metric_group": "EXPENSES_PROFIT"
    },
    "Lợi ích của cổ đông thiểu số và cổ tức ưu đãi": {
        "metric_code": "MINORITY_INTERESTS_AND_PREFERRED_DIVIDENDS",
        "metric_name_en": "Minority Interests and Preferred Dividends",
        "metric_group": "EXPENSES_PROFIT"
    },
    "LNST sau khi điều chỉnh lợi ích của CĐTS và cổ tức ưu đãi": {
        "metric_code": "NET_PROFIT_ATTRIBUTABLE_TO_PARENT",
        "metric_name_en": "Net Profit Attributable to Parent Shareholders",
        "metric_group": "EXPENSES_PROFIT"
    }
}



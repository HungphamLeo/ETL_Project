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
                "price": r.get("close_price") or r.get("close") or r.get("price"),
                "volume": r.get("volume"),
                "value": (r.get("close_price") or r.get("close") or 0) * (r.get("volume") or 0),
                "side": r.get("side"),
                "source_json": r
            })
        return rows


@dataclass
class FinancialInfoDoc(BaseDoc):
    def to_fact_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for r in self.data:
            rows.append({
                "period": r.get("period") or r.get("report_date") or r.get("date"),
                "pe": r.get("pe"),
                "roe": r.get("roe"),
                "roa": r.get("roa"),
                "debt_equity": r.get("debt_equity") or r.get("debtToEquity"),
                "market_cap": r.get("market_cap") or r.get("marketCap"),
                "source_json": r
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
                "broker": r.get("Broker") or r.get("broker"),
                "source_json": r
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
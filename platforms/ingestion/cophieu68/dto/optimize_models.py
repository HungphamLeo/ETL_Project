
from dataclasses import dataclass
from typing import Optional
from platforms.ingestion.cophieu68.dto.transform_models import Pattern_BalanceSheetStandardLoadToDW, Pattern_IncomeStatementStandardLoadToDW


@dataclass
class BalanceSheetAsset:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    CASH_AND_VALUABLES: Optional[float] = None
    DEPOSITS_WITH_CENTRAL_BANK: Optional[float] = None
    T_BILLS_AND_ELIGIBLE_SHORT_TERM_SECURITIES: Optional[float] = None
    INTERBANK_PLACEMENTS_AND_LOANS: Optional[float] = None
    TRADING_SECURITIES_NET: Optional[float] = None
    DERIVATIVES_AND_OTHER_FINANCIAL_ASSETS: Optional[float] = None
    LOANS_TO_CUSTOMERS_NET: Optional[float] = None
    INVESTMENT_SECURITIES_NET: Optional[float] = None
    LONG_TERM_INVESTMENTS_NET: Optional[float] = None
    FIXED_ASSETS_NET: Optional[float] = None
    INVESTMENT_PROPERTIES_NET: Optional[float] = None
    OTHER_ASSETS_NET: Optional[float] = None
    TOTAL_ASSETS: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class InterbankPlacementsAndLoansDetails:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    PLACEMENTS_WITH_CREDIT_INSTITUTIONS: Optional[float] = None
    LOANS_TO_CREDIT_INSTITUTIONS: Optional[float] = None
    ECL_ALLOWANCE_LOANS_TO_CREDIT_INSTITUTIONS: Optional[float] = None
    INTERBANK_PLACEMENTS_AND_LOANS: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class TradingSecuritiesNetDetails:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    TRADING_SECURITIES_GROSS: Optional[float] = None
    ALLOWANCE_TRADING_SECURITIES: Optional[float] = None
    TRADING_SECURITIES_NET: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class LoanToCustomersNetDetails:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    LOANS_TO_CUSTOMERS_GROSS: Optional[float] = None
    ECL_ALLOWANCE_LOANS_TO_CUSTOMERS: Optional[float] = None
    LOANS_TO_CUSTOMERS_NET: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class InvesmentSecuritiesNetDetails:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    AFS_SECURITIES: Optional[float] = None
    HTM_SECURITIES: Optional[float] = None
    ALLOWANCE_INVESTMENT_SECURITIES: Optional[float] = None
    INVESTMENT_SECURITIES_NET: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class FixedAssetsNetDetails:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    TANGIBLE_FIXED_ASSETS_NET: Optional[float] = None
    FIXED_ASSETS_COST: Optional[float] = None
    ACCUMULATED_DEPRECIATION: Optional[float] = None
    HIERACHY_OF_FIXED_ASSETS_COST: Optional[float] = None
    HIERACHY_OF_ACCUMULATED_DEPRECIATION: Optional[float] = None
    PARENT_OF_ACCUMULATED_DEPRECIATION: Optional[float] = None
    PARENT_OF_FIXED_ASSETS_COST: Optional[float] = None
    FINANCE_LEASE_FIXED_ASSETS_NET: Optional[float] = None
    INTANGIBLE_ASSETS_NET: Optional[float] = None
    CIP_CONSTRUCTION_IN_PROGRESS: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class OtherAssetsNet:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    RECEIVABLES_NET: Optional[float] = None
    ACCRUED_INTEREST_AND_FEES_RECEIVABLE: Optional[float] = None
    DEFERRED_TAX_ASSETS: Optional[float] = None
    OTHER_ASSETS_GROSS: Optional[float] = None
    GOODWILL: Optional[float] = None
    ALLOWANCE_OTHER_ON_BALANCE_ASSETS: Optional[float] = None
    HIERACHY_OF_ALLOWANCE_OTHER_ON_BALANCE_ASSETS: Optional[float] = None
    HIERACHY_OF_GOODWILL: Optional[float] = None
    PARENT_OF_GOODWILL: Optional[float] = None
    PARENT_OF_ALLOWANCE_OTHER_ON_BALANCE_ASSETS: Optional[float] = None
    OTHER_ASSETS_NET: Optional[float] = None
    update_time: Optional[str] = None

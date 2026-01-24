
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

@dataclass
class BalanceSheetLiabilities:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    DUE_TO_GOVERNMENT_AND_CENTRAL_BANK: Optional[float] = None
    DUE_TO_CREDIT_INSTITUTIONS: Optional[float] = None
    DEPOSITS_FROM_CREDIT_INSTITUTIONS: Optional[float] = None
    BORROWINGS_FROM_CREDIT_INSTITUTIONS: Optional[float] = None
    HIERACHY_OF_DEPOSITS_FROM_CREDIT_INSTITUTIONS: Optional[float] = None
    HIERACHY_OF_BORROWINGS_FROM_CREDIT_INSTITUTIONS: Optional[float] = None
    PARENT_OF_DEPOSITS_FROM_CREDIT_INSTITUTIONS: Optional[float] = None
    PARENT_OF_BORROWINGS_FROM_CREDIT_INSTITUTIONS: Optional[float]
    CUSTOMER_DEPOSITS: Optional[float] = None
    DERIVATIVES_AND_OTHER_FINANCIAL_LIABILITIES: Optional[float] = None
    TRUST_AND_INVESTMENT_FUNDS_BANK_BEARING_RISK: Optional[float] = None
    DEBT_SECURITIES_ISSUED: Optional[float] = None
    OTHER_LIABILITIES: Optional[float] = None
    ACCRUED_INTEREST_AND_FEES_PAYABLE: Optional[float] = None
    DEFERRED_TAX_LIABILITIES: Optional[float] = None
    PAYABLES_AND_OTHER_LIABILITIES: Optional[float] = None
    OTHER_PROVISIONS: Optional[float] = None
    HIERACHY_OF_ACCRUED_INTEREST_AND_FEES_PAYABLE: Optional[float] = None
    HIERACHY_OF_DEFERRED_TAX_LIABILITIES: Optional[float] = None
    HIERACHY_OF_PAYABLES_AND_OTHER_LIABILITIES: Optional[float] = None
    HIERACHY_OF_OTHER_PROVISIONS: Optional[float] = None
    PARENT_OF_ACCRUED_INTEREST_AND_FEES_PAYABLE: Optional[float] = None
    PARENT_OF_DEFERRED_TAX_LIABILITIES: Optional[float] = None
    PARENT_OF_PAYABLES_AND_OTHER_LIABILITIES: Optional[float] = None
    PARENT_OF_OTHER_PROVISIONS: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class BalanceSheetTotalEquity:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    OWNERS_EQUITY_BANK: Optional[float] = None
    BANK_FUNDS: Optional[float] = None
    FX_TRANSLATION_DIFFERENCES: Optional[float] = None
    ASSET_REVALUATION_DIFFERENCES: Optional[float] = None
    RETAINED_EARNINGS_ACCUMULATED_LOSSES: Optional[float] = None
    OTHER_FUNDS: Optional[float] = None
    NON_CONTROLLING_INTERESTS: Optional[float] = None
    TOTAL_EQUITY: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class BalanceSheetOwnersEquityBank:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    PAID_IN_CAPITAL: Optional[float] = None
    CAPITAL_FOR_CONSTRUCTION: Optional[float] = None
    SHARE_PREMIUM: Optional[float] = None
    TREASURY_SHARES: Optional[float] = None
    PREFERRED_SHARES: Optional[float] = None
    OTHER_CAPITAL: Optional[float] = None
    update_time: Optional[str] = None


@dataclass
class  IncomeStatementCoreIncomeAndExpense:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    NET_INTEREST_INCOME: Optional[float] = None
    INTEREST_AND_SIMILAR_INCOME: Optional[float] = None
    INTEREST_AND_SIMILAR_EXPENSES: Optional[float] = None
    NET_FEE_COMMISSION_INCOME: Optional[float] = None
    FEE_COMMISSION_INCOME: Optional[float] = None
    NET_FEE_AND_COMMISSION_INCOME: Optional[float] = None
    FEE_COMMISSION_EXPENSES: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class IncomeStatementFinancialTrading:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    NET_FX_TRADING_INCOME: Optional[float] = None
    NET_TRADING_SECURITIES_INCOME: Optional[float] = None
    NET_INVESTMENT_SECURITIES_INCOME: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class IncomeStatementOtherOperationActivities:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    NET_OTHER_OPERATIONAL_INCOME: Optional[float] = None
    OTHER_OPERATING_INCOME: Optional[float] = None
    OTHER_OPERATING_EXPENSES: Optional[float] = None
    INCOME_FROM_EQUITY_INVESTMENTS: Optional[float] = None
    update_time: Optional[str] = None

@dataclass
class IncomeStatementExpenseAndProfit:
    symbol: str
    time_report_type:Optional[str] = None
    financial_report_type: Optional[str] = None
    year: Optional[str] = None
    period: Optional[str] = None
    OPERATING_EXPENSES: Optional[float] = None
    OPERATING_PROFIT_BEFORE_CREDIT_PROVISION: Optional[float] = None
    CREDIT_RISK_PROVISION_EXPENSES: Optional[float] = None
    PROFIT_BEFORE_TAX: Optional[float] = None
    CORPORATE_INCOME_TAX_EXPENSE: Optional[float] = None
    CURRENT_INCOME_TAX_EXPENSE: Optional[float] = None
    DEFERRED_INCOME_TAX_EXPENSE: Optional[float] = None
    PROFIT_AFTER_TAX: Optional[float] = None
    MINORITY_INTERESTS_AND_PREFERRED_DIVIDENDS: Optional[float] = None
    NET_PROFIT_ATTRIBUTABLE_TO_PARENT: Optional[float]
    update_time: Optional[str] = None
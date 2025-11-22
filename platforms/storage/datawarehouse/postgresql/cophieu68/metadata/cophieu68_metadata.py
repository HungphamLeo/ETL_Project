# cophieu68_metadata.py (update / add columns keys)

dim_market_type_info = {
    "table_name": "dim_market_type",
    "primary_key": "market_key",
    "character_specific": "CP68MARKETKEY",
    "market_name": {"HOSE": "Sở giao dịch chứng khoán Hồ Chí Minh",
                    "HNX": "Sở giao dịch chứng khoán Hà Nội",
                    "UPCOM": "Sàn UPCOM",
                    "VN30": "VN30"},
    "columns": {
        "market_key": {"type": "VARCHAR(40)", "constraints": "PRIMARY KEY"},
        "market_type": {"type": "TEXT", "constraints": "NOT NULL UNIQUE"},
        "market_name": {"type": "TEXT", "constraints": ""},
        "update_time": {"type": "TIMESTAMPTZ", "constraints": ""},
        "created_time": {"type": "TIMESTAMPTZ", "constraints": ""}
    }
}

dim_company_profile_info = {
    "table_name": "dim_company_profile",
    "primary_key": "company_profile_key",
    "character_specific": "CP68COMPANYPROFILEKEY",
    "columns": {
        "company_profile_key": {"type": "VARCHAR(40)", "constraints": "PRIMARY KEY"},
        "company_key": {"type": "VARCHAR(40)", "constraints": "NOT NULL"},
        "full_name": {"type": "TEXT", "constraints": ""},
        "english_name": {"type": "TEXT", "constraints": ""},
        "short_name": {"type": "TEXT", "constraints": ""},
        "address": {"type": "TEXT", "constraints": ""},
        "phone": {"type": "TEXT", "constraints": ""},
        "fax": {"type": "TEXT", "constraints": ""},
        "website": {"type": "TEXT", "constraints": ""},
        "email": {"type": "TEXT", "constraints": ""},
        "established_date": {"type": "TEXT", "constraints": ""},
        "listed_date": {"type": "TEXT", "constraints": ""},
        "chartered_capital": {"type": "TEXT", "constraints": ""},
        "business_license": {"type": "TEXT", "constraints": ""},
        "tax_code": {"type": "TEXT", "constraints": ""},
        "update_time": {"type": "TIMESTAMPTZ", "constraints": ""},
        "created_time": {"type": "TIMESTAMPTZ", "constraints": ""}
    }
}

dim_industry_mapping_info = {
    "table_name": "dim_industry",
    "primary_key": "industry_key",
    "character_specific": "CP68INDUSTRYMAPPINGKEY",
    "industry_mapping": {
        "Bán buôn": "^bb",
        # ... rest omitted for brevity (keep your mapping)
    },
    "columns": {
        "industry_key": {"type": "VARCHAR(40)", "constraints": "PRIMARY KEY"},
        "industry_code": {"type": "TEXT", "constraints": "NOT NULL"},
        "industry_name": {"type": "TEXT", "constraints": "NOT NULL"},
        "update_time": {"type": "TIMESTAMPTZ", "constraints": ""},
        "created_time": {"type": "TIMESTAMPTZ", "constraints": ""}
    }
}

# minimal placeholders for FACT config keys used in loaders
dim_trade_info = {"character_specific": "CP68TRADEKEY"}
dim_match_info = {"character_specific": "CP68MATCHKEY"}
dim_income_info = {"character_specific": "CP68INCOMEKEY"}
dim_balance_info = {"character_specific": "CP68BSKEY"}
fact_business_plan_info = {"character_specific": "CP68PLANKEY"}
fact_financial_metrics_info = {"character_specific": "CP68METRICKEY"}

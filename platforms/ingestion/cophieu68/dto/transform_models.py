DATA_WAREHOUSE_SCHEMA = {
    "schema_name": "dw",
    "dimensions": {

        "dim_market_type": {
            "grain": "1 record per stock exchange",
            "columns": {
                "market_key":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "market_type":     {"type": "TEXT",        "constraints": "UNIQUE NOT NULL"},
                "market_name":     {"type": "TEXT",        "constraints": ""},
                "update_time":     {"type": "TIMESTAMPTZ", "constraints": ""},
                "created_time":    {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
            }
        },

        "dim_industry": {
            "grain": "1 record per industry",
            "columns": {
                "industry_metric": {"type": "VARCHAR(32)", "constraints": "NOT NULL"},
                "industry_code": {"type": "TEXT", "constraints": "NOT NULL"},
                "industry_code_replace": {"type": "TEXT"},
                "industry_craw_url": {"type": "TEXT"},
                "effective_date": {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
                "end_date": {"type": "TIMESTAMPTZ"},
                "is_current": {"type": "BOOLEAN", "constraints": "DEFAULT TRUE"},
                "update_time": {"type": "TIMESTAMPTZ"},
                "created_time": {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
            },
            "constraints": [
                "PRIMARY KEY (industry_metric, industry_code)"
            ]
        },

        "dim_company": {
            "grain": "1 record per company (SCD2)",
            "columns": {
                "company_key":     {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "symbol":          {"type": "TEXT",        "constraints": "NOT NULL"},
                "company_name":    {"type": "TEXT"},
                "full_name":       {"type": "TEXT"},
                "english_name":    {"type": "TEXT"},
                "short_name":      {"type": "TEXT"},
                "address":         {"type": "TEXT"},
                "phone":           {"type": "TEXT"},
                "fax":             {"type": "TEXT"},
                "website":         {"type": "TEXT"},
                "email":           {"type": "TEXT"},
                "established_date":{"type": "DATE"},
                "listed_date":     {"type": "DATE"},
                "chartered_capital":{"type": "TEXT"},
                "business_license":{"type": "TEXT"},
                "tax_code":        {"type": "TEXT"},
                "market_key":      {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_market_type(market_key)"},
                "effective_date": {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
                "end_date": {"type": "TIMESTAMPTZ"},
                "is_current": {"type": "BOOLEAN", "constraints": "DEFAULT TRUE"},
                "update_time": {"type": "TIMESTAMPTZ"},
                "created_time": {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
            }
        },

        "dim_report_type": {
            "grain": "Yearly or Quarterly",
            "columns": {
                "report_type_key": {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "report_type_code":{"type": "TEXT",        "constraints": "NOT NULL"},  # Y, Q
                "description":     {"type": "TEXT",        "constraints": ""}
            }
        }
    },

    "facts": {
        "fact_industry_summary": {
            "grain": "1 record per trade tick",
            "partitions": "RANGE (trade_date_key) monthly",
            "columns": {
                "industry_key": {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "industry_metric": {"type": "VARCHAR(32)", "constraints": "NOT NULL"},
                "industry_code": {"type": "TEXT", "constraints": "NOT NULL"},
                "industry_index":  {"type": "FLOAT", "constraints": ""},
                "Percentage_change": {"type": "FLOAT", "constraints": ""},
                "Liquidity": {"type": "FLOAT", "constraints": ""},
                "Total_Capital": {"type": "FLOAT", "constraints": ""},
                "Average_Price": {"type": "FLOAT", "constraints": ""},
                "Book_Value": {"type": "FLOAT", "constraints": ""},
                "Earning_Per_Share(EPS)": {"type": "FLOAT", "constraints": ""},
                "Price on Earning(P/E)": {"type": "FLOAT", "constraints": ""},
                "Return on Asset(ROA)": {"type": "FLOAT", "constraints": ""},
                "Return on Equity(ROE)": {"type": "FLOAT", "constraints": ""},
                "Supply_Volumn": {"type": "FLOAT", "constraints": ""},
                "Total_Asset": {"type": "FLOAT", "constraints": ""},
                "Total_Equity": {"type": "FLOAT", "constraints": ""},
                "Total_Liabilities": {"type": "FLOAT", "constraints": ""},
                "Percentage_Debt_on_Equity": {"type": "FLOAT", "constraints": ""},
                "Percentage_Equity_on_Assets": {"type": "FLOAT", "constraints": ""},
                "Revenue": {"type": "FLOAT", "constraints": ""},
                "Profit_Before_Tax": {"type": "FLOAT", "constraints": ""},
                "created_time":  {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
                "updated_time":  {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"}
                
            }
        },

        "fact_trade_history": {
            "grain": "1 record per trade tick",
            "partitions": "RANGE (trade_date_key) monthly",
            "columns": {
                "trade_key":       {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "trade_date":      {"type": "DATE",       "constraints": "NOT NULL"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "close_price":           {"type": "NUMERIC",     "constraints": ""},
                "open_price":            {"type": "NUMERIC",     "constraints": ""},
                "high_price":            {"type": "NUMERIC",     "constraints": ""},
                "low_price":             {"type": "NUMERIC",     "constraints": ""},
                "volume":          {"type": "BIGINT",      "constraints": ""},
                "foreign_buy":           {"type": "NUMERIC",     "constraints": ""},
                "foreign_sell":          {"type": "NUMERIC",     "constraints": ""},
                "foreign_net_value":     {"type": "NUMERIC",       "constraints": ""},
                "update_time":     {"type": "TIMESTAMPTZ", "constraints": ""},
            }
        },

        "fact_match_detail": {
            "grain": "1 record per match event",
            "columns": {
                "match_key":       {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "match_datetime":  {"type": "TIMESTAMPTZ", "constraints": ""},
                "price":           {"type": "NUMERIC",     "constraints": ""},
                "volume":          {"type": "BIGINT",      "constraints": ""},
                "fluctuation_range":{"type": "NUMERIC",     "constraints": ""},
                "accum_volume":          {"type": "BIGINT",        "constraints": ""},
                "update_time":     {"type": "TIMESTAMPTZ", "constraints": ""},
                
            }
        },

        "fact_income_statement_yearly": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "income_key":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_report_type(report_type_key)"},
                "period_date_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "revenue":         {"type": "NUMERIC",     "constraints": ""},
                "operating_profit":{"type": "NUMERIC",     "constraints": ""},
                "net_income":      {"type": "NUMERIC",     "constraints": ""},
                "eps":             {"type": "NUMERIC",     "constraints": ""}
                
            }
        },
        "fact_income_statement_quarterly": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "income_key":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_report_type(report_type_key)"},
                "period_date_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "revenue":         {"type": "NUMERIC",     "constraints": ""},
                "operating_profit":{"type": "NUMERIC",     "constraints": ""},
                "net_income":      {"type": "NUMERIC",     "constraints": ""},
                "eps":             {"type": "NUMERIC",     "constraints": ""}
            }
        },

        "fact_balance_sheet_yearly": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "bs_key":          {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_report_type(report_type_key)"},
                "period_date_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "total_assets":    {"type": "NUMERIC",     "constraints": ""},
                "total_liabilities":{"type": "NUMERIC",    "constraints": ""},
                "shareholder_equity":{"type": "NUMERIC",   "constraints": ""},
                "cash":            {"type": "NUMERIC",     "constraints": ""},
                "inventory":       {"type": "NUMERIC",     "constraints": ""}
            }
        },
        "fact_balance_sheet_quarterly": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "bs_key":          {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_report_type(report_type_key)"},
                "period_date_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "total_assets":    {"type": "NUMERIC",     "constraints": ""},
                "total_liabilities":{"type": "NUMERIC",    "constraints": ""},
                "shareholder_equity":{"type": "NUMERIC",   "constraints": ""},
                "cash":            {"type": "NUMERIC",     "constraints": ""},
                "inventory":       {"type": "NUMERIC",     "constraints": ""}
            }
        },

        "fact_business_plan": {
            "grain": "company × year plan",
            "columns": {
                "plan_key":        {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "year_key":        {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "target_revenue":  {"type": "NUMERIC",     "constraints": ""},
                "target_profit":   {"type": "NUMERIC",     "constraints": ""},
                "capex_plan":      {"type": "NUMERIC",     "constraints": ""}
            }
        },

        "fact_financial_metrics": {
            "grain": "company × period",
            "columns": {
                "financial_ratio_key":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "reference_price": {"type": "NUMERIC", "constraints": "NOT NULL "},
                "company_name":    {"type": "TEXT", "constraints": "NOT NULL "},
                "open_price":      {"type": "NUMERIC", "constraints": "NOT NULL "},
                "high_price":      {"type": "NUMERIC", "constraints": "NOT NULL "},
                "low_price":       {"type": "NUMERIC", "constraints": "NOT NULL "},
                "volume":          {"type": "BIGINT", "constraints": "NOT NULL "},
                "book_value":      {"type": "NUMERIC", "constraints": "NOT NULL "},
                "earning_per_share(EPS)":{"type": "NUMERIC", "constraints": "NOT NULL "},
                "price_on_earning(P/E)": {"type": "NUMERIC",     "constraints": ""},
                "price_on_book_value(P/B)": {"type": "NUMERIC",     "constraints": ""},
                "return_on_equity(ROE)": {"type": "NUMERIC",     "constraints": ""},
                "return_on_assets(ROA)": {"type": "NUMERIC",     "constraints": ""},
                "beta":            {"type": "NUMERIC",     "constraints": ""},
                "market_cap":      {"type": "NUMERIC",     "constraints": ""},
                "listed_volume":   {"type": "NUMERIC",     "constraints": ""},
                "average_volume_52_weeks":  {"type": "NUMERIC",     "constraints": ""},
                "high_low_52_weeks": {"type": "NUMERIC",     "constraints": ""},
                "debt":             {"type": "NUMERIC",     "constraints": ""},
                "equity":           {"type": "NUMERIC",     "constraints": ""},
                "debt_to_equity":   {"type": "NUMERIC",     "constraints": ""},
                "equity_to_assets": {"type": "NUMERIC",     "constraints": ""},
                "cash":             {"type": "NUMERIC",     "constraints": ""},
                "eps_power":        {"type": "NUMERIC",     "constraints": ""},
                "roe_power":        {"type": "NUMERIC",     "constraints": ""},
                "invest_efficiency":{"type": "NUMERIC",     "constraints": ""},
                "pb_power":         {"type": "NUMERIC",     "constraints": ""},
                "price_growth_power":{"type": "NUMERIC",     "constraints": ""},
                "update_time":      {"type": "TIMESTAMP", "constraints": "NOT NULL "},
               
            }
        }
    },

    "staging": {
        "stg_list_stock":       {"columns": {"symbol": "TEXT", "raw_json": "JSONB", "extracted_at": "TIMESTAMPTZ"}},
        "stg_industry_info":    {"columns": {"industry_metric": "TEXT", "raw_json": "JSONB", "extracted_at": "TIMESTAMPTZ"}},
        "stg_stock_info":       {"columns": {"symbol": "TEXT", "raw_json": "JSONB", "extracted_at": "TIMESTAMPTZ"}},
        "stg_financial_info":   {"columns": {"symbol": "TEXT", "raw_json": "JSONB", "extracted_at": "TIMESTAMPTZ"}},
        "stg_trading_data":     {"columns": {"symbol": "TEXT", "raw_json": "JSONB", "extracted_at": "TIMESTAMPTZ"}},
        "stg_income_statement": {"columns": {"symbol": "TEXT", "report_type": "TEXT", "raw_json": "JSONB", "extracted_at": "TIMESTAMPTZ"}},
        "stg_balance_sheet":    {"columns": {"symbol": "TEXT", "report_type": "TEXT", "raw_json": "JSONB"}},
        "stg_match_details":    {"columns": {"symbol": "TEXT", "raw_json": "JSONB"}},
        "stg_business_plan":    {"columns": {"symbol": "TEXT", "raw_json": "JSONB"}},
        "stg_financial_summary":{"columns": {"symbol": "TEXT", "raw_json": "JSONB"}}
    },

    "metadata": {
        "meta_etl_run": {
            "columns": {
                "run_id":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "job_name":    {"type": "TEXT"},
                "start_time":  {"type": "TIMESTAMPTZ"},
                "end_time":    {"type": "TIMESTAMPTZ"},
                "status":      {"type": "TEXT"}
            }
        },
        "meta_etl_error": {
            "columns": {
                "err_id":       {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "run_id":       {"type": "VARCHAR(32)", "constraints": "REFERENCES meta_etl_run(run_id)"},
                "error_message":{"type": "TEXT"},
                "raw_json":     {"type": "JSONB"},
                "err_time":     {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
            }
        },
        "meta_surrogate_map": {
            "columns": {
                "natural_key":   {"type": "TEXT"},
                "surrogate_key": {"type": "VARCHAR(32)"},
                "type":          {"type": "TEXT"},
                "valid_from":    {"type": "DATE"},
                "valid_to":      {"type": "DATE"}
            }
        },
        "meta_data_quality": {
            "columns": {
                "table_name": {"type": "TEXT"},
                "check_name": {"type": "TEXT"},
                "status":     {"type": "TEXT"},
                "checked_at": {"type": "TIMESTAMPTZ"},
                "failed_rows":{"type": "INT"}
            }
        }
    }
}
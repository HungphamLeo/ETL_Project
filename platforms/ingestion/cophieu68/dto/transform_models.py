DATA_WAREHOUSE_SCHEMA = {
    "schema_name": "dw",
    "dimensions": {

        "dim_market_type": {
            "grain": "1 record per stock exchange",
            "columns": {  
                "market_key":     {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},              
                "market_type":     {"type": "TEXT",        "constraints": "NOT NULL"},
                "market_name":     {"type": "TEXT",        "constraints": ""},
                "update_time":     {"type": "TIMESTAMPTZ", "constraints": ""},
                "created_time":    {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
            }
        },

        "dim_industry": {
            "grain": "1 record per industry",
            "columns": {
                "industry_sk":     {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
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
                "UNIQUE (industry_sk, industry_metric, industry_code, effective_date)"
            ]
        },

        "dim_company": {
            "grain": "1 record per company (SCD2)",
            "columns": {
                "company_key":     {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "symbol":          {"type": "TEXT",        "constraints": "NOT NULL"},
                "company_name":    {"type": "TEXT"},
                "current_price":   {"type": "VARCHAR(8)"},
                "full_name":       {"type": "TEXT"},
                "english_name":    {"type": "TEXT"},
                "short_name":      {"type": "TEXT"},
                "address":         {"type": "TEXT"},
                "phone":           {"type": "TEXT"},
                "fax":             {"type": "TEXT"},
                "website":         {"type": "TEXT"},
                "email_address":           {"type": "TEXT"},
                "established_date" :{"type": "TEXT"},
                "listed_date":     {"type": "DATE"},
                "listed_volume_initial": {"type": "VARCHAR(32)"},
                "listed_volume":   {"type": "VARCHAR(32)"},
                "circulating_volume": {"type": "VARCHAR(32)"},
                "market_capitalization": {"type": "VARCHAR(32)"},
                "end_date": {"type": "TIMESTAMPTZ"},
                "is_current": {"type": "BOOLEAN", "constraints": "DEFAULT TRUE"},
                "update_time": {"type": "TIMESTAMPTZ"},
                "created_time": {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
            }
        },

        "dim_report_type": {
            "grain": "annually or Quarterly",
            "columns": {
                "report_type_key": {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
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
                "industry_sk": {"type": "VARCHAR(64)", "constraints": "REFERENCES dw.dim_industry(industry_sk)"},
                "industry_code": {"type": "TEXT", "constraints": "NOT NULL"},
                "industry_name": {"type": "TEXT", "constraints": ""},
                "industry_index":  {"type": "FLOAT", "constraints": ""},
                "percentage_change": {"type": "FLOAT", "constraints": ""},
                "liquidity": {"type": "FLOAT", "constraints": ""},
                "total_capital": {"type": "FLOAT", "constraints": ""},
                "average_price": {"type": "VARCHAR(8)", "constraints": ""},
                "book_value": {"type": "VARCHAR(8)", "constraints": ""},
                "earning_per_share_eps": {"type": "VARCHAR(8)", "constraints": ""},
                "price_on_earning_pe": {"type": "FLOAT", "constraints": ""},
                "return_on_asset_roa": {"type": "FLOAT", "constraints": ""},
                "return_on_equity_roe": {"type": "FLOAT", "constraints": ""},
                "supply_volumn": {"type": "FLOAT", "constraints": ""},
                "total_asset": {"type": "FLOAT", "constraints": ""},
                "total_equity": {"type": "FLOAT", "constraints": ""},
                "total_liabilities": {"type": "FLOAT", "constraints": ""},
                "percentage_debt_on_equity": {"type": "FLOAT", "constraints": ""},
                "percentage_equity_on_assets": {"type": "FLOAT", "constraints": ""},
                "revenue": {"type": "FLOAT", "constraints": ""},
                "profit_before_tax": {"type": "FLOAT", "constraints": ""},
                "created_time":  {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
                "updated_time":  {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"}
                
            }
        },

        "fact_trade_history": {
            "grain": "1 record per trade tick",
            "partitions": "RANGE (trade_date_key) monthly",
            "columns": {
                "trade_key":       {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "trade_date":      {"type": "DATE",       "constraints": "NOT NULL"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
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
                "match_key":       {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(64)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "match_datetime":  {"type": "TIMESTAMPTZ", "constraints": ""},
                "price":           {"type": "VARCHAR(32)",     "constraints": ""},
                "volume":          {"type": "BIGINT",      "constraints": ""},
                "fluctuation_range":{"type": "VARCHAR(32)",     "constraints": ""},
                "accum_volume":          {"type": "BIGINT",        "constraints": ""},
                "update_time":     {"type": "TIMESTAMPTZ", "constraints": ""},
                
            }
        },

        "fact_income_statement_annually": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "income_key":      {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "time_report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_report_type(report_type_key)"},
                "symbol":          {"type": "VARCHAR(32)", "constraints": ""},
                "time_report_type":{"type": "VARCHAR(32)", "constraints": ""},
                "financial_report_type":         {"type": "VARCHAR(32)",     "constraints": ""},
                "year":         {"type": "VARCHAR(32)",     "constraints": ""},   
                "metric_code":      {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_name_en":             {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_group":          {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_value":          {"type": "VARCHAR(32)",     "constraints": ""},
                "currency":          {"type": "VARCHAR(3)",     "constraints": ""},
                "unit":          {"type": "VARCHAR(32)",     "constraints": ""},
                "update_time":          {"type": "TIMESTAMPTZ",     "constraints": ""},
            },
            "constraints": [
                "PRIMARY KEY (income_key, company_key)"
            ]
        },
        "fact_income_statement_quarterly": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "income_key":      {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "time_report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_report_type(report_type_key)"},
                "symbol":          {"type": "VARCHAR(32)", "constraints": ""},
                "time_report_type":{"type": "VARCHAR(32)", "constraints": ""},
                "financial_report_type":         {"type": "VARCHAR(32)",     "constraints": ""},
                "year":          {"type": "VARCHAR(32)",     "constraints": ""},
                "period":          {"type": "VARCHAR(32)",     "constraints": ""},
                "metric_code":      {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_name_en":             {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_group":          {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_value":          {"type": "VARCHAR(32)",     "constraints": ""},
                "currency":          {"type": "VARCHAR(3)",     "constraints": ""},
                "unit":          {"type": "VARCHAR(32)",     "constraints": ""},
                "update_time":          {"type": "TIMESTAMPTZ",     "constraints": ""},
            },
            "constraints": [
                "PRIMARY KEY (income_key, company_key)"
            ]
        },

        "fact_balance_sheet_annually": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "balance_key":      {"type": "VARCHAR(128)", "constraints": "NOT NULL"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "time_report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_report_type(report_type_key)"},
                "symbol":          {"type": "VARCHAR(32)", "constraints": ""},
                "time_report_type":{"type": "VARCHAR(32)", "constraints": ""},
                "financial_report_type":         {"type": "VARCHAR(32)",     "constraints": ""},
                "year":          {"type": "VARCHAR(32)",     "constraints": ""},
                "metric_code":      {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_name_en":             {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_group":          {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_value":          {"type": "NUMERIC",     "constraints": ""},
                "currency":          {"type": "VARCHAR(3)",     "constraints": ""},
                "unit":          {"type": "VARCHAR(32)",     "constraints": ""},
                "update_time":          {"type": "TIMESTAMPTZ",     "constraints": ""},
            },
             "constraints": [
                "PRIMARY KEY (balance_key, company_key)"
            ]
        },
        "fact_balance_sheet_quarterly": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "balance_key":      {"type": "VARCHAR(128)", "constraints": "NOT NULL"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "time_report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_report_type(report_type_key)"},
                "symbol":          {"type": "VARCHAR(32)", "constraints": ""},
                "time_report_type":{"type": "VARCHAR(32)", "constraints": ""},
                "financial_report_type":         {"type": "VARCHAR(32)",     "constraints": ""},
                "year":          {"type": "VARCHAR(32)",     "constraints": ""},
                "period":          {"type": "VARCHAR(32)",     "constraints": ""},
                "metric_code":      {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_name_en":             {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_group":          {"type": "VARCHAR(128)",     "constraints": ""},
                "metric_value":          {"type": "NUMERIC",     "constraints": ""},
                "currency":          {"type": "VARCHAR(3)",     "constraints": ""},
                "unit":          {"type": "VARCHAR(32)",     "constraints": ""},
                "update_time":          {"type": "TIMESTAMPTZ",     "constraints": ""},
            },
             "constraints": [
                "PRIMARY KEY (balance_key, company_key)"
            ]
        },

        "fact_business_plan": {
            "grain": "company × year plan",
            "columns": {
                "plan_key":        {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "symbol":          {"type": "VARCHAR(32)", "constraints": ""},
                "year":        {"type": "VARCHAR(5)", "constraints": ""},
                "plan_revenue":  {"type": "VARCHAR(12)",     "constraints": ""},
                "revenue_achived":   {"type": "VARCHAR(12)",     "constraints": ""},
                "plan_profit":      {"type": "VARCHAR(12)",     "constraints": ""},
                "profit_achived":    {"type": "VARCHAR(12)",     "constraints": ""},
                "update_time":          {"type": "TIMESTAMPTZ",     "constraints": ""},
            }
        },

        "fact_financial_metrics": {
            "grain": "company × period",
            "columns": {
                "financial_ratio_key":      {"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dw.dim_company(company_key)"},
                "reference_price": {"type": "NUMERIC", "constraints": "NOT NULL "},
                "company_name":    {"type": "TEXT", "constraints": ""},
                "open_price":      {"type": "NUMERIC", "constraints": "NOT NULL "},
                "high_price":      {"type": "NUMERIC", "constraints": "NOT NULL "},
                "low_price":       {"type": "NUMERIC", "constraints": "NOT NULL "},
                "volume":          {"type": "NUMERIC", "constraints": "NOT NULL "},
                "book_value":      {"type": "VARCHAR(24)", "constraints": "NOT NULL "},
                "earning_per_share_eps":{"type": "VARCHAR(24)", "constraints": "NOT NULL "},
                "price_on_earning_pe": {"type": "VARCHAR(24)",     "constraints": ""},
                "price_on_book_value_pb": {"type": "VARCHAR(24)",     "constraints": ""},
                "return_on_equity_roe": {"type": "VARCHAR(24)",     "constraints": ""},
                "return_on_assets_roa": {"type": "VARCHAR(24)",     "constraints": ""},
                "beta":            {"type": "VARCHAR(24)",     "constraints": ""},
                "market_cap":      {"type": "VARCHAR(24)",     "constraints": ""},
                "listed_volume":   {"type": "VARCHAR(24)",     "constraints": ""},
                "average_volume_52_weeks":  {"type": "VARCHAR(24)",     "constraints": ""},
                "high_low_52_weeks": {"type": "VARCHAR(24)",     "constraints": ""},
                "debt":             {"type": "VARCHAR(24)",     "constraints": ""},
                "equity":           {"type": "VARCHAR(24)",     "constraints": ""},
                "debt_to_equity":   {"type": "VARCHAR(24)",     "constraints": ""},
                "equity_to_assets": {"type": "VARCHAR(24)",     "constraints": ""},
                "cash":             {"type": "VARCHAR(24)",     "constraints": ""},
                "eps_power":        {"type": "VARCHAR(24)",     "constraints": ""},
                "roe_power":        {"type": "VARCHAR(24)",     "constraints": ""},
                "invest_efficiency":{"type": "VARCHAR(24)",     "constraints": ""},
                "pb_power":         {"type": "VARCHAR(24)",     "constraints": ""},
                "price_growth_power":{"type": "VARCHAR(24)",     "constraints": ""},
                "update_time":      {"type": "TIMESTAMP", "constraints": "NOT NULL"},
               
            }
        },
        "fact_company_belong_to_industry": {
            "grain": "company × period",
            "columns": {
                "company_industry_sector_keys":{"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "industry_code": {"type": "VARCHAR(24)",     "constraints": ""},
                "industry_name": {"type": "VARCHAR(24)",     "constraints": ""},
                "symbol":        {"type": "VARCHAR(4)",     "constraints": ""},
                "company_name":    {"type": "VARCHAR(56)",     "constraints": ""},
                "close_price":   {"type": "VARCHAR(12)",     "constraints": ""},
                "fluctuation_range":   {"type": "VARCHAR(6)",     "constraints": ""},
                "volumn24h":   {"type": "VARCHAR(24)",     "constraints": ""},
                "volumn52w":   {"type": "VARCHAR(24)",     "constraints": ""},
                "listed_volumn":   {"type": "VARCHAR(24)",     "constraints": ""},
                "market_capitalization": {"type": "VARCHAR(24)",     "constraints": ""},
                "currency":         {"type": "VARCHAR(3)",     "constraints": ""},
                "unit":          {"type": "VARCHAR(12)",     "constraints": ""},
                "update_time":      {"type": "TIMESTAMP", "constraints": "NOT NULL"}
            }
        },
        "fact_company_belong_to_market_type": {
            "grain": "company × period",
            "columns": {
                "company_market_type_sector_keys":{"type": "VARCHAR(64)", "constraints": "PRIMARY KEY"},
                "market_type_code": {"type": "VARCHAR(24)",     "constraints": ""},
                "market_type_name": {"type": "VARCHAR(24)",     "constraints": ""},
                "symbol":        {"type": "VARCHAR(4)",     "constraints": ""},
                "company_name":    {"type": "VARCHAR(56)",     "constraints": ""},
                "close_price":   {"type": "VARCHAR(12)",     "constraints": ""},
                "fluctuation_range":   {"type": "VARCHAR(6)",     "constraints": ""},
                "volumn24h":   {"type": "VARCHAR(24)",     "constraints": ""},
                "volumn52w":   {"type": "VARCHAR(24)",     "constraints": ""},
                "listed_volumn":   {"type": "VARCHAR(24)",     "constraints": ""},
                "market_capitalization": {"type": "VARCHAR(24)",     "constraints": ""},
                "currency":         {"type": "VARCHAR(3)",     "constraints": ""},
                "unit":          {"type": "VARCHAR(12)",     "constraints": ""},
                "update_time":      {"type": "TIMESTAMP", "constraints": "NOT NULL"}
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
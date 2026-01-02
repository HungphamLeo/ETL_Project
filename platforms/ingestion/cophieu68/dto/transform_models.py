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
                "symbol":          {"type": "TEXT",        "constraints": "UNIQUE NOT NULL"},
                "company_name":    {"type": "TEXT",        "constraints": ""},
                "market_key":      {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_market_type(market_key)"},
                "industry_key":    {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_industry(industry_key)"},
                "profile_json":    {"type": "JSONB",       "constraints": ""},
                "effective_from":  {"type": "DATE",        "constraints": ""},
                "effective_to":    {"type": "DATE",        "constraints": ""},
                "is_current":      {"type": "BOOLEAN",     "constraints": "DEFAULT TRUE"},
                "created_time":    {"type": "TIMESTAMPTZ", "constraints": "DEFAULT NOW()"}
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

        "fact_trade": {
            "grain": "1 record per trade tick",
            "partitions": "RANGE (trade_date_key) monthly",
            "columns": {
                "trade_key":       {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "trade_datetime":  {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
                "trade_date_key":  {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "price":           {"type": "NUMERIC",     "constraints": ""},
                "volume":          {"type": "BIGINT",      "constraints": ""},
                "value":           {"type": "NUMERIC",     "constraints": ""},
                "side":            {"type": "TEXT",        "constraints": ""},
                "source_json":     {"type": "JSONB",       "constraints": ""}
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
                "broker":          {"type": "TEXT",        "constraints": ""},
                "source_json":     {"type": "JSONB",       "constraints": ""}
            }
        },

        "fact_income_statement": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "income_key":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "report_type_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_report_type(report_type_key)"},
                "period_date_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "revenue":         {"type": "NUMERIC",     "constraints": ""},
                "operating_profit":{"type": "NUMERIC",     "constraints": ""},
                "net_income":      {"type": "NUMERIC",     "constraints": ""},
                "eps":             {"type": "NUMERIC",     "constraints": ""},
                "source_json":     {"type": "JSONB",       "constraints": ""}
            }
        },

        "fact_balance_sheet": {
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
                "inventory":       {"type": "NUMERIC",     "constraints": ""},
                "source_json":     {"type": "JSONB",       "constraints": ""}
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
                "capex_plan":      {"type": "NUMERIC",     "constraints": ""},
                "source_json":     {"type": "JSONB",       "constraints": ""}
            }
        },

        "fact_financial_metrics": {
            "grain": "company × period",
            "columns": {
                "metric_key":      {"type": "VARCHAR(32)", "constraints": "PRIMARY KEY"},
                "company_key":     {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_company(company_key)"},
                "period_date_key": {"type": "VARCHAR(32)", "constraints": "REFERENCES dim_date(date_key)"},
                "pe":              {"type": "NUMERIC",     "constraints": ""},
                "roe":             {"type": "NUMERIC",     "constraints": ""},
                "roa":             {"type": "NUMERIC",     "constraints": ""},
                "debt_equity":     {"type": "NUMERIC",     "constraints": ""},
                "market_cap":      {"type": "NUMERIC",     "constraints": ""},
                "source_json":     {"type": "JSONB",       "constraints": ""}
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
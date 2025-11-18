DATA_WAREHOUSE_SCHEMA = {
    "dimensions": {
        # "dim_date": {
        #     "grain": "1 record per calendar day",
        #     "rules": {
        #         "date_key": {"type": "VARCHAR(32)","constraints": "PRIMARY KEY"},  
        #         "date": {"type": "DATE"},
        #         "year": {"type": "DATE"},
        #         "quarter": {"type": "DATE"},
        #         "month": {"type": "DATE"},
        #         "day": {"type": "DATE"},
        #         "is_weekend": {"type": "BOOLEAN"},
        #     }
        # },
        "dim_market_type": {
            "grain": "1 record per stock exchange",
            "rules": {
                "market_key":{"type": "VARCHAR(32)","constraints": "PRIMARY KEY"},  
                "market_type": {"type": "TEXT","constraints": "UNIQUE NOT NULL"},
                "market_name": {"type": "TEXT"},
                "update_time": {"type": "TIMESTAMPTZ"},
                "created_time": {"type": "TIMESTAMPTZ"}
                }
        },
        "dim_industry": {
            "grain": "1 record per industry",
            "rules": {
                "industry_key": {"type": "VARCHAR(32)","constraints": "PRIMARY KEY"},  
                "industry_code": {"type": "TEXT","constraints": "UNIQUE NOT NULL"},
                "industry_name": {"type": "TEXT"},
                "update_time": {"type": "TIMESTAMPTZ"},
                "created_time": {"type": "TIMESTAMPTZ"}
            }
        },
        "dim_company": {
            "grain": "1 record per company (SCD2)",
            "columns": {
                "company_key": "BIGSERIAL PK",
                "symbol": "TEXT UNIQUE",
                "company_name": "TEXT",
                "market_key": "BIGINT FK → dim_market_type",
                "industry_key": "BIGINT FK → dim_industry",
                "profile_json": "JSONB",
                "effective_from": "DATE",
                "effective_to": "DATE",
                "is_current": "BOOLEAN"
            }
        },
        "dim_report_type": {
            "grain": "Yearly or Quarterly",
            "columns": {
                "report_type_key": "SERIAL PK",
                "report_type_code": "TEXT",   # 'Y', 'Q'
                "description": "TEXT"
            }
        }
    },

    "facts": {
        "fact_trade": {
            "grain": "1 record per trade tick",
            "partitions": "RANGE (trade_date_key) monthly",
            "columns": {
                "trade_key": "BIGSERIAL PK",
                "trade_datetime": "TIMESTAMPTZ",
                "trade_date_key": "INT FK → dim_date",
                "company_key": "BIGINT FK → dim_company",
                "price": "NUMERIC",
                "volume": "BIGINT",
                "value": "NUMERIC",
                "side": "TEXT",
                "source_json": "JSONB"
            }
        },

        "fact_match_detail": {
            "grain": "1 record per match event",
            "columns": {
                "match_key": "BIGSERIAL PK",
                "company_key": "BIGINT FK → dim_company",
                "match_datetime": "TIMESTAMPTZ",
                "price": "NUMERIC",
                "volume": "BIGINT",
                "broker": "TEXT",
                "source_json": "JSONB"
            }
        },

        "fact_income_statement": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "income_key": "BIGSERIAL PK",
                "company_key": "BIGINT FK → dim_company",
                "report_type_key": "INT FK → dim_report_type",
                "period_date_key": "INT FK → dim_date",
                "revenue": "NUMERIC",
                "operating_profit": "NUMERIC",
                "net_income": "NUMERIC",
                "eps": "NUMERIC",
                "source_json": "JSONB"
            }
        },

        "fact_balance_sheet": {
            "grain": "company × period (Y or Q)",
            "columns": {
                "bs_key": "BIGSERIAL PK",
                "company_key": "BIGINT FK → dim_company",
                "report_type_key": "INT FK → dim_report_type",
                "period_date_key": "INT FK → dim_date",
                "total_assets": "NUMERIC",
                "total_liabilities": "NUMERIC",
                "shareholder_equity": "NUMERIC",
                "cash": "NUMERIC",
                "inventory": "NUMERIC",
                "source_json": "JSONB"
            }
        },

        "fact_business_plan": {
            "grain": "company × year plan",
            "columns": {
                "plan_key": "BIGSERIAL PK",
                "company_key": "BIGINT FK → dim_company",
                "year_key": "INT FK → dim_date",
                "target_revenue": "NUMERIC",
                "target_profit": "NUMERIC",
                "capex_plan": "NUMERIC",
                "source_json": "JSONB"
            }
        },

        "fact_financial_metrics": {
            "grain": "company × period",
            "columns": {
                "metric_key": "BIGSERIAL PK",
                "company_key": "BIGINT FK → dim_company",
                "period_date_key": "INT FK → dim_date",
                "pe": "NUMERIC",
                "roe": "NUMERIC",
                "roa": "NUMERIC",
                "debt_equity": "NUMERIC",
                "market_cap": "NUMERIC",
                "source_json": "JSONB"
            }
        }
    },

    "staging": {
        "stg_list_stock": ["symbol", "raw_json", "extracted_at"],
        "stg_industry_info": ["industry_metric", "raw_json", "extracted_at"],
        "stg_stock_info": ["symbol", "raw_json", "extracted_at"],
        "stg_financial_info": ["symbol", "raw_json", "extracted_at"],
        "stg_trading_data": ["symbol", "raw_json", "extracted_at"],
        "stg_income_statement": ["symbol", "report_type", "raw_json", "extracted_at"],
        "stg_balance_sheet": ["symbol", "report_type", "raw_json"],
        "stg_match_details": ["symbol", "raw_json"],
        "stg_business_plan": ["symbol", "raw_json"],
        "stg_financial_summary": ["symbol", "raw_json"]
    },

    "metadata": {
        "meta_etl_run": ["run_id", "job_name", "start_time", "end_time", "status"],
        "meta_etl_error": ["err_id", "run_id", "error_message", "raw_json", "err_time"],
        "meta_surrogate_map": ["natural_key", "surrogate_key", "type", "valid_from", "valid_to"],
        "meta_data_quality": ["table_name", "check_name", "status", "checked_at", "failed_rows"]
    }
}

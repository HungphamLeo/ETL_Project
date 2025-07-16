from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, round as spark_round
from pyspark.sql.types import *
from src.logger import FastLogger
from config.load_config import load_config
from dags.extract import wbapi_extract
from models import EconomyTransformDataFrameOutput


class TransformEconomy:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_economy_dataframe(self, spark_session: SparkSession, df: DataFrame) -> DataFrame:
        try:
            rules = EconomyTransformDataFrameOutput.ECONOMY_DF_RULES

            # Step 1: Drop columns
            drop_cols = rules.get("drop_columns", [])
            df = df.drop(*drop_cols) if drop_cols else df

            # Step 2: Apply column-specific rules
            column_rules = rules.get("columns", {})
            for col_name, rule in column_rules.items():
                if col_name in df.columns:
                    expr = col(col_name)

                    # Replace null values
                    if "replace_null_with" in rule:
                        expr = when(expr.isNull(), lit(rule["replace_null_with"])).otherwise(expr)

                    # Round values
                    if "round" in rule:
                        expr = spark_round(expr, rule["round"])

                    # Cast type
                    if "cast" in rule:
                        expr = expr.cast(rule["cast"])

                    # Rename column (optional)
                    new_col_name = rule.get("rename", col_name)
                    df = df.withColumn(new_col_name, expr)

                    # Drop old column if renamed
                    if new_col_name != col_name:
                        df = df.drop(col_name)

            return df

        except Exception as e:
            self.logger.error(f"Error transforming economy dataframe: {e}")
            return None

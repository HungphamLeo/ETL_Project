from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, round as spark_round
from pyspark.sql.types import *
from src.logger import FastLogger
from internal.config.load_config import load_config
from internal.dags.extract import wbapi_extract
from internal.models import EconomyTransformDataFrameOutput
import pandas as pd


class TransformEconomy:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_economy_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Chuyển đổi DataFrame kinh tế:
        - Nếu dữ liệu nhỏ → xử lý bằng Pandas
        - Nếu dữ liệu lớn → convert sang PySpark để xử lý hiệu quả hơn
        """
        from pyspark.sql.functions import col, when, round as spark_round, lit
        from pyspark.sql.types import DoubleType

        try:
            rules = EconomyTransformDataFrameOutput.ECONOMY_DF_RULES
            drop_cols = rules.get("drop_columns", [])

            if len(df) < rules.get("switch_pyspark", 100_000):
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df

                for col_name, rule in rules.items():
                    if col_name == "drop_columns":
                        continue
                    if col_name in df.columns:
                        if "replace_null_with" in rule:
                            df[col_name] = df[col_name].fillna(rule["replace_null_with"])
                        if "round" in rule:
                            df[col_name] = df[col_name].round(rule["round"])
                return df

            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)

                for col_name, rule in rules.items():
                    if col_name == "drop_columns":
                        continue
                    if col_name in spark_df.columns:
                        expr = col(col_name)

                        if "replace_null_with" in rule:
                            expr = when(expr.isNull(), lit(rule["replace_null_with"])).otherwise(expr)
                        if "round" in rule:
                            expr = spark_round(expr, rule["round"])

                        spark_df = spark_df.withColumn(col_name, expr)

                return spark_df.toPandas()

        except Exception as e:
            self.logger.error(f"Error transforming economy dataframe: {e}")
            return None
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round as spark_round
from src.logger import FastLogger
from internal.config.load_config import load_config
from internal.dags.extract import wbapi_extract
from internal.models.transform_models import (
    EconomyTransform, SeriesTransform, TopicTransform, TimeTransform,
    SourceTransform, RegionTransform, IncomeTransform, LendingTransform
)
import pandas as pd

class TransformEconomy:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_economy_dataframe(self, spark, economy_data_df):
        """
        Transform Economy DataFrame using rules.
        """
        try:
            rules = EconomyTransform.ECONOMY_DF_RULES.get("transform_economy_dataframe", {})
            switch = EconomyTransform.ECONOMY_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(economy_data_df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(economy_data_df) < switch:
                # Xử lý các rule đặc biệt cho Longitude, Latitude nếu có
                if "Longitude" in rules and "Longitude" in economy_data_df.columns:
                    economy_data_df["Longitude"] = economy_data_df["Longitude"].round(rules["Longitude"].get("round", 3)).fillna(rules["Longitude"].get("replace_null_with", ""))
                if "Latitude" in rules and "Latitude" in economy_data_df.columns:
                    economy_data_df["Latitude"] = economy_data_df["Latitude"].round(rules["Latitude"].get("round", 3)).fillna(rules["Latitude"].get("replace_null_with", ""))
                economy_data_df = economy_data_df.drop(columns=[c for c in drop_cols if c in economy_data_df.columns]) if drop_cols else economy_data_df
                return economy_data_df
            else:
                spark_df = spark.createDataFrame(economy_data_df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                # Xử lý Longitude, Latitude nếu cần
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming economy dataframe: {e}")
            return None

    def transform_economy_metadata(self, spark, economy_metadata_df):
        """
        Transform Economy Metadata DataFrame using rules.
        """
        try:
            rules = EconomyTransform.ECONOMY_DF_RULES.get("transform_economy_metadata", {})
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(economy_metadata_df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            economy_metadata_df = economy_metadata_df.drop(columns=[c for c in drop_cols if c in economy_metadata_df.columns]) if drop_cols else economy_metadata_df
            return economy_metadata_df
        except Exception as e:
            self.logger.error(f"Error transforming economy metadata: {e}")
            return None

class TransformSeries:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_series_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Series DataFrame using rules.
        """
        try:
            rules = SeriesTransform.SERIES_DF_RULES.get("transform_series_dataframe", {})
            switch = SeriesTransform.SERIES_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming series dataframe: {e}")
            return None

    def transform_series_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Series Metadata DataFrame using rules.
        """
        try:
            rules = SeriesTransform.SERIES_DF_RULES.get("transform_series_metadata", {})
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
            # Có thể bổ sung rule khác nếu cần
            return df
        except Exception as e:
            self.logger.error(f"Error transforming series metadata: {e}")
            return None
    

class TransformTopic:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_topic_info_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Topic Info DataFrame using rules.
        """
        try:
            rules = TopicTransform.TOPIC_DF_RULES.get("transform_topic_info_dataframe", {})
            switch = TopicTransform.TOPIC_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming topic info dataframe: {e}")
            return None

    def transform_topic_series_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Topic Series DataFrame using rules.
        """
        try:
            rules = TopicTransform.TOPIC_DF_RULES.get("transform_topic_series_dataframe", {})
            switch = TopicTransform.TOPIC_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming topic series dataframe: {e}")
            return None

    def transform_topic_metadata_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Topic Metadata DataFrame using rules.
        """
        try:
            rules = TopicTransform.TOPIC_DF_RULES.get("transform_topic_metadata_dataframe", {})
            switch = TopicTransform.TOPIC_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming topic metadata dataframe: {e}")
            return None

class TransformTime:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_time_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Time DataFrame using rules.
        """
        try:
            rules = TimeTransform.TIME_DF_RULES.get("transform_time_dataframe", {})
            switch = TimeTransform.TIME_DF_RULES.get("switch_pyspark", 100000)
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            drop_cols = rules.get("drop_columns", [])
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming time dataframe: {e}")
            return None

class TransformSource:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_source_info_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Source Info DataFrame using rules.
        """
        try:
            rules = SourceTransform.SOURCE_DF_RULES.get("transform_source_info_dataframe", {})
            switch = SourceTransform.SOURCE_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming source info dataframe: {e}")
            return None

    def transform_source_series_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Source Series DataFrame using rules.
        """
        try:
            rules = SourceTransform.SOURCE_DF_RULES.get("transform_source_series_dataframe", {})
            switch = SourceTransform.SOURCE_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming source series dataframe: {e}")
            return None

class TransformRegion:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_region_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Region DataFrame using rules.
        """
        try:
            rules = RegionTransform.REGION_DF_RULES.get("transform_region_dataframe", {})
            switch = RegionTransform.REGION_DF_RULES.get("switch_pyspark", 100000)
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            drop_cols = rules.get("drop_columns", [])
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming region dataframe: {e}")
            return None

class TransformIncome:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_income_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Income DataFrame using rules.
        """
        try:
            rules = IncomeTransform.INCOME_DF_RULES.get("transform_income_dataframe", {})
            switch = IncomeTransform.INCOME_DF_RULES.get("switch_pyspark", 100000)
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            drop_cols = rules.get("drop_columns", [])
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming income dataframe: {e}")
            return None

class TransformLending:
    def __init__(self):
        self.logger = FastLogger(load_config()).get_logger()
        self.extract_object = wbapi_extract()

    def transform_lending_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Lending DataFrame using rules.
        """
        try:
            rules = LendingTransform.LENDING_DF_RULES.get("transform_lending_dataframe", {})
            switch = LendingTransform.LENDING_DF_RULES.get("switch_pyspark", 100000)
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            drop_cols = rules.get("drop_columns", [])
            if len(df) < switch:
                df = df.drop(columns=[c for c in drop_cols if c in df.columns]) if drop_cols else df
                return df
            else:
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                spark_df = spark_df.drop(*drop_cols_spark)
                return spark_df.toPandas()
        except Exception as e:
            self.logger.error(f"Error transforming lending dataframe: {e}")
            return None
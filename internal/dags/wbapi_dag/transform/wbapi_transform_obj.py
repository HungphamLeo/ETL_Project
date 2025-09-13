"""
WorldBank Transform Objects - Refactored existing transform classes
Maintains all existing transform logic while adding better error handling and logger
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round as spark_round
from src.logger import FastLogger
from cmd_.load_config import load_config
from internal.dags.wbapi_dag.extract import wbapi_extract
from internal.models.wbgapi_model.transform_models import (
    EconomyTransform, SeriesTransform, TopicTransform, TimeTransform,
    SourceTransform, RegionTransform, IncomeTransform, LendingTransform
)
import pandas as pd




class base_transform_logger_obj:
    def __init__(self, pipeline_logger):
        self.logger = pipeline_logger


class TransformEconomy(base_transform_logger_obj):
    """Economy data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_economy_dataframe(self, spark: SparkSession, economy_data_df):
        """Transform Economy DataFrame using rules - improved version"""
        try:
            rules = EconomyTransform.ECONOMY_DF_RULES.get("transform_economy_dataframe", {})
            switch = EconomyTransform.ECONOMY_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(economy_data_df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
                
            # Check if DataFrame is empty
            if economy_data_df.empty:
                self.logger.warning("Input DataFrame is empty")
                return economy_data_df
            
            if len(economy_data_df) < switch:
                # Pandas processing for small datasets
                if "Longitude" in rules and "Longitude" in economy_data_df.columns:
                    longitude_rules = rules["Longitude"]
                    economy_data_df["Longitude"] = (
                        economy_data_df["Longitude"]
                        .round(longitude_rules.get("round", 3))
                        .fillna(longitude_rules.get("replace_null_with", ""))
                    )
                
                if "Latitude" in rules and "Latitude" in economy_data_df.columns:
                    latitude_rules = rules["Latitude"]
                    economy_data_df["Latitude"] = (
                        economy_data_df["Latitude"]
                        .round(latitude_rules.get("round", 3))
                        .fillna(latitude_rules.get("replace_null_with", ""))
                    )
                
                # Drop columns
                cols_to_drop = [c for c in drop_cols if c in economy_data_df.columns]
                if cols_to_drop:
                    economy_data_df = economy_data_df.drop(columns=cols_to_drop)
                    self.logger.info(f"Dropped columns: {cols_to_drop}")
                
                return economy_data_df
            else:
                # Spark processing for large datasets
                self.logger.info("Using Spark processing for large dataset")
                spark_df = spark.createDataFrame(economy_data_df)
                
                # Apply Longitude/Latitude transformations if needed
                if "Longitude" in rules and "Longitude" in spark_df.columns:
                    longitude_rules = rules["Longitude"]
                    spark_df = spark_df.withColumn(
                        "Longitude", 
                        spark_round(col("Longitude"), longitude_rules.get("round", 3))
                    )
                
                if "Latitude" in rules and "Latitude" in spark_df.columns:
                    latitude_rules = rules["Latitude"] 
                    spark_df = spark_df.withColumn(
                        "Latitude",
                        spark_round(col("Latitude"), latitude_rules.get("round", 3))
                    )
                
                # Drop columns
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                if drop_cols_spark:
                    spark_df = spark_df.drop(*drop_cols_spark)
                    self.logger.info(f"Dropped columns using Spark: {drop_cols_spark}")
                
                return spark_df.toPandas()
                
        except Exception as e:
            self.logger.error(f"Error transforming economy dataframe: {e}")
            return None

    def transform_economy_metadata(self, spark, economy_metadata_df):
        """Transform Economy Metadata DataFrame using rules - improved version"""
        try:
            rules = EconomyTransform.ECONOMY_DF_RULES.get("transform_economy_metadata", {})
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(economy_metadata_df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if economy_metadata_df.empty:
                self.logger.warning("Input metadata DataFrame is empty")
                return economy_metadata_df
            
            # Drop columns
            cols_to_drop = [c for c in drop_cols if c in economy_metadata_df.columns]
            if cols_to_drop:
                economy_metadata_df = economy_metadata_df.drop(columns=cols_to_drop)
                self.logger.info(f"Dropped metadata columns: {cols_to_drop}")
            
            return economy_metadata_df
            
        except Exception as e:
            self.logger.error(f"Error transforming economy metadata: {e}")
            return None


class TransformSeries(base_transform_logger_obj):
    """Series data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_series_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Series DataFrame using rules - improved version"""
        try:
            rules = SeriesTransform.SERIES_DF_RULES.get("transform_series_dataframe", {})
            switch = SeriesTransform.SERIES_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if df.empty:
                self.logger.warning("Input DataFrame is empty")
                return df
            
            if len(df) < switch:
                # Pandas processing
                cols_to_drop = [c for c in drop_cols if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    self.logger.info(f"Dropped series columns: {cols_to_drop}")
                return df
            else:
                # Spark processing
                self.logger.info("Using Spark processing for large series dataset")
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                if drop_cols_spark:
                    spark_df = spark_df.drop(*drop_cols_spark)
                    self.logger.info(f"Dropped series columns using Spark: {drop_cols_spark}")
                return spark_df.toPandas()
                
        except Exception as e:
            self.logger.error(f"Error transforming series dataframe: {e}")
            return None

    def transform_series_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Series Metadata DataFrame using rules - improved version"""
        try:
            rules = SeriesTransform.SERIES_DF_RULES.get("transform_series_metadata", {})
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if df.empty:
                self.logger.warning("Input metadata DataFrame is empty")
                return df
            
            cols_to_drop = [c for c in drop_cols if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                self.logger.info(f"Dropped series metadata columns: {cols_to_drop}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error transforming series metadata: {e}")
            return None


class TransformTopic(base_transform_logger_obj):
    """Topic data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_topic_info_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Topic Info DataFrame using rules - improved version"""
        try:
            rules = TopicTransform.TOPIC_DF_RULES.get("transform_topic_info_dataframe", {})
            switch = TopicTransform.TOPIC_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            return self._apply_transform_logic(spark, df, drop_cols, switch, "topic info")
            
        except Exception as e:
            self.logger.error(f"Error transforming topic info dataframe: {e}")
            return None

    def transform_topic_series_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Topic Series DataFrame using rules - improved version"""
        try:
            rules = TopicTransform.TOPIC_DF_RULES.get("transform_topic_series_dataframe", {})
            switch = TopicTransform.TOPIC_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            return self._apply_transform_logic(spark, df, drop_cols, switch, "topic series")
            
        except Exception as e:
            self.logger.error(f"Error transforming topic series dataframe: {e}")
            return None

    def transform_topic_metadata_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Topic Metadata DataFrame using rules - improved version"""
        try:
            rules = TopicTransform.TOPIC_DF_RULES.get("transform_topic_metadata_dataframe", {})
            switch = TopicTransform.TOPIC_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            return self._apply_transform_logic(spark, df, drop_cols, switch, "topic metadata")
            
        except Exception as e:
            self.logger.error(f"Error transforming topic metadata dataframe: {e}")
            return None

    def _apply_transform_logic(self, spark: SparkSession, df: pd.DataFrame, drop_cols: list, switch: int, data_type: str) -> pd.DataFrame:
        """Common transform logic for topic data"""
        if not isinstance(df, pd.DataFrame):
            self.logger.error("Input df is not a pandas DataFrame")
            return None
        
        if df.empty:
            self.logger.warning(f"Input {data_type} DataFrame is empty")
            return df
        
        if len(df) < switch:
            # Pandas processing
            cols_to_drop = [c for c in drop_cols if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                self.logger.info(f"Dropped {data_type} columns: {cols_to_drop}")
            return df
        else:
            # Spark processing
            self.logger.info(f"Using Spark processing for large {data_type} dataset")
            spark_df = spark.createDataFrame(df)
            drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
            if drop_cols_spark:
                spark_df = spark_df.drop(*drop_cols_spark)
                self.logger.info(f"Dropped {data_type} columns using Spark: {drop_cols_spark}")
            return spark_df.toPandas()


class TransformTime(base_transform_logger_obj):
    """Time data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_time_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Time DataFrame using rules - improved version"""
        try:
            rules = TimeTransform.TIME_DF_RULES.get("transform_time_dataframe", {})
            switch = TimeTransform.TIME_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if df.empty:
                self.logger.warning("Input time DataFrame is empty")
                return df
            
            if len(df) < switch:
                cols_to_drop = [c for c in drop_cols if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    self.logger.info(f"Dropped time columns: {cols_to_drop}")
                return df
            else:
                self.logger.info("Using Spark processing for large time dataset")
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                if drop_cols_spark:
                    spark_df = spark_df.drop(*drop_cols_spark)
                    self.logger.info(f"Dropped time columns using Spark: {drop_cols_spark}")
                return spark_df.toPandas()
                
        except Exception as e:
            self.logger.error(f"Error transforming time dataframe: {e}")
            return None


class TransformSource(base_transform_logger_obj):
    """Source data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_source_info_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Source Info DataFrame using rules - improved version"""
        try:
            rules = SourceTransform.SOURCE_DF_RULES.get("transform_source_info_dataframe", {})
            switch = SourceTransform.SOURCE_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            return self._apply_transform_logic(spark, df, drop_cols, switch, "source info")
            
        except Exception as e:
            self.logger.error(f"Error transforming source info dataframe: {e}")
            return None

    def transform_source_series_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Source Series DataFrame using rules - improved version"""
        try:
            rules = SourceTransform.SOURCE_DF_RULES.get("transform_source_series_dataframe", {})
            switch = SourceTransform.SOURCE_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            return self._apply_transform_logic(spark, df, drop_cols, switch, "source series")
            
        except Exception as e:
            self.logger.error(f"Error transforming source series dataframe: {e}")
            return None

    def _apply_transform_logic(self, spark: SparkSession, df: pd.DataFrame, drop_cols: list, switch: int, data_type: str) -> pd.DataFrame:
        """Common transform logic for source data"""
        if not isinstance(df, pd.DataFrame):
            self.logger.error("Input df is not a pandas DataFrame")
            return None
        
        if df.empty:
            self.logger.warning(f"Input {data_type} DataFrame is empty")
            return df
        
        if len(df) < switch:
            cols_to_drop = [c for c in drop_cols if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                self.logger.info(f"Dropped {data_type} columns: {cols_to_drop}")
            return df
        else:
            self.logger.info(f"Using Spark processing for large {data_type} dataset")
            spark_df = spark.createDataFrame(df)
            drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
            if drop_cols_spark:
                spark_df = spark_df.drop(*drop_cols_spark)
                self.logger.info(f"Dropped {data_type} columns using Spark: {drop_cols_spark}")
            return spark_df.toPandas()


class TransformRegion(base_transform_logger_obj):
    """Region data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_region_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Region DataFrame using rules - improved version"""
        try:
            rules = RegionTransform.REGION_DF_RULES.get("transform_region_dataframe", {})
            switch = RegionTransform.REGION_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if df.empty:
                self.logger.warning("Input region DataFrame is empty")
                return df
            
            if len(df) < switch:
                cols_to_drop = [c for c in drop_cols if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    self.logger.info(f"Dropped region columns: {cols_to_drop}")
                return df
            else:
                self.logger.info("Using Spark processing for large region dataset")
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                if drop_cols_spark:
                    spark_df = spark_df.drop(*drop_cols_spark)
                    self.logger.info(f"Dropped region columns using Spark: {drop_cols_spark}")
                return spark_df.toPandas()
                
        except Exception as e:
            self.logger.error(f"Error transforming region dataframe: {e}")
            return None


class TransformIncome(base_transform_logger_obj):
    """Income data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_income_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Income DataFrame using rules - improved version"""
        try:
            rules = IncomeTransform.INCOME_DF_RULES.get("transform_income_dataframe", {})
            switch = IncomeTransform.INCOME_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if df.empty:
                self.logger.warning("Input income DataFrame is empty")
                return df
            
            if len(df) < switch:
                cols_to_drop = [c for c in drop_cols if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    self.logger.info(f"Dropped income columns: {cols_to_drop}")
                return df
            else:
                self.logger.info("Using Spark processing for large income dataset")
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                if drop_cols_spark:
                    spark_df = spark_df.drop(*drop_cols_spark)
                    self.logger.info(f"Dropped income columns using Spark: {drop_cols_spark}")
                return spark_df.toPandas()
                
        except Exception as e:
            self.logger.error(f"Error transforming income dataframe: {e}")
            return None


class TransformLending(base_transform_logger_obj):
    """Lending data transformer - maintains existing logic with improvements"""
    
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.extract_object = wbapi_extract()

    def transform_lending_dataframe(self, spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Lending DataFrame using rules - improved version"""
        try:
            rules = LendingTransform.LENDING_DF_RULES.get("transform_lending_dataframe", {})
            switch = LendingTransform.LENDING_DF_RULES.get("switch_pyspark", 100000)
            drop_cols = rules.get("drop_columns", [])
            
            if not isinstance(df, pd.DataFrame):
                self.logger.error("Input df is not a pandas DataFrame")
                return None
            
            if df.empty:
                self.logger.warning("Input lending DataFrame is empty")
                return df
            
            if len(df) < switch:
                cols_to_drop = [c for c in drop_cols if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    self.logger.info(f"Dropped lending columns: {cols_to_drop}")
                return df
            else:
                self.logger.info("Using Spark processing for large lending dataset")
                spark_df = spark.createDataFrame(df)
                drop_cols_spark = [c for c in drop_cols if c in spark_df.columns]
                if drop_cols_spark:
                    spark_df = spark_df.drop(*drop_cols_spark)
                    self.logger.info(f"Dropped lending columns using Spark: {drop_cols_spark}")
                return spark_df.toPandas()
                
        except Exception as e:
            self.logger.error(f"Error transforming lending dataframe: {e}")
            return None
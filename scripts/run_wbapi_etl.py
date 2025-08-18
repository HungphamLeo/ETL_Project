import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
import json
import yaml
from pathlib import Path




import wbgapi as wb   # I use wb as a namespace in all my work
import os
# print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
from internal.models import *
from internal.dags.wbai_dag.extract import wbapi_extract
from internal.dags.wbai_dag.transform import wbapi_transform
from internal.dags.wbai_dag.load import BaseDBLoader
from internal.models.transform_models import (
    EconomyTransform, SeriesTransform, TopicTransform, TimeTransform,
    SourceTransform, RegionTransform, IncomeTransform, LendingTransform
)
from cmd_.load_config import load_config
from src.utils import TableCreator
from src.logger import FastLogger


def run_pipeline():
    """
    Chạy toàn bộ pipeline: Extract -> Transform -> Load
    """
    config = load_config()
    logger = FastLogger(load_config()).get_logger()
    if not config:
        logger.error("Configuration loading failed.")
        return
    
    # Initialize Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("World Bank API Data Pipeline").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()
    extract_obj = wbapi_extract()
    try:
        extract_obj = wbapi_extract()
        transform_obj = wbapi_transform()
        loader_obj = BaseDBLoader()
    except Exception as e:
        logger.error(f"Error initializing objects: {e}")
        return
    

    try:
        economy_data_df = extract_obj.economy.dataframe_display(EconomyDataFrameInput(id='all'))
        economy_metadata_df = extract_obj.economy.get_metadata(EconomyMetadataInput(id='all'))
        series_data_df = extract_obj.series.get_series(SeriesGetInput(id='all'))
        series_metadata_df = extract_obj.series.get_series_metadata(SeriesMetadataInput(id='all'))
        topic_info_df = extract_obj.topic.get_info(TopicInfoInput(id='all'))
        topic_series_df = extract_obj.topic.get_series(TopicSeriesInput(id='all'))
        topic_metadata_df = extract_obj.topic.get_members(TopicMembersInput(id='all'))
        time_series_df = extract_obj.time.get_time_periods_series(TimeSeriesInput(id='all'))
        source_info_df = extract_obj.source.get_info(SourceInfoInput(id='all'))
        source_series_df = extract_obj.source.get_series(SourceSeriesInput(id='all'))
        region_info_df = extract_obj.region.get_series(RegionSeriesInput(id='all'))
        income_series_df = extract_obj.income.get_series(IncomeSeriesInput(id='all'))
        lending_series_df = extract_obj.lending.get_series(LendingSeriesInput(id='all'))
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        return
    

    # Transform
    transform_obj = wbapi_transform()
    try:
        transformed_economy_df = transform_obj.economy.transform_economy_dataframe(spark = spark, economy_data_df=economy_data_df)
        transformed_economy_metadata_df = transform_obj.economy.transform_economy_metadata(spark = spark, economy_metadata_df=economy_metadata_df)
        transformed_series_data = transform_obj.series.transform_series_dataframe(spark = spark, series_data_df=series_data_df)
        transformed_series_metadata = transform_obj.series.transform_series_metadata(spark = spark, series_metadata_df=series_metadata_df)
        transformed_topic_info_df = transform_obj.topic.transform_topic_info_dataframe(spark = spark, topic_info_df=topic_info_df)
        transformed_topic_series_df = transform_obj.topic.transform_topic_series_dataframe(spark = spark, topic_series_df=topic_series_df)
        transformed_topic_metadata_df = transform_obj.topic.transform_topic_metadata_dataframe(spark = spark, topic_metadata_df=topic_metadata_df)
        transformed_time_series_df = transform_obj.time.transform_time_dataframe(spark = spark, time_series_df=time_series_df)
        transformed_source_info_df = transform_obj.source.transform_source_info_dataframe(spark = spark, source_info_df=source_info_df)
        transformed_source_series_df = transform_obj.source.transform_source_series_dataframe(spark = spark, source_series_df=source_series_df)
        transformed_region_info_df = transform_obj.region.transform_region_dataframe(spark = spark, region_info_df=region_info_df)
        transformed_income_series_df = transform_obj.income.transform_income_dataframe(spark = spark, income_series_df=income_series_df)
        transformed_lending_series_df = transform_obj.lending.transform_lending_dataframe(spark = spark, lending_series_df=lending_series_df)
                                                                                    
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        return
    

    # Load
    import pymysql
    
    
    try:
        conn = pymysql.connect(
        host=config['database']['primary']['host'],
        user=config['database']['primary']['username'],
        password=config['database']['primary']['password'],
        database=config['database']['primary']['database'],
        charset=config['database']['primary']['charset'],
        cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
    
    except Exception as e:
        logger.error(f"Error during loading: {e}")
        return

    loader = BaseDBLoader(cursor=cursor, connection=conn, logger=logger, config = "./internal/config/data_craw_web_config/data_craw_web_config.yaml")
    try:
        loader.create_or_replace_database(config['database']['primary']['database'])
        # Danh sách các class transform và rule mapping
        transform_classes = [
            (EconomyTransform, "ECONOMY", EconomyTransform.ECONOMY_DF_RULES),
            (SeriesTransform, "SERIES", SeriesTransform.SERIES_DF_RULES),
            (TopicTransform, "TOPIC", TopicTransform.TOPIC_DF_RULES),
            (TimeTransform, "TIME", TimeTransform.TIME_DF_RULES),
            (SourceTransform, "SOURCE", SourceTransform.SOURCE_DF_RULES),
            (RegionTransform, "REGION", RegionTransform.REGION_DF_RULES),
            (IncomeTransform, "INCOME", IncomeTransform.INCOME_DF_RULES),
            (LendingTransform, "LENDING", LendingTransform.LENDING_DF_RULES),
        ]

        # Mapping tên bảng với dataframe đã transform
        table_df_map = {
            "economy_data": transformed_economy_df,
            "economy_metadata": transformed_economy_metadata_df,
            "series_data": transformed_series_data,
            "series_metadata": transformed_series_metadata,
            "topic_info": transformed_topic_info_df,
            "topic_series": transformed_topic_series_df,
            "topic_metadata": transformed_topic_metadata_df,
            "time_series": transformed_time_series_df,
            "source_info": transformed_source_info_df,
            "source_series": transformed_source_series_df,
            "region_info": transformed_region_info_df,
            "income_series": transformed_income_series_df,
            "lending_series": transformed_lending_series_df,
        }

        for transform_cls, prefix, rules in transform_classes:
            for rule_name, rule_dict in rules.items():
                if not rule_name.startswith("transform_"):
                    continue
                # Tên bảng: prefix + phần sau của rule_name
                table_name = f"{prefix.lower()}_{rule_name.replace('transform_', '').replace('_dataframe','').replace('_metadata','')}"
                # character_specific là tên bảng camelCase
                character_specific = ''.join([w.capitalize() for w in table_name.split('_')])
                creator = TableCreator(machine_id=1, character_specific=character_specific)

                try:
                    sql = creator.generate_create_table_sql(table_name, rule_dict)
                except Exception as e:
                    logger.error(f"Error generating SQL for {table_name}: {e}")
                    return
                
                # Tạo hoặc thay thế bảng
                logger.info(f"Creating or replacing table: {table_name}")
                try:
                    loader.create_or_replace_table(sql)
                except Exception as e:
                    logger.error(f"Error creating or replacing table {table_name}: {e}")
                    return

                logger.info(f"Table {table_name} created or replaced successfully.")


                # Insert dữ liệu nếu có
                try:
                    df = table_df_map.get(table_name)
                    if df is None:
                        logger.warning(f"No data found for table {table_name}. Skipping insert.")
                        continue
                    # Nếu df là None, bỏ qua insert
                    if df is not None:
                        df_with_id = creator.add_id_column(df)
                        # Insert từng dòng
                        cols = ','.join(df_with_id.columns)
                        placeholders = ','.join(['%s'] * len(df_with_id.columns))
                        insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                        for row in df_with_id.itertuples(index=False, name=None):
                            loader.insert_table(insert_sql, row)
                except Exception as e:
                    logger.error(f"Error inserting data into table {table_name}: {e}")
                    return
                
                
    except Exception as e:
        logger.error(f"Error creating or replacing database: {e}")
        return
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


    

    



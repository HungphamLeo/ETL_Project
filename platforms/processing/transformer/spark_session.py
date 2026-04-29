# spark_session.py
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
import os

def get_lakehouse_spark_session():
    """Production Spark session với Delta Lake"""
    
    spark = SparkSession.builder \
        .appName("StockFinancialLakehouse") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 20)
    
    return spark

# Global session
spark = get_lakehouse_spark_session()
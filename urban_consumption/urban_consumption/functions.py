"""This file contains query functions to answer the questions"""
import logging
from pyspark.sql import SparkSession, DataFrame

# import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)


def init_spark():
    """Initiallize a spark session"""
    spark = SparkSession.builder.appName("Urban_consumption").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.info("Opened a spark session ")
    return spark


def read_csv(spark_session: SparkSession, path: str, sep: str = ",") -> DataFrame:
    """Read csv file"""
    logging.info("Reading file %s ...", path)
    data = (
        spark_session.read.option("header", True)
        .option("delimiter", sep)
        .csv(path, inferSchema=True)
    )
    data.show(5)
    logging.info(
        "Data file %s has %s columns and %s rows", path, len(data.columns), data.count()
    )
    data.printSchema()
    return data

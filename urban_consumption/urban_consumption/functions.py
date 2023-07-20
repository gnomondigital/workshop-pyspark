"""This file contains query functions to answer the questions"""
import logging
from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as F

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


def filter_dates(data_df: DataFrame) -> DataFrame:
    """Keep oonly dates between 2008 and 2012"""
    logging.info("Data has %s rows before filtering", data_df.count())
    filtered_df = data_df.filter((F.col("Year") >= 2008) & (F.col("Year") <= 2012))
    filtered_df.show()
    logging.info("Filtered data has %s rows", filtered_df.count())
    return filtered_df

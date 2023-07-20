"""This file contains query functions to answer the questions"""
import logging
from typing import List
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


def merge_dataframes(df_1: DataFrame, df_2: DataFrame, df_3: DataFrame) -> DataFrame:
    """Merge all dataframes into one"""
    return df_1.join(df_2, on=["Entity", "Code", "year"]).join(
        df_3, on=["Entity", "Code", "year"]
    )


def convert_to_per_capita(data_df: DataFrame, columns: List) -> DataFrame:
    """Convert columns from total to per population"""
    for col in columns:
        logging.info("Converting %s", col)
        data_df = data_df.withColumn(
            f"{col}_per_capita", F.col(col) / F.col("Population")
        )
        data_df.select("Population", col, f"{col}_per_capita").show()
    return data_df


def remove_world_entity(data_df: DataFrame) -> DataFrame:
    """Remove rows with entity having the value World"""
    logging.info("Data has %s rows before filtering", data_df.count())
    filtered_data = data_df.filter(F.col("Entity") != "World")
    logging.info("Filtered data has %s rows", filtered_data.count())
    return filtered_data


def cleanup_data(data_df: DataFrame) -> DataFrame:
    """Cleanup data"""
    logging.info("Dropping no longer needed columns")
    data_df = data_df.drop(
        *[
            "meat_prod_tonnes",
            "Food Balance Sheets: Eggs - Production (FAO (2017)) (tonnes)",
            "Oil production (Etemad & Luciana) (terawatt-hours)",
            "Population",
        ]
    )
    logging.info("Replacing null values with zero")
    data_df = data_df.na.fill(0)
    logging.info("Renaming columns with spaces")
    for col in data_df.columns:
        data_df = data_df.withColumnRenamed(col, col.replace(" ", "_"))
    data_df = data_df.withColumnRenamed("GDP_per_capita_(int.-$)_($)", "GDP_per_capita")
    data_df.show()
    return data_df


def average_per_year(data_df: DataFrame) -> DataFrame:
    """Calculate the average of all columns per year"""
    avg_cols = [c for c in data_df.columns if c not in ["Entity", "Code", "Year"]]
    avg_year = data_df.groupBy(F.col("Entity")).agg(
        *[F.mean(c).alias(c) for c in avg_cols]
    )
    avg_year.show()
    return avg_year


def country_with_highest_avg_meat(data_df: DataFrame) -> str:
    """Get the country that has the highest average meat production
    per capita for the years 2008-2012"""
    highest_meat = data_df.agg(
        F.max(F.col("meat_prod_tonnes_per_capita")).alias("max_meat_prod"),
    ).first()["max_meat_prod"]
    highest_country = data_df.filter(F.col("meat_prod_tonnes_per_capita") == highest_meat).first()["Entity"]
    logging.info("The country with highest average meat production is %s.",highest_country)

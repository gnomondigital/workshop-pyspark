import pytest
from pyspark.sql import SparkSession


@pytest.fixture(name="init_spark", scope="session")
def init_spark_session():
    "Initialize a spark session"
    spark = SparkSession.builder.master("local[1]").appName("local-tests").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(name="test_data", scope="session")
def generate_data(init_spark):
    """Generate data for tests"""
    data = [
        (1, 2007, 2, 10, 100),
        (1, 2008, 4, 20, 100),
        (1, 2012, 6, 30, 300),
        (2, 2007, 1, 10, 100),
        (2, 2008, 2, 20, 100),
        (2, 2012, 3, 30, 300),
    ]
    data_df = init_spark.createDataFrame(
        data,
        ["Entity", "Year", "meat_prod_tonnes_per_capita", "col_1", "Population"],
    )
    data_df.show()
    yield data_df

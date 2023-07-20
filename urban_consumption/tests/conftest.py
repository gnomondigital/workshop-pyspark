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
        (1, 2007, 2),
        (1, 2008, 4),
        (1, 2012, 6),
        (2, 2007, 1),
        (2, 2008, 2),
        (2, 2012, 3),
    ]
    data_df = init_spark.createDataFrame(
        data,
        [
            "Entity",
            "Year",
            "Oil_production_(Etemad_&_Luciana)_(terawatt-hours)_per_capita",
        ],
    )
    data_df.show()
    yield data_df
